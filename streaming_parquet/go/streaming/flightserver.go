package streaming

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The embedded Flight face: readers pull the live rows.
//
// A plain RPC, not a streaming push. Each DoGet asks for the rows from
// one seq on and gets one answer: the committed rows from that seq to
// the newest, as one batch, with the origin seq in the side data. If
// there are no rows yet, the call waits up to waitFor for one to arrive,
// then answers with an empty batch. The reader appends what it got and
// asks again from the seq it reached. The next request is the
// acknowledgment of the last answer.
//
// Why not a stream of pushed batches, as this file once was. A push has
// to choose how much may be in flight to a reader before the publisher
// stops sending. Too little and the pipe idles for a round trip; too
// much and a reader that stopped reading holds rows the publisher must
// keep, and a batch stuck in a blocked send held a ring pin, which on
// 2026-11-07 stopped the writer and ended the publisher. Two writes in
// flight alternate a full write with a one-row write for as long as the
// path is slow. Every choice needs a timeout, a watchdog and a way to
// abort one reader's blocked send. The pull has none of that to choose.
// One answer is in flight per reader, it carries everything since the
// last one, so a reader that keeps up gets a write per commit and a
// reader on a slow path gets a write per round trip, sized by the round
// trip. The publisher keeps no state per reader: no queue, no sender
// goroutine, no pin held past the answer's own write. A reader that
// stops asking leaves nothing behind. Simplicity wins here, and it keeps
// the ring's one rule: the writer never waits for a reader.
//
// Wire-compatible with the Python module (_flight_server.py):
// GetFlightInfo by path descriptor answers the schema and, in
// TotalRecords, the committed row count, where a new reader starts. The
// DoGet ticket is "<name>@<from_seq>" or "<name>@<from_seq>:col1,col2".
// The answer's app_metadata is the 8-byte LE origin seq of the batch's
// first row (-1 for an empty batch); Python readers accept 8 or 16.

// how long a DoGet waits for a row before answering empty
const waitFor = time.Second

// the most rows one answer carries; a reader further behind asks again
const maxAnswerRows = 65536

type flightStream struct {
	name   string
	schema *arrow.Schema
	stream *Stream
	mu     sync.Mutex
	wake   chan struct{} // closed and replaced on every commit: a broadcast
	closed bool
}

func (fs *flightStream) notify() {
	fs.mu.Lock()
	close(fs.wake)
	fs.wake = make(chan struct{})
	fs.mu.Unlock()
}

type flightFace struct {
	flight.BaseFlightServer
	mu      sync.Mutex
	streams map[string]*flightStream
	server  flight.Server
	port    int
}

// newFlightFace listens at addr; "" means any free port on every
// interface, the ticket announces it. A fixed address serves a process
// found by a name (DNS) rather than by a ticket.
func newFlightFace(addr string) (*flightFace, error) {
	if addr == "" {
		addr = "0.0.0.0:0"
	}
	f := &flightFace{streams: map[string]*flightStream{}}
	f.server = flight.NewServerWithMiddleware(nil)
	if err := f.server.Init(addr); err != nil {
		return nil, err
	}
	f.server.RegisterFlightService(f)
	bound := f.server.Addr().String()
	fmt.Sscanf(bound[strings.LastIndex(bound, ":")+1:], "%d", &f.port)
	go f.server.Serve()
	return f, nil
}

func (f *flightFace) register(name string, schema *arrow.Schema, s *Stream) {
	f.mu.Lock()
	f.streams[name] = &flightStream{name: name, schema: schema, stream: s,
		wake: make(chan struct{})}
	f.mu.Unlock()
}

// unregister removes the stream and wakes its waiting readers, who get
// an error and reconnect through the ticket: a closed publisher must
// look closed.
func (f *flightFace) unregister(name string) {
	f.mu.Lock()
	fs := f.streams[name]
	delete(f.streams, name)
	f.mu.Unlock()
	if fs != nil {
		fs.mu.Lock()
		fs.closed = true
		close(fs.wake)
		fs.wake = make(chan struct{})
		fs.mu.Unlock()
	}
}

// notify wakes the readers waiting on a stream: rows committed.
func (f *flightFace) notify(name string) {
	f.mu.Lock()
	fs := f.streams[name]
	f.mu.Unlock()
	if fs != nil {
		fs.notify()
	}
}

func selectColumns(rec arrow.RecordBatch, cols []int) arrow.RecordBatch {
	fields := make([]arrow.Field, len(cols))
	arrs := make([]arrow.Array, len(cols))
	for i, c := range cols {
		fields[i] = rec.Schema().Field(c)
		arrs[i] = rec.Column(c)
	}
	sub := arrow.NewSchema(fields, nil)
	return array.NewRecordBatch(sub, arrs, rec.NumRows())
}

func (f *flightFace) GetFlightInfo(ctx context.Context,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(desc.Path) == 0 {
		return nil, fmt.Errorf("no path in descriptor")
	}
	name := desc.Path[0]
	f.mu.Lock()
	fs := f.streams[name]
	f.mu.Unlock()
	if fs == nil {
		return nil, fmt.Errorf("unknown stream: %s", name)
	}
	committed := fs.stream.ring.NewestSeq() + 1
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(fs.schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: []byte(fmt.Sprintf("%s@%d", name, committed))}}},
		TotalRecords: committed,
		TotalBytes:   -1,
	}, nil
}

// DoGet: one answer, the rows from the ticket's seq to the newest, or
// an empty batch after waitFor.
func (f *flightFace) DoGet(tkt *flight.Ticket,
	srv flight.FlightService_DoGetServer) error {
	raw := string(tkt.Ticket)
	head, colsCSV, hasCols := strings.Cut(raw, ":")
	name, fromStr, hasAt := strings.Cut(head, "@")
	from, err := strconv.ParseInt(fromStr, 10, 64)
	if !hasAt || err != nil || from < 0 {
		return fmt.Errorf("ticket %q: want '<name>@<from_seq>[:col1,col2]'", raw)
	}
	f.mu.Lock()
	fs := f.streams[name]
	f.mu.Unlock()
	if fs == nil {
		return fmt.Errorf("unknown stream: %s", name)
	}
	schema := fs.schema
	var cols []int
	if hasCols {
		for _, c := range strings.Split(colsCSV, ",") {
			idx := schema.FieldIndices(c)
			if len(idx) == 0 {
				return fmt.Errorf("stream %s: no column %q", name, c)
			}
			cols = append(cols, idx[0])
		}
		fields := make([]arrow.Field, len(cols))
		for i, c := range cols {
			fields[i] = schema.Field(c)
		}
		schema = arrow.NewSchema(fields, nil)
	}
	// wait for a row at or past from, the stream's close, the reader's
	// cancel, or the deadline
	deadline := time.NewTimer(waitFor)
	defer deadline.Stop()
	for {
		fs.mu.Lock()
		wake, closed := fs.wake, fs.closed
		fs.mu.Unlock()
		if closed {
			return fmt.Errorf("stream %s closed", name)
		}
		if fs.stream.ring.NewestSeq() >= from {
			break
		}
		select {
		case <-wake:
			continue
		case <-deadline.C:
		case <-srv.Context().Done():
			return nil
		}
		break
	}
	w := flight.NewRecordWriter(srv, ipc.WithSchema(schema))
	defer w.Close()
	committed := fs.stream.ring.NewestSeq() + 1
	if committed <= from {
		empty := array.NewRecordBatch(schema, emptyColumns(schema), 0)
		defer empty.Release()
		var meta [8]byte
		binary.LittleEndian.PutUint64(meta[:], ^uint64(0)) // -1: no rows
		return w.WriteWithAppMetadata(empty, meta[:])
	}
	// Pin the rows this answer carries, and no others; released once
	// written. Rows below from may be recycled: the answer starts at
	// the oldest row left, and the reader sees the gap in the seq.
	hi := committed
	if hi > from+maxAnswerRows {
		hi = from + maxAnswerRows
	}
	rs, err := fs.stream.ring.Pin(from, hi)
	if err != nil {
		return err
	}
	defer rs.Release()
	recs, err := rs.Records()
	if err != nil {
		return err
	}
	defer func() {
		for _, rec := range recs {
			rec.Release()
		}
	}()
	pos := rs.Lo
	for _, rec := range recs {
		out := rec
		if cols != nil {
			out = selectColumns(rec, cols)
		}
		var meta [8]byte
		binary.LittleEndian.PutUint64(meta[:], uint64(pos))
		err = w.WriteWithAppMetadata(out, meta[:])
		if cols != nil {
			out.Release()
		}
		pos += rec.NumRows()
		if err != nil {
			return err // the reader went away mid-answer
		}
	}
	return nil
}

func emptyColumns(schema *arrow.Schema) []arrow.Array {
	cols := make([]arrow.Array, len(schema.Fields()))
	for i, fld := range schema.Fields() {
		b := array.NewBuilder(memory.DefaultAllocator, fld.Type)
		cols[i] = b.NewArray()
		b.Release()
	}
	return cols
}

func (f *flightFace) shutdown() {
	f.mu.Lock()
	names := make([]string, 0, len(f.streams))
	for n := range f.streams {
		names = append(names, n)
	}
	f.mu.Unlock()
	for _, n := range names {
		f.unregister(n)
	}
	f.server.Shutdown()
}
