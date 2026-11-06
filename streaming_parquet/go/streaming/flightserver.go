package streaming

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The embedded Flight face. There is no server "role": every process
// that creates a stream grows this outbound face on an OS-assigned
// port; the ticket announces it. Wire-compatible with the Python
// module: GetFlightInfo by path descriptor, DoGet with ticket bytes
// "<name>" or "<name>:col1,col2".

type qitem struct {
	rec arrow.RecordBatch
	seq int64 // stream seq of the first row; -1 = unknown
}

type subscriber struct {
	ch     chan qitem // drop-oldest on overflow
	cols   []int      // nil = all columns
	closed chan struct{}
}

type flightStream struct {
	name   string
	schema *arrow.Schema
	mu     sync.Mutex
	subs   []*subscriber
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

func (f *flightFace) register(name string, schema *arrow.Schema) {
	f.mu.Lock()
	f.streams[name] = &flightStream{name: name, schema: schema}
	f.mu.Unlock()
}

func (f *flightFace) unregister(name string) {
	f.mu.Lock()
	fs := f.streams[name]
	delete(f.streams, name)
	f.mu.Unlock()
	if fs != nil {
		fs.mu.Lock()
		for _, s := range fs.subs {
			close(s.closed)
		}
		fs.subs = nil
		fs.mu.Unlock()
	}
}

// push fans a record out to every subscriber (retained per subscriber;
// each releases after send). Overflow drops the OLDEST batch, like the
// Python queue.
func (f *flightFace) push(name string, rec arrow.RecordBatch, firstSeq int64) int {
	f.mu.Lock()
	fs := f.streams[name]
	f.mu.Unlock()
	if fs == nil {
		return 0
	}
	fs.mu.Lock()
	subs := append([]*subscriber(nil), fs.subs...)
	fs.mu.Unlock()
	for _, s := range subs {
		out := rec
		if s.cols != nil {
			out = selectColumns(rec, s.cols)
		} else {
			out.Retain()
		}
		item := qitem{rec: out, seq: firstSeq}
		select {
		case s.ch <- item:
		default:
			select {
			case old := <-s.ch:
				old.rec.Release()
			default:
			}
			select {
			case s.ch <- item:
			default:
				out.Release() // best-effort, like Python
			}
		}
	}
	return len(subs)
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
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(fs.schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: []byte(name)}}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (f *flightFace) DoGet(tkt *flight.Ticket,
	fs flight.FlightService_DoGetServer) error {
	raw := string(tkt.Ticket)
	name, colsCSV, hasCols := strings.Cut(raw, ":")
	f.mu.Lock()
	st := f.streams[name]
	f.mu.Unlock()
	if st == nil {
		return fmt.Errorf("unknown stream: %s", name)
	}
	schema := st.schema
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
	sub := &subscriber{ch: make(chan qitem, 100),
		cols: cols, closed: make(chan struct{})}
	st.mu.Lock()
	st.subs = append(st.subs, sub)
	st.mu.Unlock()
	defer func() {
		st.mu.Lock()
		for i, s := range st.subs {
			if s == sub {
				st.subs = append(st.subs[:i], st.subs[i+1:]...)
				break
			}
		}
		st.mu.Unlock()
		for {
			select {
			case item := <-sub.ch:
				item.rec.Release()
			default:
				return
			}
		}
	}()
	w := flight.NewRecordWriter(fs, ipc.WithSchema(schema))
	defer w.Close()
	for {
		select {
		case item := <-sub.ch:
			var err error
			if item.seq >= 0 {
				// 8-byte LE origin seq: the attach-side alignment
				// signal (readers without the feature ignore it)
				var meta [8]byte
				binary.LittleEndian.PutUint64(meta[:], uint64(item.seq))
				err = w.WriteWithAppMetadata(item.rec, meta[:])
			} else {
				err = w.Write(item.rec)
			}
			item.rec.Release()
			if err != nil {
				return err // subscriber went away
			}
		case <-sub.closed:
			return nil
		case <-fs.Context().Done():
			return nil
		}
	}
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
