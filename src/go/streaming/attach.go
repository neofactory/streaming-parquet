package streaming

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/neofactory/streaming-parquet/src/go/ring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Attached is the read face of a remote stream: a local ring fed by
// Flight, discovered by polling the root's ticket (or dialed directly
// with a Ticket). The same ring calls work as on a created stream — and so
// does archival: an attached process may StartParquet, because the
// parquet path consumes a ring and any ring can archive.
type Attached struct {
	Name string

	// last ticket seen (host/port/archive); nil before discovery
	lastTicket atomic.Pointer[Ticket]

	mu      sync.Mutex
	ring    *ring.Ring
	ownRing bool // this reader made the ring (it is not a local stream's)
	arch    *archiver
	archOpt *ParquetOptions // nil: this reader writes no files
	armed   atomic.Bool     // StartParquet requested (possibly pre-schema)
	label   string

	capacity int64
	columns  []string
	state    atomic.Value // string
	// the rules: where the writer is, when to wait, when to start again
	// (core.go: readerRules). The feeder goroutine makes its calls;
	// reason is what it said last of why the reader is not streaming.
	rules  *readerRules
	reason atomic.Value // string
	direct *Ticket      // the caller's ticket, or nil
	local  *Stream      // the stream of this process read in place
	first  readerStep   // the step of the poll Attach made
	closed chan struct{}
	done   sync.WaitGroup
}

type AttachOptions struct {
	RingRows int64           // local ring rows; see ringSizes for the default
	Columns  []string        // subset subscription; nil = all
	Ticket   *Ticket         // dial directly, skipping root discovery
	Parquet  *ParquetOptions // for a reader that writes the rows it receives to parquet; nil = it writes none
}

// Attach opens the read face. Remote: a feeder goroutine waits for a
// live ticket, connects, and appends every received batch into the
// local ring (reconnecting with backoff). State() reports where it is.
func (t *Telemetry) Attach(name string, o AttachOptions) (*Attached, error) {
	var archOpt *ParquetOptions
	var givenGroupRows int64
	if o.Parquet != nil {
		givenGroupRows = o.Parquet.RowGroupRows
	}
	ringRows, groupRows, err := ringSizes(name, o.RingRows, givenGroupRows)
	if err != nil {
		return nil, err
	}
	if o.Parquet != nil {
		opts := *o.Parquet
		opts.RowGroupRows = groupRows
		archOpt = &opts
	}
	a := &Attached{Name: name, capacity: ringRows, columns: o.Columns,
		archOpt: archOpt, closed: make(chan struct{})}
	rules, err := newReaderRules(fmt.Sprintf("Attach(%q)", name), name, t.root,
		o.Ticket != nil)
	if err != nil {
		return nil, err
	}
	a.rules, a.direct = rules, o.Ticket
	a.state.Store("no-ticket")
	a.reason.Store("")
	// The first poll is made here: a name whose folder does not exist
	// is refused now, not waited for.
	a.first = a.poll(t)
	if a.first.action == doFail {
		return nil, fmt.Errorf("%s", rules.reason())
	}
	a.done.Add(1)
	go a.lifecycle(t)
	return a, nil
}

func (a *Attached) State() string { return a.state.Load().(string) }

// Reason says why the reader is not streaming, in words; "" while it is.
func (a *Attached) Reason() string {
	if a.State() == "streaming" {
		return ""
	}
	return a.reason.Load().(string)
}

// took notes what the core said, and logs its lines.
func (a *Attached) took(st readerStep) readerStep {
	if st.action != doLocal {
		// a local stream's state shows once its ring is in place
		// (run): "streaming" with no ring would be a state to trip on
		a.state.Store(st.state)
	}
	a.reason.Store(a.rules.reason())
	for _, line := range a.rules.messages() {
		fmt.Fprintln(os.Stderr, line)
	}
	return st
}

// lookLocal: the stream of this name in this process (nil for none, and
// for a reader that needs its own ring: one with a column subset or
// one that writes files), and whether this process has a stream in the
// name's folder.
func (a *Attached) lookLocal(t *Telemetry) (*Stream, bool) {
	if a.direct != nil {
		return nil, false
	}
	folder := a.rules.folder() + "/"
	t.mu.Lock()
	defer t.mu.Unlock()
	inFolder := false
	for n := range t.streams {
		if strings.HasPrefix(n, folder) {
			inFolder = true
			break
		}
	}
	s := t.streams[a.Name]
	if s != nil && (a.columns != nil || a.archOpt != nil || s.isClosed()) {
		s = nil
	}
	return s, inFolder
}

// poll looks for the stream, in this process and then in the store,
// and takes the core's step for what was found.
func (a *Attached) poll(t *Telemetry) readerStep {
	local, localFolder := a.lookLocal(t)
	if local != nil {
		a.local = local
		return a.took(a.rules.poll(foundLocal, nil, nil))
	}
	a.local = nil
	var st readerStep
	switch {
	case a.direct != nil:
		a.lastTicket.Store(a.direct)
		st = a.rules.poll(foundTicket, a.direct, nil)
	case t.tickets == nil:
		a.lastTicket.Store(nil)
		st = a.rules.poll(foundNone, nil, nil) // no store: this process alone
	default:
		tk, err := t.tickets.get(a.Name)
		switch {
		case err != nil:
			st = a.rules.poll(foundUnreadable, nil, err)
		case tk == nil:
			a.lastTicket.Store(nil)
			st = a.rules.poll(foundNone, nil, nil)
		default:
			a.lastTicket.Store(tk)
			st = a.rules.poll(foundTicket, tk, nil)
		}
	}
	if st.action == doList {
		exists := localFolder
		var err error
		if !exists && t.tickets != nil {
			exists, err = t.tickets.hasFolder(a.rules.folder())
		}
		if err != nil {
			st = a.rules.poll(foundUnreadable, nil, err)
		} else {
			st = a.rules.folderIs(exists)
		}
	}
	return a.took(st)
}

// Ring returns the local ring once the schema is known (nil before
// the first connection).
func (a *Attached) Ring() *ring.Ring {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.ring
}

// WaitRing blocks until the ring exists (schema learned) or timeout.
func (a *Attached) WaitRing(timeout time.Duration) (*ring.Ring, error) {
	deadline := time.Now().Add(timeout)
	for {
		if r := a.Ring(); r != nil {
			return r, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("attach %s: no connection within %s "+
				"(state %s)", a.Name, timeout, a.State())
		}
		select {
		case <-a.closed:
			return nil, fmt.Errorf("attach %s: closed", a.Name)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// StartParquet begins archiving the attached stream — symmetric with
// the created-stream verb. Safe before the first connection: archival
// arms now and starts when the schema arrives.
func (a *Attached) StartParquet(label string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.archOpt == nil {
		return fmt.Errorf("attach %s: StartParquet needs AttachOptions.Parquet: "+
			"it says where the files go", a.Name)
	}
	a.label = label
	a.armed.Store(true)
	if a.arch != nil {
		return a.arch.start(label)
	}
	return nil // arms; the feeder starts it on connect
}

// StopParquet flushes and finalizes.
func (a *Attached) StopParquet() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.armed.Store(false)
	if a.arch != nil {
		return a.arch.stop()
	}
	return nil
}

// dropRing: the ring in hand goes (the writer started again, or the
// local stream it belonged to closed). A ring this reader made closes,
// so a holder fails loudly and does not read a run that is over; a
// recording of that run is finished first.
func (a *Attached) dropRing(own bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if own {
		if a.arch != nil && a.arch.recording() {
			if err := a.arch.stop(); err != nil {
				fmt.Fprintf(os.Stderr, "[%s] a new run: finishing the "+
					"earlier run's file failed: %v\n", a.Name, err)
			}
		}
		if a.ring != nil {
			a.ring.Close()
		}
	}
	a.ring, a.arch, a.ownRing = nil, nil, false
}

func (a *Attached) lifecycle(t *Telemetry) {
	defer a.done.Done()
	defer a.rules.close()
	wait := func(d time.Duration) bool { // false = closed
		select {
		case <-time.After(d):
			return true
		case <-a.closed:
			a.state.Store("closed")
			return false
		}
	}
	st, have := a.first, true
	for {
		select {
		case <-a.closed:
			a.state.Store("closed")
			return
		default:
		}
		if !have {
			st = a.poll(t)
		}
		have = false
		if st.freshRing && st.action != doLocal {
			a.dropRing(a.ownRing)
		}
		switch st.action {
		case doWait:
			if !wait(st.wait) {
				return
			}
		case doLocal:
			// read the writer's own ring in place, until it closes
			a.dropRing(a.ownRing)
			local := a.local
			a.mu.Lock()
			a.ring = local.ring
			a.mu.Unlock()
			a.state.Store(st.state)
			for !local.isClosed() {
				if !wait(ticketPollInterval) {
					return
				}
			}
			a.dropRing(false) // the ring was the writer's
			a.local = nil
			st, have = a.took(a.rules.lost(nil)), true
		case doConnect:
			// A fresh ring (above) when the writer started again: its
			// row numbers start again at 0, and the ring rightly refuses
			// a row number that goes back.
			err := a.receive(a.lastTicket.Load())
			select {
			case <-a.closed:
				a.state.Store("closed")
				return
			default:
			}
			st, have = a.took(a.rules.lost(err)), true
		}
	}
}

// ArchiveLocation is the publisher-advertised archive directory/URI
// from the last ticket ("" before discovery or when unadvertised).
func (a *Attached) ArchiveLocation() string {
	if t := a.lastTicket.Load(); t != nil {
		return t.Archive
	}
	return ""
}

func (a *Attached) receive(tk *Ticket) error {
	addr := fmt.Sprintf("%s:%d", tk.Host, tk.Port)
	client, err := flight.NewClientWithMiddleware(addr, nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-a.closed:
			cancel()
		case <-ctx.Done():
		}
	}()
	info, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{
		Type: flight.DescriptorPATH, Path: []string{a.Name}})
	if err != nil {
		return err
	}
	schema, err := flight.DeserializeSchema(info.Schema,
		memory.DefaultAllocator)
	if err != nil {
		return err
	}
	ticketStr := a.Name
	if a.columns != nil {
		fields := make([]arrow.Field, len(a.columns))
		for i, c := range a.columns {
			idx := schema.FieldIndices(c)
			if len(idx) == 0 {
				return fmt.Errorf("stream %s: no column %q", a.Name, c)
			}
			fields[i] = schema.Field(idx[0])
		}
		schema = arrow.NewSchema(fields, nil)
		csv := a.columns[0]
		for _, c := range a.columns[1:] {
			csv += "," + c
		}
		ticketStr = a.Name + ":" + csv
	}
	a.mu.Lock()
	if a.ring == nil {
		r, err := ring.NewRing(schema, a.capacity, nil)
		if err != nil {
			a.mu.Unlock()
			return err
		}
		a.ring, a.ownRing = r, true
		if a.archOpt != nil {
			arch, err := newArchiver(r, a.Name, tk.Run, *a.archOpt, nil, nil)
			if err != nil {
				a.mu.Unlock()
				return err
			}
			a.arch = arch
			if a.armed.Load() {
				if err := arch.start(a.label); err != nil {
					a.mu.Unlock()
					return err
				}
			}
		}
	}
	a.mu.Unlock()

	// The pull loop (flightserver.go says why a pull): ask for the rows
	// from `from` on, append the answer, ask again from the seq reached.
	// TotalRecords is the committed row count: start at the newest row.
	name, colsCSV, hasCols := strings.Cut(ticketStr, ":")
	from := info.TotalRecords
	a.rules.connected()
	a.state.Store("streaming")
	for ctx.Err() == nil {
		tk := fmt.Sprintf("%s@%d", name, from)
		if hasCols {
			tk += ":" + colsCSV
		}
		stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: []byte(tk)})
		if err != nil {
			return err
		}
		rdr, err := flight.NewRecordReader(stream)
		if err != nil {
			return err
		}
		for rdr.Next() {
			rec := rdr.RecordBatch()
			if rec.NumRows() == 0 {
				continue // a quiet stream: nothing new yet
			}
			firstSeq := int64(-1)
			if meta := rdr.LatestAppMetadata(); len(meta) == 8 || len(meta) == 16 {
				// publisher's origin seq: keep the local ring on the
				// publisher's seq axis (a jump past `from` is a gap:
				// rows recycled before this reader asked). A 16-byte
				// side data carries the ticket revision after it,
				// which this reader does not use.
				firstSeq = int64(binary.LittleEndian.Uint64(meta[:8]))
			} else if len(meta) > 0 {
				rdr.Release()
				return fmt.Errorf("stream %s: batch app_metadata is "+
					"%d bytes (want 16: the LE origin seq and ticket "+
					"revision; or 8: the seq) — malformed "+
					"publisher", a.Name, len(meta))
			}
			a.mu.Lock()
			err := a.ring.AppendRecordAt(rec, firstSeq)
			var archErr error
			if err == nil && a.arch != nil {
				archErr = a.arch.step()
			}
			a.mu.Unlock()
			if err != nil {
				rdr.Release()
				return err
			}
			if archErr != nil {
				fmt.Fprintf(os.Stderr, "[%s] archiver: %v\n", a.Name, archErr)
			}
			if firstSeq >= 0 {
				from = firstSeq + rec.NumRows()
			} else {
				from += rec.NumRows()
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

// Close stops the feeder, finalizes any recording, frees the ring.
func (a *Attached) Close() error {
	select {
	case <-a.closed:
		return nil
	default:
		close(a.closed)
	}
	a.done.Wait()
	a.mu.Lock()
	defer a.mu.Unlock()
	var err error
	if a.arch != nil && a.arch.recording() {
		err = a.arch.stop()
	} else if a.arch != nil {
		err = a.arch.rotate()
	}
	if a.ring != nil && a.ownRing {
		a.ring.Close()
	}
	a.ring = nil
	return err
}
