package streaming

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/neofactory/streaming-parquet/src/go/pqc"
	"github.com/neofactory/streaming-parquet/src/go/ring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Attached is a reader of a stream: the answer of Connect. A stream of
// this process is read in place. A stream of another process is found
// by its ticket in the store (or by a Ticket the caller gives) and its
// rows are pulled over Flight into memory of this reader's own. The
// reading calls are the same as a Stream's. A reader with parquet
// options can also write the rows it receives to files.
type Attached struct {
	Name string
	log  *slog.Logger

	// last ticket seen (host/port/archive); nil before discovery
	lastTicket atomic.Pointer[Ticket]

	mu      sync.Mutex
	ring    *ring.Ring
	ownRing bool // this reader made the ring (it is not a local stream's)
	arch    *archiver
	// This reader's own files (ConnectOptions.ParquetStore): the rows it
	// receives, written as a stream's are. Under mu.
	files        pqc.Sink        // the store they go to; nil: this reader writes none
	parquetStore string          // that store's name, and where its tickets are:
	copyTickets  ticketStore     //   the lock looks for a live writer of the folder
	archOpt      *ParquetOptions // the options; nil: the defaults
	armed        atomic.Bool     // files are on: from the next row received
	label        string
	run          string     // the run of the ring in hand: every file's sp_run
	owner        *ownership // the folder's writer lock, held until Close
	archFailing  bool       // the last write failed, or a start was refused
	archError    string
	refused      bool  // a start was refused: tried again at the next StartParquet
	durable      int64 // the newest row in a file or a checkpoint; -1 for none

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
	first  readerStep   // the step of the poll Connect made
	closed chan struct{}
	done   sync.WaitGroup

	proc    *process
	root    string      // the store this reader looks in, "" for none
	tickets ticketStore // nil with no store
}

type ConnectOptions struct {
	Store   string         // the store the stream is in; "" = $STREAMING_PARQUET_STORE
	Memory  *MemoryOptions // rows this reader keeps in memory; nil = the default
	Columns []string       // subset subscription; nil = all
	Ticket  *Ticket        // dial directly, with no look in the store
	// ParquetStore is a store of this reader's own: the rows it receives
	// are written to <ParquetStore>/streams/<name>/ as a stream's are,
	// with the row numbers and the run the writer gave them. The files
	// are plain parquet files: no ticket and no catalog names them. It
	// is not the store the stream is in: that folder is the writer's.
	ParquetStore string
	// Parquet writes the files from the first row received. Nil: no
	// file until a StartParquet. It needs ParquetStore.
	Parquet *ParquetOptions
}

// Connect opens a reader of a stream. Remote: a feeder goroutine waits for a
// live ticket, connects, and appends every received batch into the
// local ring (reconnecting with backoff). State() reports where it is.
func (t *process) Connect(name string, o ConnectOptions) (*Attached, error) {
	root, tickets, err := t.resolve(o.Store)
	if err != nil {
		return nil, err
	}
	var archOpt *ParquetOptions
	var givenGroupRows int64
	if o.Parquet != nil {
		givenGroupRows = o.Parquet.RowGroupRows
	}
	ringRows, groupRows, err := ringSizes(name, o.Memory.rows(), givenGroupRows)
	if err != nil {
		return nil, err
	}
	if o.Parquet != nil {
		opts := *o.Parquet
		opts.RowGroupRows = groupRows
		archOpt = &opts
	}
	var files pqc.Sink
	var copyTickets ticketStore
	who := fmt.Sprintf("Connect(%q)", name)
	if o.Parquet != nil && o.ParquetStore == "" {
		return nil, fmt.Errorf("%s: a reader with ConnectOptions.Parquet "+
			"needs ConnectOptions.ParquetStore: the store its files go to", who)
	}
	if o.ParquetStore != "" {
		if sameStore(o.ParquetStore, root) {
			return nil, fmt.Errorf("%s: ParquetStore %s is the store the "+
				"stream is in, and streams/%s/ there holds its writer's "+
				"files. Give this reader a store of its own for its files",
				who, o.ParquetStore, name)
		}
		if _, copyTickets, err = t.resolve(o.ParquetStore); err != nil {
			return nil, err
		}
		if files, err = t.files(o.ParquetStore); err != nil {
			return nil, err
		}
	}
	a := &Attached{Name: name, capacity: ringRows, columns: o.Columns,
		archOpt: archOpt, files: files, parquetStore: o.ParquetStore,
		copyTickets: copyTickets, durable: -1,
		closed: make(chan struct{}), log: t.log,
		proc: t, root: root, tickets: tickets}
	a.armed.Store(o.Parquet != nil)
	rules, err := newReaderRules(fmt.Sprintf("Connect(%q)", name), name, root,
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
	t.mu.Lock()
	t.readers[a] = struct{}{}
	t.mu.Unlock()
	a.done.Add(1)
	go a.lifecycle(t)
	return a, nil
}

// sameStore reports whether two store names are one place.
func sameStore(a, b string) bool {
	clean := func(s string) string {
		s = strings.TrimPrefix(s, "file://")
		if strings.Contains(s, "://") {
			return strings.TrimRight(s, "/")
		}
		if abs, err := filepath.Abs(s); err == nil {
			return abs
		}
		return s
	}
	return a != "" && b != "" && clean(a) == clean(b)
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
		a.log.Log(context.Background(), line.level, line.text)
	}
	return st
}

// lookLocal: the stream of this name in this process (nil for none, and
// for a reader that needs its own ring: one with a column subset or
// one that writes files), and whether this process has a stream in the
// name's folder.
func (a *Attached) lookLocal(t *process) (*Stream, bool) {
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
	if s != nil && (a.columns != nil || a.files != nil || s.isClosed()) {
		s = nil
	}
	return s, inFolder
}

// poll looks for the stream, in this process and then in the store,
// and takes the core's step for what was found.
func (a *Attached) poll(t *process) readerStep {
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
	case a.tickets == nil:
		a.lastTicket.Store(nil)
		st = a.rules.poll(foundNone, nil, nil) // no store: this process alone
	default:
		tk, err := a.tickets.get(a.Name)
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
		if !exists && a.tickets != nil {
			exists, err = a.tickets.hasFolder(a.rules.folder())
		}
		if err != nil {
			st = a.rules.poll(foundUnreadable, nil, err)
		} else {
			st = a.rules.folderIs(exists)
		}
	}
	return a.took(st)
}

// waitRing waits until the reader has its memory (the schema is known,
// at the first connection) or timeout.
func (a *Attached) waitRing(timeout time.Duration) (*ring.Ring, error) {
	deadline := time.Now().Add(timeout)
	for {
		if r := a.current(); r != nil {
			return r, nil
		}
		if time.Now().After(deadline) {
			reason := a.Reason()
			if reason != "" {
				reason = ": " + reason
			}
			return nil, fmt.Errorf("reader of %s: no connection within %s "+
				"(state %s)%s", a.Name, timeout, a.State(), reason)
		}
		select {
		case <-a.closed:
			return nil, fmt.Errorf("reader of %s: it is closed", a.Name)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// beginFile starts a file of this reader's rows, the mark at row number
// mark (negative: after the newest row received). The first file makes
// what writes them and takes the folder's writer lock. Caller holds mu.
func (a *Attached) beginFile(mark int64) error {
	if a.arch == nil {
		var opts ParquetOptions
		if a.archOpt != nil {
			opts = *a.archOpt
		}
		_, groupRows, err := ringSizes(a.Name, a.capacity, opts.RowGroupRows)
		if err != nil {
			return err
		}
		opts.RowGroupRows = groupRows
		if a.owner == nil {
			own, err := a.proc.claim(
				fmt.Sprintf("Connect(%q, ParquetStore: %q)", a.Name, a.parquetStore),
				copyLockDir(a.parquetStore), a.Name, a.run, a.parquetStore, a.copyTickets)
			if err != nil {
				return err
			}
			a.owner = own
		}
		arch, err := newArchiver(a.ring, a.Name, a.run, opts, a.files, nil, nil, nil, a.log)
		if err != nil {
			return err
		}
		a.arch = arch
	}
	return a.arch.startAt(a.label, mark)
}

// wroteFiles notes the outcome of a write of this reader's files.
// Caller holds mu.
func (a *Attached) wroteFiles(err error) {
	if a.arch != nil && a.arch.storedRow > a.durable {
		a.durable = a.arch.storedRow
	}
	if err != nil {
		if !a.archFailing {
			a.log.Error(fmt.Sprintf("[%s] this reader's files failing: %v", a.Name, err))
		}
		a.archFailing, a.archError = true, err.Error()
		return
	}
	if a.archFailing {
		a.log.Info(fmt.Sprintf("[%s] this reader's files recovered", a.Name))
		a.archFailing, a.archError = false, ""
	}
}

func (a *Attached) needsStore(call string) error {
	if a.files == nil {
		return fmt.Errorf("reader of %s: %s: this reader has no store for "+
			"files: ConnectOptions.ParquetStore names it", a.Name, call)
	}
	return nil
}

// StartParquet starts a new file of the rows this reader receives, as
// Stream.StartParquet does for the rows a stream writes: a mark in the
// rows, label in the file's name, o for the files from this one on (nil:
// the options the reader has, or the defaults). The files go to
// <ParquetStore>/streams/<name>/. Before the first connection it is
// noted, and the files begin with the first row received.
func (a *Attached) StartParquet(label string, o *ParquetOptions) error {
	if err := a.needsStore("StartParquet"); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if o != nil {
		_, groupRows, err := ringSizes(a.Name, a.capacity, o.RowGroupRows)
		if err != nil {
			return err
		}
		opts := *o
		opts.RowGroupRows = groupRows
		if a.arch != nil {
			// new options: the open file ends at the mark with the old ones
			err := a.arch.stop()
			a.wroteFiles(err)
			if err != nil {
				return err
			}
			a.arch = nil
		}
		a.archOpt = &opts
	}
	a.label = label
	a.armed.Store(true)
	a.refused = false
	if a.ring == nil || !a.ownRing {
		return nil // noted: the files begin with the first row received
	}
	err := a.beginFile(-1)
	a.wroteFiles(err)
	return err
}

// StopParquet ends the open file, with every row received up to now in
// it. The reader keeps its options for the next StartParquet.
func (a *Attached) StopParquet() error {
	if err := a.needsStore("StopParquet"); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.armed.Store(false)
	if a.arch == nil {
		return nil
	}
	err := a.arch.stop()
	a.wroteFiles(err)
	return err
}

// Cut ends the current file at the last row received; the next row
// begins a new file with the same label.
func (a *Attached) Cut() error { return a.cut(false, "") }

// CutLabel is Cut, and the next file carries label in its name ("" for
// no label).
func (a *Attached) CutLabel(label string) error { return a.cut(true, label) }

func (a *Attached) cut(setLabel bool, label string) error {
	if err := a.needsStore("Cut"); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if setLabel {
		a.label = label
	}
	if a.arch == nil {
		return nil
	}
	err := a.arch.cut(setLabel, label)
	if err == nil {
		err = a.arch.drain()
	}
	a.wroteFiles(err)
	return err
}

// ArchiveState answers the standing of this reader's own files.
func (a *Attached) ArchiveState() ArchiveState {
	a.mu.Lock()
	defer a.mu.Unlock()
	st := ArchiveState{Failing: a.archFailing, Error: a.archError}
	if a.arch != nil {
		st.LostRows = a.arch.lost()
		st.Location = a.arch.location
		st.File = a.arch.fileName
	}
	return st
}

// DurableRowNumber answers the number of the newest row that is in a
// written piece of one of this reader's files or in a checkpoint, and
// -1 when no row is there yet.
func (a *Attached) DurableRowNumber() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.durable
}

// dropRing: the ring in hand goes (the writer started again, or the
// local stream it belonged to closed). A ring this reader made closes,
// so a holder fails loudly and does not read a run that is over; a
// recording of that run is finished first.
func (a *Attached) dropRing(own bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.arch != nil && a.arch.storedRow > a.durable {
		a.durable = a.arch.storedRow
	}
	if own {
		if a.arch != nil && a.arch.recording() {
			if err := a.arch.stop(); err != nil {
				a.log.Error(fmt.Sprintf("[%s] a new run: finishing the "+
					"earlier run's file failed: %v", a.Name, err))
			}
		}
		if a.ring != nil {
			a.ring.Close()
		}
	}
	a.ring, a.arch, a.ownRing = nil, nil, false
}

func (a *Attached) lifecycle(t *process) {
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
		// this goroutine is the ring's writer and may do output: the
		// lines the ring's core makes are logged at once
		r, err := ring.NewRing(schema, a.capacity, &ring.Options{Logger: a.log})
		if err != nil {
			a.mu.Unlock()
			return err
		}
		a.ring, a.ownRing = r, true
		a.run = tk.Run
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
				// a quiet stream: nothing new yet; a checkpoint of this
				// reader's files may be due by time
				a.mu.Lock()
				if a.arch != nil {
					a.wroteFiles(a.arch.step())
				}
				a.mu.Unlock()
				continue
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
			if a.armed.Load() && a.files != nil && !a.refused &&
				(a.arch == nil || !a.arch.recording()) {
				// the files are on: this batch's first row begins one
				mark := firstSeq
				if mark < 0 {
					mark = from
				}
				if err := a.beginFile(mark); err != nil {
					a.refused = true // said once; a StartParquet tries again
					a.wroteFiles(err)
				}
			}
			err := a.ring.AppendRecordAt(rec, firstSeq)
			if err == nil && a.arch != nil {
				a.wroteFiles(a.arch.step())
			}
			a.mu.Unlock()
			if err != nil {
				rdr.Release()
				return err
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
	a.proc.mu.Lock()
	delete(a.proc.readers, a)
	a.proc.mu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	var err error
	if a.arch != nil && a.arch.recording() {
		err = a.arch.stop()
	} else if a.arch != nil {
		err = a.arch.rotate()
	}
	if a.arch != nil && a.arch.storedRow > a.durable {
		a.durable = a.arch.storedRow
	}
	if a.ring != nil && a.ownRing {
		a.ring.Close()
	}
	a.ring = nil
	a.owner.release() // another reader may write the folder now
	a.owner = nil
	return err
}
