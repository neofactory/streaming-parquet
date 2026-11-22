package streaming

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/neofactory/streaming-parquet/src/go/pqc"
	"github.com/neofactory/streaming-parquet/src/go/ring"
)

// Stream is a stream this process writes: a ring, and behind it a
// goroutine that writes the parquet files. Write rows through Ring()
// and call Commit(): readers see the rows at once, and the goroutine
// does everything slow. One writer goroutine, like the ring itself.
//
// A published stream (the default) has a ticket under the root, so
// other processes find it, and the Flight face serves it, so they read
// it. An internal one has neither.
type Stream struct {
	Name string

	tel      *Telemetry
	ring     *ring.Ring
	ringRows int64
	run      string // the writer's start time; the ticket's run, every file's sp_run

	// The parquet path, under two locks. mu is cheap: it covers the plan
	// (which rows are due, their pins, the queue of writes owed) and is
	// never held across I/O, so Commit takes it. io is held across a
	// write. A call that needs both (StartParquet, StopParquet, Close)
	// takes io first, then mu.
	mu       sync.Mutex
	io       sync.Mutex
	arch     *archiver       // nil until a StartParquet
	parquet  *ParquetOptions // the options the files are written with; nil = none yet
	failing  bool            // the last write failed
	errText  string
	file     string       // the file open right now, "" when none is
	location atomic.Value // string: where the files land, "" before any
	durable  atomic.Int64 // first seq not yet in a file or a checkpoint

	wake     chan struct{} // a commit: the work goroutine has rows to look at
	stopWork chan struct{}
	workDone sync.WaitGroup

	// Published or internal. pubMu orders Publish, Unpublish and Close.
	pubMu     sync.Mutex
	published bool
	stopHB    chan struct{}
	hbDone    sync.WaitGroup

	// What the store holds of this run, for the ticket; ticketNow asks
	// the ticket's goroutine to write it at once: the store changed.
	store     *storeState
	ticketNow chan struct{}
	// counts this run's writes of the ticket; every Flight answer carries it
	ticketRevision atomic.Int64
	ticketBegun    atomic.Int64 // the revision of the newest write begun
	ticketStored   atomic.Int64 // the revision of the newest write that worked

	// The catalogs of the stream's finished files, one for each folder
	// it wrote files to (catalog.go). Under mu.
	catalogs map[string]*catalog

	// The writer lock (owner.go): taken the first time the stream is
	// published or starts a file, held until Close.
	ownMu sync.Mutex
	owner *ownership

	closed bool
}

// own holds the stream's writer lock, or returns an error that names
// the writer that has it.
func (s *Stream) own() error {
	s.ownMu.Lock()
	defer s.ownMu.Unlock()
	if s.owner != nil {
		return nil
	}
	o, err := s.tel.claim(s.Name, s.run)
	if err != nil {
		return err
	}
	s.owner = o
	return nil
}

// StreamOptions are given once, at CreateStream.
//
// Parquet says how the stream's rows are written to parquet, and with
// its BaseDir or Sink where. With it, the stream writes files from its
// first row. Nil means no files until a StartParquet that gives options.
//
// Internal makes a stream that other processes cannot find or read: no
// ticket, not served. Publish and Unpublish change it later.
type StreamOptions struct {
	RingRows int64 // ring rows; see ringSizes for the default
	Parquet  *ParquetOptions
	Internal bool
}

func (t *Telemetry) CreateStream(name string, schema *arrow.Schema,
	o StreamOptions) (*Stream, error) {
	var givenGroupRows int64
	if o.Parquet != nil {
		givenGroupRows = o.Parquet.RowGroupRows
	}
	ringRows, _, err := ringSizes(name, o.RingRows, givenGroupRows)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	_, dup := t.streams[name]
	t.mu.Unlock()
	if dup {
		return nil, &DupStreamError{name}
	}
	r, err := ring.NewRing(schema, ringRows, nil)
	if err != nil {
		return nil, err
	}
	s := &Stream{Name: name, tel: t, ring: r, ringRows: ringRows,
		run: runStart(), wake: make(chan struct{}, 1),
		store: newStoreState(pqc.SupersededKeep), ticketNow: make(chan struct{}, 1),
		stopWork: make(chan struct{})}
	s.location.Store("")
	// Own the stream before anything of it is written: one writer per
	// stream. An internal stream with no files takes no lock.
	if !o.Internal || o.Parquet != nil {
		if err := s.own(); err != nil {
			r.Close()
			return nil, err
		}
	}
	if o.Parquet != nil {
		if err := s.StartParquet("", o.Parquet); err != nil {
			s.owner.release()
			r.Close()
			return nil, err
		}
	}
	t.mu.Lock()
	if _, dup := t.streams[name]; dup {
		t.mu.Unlock()
		s.owner.release()
		r.Close()
		return nil, &DupStreamError{name}
	}
	t.streams[name] = s
	t.mu.Unlock()
	s.workDone.Add(1)
	go s.work()
	if !o.Internal {
		if err := s.Publish(); err != nil {
			s.Close()
			return nil, err
		}
	}
	return s, nil
}

type DupStreamError struct{ Name string }

func (e *DupStreamError) Error() string {
	return "stream '" + e.Name + "' already exists"
}

// Ring exposes the stream's ring: RowBegin and the setters to write a
// row, getters and record sets to read. ONE writer goroutine. Commit
// through the stream (Commit below), not through the ring.
func (s *Stream) Ring() *ring.Ring { return s.ring }

// Commit freezes the rows written since the last Commit and returns how
// many. Readers see them at once. If a row group or a checkpoint is due,
// Commit pins its rows and hands them to the stream's goroutine, which
// writes them: nothing slow runs here. Call it also after a bulk append
// on the ring, which commits by itself.
func (s *Stream) Commit() (int64, error) {
	n, err := s.ring.Commit()
	s.tel.flight.notify(s.Name)
	s.plan()
	return n, err
}

// plan hands the goroutine the rows of a write that is due, pinned.
func (s *Stream) plan() {
	s.mu.Lock()
	owed := false
	if s.arch != nil {
		if err := s.arch.plan(); err != nil {
			s.fault(err)
		}
		owed = len(s.arch.queue) > 0
	}
	s.mu.Unlock()
	if owed {
		select {
		case s.wake <- struct{}{}:
		default: // a wake is already waiting
		}
	}
}

// work is the stream's goroutine: it writes what Commit hands it. With
// no commit it wakes when a checkpoint or a row group is due by time,
// and to try again a write that failed.
func (s *Stream) work() {
	defer s.workDone.Done()
	timer := time.NewTimer(s.workWait())
	defer timer.Stop()
	for {
		select {
		case <-s.stopWork:
			return
		case <-s.wake:
		case <-timer.C:
			s.tel.flight.notify(s.Name) // rows committed on the ring directly
			s.plan()
		}
		s.write()
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(s.workWait())
	}
}

// workWait is how long the goroutine sleeps with no commit: half the
// shortest time-driven cadence of the files, and at most the second a
// waiting Flight reader waits (waitFor).
func (s *Stream) workWait() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	wait := waitFor
	if s.arch != nil && s.arch.recording() {
		for _, seconds := range []float64{s.arch.ckptSeconds, s.arch.rowGroupSeconds} {
			if seconds > 0 {
				if d := time.Duration(seconds * float64(time.Second) / 2); d < wait {
					wait = d
				}
			}
		}
	}
	if wait < 10*time.Millisecond {
		wait = 10 * time.Millisecond
	}
	return wait
}

// write does the writes owed, oldest first, until none is left or one
// fails. A write that fails stays at the head, its rows pinned, and is
// tried again at the next wake.
func (s *Stream) write() {
	s.io.Lock()
	defer s.io.Unlock()
	for {
		s.mu.Lock()
		arch := s.arch
		var w *work
		if arch != nil && len(arch.queue) > 0 {
			w = arch.queue[0]
		}
		s.mu.Unlock()
		if w == nil {
			return
		}
		err := arch.write(w)
		s.mu.Lock()
		if err == nil {
			arch.pop()
		}
		s.wrote(err)
		s.mu.Unlock()
		select { // the ticket says what the store holds: write it now
		case s.ticketNow <- struct{}{}:
		default:
		}
		if err != nil {
			return
		}
	}
}

// wrote records the outcome of a write. Caller holds io and mu.
func (s *Stream) wrote(err error) {
	if s.arch != nil {
		s.durable.Store(s.arch.durable)
		s.file = s.arch.fileName
	}
	if err != nil {
		s.fault(err)
		return
	}
	if s.failing {
		fmt.Fprintf(os.Stderr, "[%s] archive recovered\n", s.Name)
		s.failing, s.errText = false, ""
	}
}

// fault records a failure on the parquet path. Caller holds mu.
func (s *Stream) fault(err error) {
	if !s.failing {
		fmt.Fprintf(os.Stderr, "[%s] archive failing: %v\n", s.Name, err)
	}
	s.failing, s.errText = true, err.Error()
}

// ArchiveState is the standing of a stream's parquet files. A failing
// archive means rows are durable nowhere but the ring. LostRows never
// resets: rows the ring reused before they were written. File is the
// name of the file open right now, "" when none is.
type ArchiveState struct {
	Failing  bool
	Error    string
	LostRows int64
	Location string
	File     string
	// The catalog of the finished files: how many the published one
	// lists, and its last pass's failure ("" when fine).
	CatalogFiles int
	CatalogError string
}

// ArchiveState answers the standing of the stream's parquet files: a
// write fault shows here, not as the result of a call.
func (s *Stream) ArchiveState() ArchiveState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := ArchiveState{Failing: s.failing, Error: s.errText,
		Location: s.location.Load().(string), File: s.file}
	if s.arch != nil {
		st.LostRows = s.arch.lost()
		if s.arch.cat != nil {
			st.CatalogFiles, st.CatalogError = s.arch.cat.state()
		}
	}
	return st
}

// ---- published or internal ------------------------------------------------

// Published reports whether other processes can find and read the stream.
func (s *Stream) Published() bool {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	return s.published
}

// Publish lets other processes find the stream (its ticket under the
// root) and read it (the Flight face). It takes the stream's writer
// lock: the error names the holder when another writer has it.
func (s *Stream) Publish() error {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	if s.published || s.closed {
		return nil
	}
	if err := s.own(); err != nil {
		return err
	}
	s.published = true
	s.tel.flight.register(s.Name, s.ring.Schema(), s)
	if s.tel.tickets != nil {
		s.stopHB = make(chan struct{})
		s.hbDone.Add(1)
		go s.heartbeat(s.stopHB)
	} else {
		fmt.Fprintf(os.Stderr, "[%s] no discovery root configured "+
			"(Config.Root / $STREAMING_PARQUET_ROOT) — no ticket; "+
			"the stream is local + direct-ticket only\n", s.Name)
	}
	return nil
}

// Unpublish makes the stream internal: its ticket is deleted and the
// Flight face stops serving it. A connected reader sees what it sees
// when a writer closes.
func (s *Stream) Unpublish() {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	s.unpublish()
}

// unpublish: caller holds pubMu.
func (s *Stream) unpublish() {
	if !s.published {
		return
	}
	s.published = false
	if s.tel.tickets != nil {
		close(s.stopHB)
		s.hbDone.Wait()
		if err := s.tel.tickets.delete(s.Name); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] ticket delete failed: %v\n",
				s.Name, err)
		}
	}
	s.tel.flight.unregister(s.Name)
}

// catalogFor answers the catalog of the files in folder, made the first
// time the stream writes there. The caller holds mu.
func (s *Stream) catalogFor(sink pqc.Sink, folder string) *catalog {
	if c := s.catalogs[folder]; c != nil {
		return c
	}
	c := newCatalog(s.Name, folder, sink, s.store, s.ticketWritten)
	if c != nil {
		if s.catalogs == nil {
			s.catalogs = map[string]*catalog{}
		}
		s.catalogs[folder] = c
	}
	return c
}

// ticketWritten has the ticket written now, and answers true once a
// write that began after this call is in the store. A stream that is
// not published, or that has no store for tickets, has no ticket: true
// at once. False after 5 seconds.
func (s *Stream) ticketWritten() bool {
	s.pubMu.Lock()
	published := s.published
	s.pubMu.Unlock()
	if !published || s.tel.tickets == nil {
		return true
	}
	begun := s.ticketBegun.Load()
	deadline := time.Now().Add(5 * time.Second)
	for s.ticketStored.Load() <= begun {
		select {
		case s.ticketNow <- struct{}{}:
		default:
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

func (s *Stream) heartbeat(stop chan struct{}) {
	defer s.hbDone.Done()
	tick := time.NewTicker(heartbeatInterval)
	defer tick.Stop()
	pub := func() {
		revision := s.ticketRevision.Load() + 1
		s.ticketBegun.Store(revision)
		t := Ticket{
			Name: s.Name, Host: localIP(), Port: s.tel.flight.port,
			Ts:          float64(time.Now().UnixNano()) / 1e9,
			Run:         s.run,
			BeatSeconds: heartbeatInterval.Seconds(),
			StaleAfter:  pqc.DeadAfter(heartbeatInterval).Seconds(),
			Revision:    revision,
			Archive:     s.location.Load().(string)}
		s.mu.Lock()
		where := ""
		if s.arch != nil {
			where = strings.TrimSuffix(s.arch.location, "/") + "/"
			if s.arch.recording() {
				t.CheckpointSeconds = s.arch.ckptSeconds
			}
		}
		s.mu.Unlock()
		if where != "" {
			// what a reader can take from the store: the core's record
			if err := s.store.fill(&t, where); err != nil {
				fmt.Fprintf(os.Stderr, "[%s] ticket: %v\n", s.Name, err)
			}
		}
		if err := s.tel.tickets.put(s.Name, t); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] ticket publish error: %v\n",
				s.Name, err)
		} else {
			s.ticketStored.Store(revision)
		}
		// after the write: a reader told of a revision finds it in the store
		s.ticketRevision.Store(revision)
	}
	pub()
	for {
		select {
		case <-tick.C:
			pub()
		case <-s.ticketNow:
			pub()
		case <-stop:
			return
		}
	}
}

// ---- parquet on and off -----------------------------------------------------

// StartParquet starts a new file now. It carries label in its name and
// holds the rows committed from this moment: the call is a mark in the
// stream. Rows committed before it are not put into the new file. If a
// file is open, it ends at the mark, with every row committed before
// the mark in it. If none was open, the rows committed while the stream
// was off are in no file.
//
// o replaces the stream's parquet options whole, from this file on. Nil
// means the options the stream has; a stream that never had any needs
// them here, because they say where the files go. A row group the ring
// cannot hold is refused: the ring was sized at CreateStream.
func (s *Stream) StartParquet(label string, o *ParquetOptions) error {
	s.io.Lock()
	defer s.io.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if o == nil {
		if s.parquet == nil {
			return fmt.Errorf("stream %s: StartParquet needs ParquetOptions: "+
				"the stream has none, and they say where the files go", s.Name)
		}
		if s.arch != nil {
			err := s.arch.start(label)
			s.wrote(err)
			return err
		}
		o = s.parquet
	}
	opts := *o
	_, groupRows, err := ringSizes(s.Name, s.ringRows, opts.RowGroupRows)
	if err != nil {
		return err
	}
	if err := s.own(); err != nil {
		return err
	}
	opts.RowGroupRows = groupRows
	var lost int64
	if s.arch != nil {
		// new options: the open file ends at the mark with the old ones
		if err := s.arch.stop(); err != nil {
			return err
		}
		lost = s.arch.lost()
	}
	arch, err := newArchiver(s.ring, s.Name, s.run, opts, s.store, s.catalogFor)
	if err != nil {
		return err
	}
	arch.lostBefore = lost
	s.arch, s.parquet = arch, &opts
	s.location.Store(arch.location)
	err = arch.start(label)
	s.wrote(err)
	return err
}

// ParquetOptions answers the options the stream writes its files with:
// given at CreateStream or to a StartParquet. Nil when it has none.
func (s *Stream) ParquetOptions() *ParquetOptions {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.parquet == nil {
		return nil
	}
	o := *s.parquet
	return &o
}

// Cut ends the open file at the last committed row; the next committed
// row begins a new file with the same label. No committed row is
// skipped or written twice. The boundary call for a stream that records
// all the time. It returns at once: the stream's goroutine writes the
// rows and ends the file. A stream that is not recording does nothing.
func (s *Stream) Cut() { s.cut(false, "") }

// CutLabel is Cut, and the next file carries label in its name ("" for
// no label).
func (s *Stream) CutLabel(label string) { s.cut(true, label) }

func (s *Stream) cut(setLabel bool, label string) {
	s.tel.flight.notify(s.Name)
	s.mu.Lock()
	owed := false
	if s.arch != nil {
		if err := s.arch.cut(setLabel, label); err != nil {
			s.fault(err)
		}
		owed = len(s.arch.queue) > 0
	}
	s.mu.Unlock()
	if owed {
		select {
		case s.wake <- struct{}{}:
		default: // a wake is already waiting
		}
	}
}

// StopParquet ends the open file, with every row committed up to now in
// it. The stream keeps its options for the next StartParquet.
func (s *Stream) StopParquet() error {
	s.io.Lock()
	defer s.io.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.arch == nil {
		return nil
	}
	err := s.arch.stop()
	s.wrote(err)
	return err
}

// Durable answers the first seq not yet in the archive: every row below
// it is in a written chunk or the checkpoint. A producer that must
// acknowledge its input only once it is durable — a queue consumer —
// compares the row's seq against this. It moves when the stream's
// goroutine has written, a moment after the commit; a write fault shows
// in ArchiveState. Meaningful while recording; equal to the recording's
// first seq before anything landed, and 0 on a stream that never wrote
// a file.
func (s *Stream) Durable() int64 { return s.durable.Load() }

func (s *Stream) isClosed() bool {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	return s.closed
}

// Close ends the stream: the goroutine stops, the open parquet file is
// finished with every committed row in it, the ticket is cleared, and
// the ring is freed.
func (s *Stream) Close() error {
	s.pubMu.Lock()
	if s.closed {
		s.pubMu.Unlock()
		return nil
	}
	s.closed = true
	s.pubMu.Unlock()

	close(s.stopWork)
	s.workDone.Wait()
	s.tel.flight.notify(s.Name)

	s.io.Lock()
	s.mu.Lock()
	var err error
	if s.arch != nil {
		err = s.arch.stop() // the rows still owed, then the file is finished
		s.wrote(err)
		s.arch.abandon() // a store that is failing: the pins go with the ring
	}
	var cats []*catalog
	for _, c := range s.catalogs {
		cats = append(cats, c)
	}
	s.mu.Unlock()
	s.io.Unlock()

	// The catalog lists the last file before the stream goes, when it
	// can in a few seconds; the ticket must still be there for it.
	for _, c := range cats {
		c.finish(catalogCloseWait)
	}

	s.pubMu.Lock()
	s.unpublish()
	s.pubMu.Unlock()

	s.tel.mu.Lock()
	delete(s.tel.streams, s.Name)
	s.tel.mu.Unlock()
	s.ring.Close()
	s.ownMu.Lock()
	s.owner.release() // the next writer may start now
	s.owner = nil
	s.ownMu.Unlock()
	return err
}
