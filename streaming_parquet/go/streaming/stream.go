package streaming

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/neofactory/streaming-parquet/arrow_ring/go/ring"
	"github.com/neofactory/streaming-parquet/parquet_checkpoint/go/pqc"
)

// Stream is a local publishing stream: a ring, the process's Flight
// face, a ticket heartbeat, and an archiver. Write rows through
// Ring(); call Publish() from your loop to fan committed rows out and
// advance the archiver. One writer goroutine, like the ring itself.
type Stream struct {
	Name string

	tel       *Telemetry
	ring      *ring.Ring
	arch      *archiver       // nil until a StartParquet
	parquet   *ParquetOptions // the options the files are written with; nil = none yet
	ringRows  int64
	location  atomic.Value // string: where the files land, "" before any
	run       string       // the writer's start time; the ticket's run, every file's sp_run
	published int64        // committed rows as of the last Publish
	stopHB    chan struct{}
	hbDone    sync.WaitGroup
	closed    bool
}

// StreamOptions are given once, at CreateStream.
//
// Parquet says how the stream's rows are written to parquet, and with
// its BaseDir or Sink where. With it, the stream writes files from its
// first row. Nil means no files until a StartParquet that gives options.
type StreamOptions struct {
	RingRows int64 // ring rows; see ringSizes for the default
	Parquet  *ParquetOptions
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
	r, err := ring.NewRing(schema, ringRows, nil)
	if err != nil {
		return nil, err
	}
	s := &Stream{Name: name, tel: t, ring: r, ringRows: ringRows,
		run: runStart(), stopHB: make(chan struct{})}
	s.location.Store("")
	if o.Parquet != nil {
		if err := s.StartParquet("", o.Parquet); err != nil {
			r.Close()
			return nil, err
		}
	}
	t.mu.Lock()
	if _, dup := t.streams[name]; dup {
		t.mu.Unlock()
		r.Close()
		return nil, &DupStreamError{name}
	}
	t.streams[name] = s
	t.mu.Unlock()
	t.flight.register(name, schema, s)
	if t.tickets != nil {
		s.hbDone.Add(1)
		go s.heartbeat()
	} else {
		fmt.Fprintf(os.Stderr, "[%s] no discovery root configured "+
			"(Config.Root / $STREAMING_PARQUET_ROOT) — ticket "+
			"publishing disabled; stream is local + direct-ticket "+
			"only\n", name)
	}
	return s, nil
}

type DupStreamError struct{ Name string }

func (e *DupStreamError) Error() string {
	return "stream '" + e.Name + "' already exists"
}

// Ring exposes the stream's ring: RowBegin/Set*/Commit to write,
// getters and relations to read. ONE writer goroutine.
func (s *Stream) Ring() *ring.Ring { return s.ring }

func (s *Stream) heartbeat() {
	defer s.hbDone.Done()
	tick := time.NewTicker(heartbeatInterval)
	defer tick.Stop()
	pub := func() {
		err := s.tel.tickets.put(s.Name, Ticket{
			Name: s.Name, Host: localIP(), Port: s.tel.flight.port,
			Ts:         float64(time.Now().UnixNano()) / 1e9,
			Run:        s.run,
			Archive:    s.location.Load().(string),
			StaleAfter: pqc.DeadAfter(heartbeatInterval).Seconds()})
		if err != nil {
			fmt.Fprintf(os.Stderr, "[%s] ticket publish error: %v\n",
				s.Name, err)
		}
	}
	pub()
	for {
		select {
		case <-tick.C:
			pub()
		case <-s.stopHB:
			return
		}
	}
}

// Publish makes committed rows visible outward: Flight readers waiting
// on this stream are woken (they pull; flightserver.go says why), and
// the archiver advances. Returns the rows committed since the last
// Publish.
func (s *Stream) Publish() (int64, error) {
	committed := s.ring.NewestSeq() + 1
	var fresh int64
	if committed > s.published {
		fresh = committed - s.published
		s.published = committed
		s.tel.flight.notify(s.Name)
	}
	if s.arch == nil {
		return fresh, nil
	}
	return fresh, s.arch.step()
}

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
	if o == nil {
		if s.parquet == nil {
			return fmt.Errorf("stream %s: StartParquet needs ParquetOptions: "+
				"the stream has none, and they say where the files go", s.Name)
		}
		if s.arch != nil {
			return s.arch.start(label)
		}
		o = s.parquet
	}
	opts := *o
	_, groupRows, err := ringSizes(s.Name, s.ringRows, opts.RowGroupRows)
	if err != nil {
		return err
	}
	opts.RowGroupRows = groupRows
	if s.arch != nil {
		// new options: the open file ends at the mark with the old ones
		if err := s.arch.stop(); err != nil {
			return err
		}
	}
	arch, err := newArchiver(s.ring, s.Name, s.run, opts)
	if err != nil {
		return err
	}
	s.arch, s.parquet = arch, &opts
	s.location.Store(arch.location)
	return arch.start(label)
}

// ParquetOptions answers the options the stream writes its files with:
// given at CreateStream or to a StartParquet. Nil when it has none.
func (s *Stream) ParquetOptions() *ParquetOptions {
	if s.parquet == nil {
		return nil
	}
	o := *s.parquet
	return &o
}

// StopParquet ends the open file, with every row committed up to now in
// it. The stream keeps its options for the next StartParquet.
func (s *Stream) StopParquet() error {
	if s.arch == nil {
		return nil
	}
	return s.arch.stop()
}

// Durable answers the first seq not yet in the archive: every row below
// it is in a written chunk or the checkpoint, so a producer that must
// acknowledge its input only once it is durable — a queue consumer —
// compares the row's seq against this after Publish. Meaningful while
// recording; equal to the recording's first seq before anything landed,
// and 0 on a stream that never wrote a file.
func (s *Stream) Durable() int64 {
	if s.arch == nil {
		return 0
	}
	return s.arch.durable
}

// Close publishes the final rows, stops the heartbeat, clears the
// ticket, finalizes any open parquet file, and frees the ring.
func (s *Stream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if _, err := s.Publish(); err != nil {
		return err
	}
	var err error
	if s.arch != nil {
		if s.arch.recording {
			err = s.arch.stop()
		} else {
			err = s.arch.rotate()
		}
	}
	if s.tel.tickets != nil {
		close(s.stopHB)
		s.hbDone.Wait()
		if derr := s.tel.tickets.delete(s.Name); derr != nil {
			fmt.Fprintf(os.Stderr, "[%s] ticket delete failed on "+
				"close: %v\n", s.Name, derr)
		}
	}
	s.tel.flight.unregister(s.Name)
	s.tel.mu.Lock()
	delete(s.tel.streams, s.Name)
	s.tel.mu.Unlock()
	s.ring.Close()
	return err
}
