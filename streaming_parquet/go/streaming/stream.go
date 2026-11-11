package streaming

import (
	"fmt"
	"os"
	"sync"
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

	tel          *Telemetry
	ring         *ring.Ring
	arch         *archiver
	run          string // the writer's start time; the ticket's run, every file's sp_run
	published    int64  // committed rows as of the last Publish
	stopHB       chan struct{}
	hbDone       sync.WaitGroup
	closed       bool
}

type StreamOptions struct {
	RingRows int64 // ring rows; see ringSizes for the default
	Parquet  bool  // recording gate at creation (StartParquet later works too)
	Archive  ParquetOptions
}

func (t *Telemetry) CreateStream(name string, schema *arrow.Schema,
	o StreamOptions) (*Stream, error) {
	ringRows, groupRows, err := ringSizes(name, o.RingRows, o.Archive.RowGroupRows)
	if err != nil {
		return nil, err
	}
	o.Archive.RowGroupRows = groupRows
	r, err := ring.NewRing(schema, ringRows, nil)
	if err != nil {
		return nil, err
	}
	run := runStart()
	arch, err := newArchiver(r, name, run, o.Archive)
	if err != nil {
		r.Close()
		return nil, err
	}
	s := &Stream{Name: name, tel: t, ring: r, arch: arch,
		run: run, stopHB: make(chan struct{})}
	if o.Parquet {
		if err := arch.start(""); err != nil {
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
			Archive:    s.arch.location,
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
	return fresh, s.arch.step()
}

// StartParquet begins a recording segment covering rows committed
// from this moment; label lands in the filename.
func (s *Stream) StartParquet(label string) error { return s.arch.start(label) }

// StopParquet flushes the pending region and finalizes the file.
func (s *Stream) StopParquet() error { return s.arch.stop() }

// Durable answers the first seq not yet in the archive: every row below
// it is in a written chunk or the checkpoint, so a producer that must
// acknowledge its input only once it is durable — a queue consumer —
// compares the row's seq against this after Publish. Meaningful while
// recording; equal to the recording's first seq before anything landed.
func (s *Stream) Durable() int64 { return s.arch.durable }

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
	if s.arch.recording {
		err = s.arch.stop()
	} else {
		err = s.arch.rotate()
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
