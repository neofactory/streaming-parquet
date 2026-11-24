// Package streaming is the Go host of streaming-parquet: live streams
// and their parquet files in one system, over the ring (the rows in
// memory) and the checkpoint format (the files). It works with the
// Python package: the same tickets at <store>/tickets/<name>.json, the
// same Arrow Flight requests, the same files.
//
// Two calls, and no server role:
//
//	s, _ := streaming.CreateStream("press/fieldbus", schema,
//	        streaming.StreamOptions{Store: "/data/streams"})
//	x, _ := s.Column("x")
//	row, _ := s.MutableRow(0)
//	row.SetF64(x, 1.0)
//	s.Commit()
//
//	a, _ := streaming.Connect("probe/voltages",
//	        streaming.ConnectOptions{Store: "/data/streams"})
//	newest, ok := a.NewestRow()
//
// A process that creates a stream serves it over Flight on a port the
// system gives it, and the stream's ticket in the store names that
// port. A reader finds the ticket and pulls the rows into memory of its
// own. Which side a process is on is decided for each stream, not for
// the process.
package streaming

import (
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// process holds what the streams of one process share: the Flight
// face, the logger, and the streams and readers it has. A program has
// one, made at its first call (theProcess). A test makes more than
// one, to be two writers in one test process.
type process struct {
	log    *slog.Logger
	flight *flightFace
	store  string // the store of a call that names none; "" = the environment's

	mu      sync.Mutex
	streams map[string]*Stream
	readers map[*Attached]struct{}
	stores  map[string]ticketStore // the ticket store of each store used
}

// processConfig is what newProcess takes.
type processConfig struct {
	Store      string
	FlightAddr string
	Logger     *slog.Logger
}

func newProcess(cfg processConfig) (*process, error) {
	f, err := newFlightFace(cfg.FlightAddr)
	if err != nil {
		return nil, err
	}
	t := &process{flight: f, log: cfg.Logger, store: cfg.Store,
		streams: map[string]*Stream{}, readers: map[*Attached]struct{}{},
		stores: map[string]ticketStore{}}
	if t.log == nil {
		t.log = slog.New(slog.NewTextHandler(os.Stderr,
			&slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	return t, nil
}

// resolve answers the store of a call: the one it names, else the
// process's, else $STREAMING_PARQUET_STORE; and where that store's
// tickets are. "" and nil when there is none.
func (t *process) resolve(store string) (string, ticketStore, error) {
	if store == "" {
		store = t.store
	}
	if store == "" {
		store = os.Getenv("STREAMING_PARQUET_STORE")
	}
	if store == "" {
		return "", nil, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if tickets, ok := t.stores[store]; ok {
		return store, tickets, nil
	}
	tickets, err := newTicketStore(store)
	if err != nil {
		return "", nil, err
	}
	t.stores[store] = tickets
	return store, tickets, nil
}

// ListStreams names every stream with a ticket in the store (a listed
// name may be the ticket of a writer that died).
func (t *process) ListStreams(store string) ([]string, error) {
	root, tickets, err := t.resolve(store)
	if err != nil {
		return nil, err
	}
	if tickets == nil {
		return nil, fmt.Errorf("ListStreams: no store: name one, or set " +
			"$STREAMING_PARQUET_STORE")
	}
	names, err := tickets.list()
	if err != nil {
		return nil, fmt.Errorf("ListStreams: the store %s: %w", root, err)
	}
	return names, nil
}

// FlightPort is the process's Flight port (tests dial it directly).
func (t *process) FlightPort() int { return t.flight.port }

// Close closes every reader and every stream, and the Flight face.
func (t *process) Close() error {
	t.mu.Lock()
	streams := make([]*Stream, 0, len(t.streams))
	for _, s := range t.streams {
		streams = append(streams, s)
	}
	readers := make([]*Attached, 0, len(t.readers))
	for a := range t.readers {
		readers = append(readers, a)
	}
	t.mu.Unlock()
	var first error
	for _, a := range readers {
		if err := a.Close(); err != nil && first == nil {
			first = err
		}
	}
	for _, s := range streams {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	t.flight.shutdown()
	return first
}

// ---- the program's one process ---------------------------------------------

var (
	theMu      sync.Mutex
	theProcess *process // made at the first call that needs it
	theLogger  *slog.Logger
	theFlight  string
)

// SetLogger says where the library's log lines go. With none set,
// warnings and errors go to standard error, as text. An ERROR is a
// fault that loses or delays data, a WARN a condition someone should
// look at, INFO a milestone (a file is finished, the catalog is
// published). Call it before the first stream or reader: it is refused
// after.
func SetLogger(l *slog.Logger) error {
	theMu.Lock()
	defer theMu.Unlock()
	if theProcess != nil {
		return fmt.Errorf("SetLogger: this process already has a stream or " +
			"a reader, and they log to the logger set before them; call " +
			"SetLogger first")
	}
	theLogger = l
	return nil
}

// SetFlightAddress sets the address the process serves its streams on,
// for a process that readers find by a fixed port (":8815"). With none
// set the system gives a free port, and each ticket names it. Call it
// before the first stream or reader: it is refused after.
func SetFlightAddress(addr string) error {
	theMu.Lock()
	defer theMu.Unlock()
	if theProcess != nil {
		return fmt.Errorf("SetFlightAddress(%q): this process already "+
			"serves Flight on port %d; call SetFlightAddress first", addr,
			theProcess.flight.port)
	}
	theFlight = addr
	return nil
}

func ours() (*process, error) {
	theMu.Lock()
	defer theMu.Unlock()
	if theProcess == nil {
		t, err := newProcess(processConfig{FlightAddr: theFlight, Logger: theLogger})
		if err != nil {
			return nil, err
		}
		theProcess = t
	}
	return theProcess, nil
}

// CreateStream creates a stream this process writes. One writer for
// each stream name in a store: the error names the writer that has it.
func CreateStream(name string, schema *arrow.Schema, o StreamOptions) (*Stream, error) {
	t, err := ours()
	if err != nil {
		return nil, err
	}
	return t.CreateStream(name, schema, o)
}

// Connect opens a reader of a stream: one this process writes, read in
// place, or one another process writes, found by its ticket in the
// store and pulled over Flight. A reader that starts before its writer
// waits for it; a name whose folder does not exist in the store is an
// error at once.
func Connect(name string, o ConnectOptions) (*Attached, error) {
	t, err := ours()
	if err != nil {
		return nil, err
	}
	return t.Connect(name, o)
}

// ListStreams names every stream with a ticket in the store ("" =
// $STREAMING_PARQUET_STORE). A listed name may be the ticket of a
// writer that died.
func ListStreams(store string) ([]string, error) {
	t, err := ours()
	if err != nil {
		return nil, err
	}
	return t.ListStreams(store)
}

// Close closes every stream and every reader of this process, and
// stops serving Flight. A call after it starts again.
func Close() error {
	theMu.Lock()
	t := theProcess
	theProcess = nil
	theMu.Unlock()
	if t == nil {
		return nil
	}
	return t.Close()
}
