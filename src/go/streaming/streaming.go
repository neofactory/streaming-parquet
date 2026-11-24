// Package streaming is the Go host of streaming-parquet: live
// telemetry streaming and parquet archival in one system, composing
// arrow-ring (the hot tier) and parquet-checkpoint (the durable
// format). Wire-compatible with the Python module: JSON tickets at
// <store>/tickets/<name>.json, Arrow Flight for batches.
//
// TWO VERBS, one machinery, no server role:
//
//	tel, _ := streaming.Init(streaming.Config{Store: "/data/streams"})
//	s, _ := tel.CreateStream("press/fieldbus", schema, opts)
//	    // write via s.Ring(): RowBegin / Set* / Commit / wire maps
//	    // s.Commit() after your rows; s.StartParquet("O0019", nil)
//	a, _ := tel.Connect("probe/voltages", streaming.ConnectOptions{})
//	    // read via a.Ring(): getters, relations
//	    // a.StartParquet("cal-run")  <- archival is symmetric: any
//	    //                               ring can archive
//
// Every process embeds the same thing: creating a stream grows a
// Flight face on an OS-assigned port and announces it by ticket;
// attaching builds a local ring fed by Flight. Which side you are is
// per-stream, not per-process.
package streaming

import (
	"log/slog"
	"os"
	"sync"
)

type Config struct {
	Store string // the store: a directory, file:// or s3:// URI. The
	//              tickets are at <store>/tickets/<name>.json.
	//              "" = $STREAMING_PARQUET_STORE; still "" = no store
	//              (a reader needs an explicit Ticket, and a stream
	//              writes no ticket and says so).
	FlightAddr string // the Flight listen address, e.g. ":8815" for a
	//                   process found by DNS; "" = any free port, which
	//                   the ticket announces.
	Logger *slog.Logger // where the library's log lines go. nil: warnings
	//                     and errors to standard error, as text. An
	//                     ERROR is a fault that loses or delays data, a
	//                     WARN a condition someone should look at, INFO a
	//                     milestone (a file is finished, the catalog is
	//                     published).
}

// Telemetry is one process's streaming context (streams share the
// process's Flight face and ticket store).
type Telemetry struct {
	tickets ticketStore // nil = no discovery configured
	log     *slog.Logger
	root    string
	flight  *flightFace
	mu      sync.Mutex
	streams map[string]*Stream
}

func Init(cfg Config) (*Telemetry, error) {
	f, err := newFlightFace(cfg.FlightAddr)
	if err != nil {
		return nil, err
	}
	t := &Telemetry{flight: f, streams: map[string]*Stream{}, log: cfg.Logger}
	if t.log == nil {
		t.log = slog.New(slog.NewTextHandler(os.Stderr,
			&slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	root := cfg.Store
	if root == "" {
		root = os.Getenv("STREAMING_PARQUET_STORE")
	}
	if root != "" {
		store, err := newTicketStore(root)
		if err != nil {
			f.shutdown()
			return nil, err
		}
		t.tickets = store
		t.root = root
	}
	return t, nil
}

// ListStreams names every stream with a ticket under the root
// (a listed name may be a dead publisher's stale ticket).
func (t *Telemetry) ListStreams() ([]string, error) {
	if t.tickets == nil {
		return nil, os.ErrNotExist
	}
	return t.tickets.list()
}

// FlightPort is the process's Flight port (tests dial it directly).
func (t *Telemetry) FlightPort() int { return t.flight.port }

// Close closes every stream and the Flight face.
func (t *Telemetry) Close() error {
	t.mu.Lock()
	streams := make([]*Stream, 0, len(t.streams))
	for _, s := range t.streams {
		streams = append(streams, s)
	}
	t.mu.Unlock()
	var first error
	for _, s := range streams {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	t.flight.shutdown()
	return first
}
