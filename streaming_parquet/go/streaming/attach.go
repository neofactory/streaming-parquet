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
	"github.com/neofactory/streaming-parquet/arrow_ring/go/ring"
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
	arch    *archiver
	archOpt *ParquetOptions // nil: this reader writes no files
	armed   atomic.Bool     // StartParquet requested (possibly pre-schema)
	label   string

	capacity int64
	columns  []string
	state    atomic.Value // string
	closed   chan struct{}
	done     sync.WaitGroup
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
	a.state.Store("no-ticket")
	if o.Ticket != nil {
		a.state.Store("connecting")
	}
	a.done.Add(1)
	go a.lifecycle(t, o.Ticket)
	return a, nil
}

func (a *Attached) State() string { return a.state.Load().(string) }

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

func (a *Attached) lifecycle(t *Telemetry, direct *Ticket) {
	defer a.done.Done()
	backoff := time.Second
	cur := direct
	rootErrLogged := false
	run := ""
	haveRun := false
	wait := func(d time.Duration) bool { // false = closed
		select {
		case <-time.After(d):
			return true
		case <-a.closed:
			a.state.Store("closed")
			return false
		}
	}
	for {
		select {
		case <-a.closed:
			a.state.Store("closed")
			return
		default:
		}
		// refresh the ticket from the root (direct tickets are as-is)
		if direct == nil && t.tickets != nil {
			tk, err := t.tickets.get(a.Name)
			if err != nil {
				if !rootErrLogged {
					fmt.Fprintf(os.Stderr, "[%s] TICKET ROOT "+
						"UNREADABLE: %s: %v\n", a.Name, t.root, err)
					rootErrLogged = true
				}
				a.state.Store("root-unreachable")
				if !wait(ticketPollInterval) {
					return
				}
				continue
			}
			if rootErrLogged {
				fmt.Fprintf(os.Stderr, "[%s] ticket root readable "+
					"again: %s\n", a.Name, t.root)
				rootErrLogged = false
			}
			cur = tk // nil = publisher closed/absent
		}
		if cur == nil {
			a.state.Store("no-ticket")
			if !wait(ticketPollInterval) {
				return
			}
			continue
		}
		if direct == nil && cur.Ts > 0 &&
			float64(time.Now().UnixNano())/1e9-cur.Ts > cur.staleAfter() {
			a.state.Store("waiting")
			if !wait(ticketPollInterval) {
				return
			}
			continue
		}
		// A new run: the publisher respawned; its seq axis
		// restarted. Resync on a fresh ring (the core rightly refuses
		// the regression). The old ring closes so stale holders fail
		// loudly rather than read a dead run.
		if haveRun && cur.Run != run {
			fmt.Fprintf(os.Stderr, "[%s] PUBLISHER RESTARTED: the "+
				"ticket at %s:%d names run %s, this reader streamed "+
				"run %s; resyncing on a fresh ring (the previous run's "+
				"rows are discarded here; the archive keeps them)\n",
				a.Name, cur.Host, cur.Port, cur.Run, run)
			a.mu.Lock()
			if a.arch != nil && a.arch.recording {
				if err := a.arch.stop(); err != nil {
					fmt.Fprintf(os.Stderr, "[%s] run resync: "+
						"finalize of the previous run's recording "+
						"failed: %v\n", a.Name, err)
				}
			}
			if a.ring != nil {
				a.ring.Close()
			}
			a.ring, a.arch = nil, nil
			a.mu.Unlock()
		}
		run, haveRun = cur.Run, true

		a.state.Store("connecting")
		err := a.receive(cur)
		select {
		case <-a.closed:
			a.state.Store("closed")
			return
		default:
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "[%s] feeder: %v\n", a.Name, err)
		}
		a.state.Store("reconnecting")
		if !wait(backoff) {
			return
		}
		if backoff *= 2; backoff > 10*time.Second {
			backoff = 10 * time.Second
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
	a.lastTicket.Store(tk)
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
		a.ring = r
		if a.archOpt != nil {
			arch, err := newArchiver(r, a.Name, tk.Run, *a.archOpt)
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
	if a.arch != nil && a.arch.recording {
		err = a.arch.stop()
	} else if a.arch != nil {
		err = a.arch.rotate()
	}
	if a.ring != nil {
		a.ring.Close()
		a.ring = nil
	}
	return err
}
