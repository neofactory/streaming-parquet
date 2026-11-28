package streaming

import (
	"bytes"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/neofactory/streaming-parquet/src/go/pqc"
	"github.com/neofactory/streaming-parquet/src/go/ring"
)

// archiver is the parquet path, shared by created streams AND
// attached streams — any ring can archive; which ring you hold is not
// a privilege level. It consumes committed rows [cursor, committed)
// from the ring and writes them through a pqc.Writer (chunks + THE
// checkpoint; crash-durable, live-readable, finalized on rotate).
//
// Its work is in two parts. plan decides that a write is due and pins
// its rows; that is cheap, and a Stream does it in Commit. write does the
// write; a Stream's goroutine does that. The Attached feeder does both
// in one call (step). The owner's locks cover every call.
//
// Which rows are due, and when, is the stream core's rule (sp.h, THE
// WRITE PLAN): wp holds it and its state.
type archiver struct {
	ring     *ring.Ring
	schema   *arrow.Schema
	name     string
	run      string // the writer's run: every file records it as sp_run
	location string // where the first copy is: the local directory, or the sink's URI
	sink     pqc.Sink
	// With a spool: sink is the spool's folder, and remote the store's.
	// An upload for each file in flight copies it to the store and ends it
	// there (pqc.Upload: the core's rules). Nil: the first copy is the
	// store.
	remote        pqc.Sink
	storeLocation string // the stream's folder in the store: what the ticket says
	uploadMu      sync.Mutex
	uploads       []*pqc.Upload // the open file's, and each ended file's until it is done
	live          string        // the name of the open file, "" for none
	remoteSwept   bool          // the remote half of the start sweep runs in the first pass
	uploadSeconds float64       // the cadence of the upload's passes
	label         string
	wp            *writePlan
	store         *storeState // what the store holds, for the ticket; the owner's
	cat           *catalog    // the catalog of the finished files; nil = none is kept
	log           *slog.Logger
	lostBefore    int64 // rows lost under this stream's earlier options

	// The writes owed, oldest first: each a row range, pinned when it
	// became due. plan adds to it; write does its head.
	queue []*work

	rowGroupRows    int64
	rowGroupSeconds float64
	ckptSeconds     float64
	compression     compress.Compression
	dictionary      []string // columns written with a dictionary; every other column has none

	w          *pqc.Writer
	fileSchema *arrow.Schema
	fileName   string
	storedRow  int64 // the newest row in a written piece or a checkpoint; -1 for none
}

// recording reports whether rows are being planned into files.
func (a *archiver) recording() bool { return a.wp.on() }

// lost is the count of rows the ring reused before they were written.
func (a *archiver) lost() int64 { return a.lostBefore + a.wp.lost() }

// ParquetOptions says how a stream's rows are written to parquet. Where
// is the stream's store: the files are at <store>/streams/<name>/.
type ParquetOptions struct {
	RowGroupRows  int64   // rows per row group; see ringSizes for the default
	UploadSeconds float64 // with a spool: the cadence of the pass that copies it to the bucket; 0 = 5 s
	// CatalogCopySeconds: the interval of the catalog's dated copies,
	// catalog.<YYYYMMDD-HHMMSS>.sqlite beside it, each made only if the
	// catalog changed since the newest one. 0 = a day; negative = none.
	CatalogCopySeconds float64
	RowGroupSeconds    float64  // also flush a partial group after this
	CheckpointSeconds  float64  // tail-checkpoint cadence between flushes
	Compression        string   // "zstd" (default, level 9) | "snappy" | "none"
	Dictionary         []string // top-level columns to dictionary-encode; default none
}

// defaultUploadSeconds is the cadence of the upload's passes when the
// options give none (the Python ParquetOptions.upload_seconds default).
const defaultUploadSeconds = 5.0

// defaultRowGroupRows is DuckDB's row group, 60 of its 2,048-row
// vectors: DuckDB, the archive's main reader, reads row groups in
// parallel.
const defaultRowGroupRows = 122_880

// ringSizes settles a stream's ring rows and row group rows. The ring
// holds two row groups. The newest half holds the unfinished row group:
// its rows stay in the ring until the group is written. The older half
// is for readers: a view reads from the ring the rows the store does
// not have yet, and keeps out of the oldest quarter, which is the time
// a running query has. Given neither size, DuckDB's row group; given
// one, the other follows; given both, a ring under two row groups is
// refused.
func ringSizes(name string, ringRows, rowGroupRows int64) (int64, int64, error) {
	switch {
	case ringRows == 0 && rowGroupRows == 0:
		return 2 * defaultRowGroupRows, defaultRowGroupRows, nil
	case ringRows == 0:
		return 2 * rowGroupRows, rowGroupRows, nil
	case rowGroupRows == 0:
		return ringRows, ringRows / 2, nil
	case ringRows < 2*rowGroupRows:
		return 0, 0, fmt.Errorf("stream %s: a ring of %d rows holds less than "+
			"two row groups of %d: rows would be reused before their row group "+
			"is written", name, ringRows, rowGroupRows)
	}
	return ringRows, rowGroupRows, nil
}

// newArchiver takes o.RowGroupRows as settled by ringSizes.
// store is the owner's record of what the store holds, which outlives
// one archiver; nil makes one for this archiver alone.
//
// catFor answers the catalog of the files in a folder; nil (a reader
// that writes files) keeps none: a finished file's checkpoints are
// deleted at once, and no ticket names the file after it.
// files is where the first copy goes: the stream's folder in it is
// streams/<name>/. remote, when not nil, is the store that first copy is
// uploaded to (files is then the spool).
func newArchiver(r *ring.Ring, name, run string, o ParquetOptions, files, remote pqc.Sink, store *storeState,
	catFor func(sink pqc.Sink, folder string) *catalog, log *slog.Logger) (*archiver, error) {
	if store == nil {
		store = newStoreState(pqc.SupersededKeep)
	}
	// The layout, the same under a directory and under a bucket:
	// <root>/streams/<name>/<files>, the ticket at <root>/tickets/<name>.json.
	sink := files.Sub("streams/" + name)
	location := sink.URI()
	comp := compress.Codecs.Zstd
	switch o.Compression {
	case "", "zstd":
	case "snappy":
		comp = compress.Codecs.Snappy
	case "none", "uncompressed":
		comp = compress.Codecs.Uncompressed
	default:
		return nil, fmt.Errorf("archiver: unknown compression %q", o.Compression)
	}
	wp, err := newWritePlan(o.RowGroupRows, o.RowGroupSeconds, o.CheckpointSeconds)
	if err != nil {
		return nil, err
	}
	if o.UploadSeconds <= 0 {
		o.UploadSeconds = defaultUploadSeconds
	}
	a := &archiver{
		storedRow:       -1,
		wp:              wp,
		log:             log,
		store:           store,
		ring:            r,
		schema:          r.Schema(),
		name:            name,
		run:             run,
		location:        location,
		sink:            sink,
		storeLocation:   location,
		uploadSeconds:   o.UploadSeconds,
		rowGroupRows:    o.RowGroupRows,
		rowGroupSeconds: o.RowGroupSeconds,
		ckptSeconds:     o.CheckpointSeconds,
		compression:     comp,
		dictionary:      o.Dictionary,
	}
	storeSink := sink
	if remote != nil {
		a.remote = remote.Sub("streams/" + name)
		a.storeLocation = a.remote.URI()
		storeSink = a.remote
	}
	if catFor != nil {
		// the catalog is the store's: readers of the store read it
		a.cat = catFor(storeSink, strings.TrimSuffix(a.storeLocation, "/")+"/")
		if a.cat != nil {
			a.cat.setCopySeconds(o.CatalogCopySeconds)
		}
	}
	if err := a.bootSweep(); err != nil {
		return nil, err
	}
	if a.cat != nil {
		a.cat.poke() // the start's pass: every finished file the catalog lacks
	}
	return a, nil
}

// bootSweep finalizes checkpoints a crashed prior run left behind in
// this stream's directory.
// hasCheckpoint reports whether file name has any checkpoint version.
// FileCutBytes: a file is cut when its row groups reach this many
// bytes, whenever the host cut it last (the Python FILE_CUT_BYTES).
// Every decision of the writer is taken by time or by bytes, whichever
// comes first; the host cuts by time, and a fast stream reaches this
// first. The reason is the footer: it has an entry for each row group
// and each column, and it is written at every checkpoint.
var FileCutBytes uint64 = 256_000_000

func (a *archiver) hasCheckpoint(name string) bool {
	vs, err := pqc.CkptVersions(a.sink, name)
	return err != nil || len(vs) > 0 // an unlistable sink: assume taken
}

// keeps reports whether readers are told of the store's unfinished
// files: a finished file's versions then stay until the catalog lists it.
func (a *archiver) keeps() bool { return a.cat != nil }

// addUpload gives file name an upload. Caller does not hold uploadMu.
func (a *archiver) addUpload(name string) error {
	m, err := pqc.NewUpload(name, a.sink, a.remote, pqc.SupersededKeep)
	if err != nil {
		return err
	}
	a.uploadMu.Lock()
	a.uploads = append(a.uploads, m)
	a.uploadMu.Unlock()
	return nil
}

func (a *archiver) inFlight(name string) bool {
	a.uploadMu.Lock()
	defer a.uploadMu.Unlock()
	for _, m := range a.uploads {
		if m.Name() == name {
			return true
		}
	}
	return false
}

// pending names the files ended locally whose copy in the store is not
// finished yet.
func (a *archiver) pending() []string {
	a.uploadMu.Lock()
	defer a.uploadMu.Unlock()
	var out []string
	for _, m := range a.uploads {
		if m.Name() != a.live {
			out = append(out, m.Name())
		}
	}
	return out
}

// unfinished groups a folder's checkpoint versions by file: for each
// file that has versions, its newest version's object and number.
func unfinished(sink pqc.Sink) (files []string, newest map[string]pqc.Version, err error) {
	names, err := sink.List("")
	if err != nil {
		return nil, nil, err
	}
	newest = map[string]pqc.Version{}
	for _, n := range names {
		final, seq, ok := pqc.ParseCkptObject(n)
		if !ok {
			continue
		}
		if have, seen := newest[final]; !seen || seq > have.Seq {
			if !seen {
				files = append(files, final)
			}
			newest[final] = pqc.Version{Seq: seq, Object: n}
		}
	}
	sort.Strings(files)
	return files, newest, nil
}

// bootSweep deals with the checkpoint versions an earlier run left in
// this stream's folder of the first copy. The stream owns its name, so
// every one is a leftover. What to do with each file is the core's rule
// (pqc.h: THE SWEEP AT START).
func (a *archiver) bootSweep() error {
	files, newest, err := unfinished(a.sink)
	if err != nil {
		return err
	}
	for _, final := range files {
		v := newest[final]
		switch pqc.SweepLocal(a.sink.Exists(final), a.remote != nil, a.keeps()) {
		case pqc.SweepAwaitCatalog:
			// finished before the crash; its versions stay until the
			// catalog lists the file
			a.store.kept(final, v.Seq, v.Object)
			a.cat.await(final)
			a.log.Info(fmt.Sprintf("[%s] boot sweep: kept %s",
				a.name, a.location+"/"+final))
		case pqc.SweepDiscard: // a spool's copy: no reader is told of it
			if err := pqc.DiscardCheckpoints(final, a.sink); err != nil {
				return fmt.Errorf("boot sweep %s: %w", final, err)
			}
		case pqc.SweepUpload:
			// pending: its passes send it and end it
			if err := a.addUpload(final); err != nil {
				return fmt.Errorf("boot sweep %s: %w", final, err)
			}
			a.log.Info(fmt.Sprintf("[%s] boot sweep: %s pending its copy in the store",
				a.name, a.location+"/"+final))
		default:
			status, err := pqc.Finalize(final, a.sink)
			if err != nil {
				return fmt.Errorf("boot sweep %s: %w", final, err)
			}
			a.log.Info(fmt.Sprintf("[%s] boot sweep: %s %s",
				a.name, status, a.location+"/"+final))
		}
	}
	return nil
}

// sweepRemote is the store's half of the start sweep, in the first
// upload pass, so that a store that cannot be reached at start delays
// the upload and never the stream.
func (a *archiver) sweepRemote() error {
	files, newest, err := unfinished(a.remote)
	if err != nil {
		return err
	}
	for _, final := range files {
		v := newest[final]
		switch pqc.SweepRemote(a.remote.Exists(final), a.keeps(), a.inFlight(final)) {
		case pqc.SweepAwaitCatalog:
			a.store.kept(final, v.Seq, v.Object)
			a.cat.await(final)
		case pqc.SweepDiscard:
			if err := pqc.DiscardCheckpoints(final, a.remote); err != nil {
				return err
			}
		case pqc.SweepUpload:
			// its local file was assembled and the crash came before the
			// store's: an upload finishes it from the store's own objects
			if err := a.addUpload(final); err != nil {
				return err
			}
			a.store.kept(final, v.Seq, v.Object) // its version is in the store
			a.log.Info(fmt.Sprintf("[%s] store sweep: %s pending its assembly",
				a.name, final))
		}
	}
	return nil
}

// upload is one upload pass over every file in flight: the open file's
// newest version goes to the store; a file the writer ended is sent,
// assembled locally and then in the store. An error ends the pass; the
// next one goes on from what was done. The stream's upload goroutine
// calls it, with none of the stream's locks held: the store may be slow
// or away, and the writer is not held up.
func (a *archiver) upload() error {
	if !a.remoteSwept {
		if err := a.sweepRemote(); err != nil {
			return err
		}
		a.remoteSwept = true
	}
	a.uploadMu.Lock()
	uploads := append([]*pqc.Upload(nil), a.uploads...)
	live := a.live
	a.uploadMu.Unlock()
	for _, m := range uploads {
		name := m.Name()
		st, err := m.Run(name == live, a.keeps(), func(object string, rows uint64) {
			// the ticket's row moves at the moment the store has a version
			_, version, _ := pqc.ParseCkptObject(object)
			a.store.inStore(name, version, object, int64(rows))
		})
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if !st.Done {
			continue
		}
		a.uploadMu.Lock()
		for i, x := range a.uploads {
			if x == m {
				a.uploads = append(a.uploads[:i], a.uploads[i+1:]...)
				break
			}
		}
		a.uploadMu.Unlock()
		last := m.LastObject()
		m.Free()
		if a.keeps() {
			if last == "" {
				if vs, err := pqc.CkptVersions(a.remote, name); err == nil && len(vs) > 0 {
					last = vs[len(vs)-1].Object
				}
			}
			if last != "" {
				_, version, _ := pqc.ParseCkptObject(last)
				a.store.kept(name, version, last)
			}
			a.cat.await(name)
		} else {
			a.store.finished(name)
		}
		a.log.Info(fmt.Sprintf("[%s] finished in the store: %s", a.name, name))
	}
	if a.cat != nil {
		// a step that writes to the store: a failed catalog pass tries
		// again, and versions kept long enough are deleted
		a.cat.poke()
		a.cat.discardDue()
	}
	return nil
}

// uploading says what an upload is doing in the store right now, "" for
// nothing.
func (a *archiver) uploading() string {
	a.uploadMu.Lock()
	defer a.uploadMu.Unlock()
	for _, m := range a.uploads {
		if d := m.Doing(); d != "" {
			return d
		}
	}
	return ""
}

// encodeMini encodes records as one complete mini parquet file.
func (a *archiver) encodeMini(recs []arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	schema := a.fileSchema
	if schema == nil {
		schema = a.schema
	}
	props := parquet.NewWriterProperties(writerProps(schema, a.compression, a.dictionary)...)
	fw, err := pqarrow.NewFileWriter(schema, &buf, props,
		pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	for _, rec := range recs {
		if err := fw.WriteBuffered(rec); err != nil {
			fw.Close()
			return nil, err
		}
	}
	if err := fw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (a *archiver) open(baseSeq int64) error {
	stamp := time.Now().Format("20060102-150405")
	suffix := ""
	if a.label != "" {
		suffix = "-" + a.label
	}
	base := stamp + suffix
	name := base + ".parquet"
	for n := 2; a.sink.Exists(name) || a.hasCheckpoint(name); n++ {
		name = fmt.Sprintf("%s-%d.parquet", base, n)
	}
	w, err := pqc.NewWriter(name, a.sink, 5<<20, true)
	if err != nil {
		return err
	}
	// the file records the stream seq of its first row: any file row's
	// seq = base + file_row_number (the complete-relation seam); and the
	// run those seqs belong to, since the seq axis restarts every run
	md := arrow.NewMetadata([]string{"sp_base_seq", "sp_run"},
		[]string{fmt.Sprintf("%d", baseSeq), a.run})
	a.fileSchema = arrow.NewSchema(a.schema.Fields(), &md)
	if a.remote != nil {
		if err := a.addUpload(name); err != nil {
			w.Free()
			return err
		}
	}
	a.w = w
	a.fileName = name
	a.uploadMu.Lock()
	a.live = name
	a.uploadMu.Unlock()
	a.store.begins(name, baseSeq)
	return nil
}

// noteStored tells the store's record that the writer's newest
// checkpoint version is written. The sink is the store: a version that
// is written is there.
func (a *archiver) noteStored() {
	ck := a.w.CkptName()
	if ck == "" {
		return
	}
	_, version, _ := pqc.ParseCkptObject(ck)
	if a.remote != nil {
		// written in the spool: the store has it when the upload sends it
		a.store.covers(a.fileName, version, ck, int64(a.w.RowsFinal()),
			int64(a.w.RowsCheckpointed()))
		return
	}
	a.store.stored(a.fileName, version, ck, int64(a.w.RowsFinal()),
		int64(a.w.RowsCheckpointed()))
	if a.cat != nil {
		// a step that writes to the store: a failed pass tries again,
		// and checkpoints kept long enough are deleted
		a.cat.poke()
		a.cat.discardDue()
	}
}

// work is one write the archiver owes: the rows [lo, hi), held by a
// pin from the moment the write became due. Or a cut: the file ends.
type work struct {
	flush    bool  // a final row group; false = a checkpoint of the tail
	lo, hi   int64 // the rows; lo is the next row of the file
	added    bool  // the core has the row group; its objects are not all stored yet
	written  bool  // the row group is in the file (a failed cut is all that is left)
	cut      bool  // no rows: the open file ends, the next row begins a new one
	setLabel bool  // a cut that changes the label, to label
	label    string
	rs       *ring.RecordSet
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// plan asks the write plan whether a write is due for the rows
// committed so far. If one is, it pins those rows and adds the write to
// the queue. No I/O: the owner calls it at each commit, and when a time
// is due.
func (a *archiver) plan() error {
	committed := a.ring.NewestSeq() + 1
	if a.wp.commit(committed) == writeNothing {
		return nil
	}
	return a.enqueue(false, committed)
}

// enqueue pins the rows [cursor, committed) and queues the write the
// plan names for them: a final row group, or a checkpoint of the tail,
// which leaves the cursor where it is so the next row group writes the
// same rows. force: a row group whatever is due.
func (a *archiver) enqueue(force bool, committed int64) error {
	cursor := a.wp.cursor()
	if committed <= cursor {
		return nil
	}
	rs, err := a.ring.Pin(cursor, committed) // these rows and no others
	if err != nil {
		return err
	}
	kind, lo, hi, lost := a.wp.plan(force, rs.Lo, min64(committed, rs.Hi))
	if lost > 0 {
		// rows reused before their write: loud, counted, then on from
		// what the ring still has
		a.log.Error(fmt.Sprintf("[%s] ARCHIVE LOSS: rows [%d,%d) recycled "+
			"before parquet flush", a.name, cursor, cursor+lost))
	}
	if kind == writeNothing {
		rs.Release()
		return nil
	}
	a.queue = append(a.queue, &work{flush: kind == writeRowGroup, lo: lo, hi: hi, rs: rs})
	return nil
}

// cut queues the end of the open file: every row committed so far goes
// into it as a final row group, and the next committed row begins a new
// file. No I/O.
func (a *archiver) cut(setLabel bool, label string) error {
	if !a.wp.on() {
		return nil
	}
	if err := a.enqueue(true, a.ring.NewestSeq()+1); err != nil {
		return err
	}
	a.queue = append(a.queue, &work{cut: true, setLabel: setLabel, label: label})
	return nil
}

// write does one queued write. On an error the write stays owed: the
// owner runs it again, and its pin holds the rows until it works.
func (a *archiver) write(w *work) error {
	if w.cut {
		if err := a.rotate(); err != nil {
			return err
		}
		if w.setLabel {
			a.label = w.label
		}
		return nil
	}
	if w.written {
		return a.rotate() // the cut that failed after the row group went in
	}
	recs, err := w.rs.Records()
	if err != nil {
		return err
	}
	var rows []arrow.RecordBatch // the batches cut down to [lo, hi)
	pos := w.rs.Lo
	for _, rec := range recs {
		n := rec.NumRows()
		lo0, hi0 := max64(w.lo-pos, 0), min64(w.hi-pos, n)
		if lo0 < hi0 {
			rows = append(rows, rec.NewSlice(lo0, hi0))
		}
		rec.Release()
		pos += n
	}
	defer func() {
		for _, r := range rows {
			r.Release()
		}
	}()
	if a.w == nil {
		if err := a.open(w.lo); err != nil {
			return err
		}
	}
	if w.flush {
		if w.added {
			// the core has the row group; a store that failed left its
			// objects owed. Store them, and the checkpoint over them.
			if _, err := a.w.Checkpoint(); err != nil {
				return err
			}
		} else {
			mini, err := a.encodeMini(rows)
			if err != nil {
				return err
			}
			final := a.w.RowsFinal()
			if _, err := a.w.AddGroupMini(mini); err != nil {
				// the core takes the group before the store is tried:
				// a second add would put the rows in the file twice
				w.added = a.w.RowsFinal() > final
				return err
			}
		}
		w.written = true
		a.storedRow = w.hi - 1
		a.noteStored()
		if a.w.FileBytes() >= FileCutBytes {
			// the file is large enough: the next row group begins a
			// new one, with the same label
			return a.rotate()
		}
		return nil
	}
	// The writer asks for the rows no object holds yet, or for a merge:
	// rows [offset, offset+rows) of the tail.
	_, err = a.w.CheckpointTail(uint64(w.hi-w.lo), func(offset, n uint64) ([]byte, error) {
		span := sliceRecords(rows, int64(offset), int64(n))
		defer func() {
			for _, r := range span {
				r.Release()
			}
		}()
		return a.encodeMini(span)
	})
	if err != nil {
		return err
	}
	a.storedRow = w.hi - 1
	a.noteStored()
	return nil
}

// pop drops the queue's head, a write that is done, and its pin.
func (a *archiver) pop() {
	if rs := a.queue[0].rs; rs != nil {
		rs.Release()
	}
	a.queue = a.queue[1:]
}

// drain runs the queue to its end, or to the first write that fails.
func (a *archiver) drain() error {
	for len(a.queue) > 0 {
		if err := a.write(a.queue[0]); err != nil {
			return err
		}
		a.pop()
	}
	return nil
}

// abandon releases the pins of the writes still owed: a stream that
// closes while its store is failing.
func (a *archiver) abandon() {
	for len(a.queue) > 0 {
		a.pop()
	}
}

// step plans and writes in one call, for an owner that writes on its
// own goroutine (the Attached feeder).
func (a *archiver) step() error {
	if err := a.plan(); err != nil {
		return err
	}
	return a.drain()
}

// flushAll writes every committed row as final row groups, now.
func (a *archiver) flushAll() error {
	if err := a.drain(); err != nil {
		return err
	}
	if err := a.enqueue(true, a.ring.NewestSeq()+1); err != nil {
		return err
	}
	return a.drain()
}

// sliceRecords returns rows [offset, offset+rows) of a list of record
// batches, as slices the caller releases.
func sliceRecords(recs []arrow.RecordBatch, offset, rows int64) []arrow.RecordBatch {
	var out []arrow.RecordBatch
	end := offset + rows
	var pos int64
	for _, rec := range recs {
		n := rec.NumRows()
		lo, hi := max64(offset-pos, 0), min64(end-pos, n)
		if lo < hi {
			out = append(out, rec.NewSlice(lo, hi))
		}
		pos += n
	}
	return out
}

// start is the mark of a new file: rows committed from THIS moment land
// in a fresh file labeled `label`. A file that is open ends here, with
// every row committed before the mark in it.
func (a *archiver) start(label string) error { return a.startAt(label, -1) }

// startAt is start with the mark at row number mark: for a ring whose
// first row is still to come (a reader's). A negative mark is after the
// newest committed row.
func (a *archiver) startAt(label string, mark int64) error {
	var err error
	if a.wp.on() {
		err = a.flushAll()
	} else {
		err = a.drain()
	}
	if err != nil {
		return err
	}
	if err := a.rotate(); err != nil {
		return err
	}
	a.label = label
	if !a.wp.on() {
		// off until now: the rows before the mark are not planned
		if mark < 0 {
			mark = a.ring.NewestSeq() + 1
		}
		a.wp.start(mark)
	}
	return nil
}

// stop writes the pending rows and finishes the file.
func (a *archiver) stop() error {
	if a.wp.on() {
		if err := a.flushAll(); err != nil {
			return err
		}
	} else if err := a.drain(); err != nil {
		return err
	}
	a.wp.stop()
	return a.rotate()
}

func (a *archiver) rotate() error {
	if a.w == nil {
		return nil
	}
	empty, err := a.encodeMini(nil)
	if err != nil {
		return err
	}
	if a.remote != nil {
		// With an upload the file is ended, not assembled: its last
		// checkpoint here, and the upload's passes do the rest in the
		// core's order. While the store cannot be reached the file
		// waits, readable and crash-durable in the spool.
		ck, final := "", int64(a.w.RowsFinal())
		if err = a.w.CheckpointFinal(empty); err == nil {
			if vs, verr := pqc.CkptVersions(a.sink, a.fileName); verr == nil && len(vs) > 0 {
				ck = vs[len(vs)-1].Object
				a.store.covers(a.fileName, vs[len(vs)-1].Seq, ck, final, final)
			}
		}
	} else if a.cat != nil {
		// finished with its checkpoints kept: the file stays in the
		// ticket until the catalog lists it
		rows := int64(a.w.RowsFinal())
		if err = a.w.CloseKeep(empty); err == nil {
			// the ticket names the last version, the one the file was made from
			if vs, verr := pqc.CkptVersions(a.sink, a.fileName); verr == nil && len(vs) > 0 {
				obj := vs[len(vs)-1].Object
				_, version, _ := pqc.ParseCkptObject(obj)
				a.store.stored(a.fileName, version, obj, rows, rows)
			}
			a.cat.await(a.fileName)
		} else {
			a.store.finished(a.fileName) // left for the next start's sweep
		}
	} else {
		err = a.w.Close(empty)
		// finished in the store, or (on an error) left for the next start's
		// sweep: no ticket of this run names it again
		a.store.finished(a.fileName)
	}
	a.w = nil
	a.fileName = ""
	a.uploadMu.Lock()
	a.live = ""
	a.uploadMu.Unlock()
	return err
}

// writerProps: the parquet writer's defaults, the same as the Python
// writer's (_parquet_writer.default_encodings): zstd level 9;
// DELTA_BINARY_PACKED on every integer leaf, BYTE_STREAM_SPLIT on every
// float32/float64 leaf; dictionary off except the named top-level
// columns, which drop their default encoding.
func writerProps(schema *arrow.Schema, comp compress.Compression, dictionary []string) []parquet.WriterProperty {
	props := []parquet.WriterProperty{
		parquet.WithCompression(comp),
		parquet.WithDictionaryDefault(false),
	}
	if comp == compress.Codecs.Zstd {
		props = append(props, parquet.WithCompressionLevel(9))
	}
	dict := map[string]bool{}
	for _, name := range dictionary {
		dict[name] = true
	}
	enc := map[string]parquet.Encoding{}
	for _, f := range schema.Fields() {
		if dict[f.Name] {
			continue
		}
		defaultEncodings(f.Name, f.Type, enc)
	}
	for path, e := range enc {
		props = append(props, parquet.WithEncodingFor(path, e))
	}
	for name := range dict {
		props = append(props, parquet.WithDictionaryFor(name, true))
	}
	return props
}

// defaultEncodings adds the default encoding of every integer and float
// leaf under path: struct leaves are parent.child, list elements
// name.list.element.
func defaultEncodings(path string, t arrow.DataType, out map[string]parquet.Encoding) {
	switch dt := t.(type) {
	case *arrow.StructType:
		for _, f := range dt.Fields() {
			defaultEncodings(path+"."+f.Name, f.Type, out)
		}
	case *arrow.ListType:
		defaultEncodings(path+".list.element", dt.Elem(), out)
	case *arrow.LargeListType:
		defaultEncodings(path+".list.element", dt.Elem(), out)
	case *arrow.FixedSizeListType:
		defaultEncodings(path+".list.element", dt.Elem(), out)
	default:
		switch {
		case arrow.IsInteger(t.ID()):
			out[path] = parquet.Encodings.DeltaBinaryPacked
		case t.ID() == arrow.FLOAT32 || t.ID() == arrow.FLOAT64:
			out[path] = parquet.Encodings.ByteStreamSplit
		}
	}
}
