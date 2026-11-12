package streaming

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/neofactory/streaming-parquet/arrow_ring/go/ring"
	"github.com/neofactory/streaming-parquet/parquet_checkpoint/go/pqc"
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
type archiver struct {
	ring      *ring.Ring
	schema    *arrow.Schema
	name      string
	run       string // the writer's run: every file records it as sp_run
	location  string // where the files land: the local directory, or the sink's URI
	sink      pqc.Sink
	recording bool
	label     string
	lost      int64 // rows the ring reused before they were written

	// The writes owed, oldest first: each a row range, pinned when it
	// became due. plan adds to it; write does its head.
	queue   []*work
	planned int64 // first seq no queued or finished write covers

	rowGroupRows    int64
	rowGroupSeconds float64
	ckptSeconds     float64
	compression     compress.Compression
	dictionary      []string // columns written with a dictionary; every other column has none

	w          *pqc.Writer
	fileSchema *arrow.Schema
	fileName   string
	cursor     int64 // first seq not yet written as a final row group
	durable    int64 // first seq not yet in the archive: rows below it are in a chunk or the checkpoint
	openMono   time.Time
	ckptMono   time.Time
	hasClock   bool
	hasCkptCl  bool
}

// ParquetOptions says how a stream's rows are written to parquet.
type ParquetOptions struct {
	BaseDir           string   // parquet lands in <BaseDir>/parquet
	Sink              pqc.Sink // where chunks, the checkpoint and finished files live; nil = a directory sink under BaseDir
	RowGroupRows      int64    // rows per row group; see ringSizes for the default
	RowGroupSeconds   float64  // also flush a partial group after this
	CheckpointSeconds float64  // tail-checkpoint cadence between flushes
	Compression       string   // "zstd" (default, level 9) | "snappy" | "none"
	Dictionary        []string // top-level columns to dictionary-encode; default none
}

// ArchiveOptions is the old name of ParquetOptions.
//
// Deprecated: use ParquetOptions. Removed on or after 2027-02-11
// (docs/deprecations.md).
type ArchiveOptions = ParquetOptions

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
func newArchiver(r *ring.Ring, name, run string, o ParquetOptions) (*archiver, error) {
	// The layout, the same under a directory and under a bucket:
	// <root>/streams/<name>/<files>, the ticket at <root>/tickets/<name>.json.
	var sink pqc.Sink
	var location string
	if o.Sink == nil {
		ds, err := pqc.NewDirSink(filepath.Join(o.BaseDir, "streams", filepath.FromSlash(name)))
		if err != nil {
			return nil, err
		}
		sink, location = ds, ds.URI()
	} else {
		sink = o.Sink.Sub("streams/" + name)
		location = sink.URI()
	}
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
	a := &archiver{
		ring:            r,
		schema:          r.Schema(),
		name:            name,
		run:             run,
		location:        location,
		sink:            sink,
		rowGroupRows:    o.RowGroupRows,
		rowGroupSeconds: o.RowGroupSeconds,
		ckptSeconds:     o.CheckpointSeconds,
		compression:     comp,
		dictionary:      o.Dictionary,
	}
	if err := a.bootSweep(); err != nil {
		return nil, err
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

func (a *archiver) bootSweep() error {
	names, err := a.sink.List("")
	if err != nil {
		return err
	}
	swept := map[string]bool{}
	for _, n := range names {
		if final, _, ok := pqc.ParseCkptObject(n); ok && !swept[final] {
			swept[final] = true
			status, err := pqc.Finalize(final, a.sink)
			if err != nil {
				return fmt.Errorf("boot sweep %s: %w", final, err)
			}
			fmt.Fprintf(os.Stderr, "[%s] boot sweep: %s %s\n",
				a.name, status, a.location+"/"+final)
		}
	}
	return nil
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
	a.w = w
	a.fileName = name
	return nil
}

// work is one write the archiver owes: the rows [lo, hi), held by a
// pin from the moment the write became due.
type work struct {
	flush   bool  // a final row group; false = a checkpoint of the tail
	lo, hi  int64 // the rows; lo is the next row of the file
	dropped bool  // rows before lo were reused before they were written
	added   bool  // the core has the row group; its objects are not all stored yet
	written bool  // the row group is in the file (a failed cut is all that is left)
	rs      *ring.RecordSet
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

// plan decides whether a write is due for the rows committed so far.
// If one is, it pins those rows and adds the write to the queue. No
// I/O: the owner calls it at each commit, and when a time is due.
func (a *archiver) plan() error {
	if !a.recording {
		return nil
	}
	committed := a.ring.NewestSeq() + 1
	pending := committed - a.cursor
	if pending <= 0 {
		return nil
	}
	if !a.hasClock {
		a.openMono = time.Now()
		a.hasClock = true
	}
	due := pending >= a.rowGroupRows ||
		(a.rowGroupSeconds > 0 &&
			time.Since(a.openMono).Seconds() >= a.rowGroupSeconds)
	if due {
		return a.enqueue(true, committed)
	}
	// Every committed row is already in a checkpoint, written or
	// queued: nothing to claim, so an idle stream costs no encode and
	// no upload.
	if a.ckptSeconds > 0 && committed > a.planned {
		base := a.ckptMono
		if !a.hasCkptCl {
			base = a.openMono
		}
		if time.Since(base).Seconds() >= a.ckptSeconds {
			return a.enqueue(false, committed)
		}
	}
	return nil
}

// enqueue pins the rows [cursor, committed) and queues their write: a
// final row group (flush), or a checkpoint of the tail, which leaves
// the cursor where it is so the next row group writes the same rows.
func (a *archiver) enqueue(flush bool, committed int64) error {
	rs, err := a.ring.Pin(a.cursor, committed) // these rows and no others
	if err != nil {
		return err
	}
	w := &work{flush: flush, rs: rs}
	if rs.Lo > a.cursor {
		// rows reused before their write: loud, counted, then on from
		// what the ring still has (the Python module's policy)
		fmt.Fprintf(os.Stderr, "[%s] ARCHIVE LOSS: rows [%d,%d) recycled "+
			"before parquet flush\n", a.name, a.cursor, rs.Lo)
		a.lost += rs.Lo - a.cursor
		a.cursor = rs.Lo
		w.dropped = true
	}
	w.lo, w.hi = a.cursor, min64(committed, rs.Hi)
	if w.hi <= w.lo {
		rs.Release()
		return nil
	}
	a.queue = append(a.queue, w)
	if flush {
		a.cursor = w.hi
		a.hasClock = false
	}
	a.planned = w.hi
	a.ckptMono = time.Now()
	a.hasCkptCl = true
	return nil
}

// write does one queued write. On an error the write stays owed: the
// owner runs it again, and its pin holds the rows until it works.
func (a *archiver) write(w *work) error {
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
		a.durable = w.hi
		if a.w.FileBytes() >= FileCutBytes {
			// the file is large enough: the next row group begins a
			// new one, with the same label
			return a.rotate()
		}
		return nil
	}
	if w.dropped {
		// the objects stored of the tail hold rows it no longer has:
		// begin the tail again
		a.w.DropUnfinished()
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
	a.durable = w.hi
	return nil
}

// pop drops the queue's head, a write that is done, and its pin.
func (a *archiver) pop() {
	a.queue[0].rs.Release()
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
	if committed := a.ring.NewestSeq() + 1; committed > a.cursor {
		if err := a.enqueue(true, committed); err != nil {
			return err
		}
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
func (a *archiver) start(label string) error {
	var err error
	if a.recording {
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
	committed := a.ring.NewestSeq() + 1
	a.label = label
	a.recording = true
	a.cursor = committed
	a.planned = committed
	a.durable = committed
	a.hasClock = false
	a.hasCkptCl = false
	return nil
}

// stop writes the pending rows and finishes the file.
func (a *archiver) stop() error {
	if a.recording {
		if err := a.flushAll(); err != nil {
			return err
		}
	} else if err := a.drain(); err != nil {
		return err
	}
	a.recording = false
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
	err = a.w.Close(empty)
	a.w = nil
	a.fileName = ""
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
