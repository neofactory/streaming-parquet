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
// Driven by its owner's loop: the Stream's Publish() or the Attached
// feeder after each ingest. All methods are called from that one
// goroutine.
type archiver struct {
	ring      *ring.Ring
	schema    *arrow.Schema
	name      string
	run       string // the writer's run: every file records it as sp_run
	location  string // where the files land: the local directory, or the sink's URI
	sink      pqc.Sink
	recording bool
	label     string

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

type ArchiveOptions struct {
	BaseDir           string   // parquet lands in <BaseDir>/parquet
	Sink              pqc.Sink // where chunks, the checkpoint and finished files live; nil = a directory sink under BaseDir
	RowGroupRows      int64    // rows per row group; see ringSizes for the default
	RowGroupSeconds   float64  // also flush a partial group after this
	CheckpointSeconds float64  // tail-checkpoint cadence between flushes
	Compression       string   // "zstd" (default, level 9) | "snappy" | "none"
	Dictionary        []string // top-level columns to dictionary-encode; default none
}

// defaultRowGroupRows is DuckDB's row group, 60 of its 2,048-row
// vectors: DuckDB, the archive's main reader, reads row groups in
// parallel.
const defaultRowGroupRows = 122_880

// ringSizes settles a stream's ring rows and row group rows. A row
// group's rows stay in the ring until the group is written, and the
// archiver's read leaves out the rows the writer may reuse meanwhile
// (2.5 s at the ring's peak rate), so the ring holds two row groups:
// given neither size, DuckDB's row group; given one, the other follows;
// given both, a ring under two row groups is refused.
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
func newArchiver(r *ring.Ring, name, run string, o ArchiveOptions) (*archiver, error) {
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

// region materializes ring rows [lo, hi) as records (retained).
func (a *archiver) region(lo, hi int64) ([]arrow.RecordBatch, func(), error) {
	rs, err := a.ring.Pin(lo, hi) // the pending region and no other rows
	if err != nil {
		return nil, nil, err
	}
	if rs.Lo > lo {
		// rows recycled before archival: loud, then continue from
		// what survives (same policy as the Python module)
		fmt.Fprintf(os.Stderr, "[%s] ARCHIVE LOSS: rows [%d,%d) recycled "+
			"before parquet flush\n", a.name, lo, rs.Lo)
		lo = rs.Lo
		a.cursor = lo
	}
	if hi > rs.Hi {
		hi = rs.Hi
	}
	recs, err := rs.Records()
	if err != nil {
		rs.Release()
		return nil, nil, err
	}
	// slice the batch list down to [lo, hi)
	var out []arrow.RecordBatch
	pos := rs.Lo
	for _, rec := range recs {
		n := rec.NumRows()
		lo0, hi0 := max64(lo-pos, 0), min64(hi-pos, n)
		if lo0 < hi0 {
			s := rec.NewSlice(lo0, hi0)
			out = append(out, s)
		}
		rec.Release()
		pos += n
	}
	cleanup := func() {
		for _, r := range out {
			r.Release()
		}
		rs.Release()
	}
	return out, cleanup, nil
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

// step advances the archiver: flush a row group when due, else a tail
// checkpoint when due. Call from the owner's loop after commits.
func (a *archiver) step() error {
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
		return a.flushRegion(committed)
	}
	// Every committed row is already in the last checkpoint: nothing to
	// publish, so an idle stream costs no encode and no upload.
	if a.ckptSeconds > 0 && committed > a.durable {
		base := a.ckptMono
		if !a.hasCkptCl {
			base = a.openMono
		}
		if time.Since(base).Seconds() >= a.ckptSeconds {
			return a.checkpointTail(committed)
		}
	}
	return nil
}

func (a *archiver) flushRegion(committed int64) error {
	recs, cleanup, err := a.region(a.cursor, committed)
	if err != nil {
		return err
	}
	defer cleanup()
	if len(recs) == 0 {
		a.cursor = committed
		return nil
	}
	if a.w == nil {
		if err := a.open(a.cursor); err != nil {
			return err
		}
	}
	mini, err := a.encodeMini(recs)
	if err != nil {
		return err
	}
	if _, err := a.w.AddGroupMini(mini); err != nil {
		return err
	}
	a.cursor = committed
	a.durable = committed
	a.hasClock = false
	a.ckptMono = time.Now()
	a.hasCkptCl = true
	return nil
}

func (a *archiver) checkpointTail(committed int64) error {
	// the whole tail, pinned once: every row since the last row group
	cursor := a.cursor
	recs, cleanup, err := a.region(a.cursor, committed)
	if err != nil {
		return err
	}
	defer cleanup()
	if len(recs) == 0 {
		return nil
	}
	if a.w == nil {
		if err := a.open(a.cursor); err != nil {
			return err
		}
	}
	if a.cursor != cursor {
		// rows at the front of the tail were recycled (region said
		// so): the objects stored of the tail hold rows it no longer
		// has. Begin the tail again.
		a.w.DropUnfinished()
	}
	// The writer asks for the rows no object holds yet, or for a merge:
	// rows [offset, offset+rows) of the tail.
	_, err = a.w.CheckpointTail(uint64(committed-a.cursor), func(offset, rows uint64) ([]byte, error) {
		span := sliceRecords(recs, int64(offset), int64(rows))
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
	a.durable = committed
	a.ckptMono = time.Now()
	a.hasCkptCl = true
	return nil
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

// StartParquet begins a recording segment: rows committed from THIS
// moment land in a fresh file labeled `label`.
func (a *archiver) start(label string) error {
	if err := a.rotate(); err != nil {
		return err
	}
	a.label = label
	a.recording = true
	a.cursor = a.ring.NewestSeq() + 1
	a.durable = a.cursor
	a.hasClock = false
	a.hasCkptCl = false
	return nil
}

// stop flushes the pending region and finalizes the file.
func (a *archiver) stop() error {
	if a.recording {
		if err := a.flushRegion(a.ring.NewestSeq() + 1); err != nil {
			return err
		}
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
