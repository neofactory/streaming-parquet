// Package ring is the Go host for the arrow_ring C core.
//
// The core (src/core/arrow_ring.h — the spec) owns storage, recycling,
// and pins; this binding is thin by design. Boundary language
// is the Arrow C Data Interface: schemas and batches go in via
// arrow-go's cdata export, record sets come out as zero-copy record
// batches via cdata import.
//
// arrow_ring_core.c and arrow_ring.h are CHECKED-IN COPIES of
// ../../core/arrow_ring.{c,h} (in-package so the Go build cache tracks
// them); run_go_compat.py refreshes them and refuses to pass while
// they drift from the core.
//
// Threading contract (from the core): ONE writer goroutine per ring;
// any number of reader goroutines.
//
// How the core reports is src/core/report.h: every call that can fail,
// or that can have a line for the log, is given a Report. Its status
// and reason become an *Error; its lines go to the ring's logger
// (Options.Logger). The core writes nowhere and never waits: RowBegin
// does the waiting for a pin here.
package ring

/*
#cgo CFLAGS: -std=c11 -O2
#cgo linux CFLAGS: -D_POSIX_C_SOURCE=200809L
#cgo LDFLAGS: -lpthread
#include <stdlib.h>
#include "arrow_ring.h"

// The leaf buffer addresses as pointers (the header answers integers
// for the Python binding); the memory is the core's, outside Go's heap.
static void *leaf_data_ptr(const ArRing *r, int32_t leaf) {
	return (void *)(uintptr_t)ar_leaf_data_address(r, leaf);
}
static void *leaf_valid_ptr(const ArRing *r, int32_t leaf) {
	return (void *)(uintptr_t)ar_leaf_valid_address(r, leaf);
}
*/
import "C"

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

// Error carries the core's status name and its loud detail message.
type Error struct {
	Status string
	Detail string
}

func (e *Error) Error() string { return e.Status + ": " + e.Detail }

// The names of the statuses of report.h that the ring gives.
var statusNames = map[int]string{
	int(C.REPORT_INVALID):      "invalid",
	int(C.REPORT_REFUSED):      "refused",
	int(C.REPORT_UNSUPPORTED):  "type-unsupported",
	int(C.REPORT_STALE):        "stale",
	int(C.REPORT_BLOCKED):      "pin-stalled",
	int(C.REPORT_NOT_NULLABLE): "not-nullable",
	int(C.REPORT_WIDTH):        "width",
	int(C.REPORT_OVERFLOW):     "offset-overflow",
	int(C.REPORT_NO_MEMORY):    "nomem",
}

// A reader's call takes a Report from this list and gives it back: C
// memory, so the garbage collector neither moves nor counts it. (The
// writer's calls use the ring's own.)
var freeReports = make(chan *C.Report, 64)

func getReport() *C.Report {
	select {
	case rep := <-freeReports:
		return rep
	default:
		return (*C.Report)(C.malloc(C.sizeof_Report))
	}
}

func putReport(rep *C.Report) {
	select {
	case freeReports <- rep:
	default:
		C.free(unsafe.Pointer(rep))
	}
}

// coreErr is the error of a call that failed: its status name and the
// reason in its report.
func coreErr(rep *C.Report) error {
	name := statusNames[int(rep.status)]
	if name == "" {
		name = fmt.Sprintf("status-%d", int(rep.status))
	}
	return &Error{Status: name, Detail: C.GoString(&rep.error[0])}
}

// readErr is coreErr for a reader's call: the report goes back to the
// list.
func readErr(rep *C.Report, st C.int) error {
	defer putReport(rep)
	if st == C.REPORT_OK {
		return nil
	}
	return coreErr(rep)
}

// Line is a line the core made for the log: its level, when the core
// made it, and its text.
type Line struct {
	Level slog.Level
	Time  time.Time
	Text  string
}

// IsStale reports whether err is the core's stale-read refusal (the
// seq was recycled or is not yet committed) — the loud signal a
// reader fell behind.
func IsStale(err error) bool {
	e, ok := err.(*Error)
	return ok && e.Status == "stale"
}

// IsPinStalled reports whether err is the writer-stall refusal (an
// overstayed record set pin blocks recycling).
func IsPinStalled(err error) bool {
	e, ok := err.(*Error)
	return ok && e.Status == "pin-stalled"
}

// Ring wraps one ArRing.
type Ring struct {
	c        *C.ArRing
	wr       *C.Report // the writer's Report: one writer goroutine per ring
	schema   *arrow.Schema
	leaves   map[string]int32
	info     []*leafInfo // by leaf index: the direct-store shape (direct.go)
	capacity int64
	slot     int64 // the open row's slot, from RowBegin

	log  *slog.Logger
	hold bool
	mu   sync.Mutex // covers held and lost
	held []Line
	lost int
}

// mostLines is how many lines a ring keeps for LogLines.
const mostLines = 256

// Options mirror ArRingOptions; zero values take the core's defaults.
type Options struct {
	SegmentBytes int64 // var-width pool segment size; default 1<<20

	// Logger gets the lines the core makes for the log (an oversized
	// value, a jump in the row numbers), as warnings, each with the
	// time the core made it. nil: slog.Default().
	Logger *slog.Logger

	// HoldLines false: a line is logged at once, by the goroutine whose
	// call made it. true: the lines are kept until LogLines, for a host
	// whose writer goroutine must do no output and which has another
	// goroutine to call LogLines from.
	HoldLines bool
}

// said takes the lines of a writer's call from its report.
func (r *Ring) said() {
	rep := r.wr
	if rep.lines == 0 && rep.dropped == 0 {
		return
	}
	for i := 0; i < int(rep.lines); i++ {
		l := &rep.line[i]
		level := slog.LevelInfo
		switch l.level {
		case C.REPORT_WARNING:
			level = slog.LevelWarn
		case C.REPORT_ERROR:
			level = slog.LevelError
		}
		line := Line{level, time.Unix(0, int64(float64(l.wall)*1e9)), C.GoString(&l.text[0])}
		if !r.hold {
			r.logLine(line)
			continue
		}
		r.mu.Lock()
		if len(r.held) < mostLines {
			r.held = append(r.held, line)
		} else {
			r.lost++
		}
		r.mu.Unlock()
	}
	if rep.dropped > 0 {
		r.mu.Lock()
		r.lost += int(rep.dropped)
		r.mu.Unlock()
	}
}

func (r *Ring) logLine(l Line) {
	ctx := context.Background()
	if !r.log.Enabled(ctx, l.Level) {
		return
	}
	r.log.Handler().Handle(ctx, slog.NewRecord(l.Time, l.Level, l.Text, 0))
}

// LinesHeld reports whether lines are kept and wait for LogLines.
func (r *Ring) LinesHeld() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.held) > 0 || r.lost > 0
}

// TakeLines returns the kept lines, oldest first; they are no longer
// kept.
func (r *Ring) TakeLines() []Line {
	r.mu.Lock()
	out, lost := r.held, r.lost
	r.held, r.lost = nil, 0
	r.mu.Unlock()
	if lost > 0 {
		out = append(out, Line{slog.LevelWarn, time.Now(), fmt.Sprintf(
			"[arrow_ring] %d more lines of the core were made and not kept", lost)})
	}
	return out
}

// LogLines logs the kept lines to the ring's logger, each with the
// time the core made it. Any goroutine but the writer's.
func (r *Ring) LogLines() {
	for _, l := range r.TakeLines() {
		r.logLine(l)
	}
}

// werr is the error of a writer's call that failed.
func (r *Ring) werr() error { return coreErr(r.wr) }

// NewRing creates a ring for the schema (see the header for the type
// surface). Capacity is rows — the total loss-free budget for every
// consumer; size it 2x the working need.
func NewRing(schema *arrow.Schema, capacity int64, opts *Options) (*Ring, error) {
	var cs cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cs)
	var co C.ArRingOptions
	log, hold := slog.Default(), false
	if opts != nil {
		co.segment_bytes = C.int64_t(opts.SegmentBytes)
		hold = opts.HoldLines
		if opts.Logger != nil {
			log = opts.Logger
		}
	}
	var cr *C.ArRing
	wr := (*C.Report)(C.malloc(C.sizeof_Report))
	st := C.ar_ring_new((*C.struct_ArrowSchema)(unsafe.Pointer(&cs)),
		C.int64_t(capacity), &co, &cr, wr)
	if st != C.REPORT_OK {
		err := coreErr(wr)
		C.free(unsafe.Pointer(wr))
		return nil, err
	}
	r := &Ring{c: cr, wr: wr, schema: schema, leaves: map[string]int32{},
		capacity: capacity, log: log, hold: hold}
	byPath := map[string]*leafInfo{}
	if err := flattenLeaves(schema.Fields(), "", byPath); err != nil {
		C.ar_ring_free(cr)
		return nil, err
	}
	n := int32(C.ar_leaf_count(cr))
	r.info = make([]*leafInfo, n)
	for i := int32(0); i < n; i++ {
		path := C.GoString(C.ar_leaf_path(cr, C.int32_t(i)))
		r.leaves[path] = i
		li := byPath[path]
		if li == nil {
			C.ar_ring_free(cr)
			return nil, fmt.Errorf("ring: core leaf %q has no schema leaf", path)
		}
		if li.kind != kindVar {
			li.data = C.leaf_data_ptr(cr, C.int32_t(i))
		}
		if li.nullable {
			li.valid = C.leaf_valid_ptr(cr, C.int32_t(i))
		}
		r.info[i] = li
	}
	return r, nil
}

// Close frees the ring. Outstanding record sets keep their buffers
// alive via refcounts.
func (r *Ring) Close() {
	if r.c != nil {
		C.ar_ring_free(r.c)
		r.c = nil
		C.free(unsafe.Pointer(r.wr))
		r.wr = nil
	}
}

func (r *Ring) Schema() *arrow.Schema { return r.schema }

// Leaf resolves a flattened dotted path ("tv", "servo.pos") to its
// leaf index, once at startup.
func (r *Ring) Leaf(path string) (int32, error) {
	if i, ok := r.leaves[path]; ok {
		return i, nil
	}
	return -1, fmt.Errorf("ring: no leaf %q", path)
}

// LeafPaths lists every flattened leaf path in leaf-index order.
func (r *Ring) LeafPaths() []string {
	out := make([]string, len(r.leaves))
	for p, i := range r.leaves {
		out[i] = p
	}
	return out
}

// ---- write side (single writer goroutine) --------------------------------

// RowBegin allocates the next slot; block bounds the wait on a pinned
// recycle target (IsPinStalled on timeout, with the pin named in the
// detail). The waiting is done here: the core answers at once.
func (r *Ring) RowBegin(block time.Duration) (int64, error) {
	var seq C.int64_t
	st := C.ar_row_begin(r.c, &seq, r.wr)
	if st == C.REPORT_BLOCKED && block > 0 {
		deadline := time.Now().Add(block)
		for st == C.REPORT_BLOCKED && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
			st = C.ar_row_begin(r.c, &seq, r.wr)
		}
	}
	if st != C.REPORT_OK {
		return -1, r.werr()
	}
	r.slot = int64(seq) % r.capacity
	return int64(seq), nil
}

// The setters store straight into the open row's slot (direct.go): no
// cgo call per field for fixed-width leaves. The refusals are the
// core's, with the same status names and detail text.

func (r *Ring) SetI64(leaf int32, v int64) error {
	li, err := r.leafInfo(leaf)
	if err != nil {
		return err
	}
	if li.kind != kindPrim {
		return r.invalidErr("field '%s': not an integer leaf", li.path)
	}
	if err := r.storeInt(li, r.slot, v, false, 0); err != nil {
		return err
	}
	li.markValid(r.slot)
	return nil
}

func (r *Ring) SetU64(leaf int32, v uint64) error {
	li, err := r.leafInfo(leaf)
	if err != nil {
		return err
	}
	if li.kind != kindPrim {
		return r.invalidErr("field '%s': not an integer leaf", li.path)
	}
	if err := r.storeInt(li, r.slot, 0, true, v); err != nil {
		return err
	}
	li.markValid(r.slot)
	return nil
}

func (r *Ring) SetF64(leaf int32, v float64) error {
	li, err := r.leafInfo(leaf)
	if err != nil {
		return err
	}
	if li.kind != kindPrim || li.class != classFloat {
		return r.invalidErr("field '%s': not a float leaf", li.path)
	}
	switch li.itemsize {
	case 8:
		*(*float64)(unsafe.Add(li.data, r.slot*8)) = v
	case 4:
		*(*float32)(unsafe.Add(li.data, r.slot*4)) = float32(v)
	default: // float16: the core converts
		if st := C.ar_set_f64(r.c, C.int32_t(leaf), C.double(v), r.wr); st != C.REPORT_OK {
			return r.werr()
		}
		return nil
	}
	li.markValid(r.slot)
	return nil
}

func (r *Ring) SetBool(leaf int32, v bool) error {
	li, err := r.leafInfo(leaf)
	if err != nil {
		return err
	}
	if li.kind != kindBool {
		return r.invalidErr("field '%s': not a bool leaf", li.path)
	}
	if v {
		bitSet(li.data, r.slot)
	} else {
		bitClear(li.data, r.slot)
	}
	li.markValid(r.slot)
	return nil
}

// SetBytes writes a fixed-width blob (decimal, fixed_size_binary) into
// the slot, or hands a var-width value to the core's pool.
func (r *Ring) SetBytes(leaf int32, v []byte) error {
	li, err := r.leafInfo(leaf)
	if err != nil {
		return err
	}
	if li.kind == kindBytes {
		if int64(len(v)) != li.width {
			return r.widthErr("field '%s': needs exactly %d bytes, got %d", li.path, li.width, len(v))
		}
		copy(unsafe.Slice((*byte)(unsafe.Add(li.data, r.slot*li.width)), li.width), v)
		li.markValid(r.slot)
		return nil
	}
	var p unsafe.Pointer
	if len(v) > 0 {
		p = unsafe.Pointer(&v[0])
	}
	st := C.ar_set_bytes(r.c, C.int32_t(leaf), p, C.int64_t(len(v)), r.wr)
	runtime.KeepAlive(v)
	r.said() // a value larger than a pool segment makes a line
	if st != C.REPORT_OK {
		return r.werr()
	}
	return nil
}

func (r *Ring) SetString(leaf int32, v string) error {
	return r.SetBytes(leaf, []byte(v))
}

// SetList writes one row of raw fixed-width elements: into the slot for
// a fixed_size_list leaf, into the core's pool for a list leaf.
func (r *Ring) SetList(leaf int32, elems []byte, nElems int64) error {
	li, err := r.leafInfo(leaf)
	if err != nil {
		return err
	}
	if li.kind == kindFSL {
		if nElems != li.width {
			return r.widthErr("field '%s': needs exactly %d elements, got %d", li.path, li.width, nElems)
		}
		n := nElems * li.itemsize
		copy(unsafe.Slice((*byte)(unsafe.Add(li.data, r.slot*n)), n), elems[:n])
		li.markValid(r.slot)
		return nil
	}
	var p unsafe.Pointer
	if len(elems) > 0 {
		p = unsafe.Pointer(&elems[0])
	}
	st := C.ar_set_list(r.c, C.int32_t(leaf), p, C.int64_t(nElems), r.wr)
	runtime.KeepAlive(elems)
	r.said()
	if st != C.REPORT_OK {
		return r.werr()
	}
	return nil
}

func (r *Ring) SetNull(leaf int32) error {
	li, err := r.leafInfo(leaf)
	if err != nil {
		return err
	}
	if !li.nullable {
		return &Error{Status: "not-nullable", Detail: fmt.Sprintf("field '%s' is not nullable", li.path)}
	}
	if li.kind == kindVar { // the core also zeroes the row's length
		if st := C.ar_set_null(r.c, C.int32_t(leaf), r.wr); st != C.REPORT_OK {
			return r.werr()
		}
		return nil
	}
	bitClear(li.valid, r.slot)
	return nil
}

// Commit freezes every allocated row. Cheap: a pointer advance.
func (r *Ring) Commit() (int64, error) {
	var n C.int64_t
	if st := C.ar_commit(r.c, &n, r.wr); st != C.REPORT_OK {
		return 0, r.werr()
	}
	return int64(n), nil
}

// AppendRecord bulk-ingests an arrow record with the ring's schema —
// the memcpy path, auto-committing. THIS is how a received Flight/IPC
// stream enters the ring.
func (r *Ring) AppendRecord(rec arrow.RecordBatch) error {
	return r.AppendRecordAt(rec, -1)
}

// AppendRecordAt ingests at an absolute sequence number — the
// attach-side alignment primitive (each wire batch carries the origin
// seq of its first row; appending there keeps the local ring on the
// publisher's seq axis). A forward jump is a loud GAP that raises the
// readable floor; a regression is an error. firstSeq -1 = next.
func (r *Ring) AppendRecordAt(rec arrow.RecordBatch, firstSeq int64) error {
	var ca cdata.CArrowArray
	cdata.ExportArrowRecordBatch(rec, &ca, nil)
	st := C.ar_append_batch_at(r.c,
		(*C.struct_ArrowArray)(unsafe.Pointer(&ca)), C.int64_t(firstSeq), r.wr)
	r.said() // a jump in the row numbers makes a line
	if st != C.REPORT_OK {
		return r.werr()
	}
	return nil
}

// ---- read side: single rows (hot path, no pin) ---------------------------

func (r *Ring) GetI64(seq int64, leaf int32) (v int64, isNull bool, err error) {
	var cv C.int64_t
	var n C.bool
	rep := getReport()
	if err := readErr(rep, C.ar_get_i64(r.c, C.int64_t(seq), C.int32_t(leaf), &cv, &n, rep)); err != nil {
		return 0, false, err
	}
	return int64(cv), bool(n), nil
}

func (r *Ring) GetU64(seq int64, leaf int32) (v uint64, isNull bool, err error) {
	var cv C.uint64_t
	var n C.bool
	rep := getReport()
	if err := readErr(rep, C.ar_get_u64(r.c, C.int64_t(seq), C.int32_t(leaf), &cv, &n, rep)); err != nil {
		return 0, false, err
	}
	return uint64(cv), bool(n), nil
}

func (r *Ring) GetF64(seq int64, leaf int32) (v float64, isNull bool, err error) {
	var cv C.double
	var n C.bool
	rep := getReport()
	if err := readErr(rep, C.ar_get_f64(r.c, C.int64_t(seq), C.int32_t(leaf), &cv, &n, rep)); err != nil {
		return 0, false, err
	}
	return float64(cv), bool(n), nil
}

func (r *Ring) GetBool(seq int64, leaf int32) (v bool, isNull bool, err error) {
	var cv, n C.bool
	rep := getReport()
	if err := readErr(rep, C.ar_get_bool(r.c, C.int64_t(seq), C.int32_t(leaf), &cv, &n, rep)); err != nil {
		return false, false, err
	}
	return bool(cv), bool(n), nil
}

// GetBytes copies a var-width value out (the borrowed-pointer peek the
// header describes is not safe to hand across the cgo boundary, so the
// Go getter copies; use record sets for bulk zero-copy access).
func (r *Ring) GetBytes(seq int64, leaf int32) (v []byte, isNull bool, err error) {
	var p unsafe.Pointer
	var n C.int64_t
	var isn C.bool
	rep := getReport()
	if err := readErr(rep, C.ar_get_var(r.c, C.int64_t(seq), C.int32_t(leaf),
		(*unsafe.Pointer)(unsafe.Pointer(&p)), &n, &isn, rep)); err != nil {
		return nil, false, err
	}
	if bool(isn) || p == nil {
		return nil, bool(isn), nil
	}
	return C.GoBytes(p, C.int(n)), false, nil
}

func (r *Ring) OldestSeq() int64 { return int64(C.ar_oldest_seq(r.c)) }
func (r *Ring) NewestSeq() int64 { return int64(C.ar_newest_seq(r.c)) }

// ---- wire frames ---------------------------------------------------------

// WireEntry maps Size bytes at byte Offset of a packed frame to the
// fixed-width leaf at Path.
type WireEntry struct {
	Path   string
	Offset int32
	Size   int32
}

type WireMap struct {
	c    *C.ArWireMap
	ring *Ring
}

func (r *Ring) NewWireMap(entries []WireEntry) (*WireMap, error) {
	n := len(entries)
	leaves := make([]C.int32_t, n)
	offsets := make([]C.int32_t, n)
	sizes := make([]C.int32_t, n)
	for i, e := range entries {
		leaf, err := r.Leaf(e.Path)
		if err != nil {
			return nil, err
		}
		leaves[i] = C.int32_t(leaf)
		offsets[i] = C.int32_t(e.Offset)
		sizes[i] = C.int32_t(e.Size)
	}
	var m *C.ArWireMap
	rep := getReport()
	if err := readErr(rep, C.ar_wiremap_new(r.c, &leaves[0], &offsets[0], &sizes[0],
		C.int32_t(n), &m, rep)); err != nil {
		return nil, err
	}
	return &WireMap{c: m, ring: r}, nil
}

func (m *WireMap) Close() {
	if m.c != nil {
		C.ar_wiremap_free(m.c)
		m.c = nil
	}
}

// FromWire scatters one frame into the open row (RowBegin first).
func (r *Ring) FromWire(m *WireMap, frame []byte) error {
	st := C.ar_from_wire(r.c, m.c, unsafe.Pointer(&frame[0]),
		C.int64_t(len(frame)), r.wr)
	runtime.KeepAlive(frame)
	if st != C.REPORT_OK {
		return r.werr()
	}
	return nil
}

// ToWire gathers leaves of a committed row back into a frame.
func (r *Ring) ToWire(m *WireMap, seq int64, frameLen int) ([]byte, error) {
	frame := make([]byte, frameLen)
	rep := getReport()
	if err := readErr(rep, C.ar_to_wire(r.c, m.c, C.int64_t(seq),
		unsafe.Pointer(&frame[0]), C.int64_t(frameLen), rep)); err != nil {
		return nil, err
	}
	return frame, nil
}

// ---- record sets (pinned, immutable, zero-copy) --------------------------

type RecordSet struct {
	c    *C.ArRecordSet
	ring *Ring
	Lo   int64 // first seq
	Hi   int64 // one past the last
	done bool
}

// Pin pins the rows [lo, hi) and no others: the publish and archive
// paths' record set, the rows they send or write. Release (or Detach)
// it. The writer meets this pin only after a whole ring of writes, and
// then the holder is a leak; a pin on the oldest row of a full ring
// makes the writer wait at once. lo below the oldest readable row is raised to it
// (rs.Lo > lo: rows recycled); hi above committed is lowered to it.
func (r *Ring) Pin(lo, hi int64) (*RecordSet, error) {
	var rs *C.ArRecordSet
	rep := getReport()
	if err := readErr(rep, C.ar_pin(r.c, C.int64_t(lo), C.int64_t(hi), &rs, rep)); err != nil {
		return nil, err
	}
	return &RecordSet{c: rs, ring: r,
		Lo: int64(C.ar_recordset_lo(rs)),
		Hi: int64(C.ar_recordset_hi(rs))}, nil
}

// Records exports the set as retained zero-copy record batches (they
// reference ring memory and keep the pin's buffers alive). Release
// each record when done with it.
func (rs *RecordSet) Records() ([]arrow.RecordBatch, error) {
	stream := (*C.struct_ArrowArrayStream)(C.malloc(C.sizeof_struct_ArrowArrayStream))
	defer C.free(unsafe.Pointer(stream))
	rep := getReport()
	if err := readErr(rep, C.ar_recordset_stream(rs.c, stream, rep)); err != nil {
		return nil, err
	}
	rdr, err := cdata.ImportCRecordReader(
		(*cdata.CArrowArrayStream)(unsafe.Pointer(stream)), nil)
	if err != nil {
		return nil, err
	}
	var out []arrow.RecordBatch
	for {
		rec, err := rdr.Read()
		if err != nil {
			break // io.EOF ends the stream
		}
		rec.Retain()
		out = append(out, rec)
	}
	// Release the imported reader NOW: arrow-go only releases the C
	// stream via a GC finalizer, and the Go GC feels no pressure from
	// the ring/pool memory the stream holds -- deferring this pins
	// dead var-width segments indefinitely (found by the torture rig:
	// string subscribers grew without bound; numeric ones, whose
	// buffers are the fixed ring storage, showed nothing).
	if rel, ok := rdr.(interface{ Release() }); ok {
		rel.Release()
	}
	return out, nil
}

// Release drops the pin. Exported records stay memory-safe (refcounts)
// but the region may recycle — done means done.
func (rs *RecordSet) Release() {
	if !rs.done {
		rs.done = true
		C.ar_recordset_release(rs.c)
	}
}

// Detach drops the caller's reference without dropping the pin; the
// pin lives exactly as long as the exported records do. For fire-and-
// forget publish paths. Call exactly one of Release/Detach.
func (rs *RecordSet) Detach() {
	if !rs.done {
		rs.done = true
		C.ar_recordset_detach(rs.c)
	}
}
