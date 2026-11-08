// Package ring is the Go host for the arrow_ring C core.
//
// The core (core/arrow_ring.h — the spec) owns storage, recycling,
// pins, and leases; this binding is thin by design. Boundary language
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
	"fmt"
	"runtime"
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

var statusNames = map[C.ArStatus]string{
	C.AR_ERR_INVALID:          "invalid",
	C.AR_ERR_TYPE_UNSUPPORTED: "type-unsupported",
	C.AR_ERR_STALE:            "stale",
	C.AR_ERR_PIN_STALLED:      "pin-stalled",
	C.AR_ERR_NOT_NULLABLE:     "not-nullable",
	C.AR_ERR_WIDTH:            "width",
	C.AR_ERR_OFFSET_OVERFLOW:  "offset-overflow",
	C.AR_ERR_NOMEM:            "nomem",
	C.AR_ERR_IO:               "io",
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
	schema   *arrow.Schema
	leaves   map[string]int32
	info     []*leafInfo // by leaf index: the direct-store shape (direct.go)
	capacity int64
	slot     int64 // the open row's slot, from RowBegin
}

// Options mirror ArRingOptions; zero values take the core's defaults.
type Options struct {
	RateHint     float64 // rows/s seed for lease exclusions
	LeaseMarginS float64 // padding on lease estimates; default 0.5
	SegmentBytes int64   // var-width pool segment size; default 1<<20
}

func (r *Ring) err(st C.ArStatus) error {
	name := statusNames[st]
	if name == "" {
		name = fmt.Sprintf("status-%d", int(st))
	}
	return &Error{Status: name, Detail: C.GoString(C.ar_last_error(r.c))}
}

// NewRing creates a ring for the schema (see the header for the type
// surface). Capacity is rows — the total loss-free budget for every
// consumer; size it 2x the working need.
func NewRing(schema *arrow.Schema, capacity int64, opts *Options) (*Ring, error) {
	var cs cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cs)
	var co C.ArRingOptions
	if opts != nil {
		co.rate_hint = C.double(opts.RateHint)
		co.lease_margin_s = C.double(opts.LeaseMarginS)
		co.segment_bytes = C.int64_t(opts.SegmentBytes)
	}
	var cr *C.ArRing
	st := C.ar_ring_new((*C.struct_ArrowSchema)(unsafe.Pointer(&cs)),
		C.int64_t(capacity), &co, &cr)
	if st != C.AR_OK {
		// no ring handle yet; the core wrote the detail to the
		// (thread-local) error slot reachable via a NULL ring
		return nil, &Error{Status: statusNames[st],
			Detail: C.GoString(C.ar_last_error(nil))}
	}
	r := &Ring{c: cr, schema: schema, leaves: map[string]int32{}, capacity: capacity}
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
// recycle target (IsPinStalled on timeout, with the overdue lease
// named in the detail).
func (r *Ring) RowBegin(block time.Duration) (int64, error) {
	var seq C.int64_t
	st := C.ar_row_begin(r.c, C.int64_t(block/time.Millisecond), &seq)
	if st != C.AR_OK {
		return -1, r.err(st)
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
		if st := C.ar_set_f64(r.c, C.int32_t(leaf), C.double(v)); st != C.AR_OK {
			return r.err(st)
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
	st := C.ar_set_bytes(r.c, C.int32_t(leaf), p, C.int64_t(len(v)))
	runtime.KeepAlive(v)
	if st != C.AR_OK {
		return r.err(st)
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
	st := C.ar_set_list(r.c, C.int32_t(leaf), p, C.int64_t(nElems))
	runtime.KeepAlive(elems)
	if st != C.AR_OK {
		return r.err(st)
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
		if st := C.ar_set_null(r.c, C.int32_t(leaf)); st != C.AR_OK {
			return r.err(st)
		}
		return nil
	}
	bitClear(li.valid, r.slot)
	return nil
}

// Commit freezes every allocated row. Cheap: a pointer advance.
func (r *Ring) Commit() (int64, error) {
	var n C.int64_t
	if st := C.ar_commit(r.c, &n); st != C.AR_OK {
		return 0, r.err(st)
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
		(*C.struct_ArrowArray)(unsafe.Pointer(&ca)), C.int64_t(firstSeq))
	if st != C.AR_OK {
		return r.err(st)
	}
	return nil
}

// ---- read side: single rows (hot path, no pin) ---------------------------

func (r *Ring) GetI64(seq int64, leaf int32) (v int64, isNull bool, err error) {
	var cv C.int64_t
	var n C.bool
	if st := C.ar_get_i64(r.c, C.int64_t(seq), C.int32_t(leaf), &cv, &n); st != C.AR_OK {
		return 0, false, r.err(st)
	}
	return int64(cv), bool(n), nil
}

func (r *Ring) GetU64(seq int64, leaf int32) (v uint64, isNull bool, err error) {
	var cv C.uint64_t
	var n C.bool
	if st := C.ar_get_u64(r.c, C.int64_t(seq), C.int32_t(leaf), &cv, &n); st != C.AR_OK {
		return 0, false, r.err(st)
	}
	return uint64(cv), bool(n), nil
}

func (r *Ring) GetF64(seq int64, leaf int32) (v float64, isNull bool, err error) {
	var cv C.double
	var n C.bool
	if st := C.ar_get_f64(r.c, C.int64_t(seq), C.int32_t(leaf), &cv, &n); st != C.AR_OK {
		return 0, false, r.err(st)
	}
	return float64(cv), bool(n), nil
}

func (r *Ring) GetBool(seq int64, leaf int32) (v bool, isNull bool, err error) {
	var cv, n C.bool
	if st := C.ar_get_bool(r.c, C.int64_t(seq), C.int32_t(leaf), &cv, &n); st != C.AR_OK {
		return false, false, r.err(st)
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
	st := C.ar_get_var(r.c, C.int64_t(seq), C.int32_t(leaf),
		(*unsafe.Pointer)(unsafe.Pointer(&p)), &n, &isn)
	if st != C.AR_OK {
		return nil, false, r.err(st)
	}
	if bool(isn) || p == nil {
		return nil, bool(isn), nil
	}
	return C.GoBytes(p, C.int(n)), false, nil
}

func (r *Ring) OldestSeq() int64 { return int64(C.ar_oldest_seq(r.c)) }
func (r *Ring) NewestSeq() int64 { return int64(C.ar_newest_seq(r.c)) }

// SetLeaseMargin adjusts the lease margin after construction (tests).
func (r *Ring) SetLeaseMargin(marginS float64) {
	C.ar_set_lease_margin(r.c, C.double(marginS))
}

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
	st := C.ar_wiremap_new(r.c, &leaves[0], &offsets[0], &sizes[0],
		C.int32_t(n), &m)
	if st != C.AR_OK {
		return nil, r.err(st)
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
		C.int64_t(len(frame)))
	runtime.KeepAlive(frame)
	if st != C.AR_OK {
		return r.err(st)
	}
	return nil
}

// ToWire gathers leaves of a committed row back into a frame.
func (r *Ring) ToWire(m *WireMap, seq int64, frameLen int) ([]byte, error) {
	frame := make([]byte, frameLen)
	st := C.ar_to_wire(r.c, m.c, C.int64_t(seq),
		unsafe.Pointer(&frame[0]), C.int64_t(frameLen))
	if st != C.AR_OK {
		return nil, r.err(st)
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

// StaticRelation pins a record set over the committed rows, excluding
// the lease window for `duration` of expected use. Release (or
// Detach) it — the pin, not the lease, is the guarantee, and an
// overstayed pin stalls the writer loudly.
func (r *Ring) StaticRelation(duration time.Duration) (*RecordSet, error) {
	var rs *C.ArRecordSet
	st := C.ar_relation(r.c, C.double(duration.Seconds()), &rs)
	if st != C.AR_OK {
		return nil, r.err(st)
	}
	return &RecordSet{c: rs, ring: r,
		Lo: int64(C.ar_recordset_lo(rs)),
		Hi: int64(C.ar_recordset_hi(rs))}, nil
}

// Pin pins the rows [lo, hi) and no others: the publish and archive
// paths' record set, the rows they send or write. No lease: the writer
// meets this pin only after a whole ring of writes, and then the
// holder is a leak. lo below the oldest readable row is raised to it
// (rs.Lo > lo: rows recycled); hi above committed is lowered to it.
func (r *Ring) Pin(lo, hi int64) (*RecordSet, error) {
	var rs *C.ArRecordSet
	st := C.ar_pin(r.c, C.int64_t(lo), C.int64_t(hi), &rs)
	if st != C.AR_OK {
		return nil, r.err(st)
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
	if st := C.ar_recordset_stream(rs.c, stream); st != C.AR_OK {
		return nil, rs.ring.err(st)
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
