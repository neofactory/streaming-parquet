// Package pqc is the Go host for the parquet_checkpoint C core.
//
// The core (core/pqc.h — the spec) does the format work: thrift offset
// rebase, footer combining, chunk cutting, the PQC2 checkpoint object.
// This host encodes parquet (see rows.go) and stores bytes through a
// Sink (sink.go). The Python package is the reference implementation;
// artifacts written here are read by it and vice versa
// (run_go_compat.py proves it).
package pqc

// pqc_core.c and pqc.h are CHECKED-IN COPIES of ../../core/pqc.{c,h}
// (copies, not a symlink or out-of-package include, so the Go build
// cache tracks their content). run_go_compat.py refreshes them and
// refuses to pass while they drift from the core.

/*
#cgo CFLAGS: -std=c11 -O2
#include <stdlib.h>
#include "pqc.h"
*/
import "C"

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// SupersededKeep: a checkpoint version superseded at least this long is
// deleted, in the checkpoint that stores a later one (the Python
// SUPERSEDED_KEEP_S): a reader told of a version less than this long
// ago still finds it.
const SupersededKeep = 10 * time.Minute

var ckptRe = regexp.MustCompile(`^(.+\.parquet)(?:\.(\d{6,}))?\.ckpt$`)

// CkptObject is the object name of version seq of file name's
// checkpoint: name.000017.ckpt. Each checkpoint is a new object,
// written once and never changed.
func CkptObject(name string, seq int) string {
	return fmt.Sprintf("%s.%06d.ckpt", name, seq)
}

// ParseCkptObject returns the file name and version of a checkpoint
// object's name. A checkpoint named without a version (x.parquet.ckpt,
// as written before versions) is version 0.
func ParseCkptObject(obj string) (name string, seq int, ok bool) {
	m := ckptRe.FindStringSubmatch(obj)
	if m == nil {
		return "", 0, false
	}
	if m[2] != "" {
		seq, _ = strconv.Atoi(m[2])
	}
	return m[1], seq, true
}

// Version is one checkpoint version of a file.
type Version struct {
	Seq    int
	Object string
}

// CkptVersions lists file name's checkpoint versions on sink, oldest
// first.
func CkptVersions(sink Sink, name string) ([]Version, error) {
	objs, err := sink.List(name + ".")
	if err != nil {
		return nil, err
	}
	var out []Version
	for _, o := range objs {
		if n, seq, ok := ParseCkptObject(o); ok && n == name {
			out = append(out, Version{seq, o})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

func cerr(buf *[512]C.char) error {
	return fmt.Errorf("%s", C.GoString(&buf[0]))
}

func bytesPtr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}

// Writer writes one crash-durable parquet file through a Sink,
// mirroring the Python CheckpointWriter: AddGroupMini appends final
// row groups (a complete mini parquet file) and implies a checkpoint;
// Checkpoint publishes a durability claim with an optional provisional
// tail; Close finalizes into an ordinary .parquet.
type Writer struct {
	name     string
	sink     Sink
	w        *C.PqcWriter
	auto     bool
	rowsCk   uint64
	minLeft  uint64    // sink part minimum, enforced at construction
	seq      int       // the newest checkpoint version's number
	versions []version // checkpoint objects stored, oldest first
}

// version is a stored checkpoint object and when a later one superseded
// it (zero while it is the newest).
type version struct {
	object     string
	superseded time.Time
}

// NewWriter refuses an existing final file or checkpoint (finalize
// first) and a chunkBytes below the sink's part minimum — the same
// loud refusals as the Python writer.
func NewWriter(name string, sink Sink, chunkBytes uint64, checkpointEveryWrite bool) (*Writer, error) {
	if !strings.HasSuffix(name, ".parquet") {
		return nil, fmt.Errorf("name must end in .parquet: %q", name)
	}
	if pm := sink.PartMin(); chunkBytes < pm {
		return nil, fmt.Errorf("chunk_bytes %d is below the sink's part minimum %d: finalize would be impossible", chunkBytes, pm)
	}
	if sink.Exists(name) {
		return nil, fmt.Errorf("%s already exists", name)
	}
	left, err := CkptVersions(sink, name)
	if err != nil {
		return nil, err
	}
	if len(left) > 0 {
		return nil, fmt.Errorf("%s exists -- a previous writer crashed or is live; Finalize it first", left[len(left)-1].Object)
	}
	var eb [512]C.char
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	w := C.pqc_writer_new(cname, C.uint64_t(chunkBytes), &eb[0], 512)
	if w == nil {
		return nil, cerr(&eb)
	}
	return &Writer{name: name, sink: sink, w: w, auto: checkpointEveryWrite}, nil
}

// AddGroupMini appends the row group(s) of a complete mini parquet
// file (offsets counting from 0 — any parquet writer over a fresh
// buffer). Cut chunks are stored before anything claims them.
func (w *Writer) AddGroupMini(mini []byte) (int64, error) {
	var eb [512]C.char
	rows := C.pqc_add_group(w.w, bytesPtr(mini), C.size_t(len(mini)), &eb[0], 512)
	if rows < 0 {
		return 0, cerr(&eb)
	}
	if err := w.drainChunks(); err != nil {
		return 0, err
	}
	if w.auto {
		if _, err := w.Checkpoint(nil); err != nil {
			return 0, err
		}
	}
	return int64(rows), nil
}

func (w *Writer) drainChunks() error {
	var name [C.PQC_NAME_MAX]C.char
	var data *C.uint8_t
	var length C.uint64_t
	var crc C.uint32_t
	for {
		rc := C.pqc_next_chunk(w.w, &name[0], &data, &length, &crc)
		if rc < 0 {
			return fmt.Errorf("pqc_next_chunk failed")
		}
		if rc == 0 {
			return nil
		}
		body := C.GoBytes(unsafe.Pointer(data), C.int(length))
		if err := w.sink.Put(C.GoString(&name[0]), body); err != nil {
			return err // chunk stays pending; the next call retries
		}
		C.pqc_chunk_stored(w.w)
	}
}

// Checkpoint publishes the durability claim. tailMini (may be nil) is
// a complete mini parquet file of rows not yet added as final groups;
// they ride this checkpoint and are superseded by a later
// AddGroupMini of the same rows.
func (w *Writer) Checkpoint(tailMini []byte) (watermark uint64, err error) {
	var eb [512]C.char
	var n, wm C.uint64_t
	p := C.pqc_build_checkpoint(w.w, bytesPtr(tailMini), C.size_t(len(tailMini)), &n, &wm, &eb[0], 512)
	if p == nil {
		return 0, cerr(&eb)
	}
	ck := C.GoBytes(unsafe.Pointer(p), C.int(n))
	C.pqc_free(unsafe.Pointer(p))
	w.seq++
	obj := CkptObject(w.name, w.seq)
	if err := w.sink.Put(obj, ck); err != nil {
		w.seq--
		return 0, err
	}
	// stored whole first: obj supersedes the newest; delete every
	// version superseded at least SupersededKeep ago
	now := time.Now()
	if n := len(w.versions); n > 0 {
		w.versions[n-1].superseded = now
	}
	kept := w.versions[:0]
	for _, v := range w.versions {
		if !v.superseded.IsZero() && now.Sub(v.superseded) >= SupersededKeep {
			if err := w.sink.Delete(v.object); err != nil {
				return 0, err
			}
			continue
		}
		kept = append(kept, v)
	}
	w.versions = append(kept, version{object: obj})
	w.rowsCk = uint64(wm)
	return uint64(wm), nil
}

// CkptName is the newest checkpoint version's object name; "" before
// the first checkpoint.
func (w *Writer) CkptName() string {
	if len(w.versions) == 0 {
		return ""
	}
	return w.versions[len(w.versions)-1].object
}

// RowsFinal is the count of rows in added groups; RowsCheckpointed the
// watermark of the last published checkpoint.
func (w *Writer) RowsFinal() uint64        { return uint64(C.pqc_rows_final(w.w)) }
func (w *Writer) RowsCheckpointed() uint64 { return w.rowsCk }

// Close publishes a final checkpoint (empty writers get a valid empty
// file via an empty tail mini) and finalizes. The writer is freed.
func (w *Writer) Close(emptyMini []byte) error {
	defer w.Free()
	var tail []byte
	if w.RowsFinal() == 0 && C.pqc_chunks_cut(w.w) == 0 {
		tail = emptyMini // schema-only file; rows.go supplies one
	}
	if _, err := w.Checkpoint(tail); err != nil {
		return err
	}
	_, err := Finalize(w.name, w.sink)
	return err
}

// Free releases the C writer without finalizing (a crash leaves the
// checkpoint + chunks for Finalize, from anywhere).
func (w *Writer) Free() {
	if w.w != nil {
		C.pqc_writer_free(w.w)
		w.w = nil
	}
}

// Reader parses a checkpoint object and composes the live file.
type Reader struct {
	Name      string
	Watermark uint64
	Manifest  []ManifestEntry
	dataOff   uint64
	dataLen   uint64
	raw       []byte
	sink      Sink
}

type ManifestEntry struct {
	Name   string
	Length uint64
	CRC    uint32
}

// OpenReader validates magic, version, and CRC (loud refusals).
func OpenReader(ckptName string, sink Sink) (*Reader, error) {
	raw, err := sink.Get(ckptName, 0, -1)
	if err != nil {
		return nil, err
	}
	var eb [512]C.char
	r := C.pqc_reader_open(bytesPtr(raw), C.size_t(len(raw)), &eb[0], 512)
	if r == nil {
		return nil, cerr(&eb)
	}
	defer C.pqc_reader_free(r)
	rd := &Reader{
		Name:      C.GoString(C.pqc_reader_name(r)),
		Watermark: uint64(C.pqc_reader_watermark(r)),
		raw:       raw,
		sink:      sink,
	}
	var off, ln C.uint64_t
	C.pqc_reader_data(r, &off, &ln)
	rd.dataOff, rd.dataLen = uint64(off), uint64(ln)
	n := uint32(C.pqc_reader_n_chunks(r))
	for i := uint32(0); i < n; i++ {
		var cn *C.char
		var cl C.uint64_t
		var crc C.uint32_t
		C.pqc_reader_chunk(r, C.uint32_t(i), &cn, &cl, &crc)
		rd.Manifest = append(rd.Manifest, ManifestEntry{C.GoString(cn), uint64(cl), crc32v(crc)})
	}
	return rd, nil
}

func crc32v(c C.uint32_t) uint32 { return uint32(c) }

// Compose returns the live file's bytes: every chunk (length+CRC
// verified against the manifest) followed by the checkpoint's data
// region. The result is an ordinary parquet file.
func (r *Reader) Compose() ([]byte, error) {
	var out []byte
	for _, m := range r.Manifest {
		b, err := r.sink.Get(m.Name, 0, -1)
		if err != nil {
			return nil, err
		}
		if uint64(len(b)) != m.Length || CRC32(b) != m.CRC {
			return nil, fmt.Errorf("%s: length/CRC mismatch vs manifest -- corrupt", m.Name)
		}
		out = append(out, b...)
	}
	return append(out, r.raw[r.dataOff:r.dataOff+r.dataLen]...), nil
}

// AssemblyPlan lists (name, offset, length) sources for Sink.Assemble:
// the chunks whole, then the checkpoint object's own data region.
func (r *Reader) AssemblyPlan(ckptName string) []Source {
	var plan []Source
	for _, m := range r.Manifest {
		plan = append(plan, Source{m.Name, 0, -1})
	}
	plan = append(plan, Source{ckptName, int64(r.dataOff), int64(r.dataLen)})
	return plan
}

// Finalize turns a checkpoint into the ordinary .parquet file —
// idempotent, assembled from the newest checkpoint version whose CRC
// holds, versions deleted last, crash-orphaned chunks swept. Same
// refusals as the Python finalize (both-exist, mispaired,
// missing/wrong-size chunk).
func Finalize(name string, sink Sink) (string, error) {
	versions, err := CkptVersions(sink, name)
	if err != nil {
		return "", err
	}
	if len(versions) == 0 {
		if sink.Exists(name) {
			return "done", nil
		}
		return "", fmt.Errorf("finalize(%q): nothing there", name)
	}
	if sink.Exists(name) {
		return "", fmt.Errorf("%s: BOTH final and checkpoint exist -- refusing to guess", name)
	}
	var r *Reader
	var used string
	var refused []string
	for i := len(versions) - 1; i >= 0; i-- {
		rr, err := OpenReader(versions[i].Object, sink)
		if err != nil {
			refused = append(refused, fmt.Sprintf("%s: %v", versions[i].Object, err))
			continue
		}
		r, used = rr, versions[i].Object
		break
	}
	if r == nil {
		return "", fmt.Errorf("finalize(%q): no checkpoint version holds -- %s", name, strings.Join(refused, "; "))
	}
	base := name
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		base = name[i+1:]
	}
	if r.Name != base {
		return "", fmt.Errorf("%s: checkpoint names %q, paired with %q -- mispaired, refusing", used, r.Name, name)
	}
	for _, m := range r.Manifest {
		sz, err := sink.Size(m.Name)
		if err != nil || uint64(sz) != m.Length {
			return "", fmt.Errorf("%s: missing or wrong size -- cannot finalize", m.Name)
		}
	}
	if err := sink.Assemble(name, r.AssemblyPlan(used)); err != nil {
		return "", err
	}
	chunks, err := sink.List(name + ".chunks/")
	if err != nil {
		return "", err
	}
	for _, c := range chunks {
		if err := sink.Delete(c); err != nil {
			return "", err
		}
	}
	for _, v := range versions {
		if v.Object != used {
			if err := sink.Delete(v.Object); err != nil {
				return "", err
			}
		}
	}
	if err := sink.Delete(used); err != nil { // last act
		return "", err
	}
	return "recovered", nil
}

// CRC32 is the core's CRC (IEEE, zlib-compatible) for host-side use.
func CRC32(b []byte) uint32 {
	return uint32(C.pqc_crc32(bytesPtr(b), C.size_t(len(b))))
}
