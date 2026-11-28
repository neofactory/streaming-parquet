// Package pqc is the Go host for the parquet_checkpoint C core.
//
// The core (src/core/pqc.h — the spec) does the format work: thrift offset
// rebase, footer combining, which rows each object holds, chunk
// cutting, the PQC2 checkpoint object. This host encodes parquet (see
// rows.go) and stores bytes through a Sink (sink.go). The Python
// writer is a host over the same core; artifacts written here are read
// by the Python package and vice versa (run_go_compat.py proves it).
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
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"
)

// SupersededKeep: a checkpoint version superseded at least this long is
// deleted, in the checkpoint that stores a later one (the Python
// SUPERSEDED_KEEP_S): a reader told of a version less than this long
// ago still finds it.
const SupersededKeep = 10 * time.Minute

// CkptObject is the object name of version seq of file name's
// checkpoint: name.000017.ckpt. Each checkpoint is a new object,
// written once and never changed. The name is the core's rule
// (pqc.h: CHECKPOINT VERSIONS).
func CkptObject(name string, seq int) string {
	cn := C.CString(name)
	defer C.free(unsafe.Pointer(cn))
	var out [C.PQC_OBJECT_MAX]C.char
	var r C.Report
	if C.pqc_ckpt_object(cn, C.int64_t(seq), &out[0], &r) != 0 {
		// a name the core has no room for: no object of it is ever stored
		return fmt.Sprintf("%s.%06d.ckpt", name, seq)
	}
	return C.GoString(&out[0])
}

// ParseCkptObject returns the file name and version of a checkpoint
// object's name. A checkpoint named without a version (x.parquet.ckpt,
// as written before versions) is version 0. The rule is the core's; a
// directory in front of the name stays in front.
func ParseCkptObject(obj string) (name string, seq int, ok bool) {
	head, tail := "", obj
	if i := strings.LastIndexByte(obj, '/'); i >= 0 {
		head, tail = obj[:i+1], obj[i+1:]
	}
	ct := C.CString(tail)
	defer C.free(unsafe.Pointer(ct))
	var out [C.PQC_OBJECT_MAX]C.char
	var n C.int64_t
	if C.pqc_ckpt_object_parse(ct, &out[0], &n) == 0 {
		return "", 0, false
	}
	return head + C.GoString(&out[0]), int(n), true
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

// cerr is the error of a call into the core that failed: the reason in
// the call's report (report.h).
func cerr(r *C.Report) error {
	return fmt.Errorf("%s", C.GoString(&r.error[0]))
}

func cbool(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

func bytesPtr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}

// Writer writes one crash-durable parquet file through a Sink, a host
// over the C core like the Python CheckpointWriter: AddGroupMini
// appends final row groups (a complete mini parquet file) and implies
// a checkpoint; CheckpointTail publishes a durability claim that
// covers the rows after the last final row group too, storing only the
// rows no object holds yet; Close finalizes into an ordinary .parquet.
// One owner at a time, like a file handle.
type Writer struct {
	name    string
	sink    Sink
	w       *C.PqcWriter
	auto    bool
	rowsCk  uint64
	minLeft uint64 // sink part minimum, enforced at construction
	seq     int    // the newest checkpoint version's number
	// The versions stored, and which are due for deletion: the core's
	// rule (pqc.h: CHECKPOINT VERSIONS).
	versions *C.PqcVersions
	ckptName string // the newest version's object name
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
	var r C.Report
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var w *C.PqcWriter
	if C.pqc_writer_new(cname, C.uint64_t(chunkBytes), &w, &r) != 0 {
		return nil, cerr(&r)
	}
	var versions *C.PqcVersions
	if C.pqc_versions_new(C.double(SupersededKeep.Seconds()), &versions, &r) != 0 {
		C.pqc_writer_free(w)
		return nil, cerr(&r)
	}
	return &Writer{name: name, sink: sink, w: w, auto: checkpointEveryWrite,
		versions: versions}, nil
}

// AddGroupMini appends the row group(s) of a complete mini parquet
// file (offsets counting from 0 — any parquet writer over a fresh
// buffer): every row since the last AddGroupMini. They replace what
// CheckpointTail stored of those rows. Every object is stored before
// anything claims it.
func (w *Writer) AddGroupMini(mini []byte) (int64, error) {
	var r C.Report
	var rows C.int64_t
	if C.pqc_add_group(w.w, bytesPtr(mini), C.size_t(len(mini)), &rows, &r) != 0 {
		return 0, cerr(&r)
	}
	if err := w.storeObjects(); err != nil {
		return 0, err
	}
	if w.auto {
		if _, err := w.Checkpoint(); err != nil {
			return 0, err
		}
	}
	return int64(rows), nil
}

// storeObjects stores every object the core owes. A Put that fails
// leaves the object with the core, which gives it again at the next
// call.
func (w *Writer) storeObjects() error {
	var name [C.PQC_NAME_MAX]C.char
	var data *C.uint8_t
	var length C.uint64_t
	var crc C.uint32_t
	var r C.Report
	for {
		var have C.int
		if C.pqc_next_object(w.w, &name[0], &data, &length, &crc, &have, &r) != 0 {
			return cerr(&r)
		}
		if have == 0 {
			return nil
		}
		body := C.GoBytes(unsafe.Pointer(data), C.int(length))
		if err := w.sink.Put(C.GoString(&name[0]), body); err != nil {
			return err // the object stays with the core; the next call retries
		}
		C.pqc_object_stored(w.w)
	}
}

// NextSpan: a checkpoint is due and newRows rows are in no object yet.
// It returns the rows of the file to encode as ONE row group,
// [firstRow, firstRow+rows): the new rows alone, or a merge that ends
// with them. firstRow - RowsFinal() is where the span begins among the
// rows after the last final row group.
func (w *Writer) NextSpan(newRows uint64) (firstRow, rows uint64) {
	var f, r C.uint64_t
	C.pqc_next_span(w.w, C.uint64_t(newRows), &f, &r)
	return uint64(f), uint64(r)
}

// AddUnfinishedMini takes the span of the last NextSpan as a complete
// mini parquet file of exactly those rows, and stores its object.
func (w *Writer) AddUnfinishedMini(mini []byte) (int64, error) {
	var r C.Report
	var rows C.int64_t
	if C.pqc_add_unfinished(w.w, bytesPtr(mini), C.size_t(len(mini)), &rows, &r) != 0 {
		return 0, cerr(&r)
	}
	if err := w.storeObjects(); err != nil {
		return 0, err
	}
	return int64(rows), nil
}

// DropUnfinished forgets the rows after the last final row group: the
// next Checkpoint claims the final rows only.
func (w *Writer) DropUnfinished() { C.pqc_drop_unfinished(w.w) }

// CheckpointTail publishes a durability claim that covers all final
// rows and the tail: EVERY row since the last AddGroupMini, tailRows of
// them, in order (provisional: a later AddGroupMini of the same rows
// replaces them). A later tail begins with the rows of the one before.
// encode(offset, rows) returns rows [offset, offset+rows) of the tail
// as a complete mini parquet file; it is asked only for the rows no
// object holds yet, or for a merge the core chose, so the cost follows
// the new rows, not the tail.
func (w *Writer) CheckpointTail(tailRows uint64, encode func(offset, rows uint64) ([]byte, error)) (uint64, error) {
	if err := w.storeObjects(); err != nil { // one a failed Put left with the core
		return 0, err
	}
	if tailRows == 0 {
		w.DropUnfinished()
		return w.Checkpoint()
	}
	have := w.RowsUnfinished()
	if tailRows < have {
		return 0, fmt.Errorf("%s: a tail of %d rows after one of %d: a tail holds every row since the last final row group, so it never gets shorter", w.name, tailRows, have)
	}
	if tailRows > have {
		first, rows := w.NextSpan(tailRows - have)
		mini, err := encode(first-w.RowsFinal(), rows)
		if err != nil {
			return 0, err
		}
		if _, err := w.AddUnfinishedMini(mini); err != nil {
			return 0, err
		}
	}
	return w.Checkpoint()
}

// Checkpoint publishes the durability claim for what the core holds:
// the final rows, and the rows CheckpointTail stored after them.
func (w *Writer) Checkpoint() (watermark uint64, err error) {
	return w.checkpoint(nil)
}

// checkpoint builds the checkpoint object (the list of the stored
// objects and the footer) and stores it as a new version. emptyMini is
// for a writer with no rows: a complete empty parquet file.
func (w *Writer) checkpoint(emptyMini []byte) (watermark uint64, err error) {
	if err := w.storeObjects(); err != nil {
		return 0, err
	}
	var rep C.Report
	var n, wm C.uint64_t
	var p *C.uint8_t
	if C.pqc_build_checkpoint(w.w, bytesPtr(emptyMini), C.size_t(len(emptyMini)), &p, &n, &wm, &rep) != 0 {
		return 0, cerr(&rep)
	}
	ck := C.GoBytes(unsafe.Pointer(p), C.int(n))
	C.pqc_free(unsafe.Pointer(p))
	w.seq++
	obj := CkptObject(w.name, w.seq)
	if err := w.sink.Put(obj, ck); err != nil {
		w.seq--
		return 0, err
	}
	// stored whole first: obj replaces the newest version; what the
	// rule says is due is deleted, a version first and then the objects
	// only it named
	if err := storedVersion(w.versions, w.sink, obj, ck); err != nil {
		return 0, err
	}
	w.ckptName = obj
	w.rowsCk = uint64(wm)
	return uint64(wm), nil
}

// manifestNames is the set of objects a checkpoint object names.
func manifestNames(ck []byte) (map[string]bool, error) {
	var rep C.Report
	var r *C.PqcReader
	if C.pqc_reader_open(bytesPtr(ck), C.size_t(len(ck)), &r, &rep) != 0 {
		return nil, cerr(&rep)
	}
	defer C.pqc_reader_free(r)
	names := map[string]bool{}
	n := uint32(C.pqc_reader_n_chunks(r))
	for i := uint32(0); i < n; i++ {
		var cn *C.char
		var cl C.uint64_t
		var crc C.uint32_t
		C.pqc_reader_chunk(r, C.uint32_t(i), &cn, &cl, &crc)
		names[C.GoString(cn)] = true
	}
	return names, nil
}

// CkptName is the newest checkpoint version's object name; "" before
// the first checkpoint.
func (w *Writer) CkptName() string { return w.ckptName }

// RowsFinal is the count of rows in added groups; RowsUnfinished the
// rows CheckpointTail stored after them; RowsCheckpointed the
// watermark of the last published checkpoint.
func (w *Writer) RowsFinal() uint64        { return uint64(C.pqc_rows_final(w.w)) }
func (w *Writer) RowsUnfinished() uint64   { return uint64(C.pqc_rows_unfinished(w.w)) }
func (w *Writer) RowsCheckpointed() uint64 { return w.rowsCk }

// FileBytes is the bytes of the file's final row groups so far: its
// size without the footer.
func (w *Writer) FileBytes() uint64 { return uint64(C.pqc_logical_end(w.w)) }

// Close publishes a final checkpoint of the final rows (empty writers
// get a valid empty file via an empty mini) and finalizes. The writer
// is freed.
func (w *Writer) Close(emptyMini []byte) error { return w.close(emptyMini, false) }

// CloseKeep is Close, but the file's chunks and checkpoint versions
// stay beside the finished file, for readers that were told of them:
// DiscardCheckpoints deletes them later.
func (w *Writer) CloseKeep(emptyMini []byte) error { return w.close(emptyMini, true) }

func (w *Writer) close(emptyMini []byte, keep bool) error {
	defer w.Free()
	w.DropUnfinished()
	var empty []byte
	if w.RowsFinal() == 0 && C.pqc_chunks_cut(w.w) == 0 {
		empty = emptyMini // schema-only file; rows.go supplies one
	}
	if _, err := w.checkpoint(empty); err != nil {
		return err
	}
	_, err := finalize(w.name, w.sink, keep)
	return err
}

// Free releases the C writer without finalizing (a crash leaves the
// checkpoint + objects for Finalize, from anywhere).
// CheckpointFinal is the writer's last checkpoint of a file that an
// upload ends: every final row claimed, no unfinished rows, and the
// file left as its versions and objects. The upload assembles it, in
// its order (THE END OF A FILE in the core's pqc.h). emptyMini is for a
// file with no rows. The writer is not used after it.
func (w *Writer) CheckpointFinal(emptyMini []byte) error {
	defer w.Free()
	w.DropUnfinished()
	var empty []byte
	if w.RowsFinal() == 0 && C.pqc_chunks_cut(w.w) == 0 {
		empty = emptyMini
	}
	_, err := w.checkpoint(empty)
	return err
}

func (w *Writer) Free() {
	if w.w != nil {
		C.pqc_writer_free(w.w)
		w.w = nil
	}
	if w.versions != nil {
		C.pqc_versions_free(w.versions)
		w.versions = nil
	}
}

// storedVersion: the version obj (bytes ck) is stored on sink. It
// replaces the newest version v holds, and what the rule says is due is
// deleted. A delete that fails stays due, and is tried at the next call.
func storedVersion(v *C.PqcVersions, sink Sink, obj string, ck []byte) error {
	co := C.CString(obj)
	defer C.free(unsafe.Pointer(co))
	var r C.Report
	if C.pqc_versions_stored(v, co, bytesPtr(ck), C.size_t(len(ck)), &r) != 0 {
		return cerr(&r)
	}
	for {
		due := C.pqc_versions_due(v, 0)
		if due == nil {
			return nil
		}
		name := C.GoString(due)
		if err := sink.Delete(name); err != nil {
			return err
		}
		if C.pqc_versions_deleted(v, due, &r) != 0 {
			return cerr(&r)
		}
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
	return readerOf(raw, sink)
}

// readerOf is the Reader of a checkpoint object's bytes.
func readerOf(raw []byte, sink Sink) (*Reader, error) {
	var rep C.Report
	var r *C.PqcReader
	if C.pqc_reader_open(bytesPtr(raw), C.size_t(len(raw)), &r, &rep) != 0 {
		return nil, cerr(&rep)
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

// Finalize turns a checkpoint into the ordinary .parquet file: it can
// be run again, it assembles from the newest checkpoint version whose
// CRC holds, and it deletes the versions last. "done": the file was
// there already; "recovered": it was assembled now. The rules are the
// core's (pqc.h: THE ASSEMBLY OF A FILE), the same ones the Python
// finalize follows: which version, the checks, the sources, and the
// order of the deletes.
func Finalize(name string, sink Sink) (string, error) { return finalize(name, sink, false) }

// FinalizeKeep is Finalize, but the chunks and checkpoint versions stay
// beside the finished file (the Python finalize(keep_checkpoints=True)):
// DiscardCheckpoints deletes them later.
func FinalizeKeep(name string, sink Sink) (string, error) { return finalize(name, sink, true) }

// DiscardCheckpoints deletes a finished file's chunks and checkpoint
// versions, kept while readers were told of them. It refuses unless the
// finished file exists: the checkpoints would be its only copy. The
// order of the deletes is the core's, the same as Finalize's.
func DiscardCheckpoints(name string, sink Sink) error {
	_, err := finish(name, sink, C.PQC_FINISH_DISCARD, false, nil, "")
	return err
}

func finalize(name string, sink Sink, keep bool) (string, error) {
	return finalizeFrom(name, sink, nil, "", keep)
}

// finalizeFrom is finalize; with ckpt, the bytes of the newest version
// (object used) when the caller just put it there (an upload): the
// read-back is then skipped.
func finalizeFrom(name string, sink Sink, ckpt []byte, object string, keep bool) (string, error) {
	assembled, err := finish(name, sink, C.PQC_FINISH_ASSEMBLE_FILE, keep, ckpt, object)
	if err != nil {
		return "", err
	}
	if assembled {
		return "recovered", nil
	}
	return "done", nil
}

// warn, when set, gets each line the core has for the log about an
// assembly (a newer checkpoint version that was passed over). The
// stream package sets it to its logger.
var warn atomic.Pointer[func(text string)]

// SetWarn says where those lines go.
func SetWarn(f func(text string)) { warn.Store(&f) }

// finish does the steps of one assembly (a PqcFinish) on sink: true
// when the file was assembled now. An error of the sink ends it; what
// was done stays done.
func finish(name string, sink Sink, mode C.int, keep bool, given []byte, object string) (bool, error) {
	versions, err := CkptVersions(sink, name)
	if err != nil {
		return false, err
	}
	var r C.Report
	var f *C.PqcFinish
	cn := C.CString(name)
	defer C.free(unsafe.Pointer(cn))
	if C.pqc_finish_new(cn, mode, cbool(keep), &f, &r) != 0 {
		return false, cerr(&r)
	}
	defer C.pqc_finish_free(f)
	for _, v := range versions {
		co := C.CString(v.Object)
		rc := C.pqc_finish_version(f, co, &r)
		C.free(unsafe.Pointer(co))
		if rc != 0 {
			return false, cerr(&r)
		}
	}
	if given != nil {
		co := C.CString(object)
		rc := C.pqc_finish_given(f, co, bytesPtr(given), C.size_t(len(given)), &r)
		C.free(unsafe.Pointer(co))
		if rc != 0 {
			return false, cerr(&r)
		}
	}
	for {
		var step C.PqcFinishStep
		if C.pqc_finish_next(f, &step, &r) != 0 {
			return false, cerr(&r)
		}
		obj := C.GoString(&step.name[0])
		switch step.action {
		case C.PQC_FINISH_DONE:
			return step.flag != 0, nil
		case C.PQC_FINISH_STAT:
			var size int64
			exists := sink.Exists(obj)
			if exists && step.flag == 0 {
				if size, err = sink.Size(obj); err != nil {
					return false, err
				}
			}
			if C.pqc_finish_has(f, cbool(exists), C.uint64_t(size), &r) != 0 {
				return false, cerr(&r)
			}
			continue
		case C.PQC_FINISH_READ:
			raw, err := sink.Get(obj, 0, -1)
			if err != nil {
				// not read is not "not a checkpoint": an older version
				// has fewer rows. The caller tries again later.
				return false, fmt.Errorf("finalize(%q): reading %s: %w", name, obj, err)
			}
			if C.pqc_finish_read(f, bytesPtr(raw), C.size_t(len(raw)), &r) != 0 {
				return false, cerr(&r)
			}
			if w := warn.Load(); w != nil {
				for i := 0; i < int(r.lines); i++ {
					(*w)(C.GoString(&r.line[i].text[0]))
				}
			}
			continue
		case C.PQC_FINISH_ASSEMBLE:
			sources := make([]Source, 0, int(step.size))
			for i := C.uint32_t(0); i < C.uint32_t(step.size); i++ {
				var sn *C.char
				var off, length C.int64_t
				if C.pqc_finish_source(f, i, &sn, &off, &length) == 0 {
					return false, fmt.Errorf("finalize(%q): the core has no source %d", name, int(i))
				}
				sources = append(sources, Source{C.GoString(sn), int64(off), int64(length)})
			}
			if err := sink.Assemble(obj, sources); err != nil {
				return false, err
			}
		case C.PQC_FINISH_LIST:
			listed, err := sink.List(obj)
			if err != nil {
				return false, err
			}
			for _, l := range listed {
				cl := C.CString(l)
				rc := C.pqc_finish_listed(f, cl, &r)
				C.free(unsafe.Pointer(cl))
				if rc != 0 {
					return false, cerr(&r)
				}
			}
		case C.PQC_FINISH_DELETE:
			if err := sink.Delete(obj); err != nil {
				return false, err
			}
		default:
			return false, fmt.Errorf("finalize(%q): the core asked for step %d, "+
				"which this host does not have", name, int(step.action))
		}
		if C.pqc_finish_did(f, &r) != 0 {
			return false, cerr(&r)
		}
	}
}

// DeadAfter is how long a writer with this beat must be silent before
// it is dead: three beats, and never sooner than 30 seconds. It is the
// core's rule (pqc_dead_after), the same one the Python host uses for
// a ticket and for the start-up sweep. A zero beat: the writer does
// not say how often it shows that it lives.
func DeadAfter(beat time.Duration) time.Duration {
	s := float64(C.pqc_dead_after(C.double(beat.Seconds())))
	return time.Duration(s * float64(time.Second))
}

// CRC32 is the core's CRC (IEEE, zlib-compatible) for host-side use.
func CRC32(b []byte) uint32 {
	return uint32(C.pqc_crc32(bytesPtr(b), C.size_t(len(b))))
}
