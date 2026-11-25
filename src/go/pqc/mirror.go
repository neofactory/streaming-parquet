package pqc

/*
#include <stdlib.h>
#include "pqc.h"
*/
import "C"

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

// Mirror copies one file's objects and checkpoint versions from a local
// sink to a remote one, and ends the file in both. The two-copies
// model: the writer writes to the local copy at its own cadence, never
// held up by the network; the mirror sends to the remote at the cadence
// the link allows.
//
// The RULES are the C core's (pqc.h: THE MIRROR): what is sent, the
// order, what is deleted, and the steps of the end of a file. This type
// does what the core asks: it reads the local sink, and it puts, deletes
// and assembles on the remote. The Python Shipper does the same with
// the same core.
//
// One goroutine at a time calls Run; Doing may be read from another.
type Mirror struct {
	name          string
	local, remote Sink
	m             *C.PqcMirror
	lastObject    string
	done          bool
	doing         atomic.Value // string
}

// MirrorStatus is what one pass did.
type MirrorStatus struct {
	// State: "idle" (nothing local yet), "synced" (nothing new),
	// "shipped" (objects or a version copied: Object names it and Rows
	// is its rows), or "local-finalized" (the finished file is in the
	// local copy, and no version).
	State  string
	Chunks int    // objects copied in this pass
	Object string // the newest version on the remote
	Rows   uint64
	Done   bool // the file is finished in both copies
	// the seconds the two assemblies took, when this pass did them
	LocalSeconds, RemoteSeconds float64
}

// NewMirror: keep is how long a replaced remote version stays.
func NewMirror(name string, local, remote Sink, keep time.Duration) (*Mirror, error) {
	cn := C.CString(name)
	defer C.free(unsafe.Pointer(cn))
	var r C.Report
	var m *C.PqcMirror
	if C.pqc_mirror_new(cn, C.double(keep.Seconds()), &m, &r) != 0 {
		return nil, cerr(&r)
	}
	out := &Mirror{name: name, local: local, remote: remote, m: m}
	out.doing.Store("")
	// a mirror its owner dropped without Free
	runtime.SetFinalizer(out, (*Mirror).Free)
	return out, nil
}

// Free releases the core's part. A Mirror is not used after it.
func (m *Mirror) Free() {
	if m.m != nil {
		C.pqc_mirror_free(m.m)
		m.m = nil
	}
}

// Name is the file's name.
func (m *Mirror) Name() string { return m.name }

// Doing says what the mirror is doing on the remote right now, for a
// caller that waits on it ("uploading object x (3.4 MB)"); "" between
// steps.
func (m *Mirror) Doing() string { return m.doing.Load().(string) }

// LastObject is the newest version this mirror stored on the remote,
// "" for none.
func (m *Mirror) LastObject() string { return m.lastObject }

// Run is one pass, as the core asks. live: the writer still has the
// file open (the pass mirrors it and stops); false: the writer ended
// it, and the pass also takes the steps of the end of the file, the
// local assembly and then the remote one (keepCheckpoints: leave the
// remote chunks and versions for readers that were told of them).
// stored(object, rows), when not nil, is called at the moment the remote
// holds a new version, also in a pass that fails after it: what a
// ticket may say. An error of a sink ends the pass; the next pass goes
// on from what was done.
func (m *Mirror) Run(live, keepCheckpoints bool, stored func(object string, rows uint64)) (MirrorStatus, error) {
	if m.done {
		return MirrorStatus{State: "idle", Done: true}, nil
	}
	versions, err := CkptVersions(m.local, m.name)
	if err != nil {
		return MirrorStatus{}, err
	}
	var object string
	var raw []byte
	final := false
	st := MirrorStatus{State: "idle"}
	if len(versions) > 0 {
		object = versions[len(versions)-1].Object
		if raw, err = m.local.Get(object, 0, -1); err != nil {
			return MirrorStatus{}, err
		}
		st.State = "synced"
	} else if final = m.local.Exists(m.name); final {
		st.State = "local-finalized"
	}
	var r C.Report
	var co *C.char
	if object != "" {
		co = C.CString(object)
		defer C.free(unsafe.Pointer(co))
	}
	cbool := func(b bool) C.int {
		if b {
			return 1
		}
		return 0
	}
	if C.pqc_mirror_local(m.m, co, bytesPtr(raw), C.size_t(len(raw)),
		cbool(final), cbool(live), &r) != 0 {
		return MirrorStatus{}, cerr(&r)
	}
	if st.State == "synced" {
		st.Object = m.lastObject
	}
	fail := func(err error) (MirrorStatus, error) {
		C.pqc_mirror_failed(m.m) // the pass is over; the next goes on
		m.doing.Store("")
		return st, err
	}
	for {
		var step C.PqcMirrorStep
		if C.pqc_mirror_next(m.m, &step, &r) != 0 {
			return fail(cerr(&r))
		}
		name := C.GoString(&step.name[0])
		m.doing.Store(C.GoString(C.pqc_mirror_doing(m.m)))
		switch step.action {
		case C.PQC_MIRROR_NOTHING:
			m.doing.Store("")
			return st, nil
		case C.PQC_MIRROR_DONE:
			m.doing.Store("")
			m.done, st.Done = true, true
			return st, nil
		case C.PQC_MIRROR_STAT:
			var size int64
			exists := m.remote.Exists(name)
			if exists {
				if size, err = m.remote.Size(name); err != nil {
					return fail(err)
				}
			}
			if C.pqc_mirror_remote_has(m.m, cbool(exists), C.uint64_t(size), &r) != 0 {
				return fail(cerr(&r))
			}
			continue
		case C.PQC_MIRROR_UPLOAD:
			data, err := m.local.Get(name, 0, -1)
			if err == nil {
				err = m.remote.Put(name, data)
			}
			if err != nil {
				return fail(err)
			}
			st.Chunks++
		case C.PQC_MIRROR_UPLOAD_VERSION:
			if err := m.remote.Put(name, raw); err != nil { // stored whole before any delete
				return fail(err)
			}
			m.lastObject = name
		case C.PQC_MIRROR_STORED:
			st.State, st.Object, st.Rows = "shipped", name, uint64(step.size)
			if stored != nil {
				stored(name, uint64(step.size))
			}
		case C.PQC_MIRROR_DELETE:
			if err := m.remote.Delete(name); err != nil {
				return fail(err)
			}
		case C.PQC_MIRROR_ASSEMBLE_LOCAL:
			t0 := time.Now()
			if _, err := Finalize(m.name, m.local); err != nil {
				return fail(err)
			}
			st.LocalSeconds = time.Since(t0).Seconds()
		case C.PQC_MIRROR_ASSEMBLE_REMOTE:
			t0 := time.Now()
			if err := m.assembleRemote(step.flag != 0, keepCheckpoints); err != nil {
				return fail(err)
			}
			st.RemoteSeconds = time.Since(t0).Seconds()
		default:
			return fail(fmt.Errorf("mirror of %s: the core asked for step %d, "+
				"which this host does not have", m.name, int(step.action)))
		}
		if C.pqc_mirror_did(m.m, &r) != 0 {
			return fail(cerr(&r))
		}
	}
}

// assembleRemote finishes the remote copy from its own objects: the
// same bytes as the local file.
func (m *Mirror) assembleRemote(fromShipped, keep bool) error {
	if fromShipped {
		// the newest remote version is the one this mirror put there:
		// from the bytes in hand, no read-back
		var ck *C.uint8_t
		var n C.size_t
		var obj *C.char
		if C.pqc_mirror_shipped(m.m, &ck, &n, &obj) == 0 {
			return fmt.Errorf("mirror of %s: the core has no shipped version", m.name)
		}
		_, err := finalizeFrom(m.name, m.remote,
			C.GoBytes(unsafe.Pointer(ck), C.int(n)), C.GoString(obj), keep)
		return err
	}
	versions, err := CkptVersions(m.remote, m.name)
	if err != nil {
		return err
	}
	if len(versions) > 0 {
		_, err := finalize(m.name, m.remote, keep)
		return err
	}
	if m.remote.Exists(m.name) {
		return nil
	}
	return fmt.Errorf("mirror of %s: nothing was sent to the remote, and "+
		"the local copy has no version to send", m.name)
}

// The sweep at start (pqc.h: THE SWEEP AT START): what to do with a
// file an earlier run left with checkpoint versions.
const (
	SweepNothing      = int(C.PQC_SWEEP_NOTHING)
	SweepFinish       = int(C.PQC_SWEEP_FINISH)
	SweepMirror       = int(C.PQC_SWEEP_MIRROR)
	SweepAwaitCatalog = int(C.PQC_SWEEP_AWAIT_CATALOG)
	SweepDiscard      = int(C.PQC_SWEEP_DISCARD)
)

func cint(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

// SweepLocal: in the local copy. finalExists: the finished file is
// beside the versions. hasRemote: the local copy is mirrored.
// keepsCheckpoints: readers are told of the store's unfinished files.
func SweepLocal(finalExists, hasRemote, keepsCheckpoints bool) int {
	return int(C.pqc_sweep_local(cint(finalExists), cint(hasRemote), cint(keepsCheckpoints)))
}

// SweepRemote: on the remote, in the first pass. inFlight: the file
// already has a mirror.
func SweepRemote(finalExists, keepsCheckpoints, inFlight bool) int {
	return int(C.pqc_sweep_remote(cint(finalExists), cint(keepsCheckpoints), cint(inFlight)))
}
