package streaming

// sp_core.c and sp.h are CHECKED-IN COPIES of ../../core/sp.{c,h}, the
// stream core (copies, not a symlink or an include from outside the
// package, so the Go build cache tracks their content).
// src/go/sync_core.py refreshes them, and the Python-against-Go tests
// refuse to pass while one differs from the core.
//
// The core holds the rules that every host language shares, written
// once. This file is how the Go host calls it. A rule that is in the
// core is not written here.

/*
#cgo CFLAGS: -std=c11 -O2
#cgo linux CFLAGS: -D_POSIX_C_SOURCE=200809L
#cgo LDFLAGS: -lm
#include <stdlib.h>
#include "sp.h"
*/
import "C"

import (
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/neofactory/streaming-parquet/src/go/pqc"
)

// ---- the report (report.h) ----
//
// Every call into the core that can fail, or that can have a line for
// the log, is given a report. The call's status and error text become
// a Go error; its lines come back to the caller, which logs them.

// coreLine is a line the core has for the log, with its level and the
// time the core made it.
type coreLine struct {
	level slog.Level
	text  string
	wall  time.Time
}

// report is one call's Report. A new one for each call: a goroutine
// has no thread of its own to keep one on.
type report struct{ c C.Report }

// err is the call's error: nil when its status is REPORT_OK.
func (r *report) err() error {
	if r.c.status == C.REPORT_OK {
		return nil
	}
	return fmt.Errorf("%s", C.GoString(&r.c.error[0]))
}

// refused reports whether the call answered REPORT_REFUSED.
func (r *report) refused() bool { return r.c.status == C.REPORT_REFUSED }

// lines are the lines the call made for the log, oldest first.
func (r *report) lines() []coreLine {
	var out []coreLine
	for i := 0; i < int(r.c.lines); i++ {
		l := &r.c.line[i]
		level := slog.LevelInfo
		switch l.level {
		case C.REPORT_WARNING:
			level = slog.LevelWarn
		case C.REPORT_ERROR:
			level = slog.LevelError
		}
		out = append(out, coreLine{level, C.GoString(&l.text[0]),
			time.Unix(0, int64(float64(l.wall)*1e9))})
	}
	if r.c.dropped > 0 {
		out = append(out, coreLine{slog.LevelWarn, fmt.Sprintf(
			"%d more lines of this call did not fit in its report", int(r.c.dropped)),
			time.Now()})
	}
	return out
}

// text is the lines' text alone, joined: for a call that makes one.
func (r *report) text() string {
	var out string
	for i, l := range r.lines() {
		if i > 0 {
			out += " "
		}
		out += l.text
	}
	return out
}

// setCoreClock is for tests: the time the core sees from now on, until
// systemCoreClock.
func setCoreClock(wall, mono float64) { C.sp_test_set_clock(1, C.double(wall), C.double(mono)) }
func systemCoreClock()                { C.sp_test_set_clock(0, 0, 0) }

// coreTicket builds the core's ticket from t. The caller frees it.
func coreTicket(t *Ticket) (*C.SpTicket, error) {
	var r report
	var ct *C.SpTicket
	if C.sp_ticket_new(&ct, &r.c) != 0 {
		return nil, r.err()
	}
	fail := func() (*C.SpTicket, error) {
		C.sp_ticket_free(ct)
		return nil, r.err()
	}
	text := func(field C.int, s string) bool {
		if s == "" {
			return true
		}
		cs := C.CString(s)
		defer C.free(unsafe.Pointer(cs))
		return C.sp_ticket_set_text(ct, field, cs, &r.c) == 0
	}
	integer := func(field C.int, v *int64) bool {
		return v == nil || C.sp_ticket_set_integer(ct, field, C.int64_t(*v), &r.c) == 0
	}
	number := func(field C.int, v float64) bool {
		return v == 0 || C.sp_ticket_set_number(ct, field, C.double(v), &r.c) == 0
	}
	var port, revision *int64
	if t.Port != 0 {
		p := int64(t.Port)
		port = &p
	}
	if t.Revision != 0 {
		revision = &t.Revision
	}
	ok := text(C.SP_T_NAME, t.Name) && text(C.SP_T_HOST, t.Host) &&
		integer(C.SP_T_PORT, port) && text(C.SP_T_RUN, t.Run) &&
		number(C.SP_T_BEAT_SECONDS, t.BeatSeconds) &&
		number(C.SP_T_STALE_AFTER, t.StaleAfter) &&
		number(C.SP_T_CHECKPOINT_SECONDS, t.CheckpointSeconds) &&
		number(C.SP_T_UPLOAD_SECONDS, t.UploadSeconds) &&
		integer(C.SP_T_STORED_ROW, t.StoredRow) &&
		number(C.SP_T_STORED_AT, t.StoredAt) &&
		number(C.SP_T_KEEP_SECONDS, t.KeepSeconds) &&
		text(C.SP_T_CATALOG_ETAG, t.CatalogEtag) &&
		integer(C.SP_T_REVISION, revision) && number(C.SP_T_TS, t.Ts) &&
		text(C.SP_T_ARCHIVE, t.Archive)
	if !ok {
		return fail()
	}
	for _, f := range t.Files {
		cs := C.CString(f.Checkpoint)
		ptr := func(v *int64) *C.int64_t { return (*C.int64_t)(unsafe.Pointer(v)) }
		rc := C.sp_ticket_add_file(ct, cs, ptr(f.FirstRow), ptr(f.FinalRows),
			ptr(f.Rows), &r.c)
		C.free(unsafe.Pointer(cs))
		if rc != 0 {
			return fail()
		}
	}
	return ct, nil
}

// ticketText is the ticket's JSON text, as the core writes it.
func ticketText(t *Ticket) ([]byte, error) {
	ct, err := coreTicket(t)
	if err != nil {
		return nil, err
	}
	defer C.sp_ticket_free(ct)
	return writtenTicket(ct)
}

// writtenTicket is the JSON text of a core ticket.
func writtenTicket(ct *C.SpTicket) ([]byte, error) {
	var r report
	var p *C.char
	var n C.size_t
	if C.sp_ticket_write(ct, &p, &n, &r.c) != 0 {
		return nil, r.err()
	}
	defer C.sp_free(unsafe.Pointer(p))
	return C.GoBytes(unsafe.Pointer(p), C.int(n)), nil
}

// ticketFromText reads a ticket's JSON text, as the core reads it. The
// core refuses a known member of the wrong kind, naming it.
func ticketFromText(b []byte) (*Ticket, error) {
	var r report
	var p *C.char
	if len(b) > 0 {
		p = (*C.char)(unsafe.Pointer(&b[0]))
	}
	var ct *C.SpTicket
	if C.sp_ticket_read(p, C.size_t(len(b)), &ct, &r.c) != 0 {
		return nil, r.err()
	}
	defer C.sp_ticket_free(ct)
	text := func(field C.int) string {
		if s := C.sp_ticket_text(ct, field); s != nil {
			return C.GoString(s)
		}
		return ""
	}
	integer := func(field C.int) *int64 {
		var v C.int64_t
		if C.sp_ticket_integer(ct, field, &v) == 0 {
			return nil
		}
		out := int64(v)
		return &out
	}
	number := func(field C.int) float64 {
		var v C.double
		C.sp_ticket_number(ct, field, &v)
		return float64(v)
	}
	t := &Ticket{
		Name: text(C.SP_T_NAME), Host: text(C.SP_T_HOST), Run: text(C.SP_T_RUN),
		BeatSeconds:       number(C.SP_T_BEAT_SECONDS),
		StaleAfter:        number(C.SP_T_STALE_AFTER),
		CheckpointSeconds: number(C.SP_T_CHECKPOINT_SECONDS),
		UploadSeconds:     number(C.SP_T_UPLOAD_SECONDS),
		StoredRow:         integer(C.SP_T_STORED_ROW),
		StoredAt:          number(C.SP_T_STORED_AT),
		KeepSeconds:       number(C.SP_T_KEEP_SECONDS),
		CatalogEtag:       text(C.SP_T_CATALOG_ETAG),
		Ts:                number(C.SP_T_TS),
		Archive:           text(C.SP_T_ARCHIVE),
	}
	if v := integer(C.SP_T_PORT); v != nil {
		t.Port = int(*v)
	}
	if v := integer(C.SP_T_REVISION); v != nil {
		t.Revision = *v
	}
	for i := C.int32_t(0); i < C.sp_ticket_file_count(ct); i++ {
		var cp *C.char
		var first, final, rows C.int64_t
		var hasFirst, hasFinal, hasRows C.int
		C.sp_ticket_file(ct, i, &cp, &first, &hasFirst, &final, &hasFinal, &rows, &hasRows)
		opt := func(has C.int, v C.int64_t) *int64 {
			if has == 0 {
				return nil
			}
			out := int64(v)
			return &out
		}
		t.Files = append(t.Files, TicketFile{Checkpoint: C.GoString(cp),
			FirstRow: opt(hasFirst, first), FinalRows: opt(hasFinal, final),
			Rows: opt(hasRows, rows)})
	}
	return t, nil
}

// isStale reports whether the ticket's writer is dead, by the wall
// clock now: the core's rule. A ticket that names no stale_after gets
// the rule for a writer with no beat.
func (t *Ticket) isStale() bool {
	ct, err := coreTicket(t)
	if err != nil {
		return true
	}
	defer C.sp_ticket_free(ct)
	return C.sp_ticket_is_stale(ct, C.double(pqc.DeadAfter(0).Seconds())) != 0
}

// staleAfter is the seconds after its write at which the ticket means a
// dead writer: the core's rule.
func (t *Ticket) staleAfter() float64 {
	ct, err := coreTicket(t)
	if err != nil {
		return pqc.DeadAfter(0).Seconds()
	}
	defer C.sp_ticket_free(ct)
	return float64(C.sp_ticket_stale_after(ct, C.double(pqc.DeadAfter(0).Seconds())))
}

// ---- the ownership decision (sp.h: THE OWNERSHIP DECISION) ----

// lockRecord is the text a lock holder writes into its lock file.
func lockRecord(pid int, host string, port int, run string) []byte {
	ch, cr := C.CString(host), C.CString(run)
	defer C.free(unsafe.Pointer(ch))
	defer C.free(unsafe.Pointer(cr))
	var r report
	var p *C.char
	var n C.size_t
	if C.sp_lock_record(C.int64_t(pid), ch, C.int64_t(port), cr, &p, &n, &r.c) != 0 {
		return nil
	}
	defer C.sp_free(unsafe.Pointer(p))
	return C.GoBytes(unsafe.Pointer(p), C.int(n))
}

// lockRefusal is the refusal for a lock another process holds; holder
// is the text of the lock file.
func lockRefusal(who, lockPath string, holder []byte) string {
	cw, cp := C.CString(who), C.CString(lockPath)
	defer C.free(unsafe.Pointer(cw))
	defer C.free(unsafe.Pointer(cp))
	var h *C.char
	if len(holder) > 0 {
		h = (*C.char)(unsafe.Pointer(&holder[0]))
	}
	var r report
	C.sp_lock_refusal(cw, cp, h, C.size_t(len(holder)), &r.c)
	return C.GoString(&r.c.error[0])
}

// ownershipDecision is the ticket check: own, take over (with the
// warning to log) or refused (with the error). ticket nil = none there.
func ownershipDecision(who, name, store string, ticket *Ticket,
	host string) (decision int, message string) {
	var ct *C.SpTicket
	if ticket != nil {
		var err error
		if ct, err = coreTicket(ticket); err != nil {
			return int(C.SP_REFUSED), fmt.Sprintf("%s: the ticket in %s: %v", who, store, err)
		}
		defer C.sp_ticket_free(ct)
	}
	cw, cn, cs, ch := C.CString(who), C.CString(name), C.CString(store), C.CString(host)
	defer C.free(unsafe.Pointer(cw))
	defer C.free(unsafe.Pointer(cn))
	defer C.free(unsafe.Pointer(cs))
	defer C.free(unsafe.Pointer(ch))
	var r report
	var d C.int
	if C.sp_ownership(cw, cn, cs, ct, ch, C.double(pqc.DeadAfter(0).Seconds()),
		&d, &r.c) != 0 {
		return int(C.SP_REFUSED), C.GoString(&r.c.error[0])
	}
	return int(d), r.text()
}

// ownershipUnread is the warning for a ticket that could not be read.
func ownershipUnread(name, store string, why error) string {
	cn, cs, cw := C.CString(name), C.CString(store), C.CString(why.Error())
	defer C.free(unsafe.Pointer(cn))
	defer C.free(unsafe.Pointer(cs))
	defer C.free(unsafe.Pointer(cw))
	var r report
	C.sp_ownership_unread(cn, cs, cw, &r.c)
	return r.text()
}

const (
	ownOwn      = int(C.SP_OWN)
	ownTakeOver = int(C.SP_TAKE_OVER)
	ownRefused  = int(C.SP_REFUSED)
)

// ---- the write plan (sp.h: THE WRITE PLAN) ----

const (
	writeNothing    = int(C.SP_W_NOTHING)
	writeRowGroup   = int(C.SP_W_ROW_GROUP)
	writeCheckpoint = int(C.SP_W_CHECKPOINT)
)

// writePlan is one stream's write plan in the core: which committed
// rows go to its files, and when. The owner's lock covers every call.
// The plan counts its times on the core's monotonic clock.
type writePlan struct{ p *C.SpWrites }

// newWritePlan: seconds of 0 mean none (no row group by time, no
// checkpoints), as ParquetOptions says.
func newWritePlan(rowGroupRows int64, rowGroupSeconds, checkpointSeconds float64) (*writePlan, error) {
	var r report
	w := &writePlan{}
	if C.sp_writes_new(&w.p, &r.c) != 0 {
		return nil, r.err()
	}
	runtime.SetFinalizer(w, func(w *writePlan) { C.sp_writes_free(w.p) })
	var rgs, cs *C.double
	if rowGroupSeconds > 0 {
		v := C.double(rowGroupSeconds)
		rgs = &v
	}
	if checkpointSeconds > 0 {
		v := C.double(checkpointSeconds)
		cs = &v
	}
	if C.sp_writes_set_options(w.p, C.int64_t(rowGroupRows), rgs, cs, &r.c) != 0 {
		return nil, r.err()
	}
	return w, nil
}

func (w *writePlan) start(committed int64) { C.sp_writes_start(w.p, C.int64_t(committed)) }
func (w *writePlan) stop()                 { C.sp_writes_stop(w.p) }
func (w *writePlan) on() bool              { return C.sp_writes_on(w.p) != 0 }
func (w *writePlan) cursor() int64         { return int64(C.sp_writes_cursor(w.p)) }
func (w *writePlan) planned() int64        { return int64(C.sp_writes_planned(w.p)) }
func (w *writePlan) lost() int64           { return int64(C.sp_writes_lost(w.p)) }

// commit reports the kind of write due for the rows committed so far.
func (w *writePlan) commit(committed int64) int {
	var begins C.int
	return int(C.sp_writes_commit(w.p, C.int64_t(committed), &begins))
}

// plan is the write for a pin that holds the rows [lo, hi); the plan
// moves with it. lost: rows before lo that the ring had reused.
func (w *writePlan) plan(forceRowGroup bool, lo, hi int64) (kind int, from, to, lost int64) {
	var out C.SpWrite
	force := C.int(0)
	if forceRowGroup {
		force = 1
	}
	C.sp_writes_plan(w.p, force, C.int64_t(lo), C.int64_t(hi), &out)
	return int(out.kind), int64(out.lo), int64(out.hi), int64(out.lost)
}

// ---- what the store holds (sp.h: WHAT THE STORE HOLDS) ----

// storeState is what the store holds of one stream's run, in the core:
// the ticket's stored row and files. It has its own lock: the stream's
// goroutine writes it and the ticket's goroutine reads it.
//
// The calls that tell it what landed fail only when memory is out; the
// first such error is kept, and fill, which every ticket write makes,
// returns it.
type storeState struct {
	mu     sync.Mutex
	p      *C.SpStore
	failed error
}

func newStoreState(keep time.Duration) *storeState {
	var r report
	st := &storeState{}
	if C.sp_store_new(C.double(keep.Seconds()), &st.p, &r.c) != 0 {
		panic(fmt.Sprintf("streaming: the store state, keep %v: %v", keep, r.err()))
	}
	runtime.SetFinalizer(st, func(st *storeState) { C.sp_store_free(st.p) })
	return st
}

// took notes a call's failure. The caller holds mu.
func (st *storeState) took(r *report) {
	if err := r.err(); err != nil && st.failed == nil {
		st.failed = err
	}
}

// begins: the file's first row has the stream row number firstRow.
func (st *storeState) begins(file string, firstRow int64) {
	cf := C.CString(file)
	defer C.free(unsafe.Pointer(cf))
	st.mu.Lock()
	defer st.mu.Unlock()
	var r report
	C.sp_store_begins(st.p, cf, C.int64_t(firstRow), &r.c)
	st.took(&r)
}

// stored: the checkpoint version is written and in the store; it holds
// finalRows in finished row groups, and rows in all.
func (st *storeState) stored(file string, version int, checkpoint string, finalRows, rows int64) {
	cf, cc := C.CString(file), C.CString(checkpoint)
	defer C.free(unsafe.Pointer(cf))
	defer C.free(unsafe.Pointer(cc))
	st.mu.Lock()
	defer st.mu.Unlock()
	var r report
	C.sp_store_covers(st.p, cf, C.int64_t(version), cc, C.int64_t(finalRows), C.int64_t(rows), &r.c)
	st.took(&r)
	C.sp_store_stored(st.p, cf, C.int64_t(version), cc, C.int64_t(rows), &r.c)
	st.took(&r)
}

// covers: a checkpoint version is written in the first copy, which is
// not the store (a spool): it holds finalRows in finished row groups,
// and rows in all. The store has it when the upload sends it (inStore).
func (st *storeState) covers(file string, version int, checkpoint string, finalRows, rows int64) {
	cf, cc := C.CString(file), C.CString(checkpoint)
	defer C.free(unsafe.Pointer(cf))
	defer C.free(unsafe.Pointer(cc))
	st.mu.Lock()
	defer st.mu.Unlock()
	var r report
	C.sp_store_covers(st.p, cf, C.int64_t(version), cc, C.int64_t(finalRows), C.int64_t(rows), &r.c)
	st.took(&r)
}

// inStore: the upload put the checkpoint version in the store; it holds
// rows of the file.
func (st *storeState) inStore(file string, version int, checkpoint string, rows int64) {
	cf, cc := C.CString(file), C.CString(checkpoint)
	defer C.free(unsafe.Pointer(cf))
	defer C.free(unsafe.Pointer(cc))
	st.mu.Lock()
	defer st.mu.Unlock()
	var r report
	C.sp_store_stored(st.p, cf, C.int64_t(version), cc, C.int64_t(rows), &r.c)
	st.took(&r)
}

// finished: the file is finished in the store; it leaves the ticket.
func (st *storeState) finished(file string) {
	cf := C.CString(file)
	defer C.free(unsafe.Pointer(cf))
	st.mu.Lock()
	defer st.mu.Unlock()
	var r report
	C.sp_store_finished(st.p, cf, &r.c)
	st.took(&r)
}

// kept: a file an earlier run finished, found beside its checkpoint
// version; readers may have been told of it.
func (st *storeState) kept(file string, version int, checkpoint string) {
	cf, cc := C.CString(file), C.CString(checkpoint)
	defer C.free(unsafe.Pointer(cf))
	defer C.free(unsafe.Pointer(cc))
	st.mu.Lock()
	defer st.mu.Unlock()
	var r report
	C.sp_store_stored(st.p, cf, C.int64_t(version), cc, 0, &r.c)
	st.took(&r)
}

// awaits: the file is finished in the store with its checkpoints kept;
// it stays in the ticket until the catalog lists it.
func (st *storeState) awaits(file string) {
	cf := C.CString(file)
	defer C.free(unsafe.Pointer(cf))
	st.mu.Lock()
	defer st.mu.Unlock()
	var r report
	C.sp_store_awaits(st.p, cf, &r.c)
	st.took(&r)
}

// listed: a catalog of the version `version` is published and lists
// `files`. In one step the awaiting files among them leave the ticket
// and the ticket names that version: a ticket with the version and one
// of the files would give a view the file's rows twice. True when a
// file left the ticket.
func (st *storeState) listed(files []string, version string) bool {
	cv := C.CString(version)
	defer C.free(unsafe.Pointer(cv))
	st.mu.Lock()
	defer st.mu.Unlock()
	left := false
	for _, f := range files {
		cf := C.CString(f)
		if C.sp_store_listed(st.p, cf) == 1 {
			left = true
		}
		C.free(unsafe.Pointer(cf))
	}
	var r report
	if version == "" {
		C.sp_store_catalog(st.p, nil, &r.c)
	} else {
		C.sp_store_catalog(st.p, cv, &r.c)
	}
	st.took(&r)
	return left
}

// dropped: the ticket without the listed files is written.
func (st *storeState) dropped() {
	st.mu.Lock()
	defer st.mu.Unlock()
	C.sp_store_dropped(st.p)
}

// due: the files whose checkpoints are due for deletion now.
func (st *storeState) due() []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	var out []string
	for i := 0; ; i++ {
		p := C.sp_store_due(st.p, C.int32_t(i))
		if p == nil {
			return out
		}
		out = append(out, C.GoString(p))
	}
}

// discarded: the file's checkpoints are deleted.
func (st *storeState) discarded(file string) {
	cf := C.CString(file)
	defer C.free(unsafe.Pointer(cf))
	st.mu.Lock()
	defer st.mu.Unlock()
	var r report
	C.sp_store_discarded(st.p, cf, &r.c)
	st.took(&r)
}

// fill puts what the store holds into t: the stored row and when, the
// keep time, the files. where is the stream's folder, ending in "/".
func (st *storeState) fill(t *Ticket, where string) error {
	var r report
	var ct *C.SpTicket
	if C.sp_ticket_new(&ct, &r.c) != 0 {
		return r.err()
	}
	defer C.sp_ticket_free(ct)
	cw := C.CString(where)
	defer C.free(unsafe.Pointer(cw))
	st.mu.Lock()
	failed := st.failed
	rc := C.sp_store_fill(st.p, ct, cw, &r.c)
	st.mu.Unlock()
	if failed != nil {
		return failed
	}
	if rc != 0 {
		return r.err()
	}
	text, err := writtenTicket(ct)
	if err != nil {
		return err
	}
	got, err := ticketFromText(text)
	if err != nil {
		return err
	}
	t.StoredRow, t.StoredAt = got.StoredRow, got.StoredAt
	t.KeepSeconds, t.Files = got.KeepSeconds, got.Files
	t.CatalogEtag = got.CatalogEtag
	return nil
}

// ---- the reader (sp.h: THE READER) ----------------------------------------

// What a poll found, and a reader's next step: the core's values.
const (
	foundNone       = int(C.SP_FOUND_NONE)
	foundTicket     = int(C.SP_FOUND_TICKET)
	foundLocal      = int(C.SP_FOUND_LOCAL)
	foundUnreadable = int(C.SP_FOUND_UNREADABLE)

	doWait    = int(C.SP_DO_WAIT)
	doConnect = int(C.SP_DO_CONNECT)
	doLocal   = int(C.SP_DO_LOCAL)
	doList    = int(C.SP_DO_LIST)
	doFail    = int(C.SP_DO_FAIL)
)

// readerStep is one step of a reader: what to do, the state's name
// after it, whether to drop the ring in hand first, and for doWait how
// long.
type readerStep struct {
	action    int
	state     string
	freshRing bool
	wait      time.Duration
}

// readerRules is the core's reader: where the writer is, when to wait,
// when to start again. The reader's own goroutine makes its calls.
type readerRules struct {
	r    *C.SpReader
	said []coreLine // the lines of the steps since the last messages()
}

func newReaderRules(who, name, store string, direct bool) (*readerRules, error) {
	cw, cn := C.CString(who), C.CString(name)
	defer C.free(unsafe.Pointer(cw))
	defer C.free(unsafe.Pointer(cn))
	var cs *C.char
	if store != "" {
		cs = C.CString(store)
		defer C.free(unsafe.Pointer(cs))
	}
	d := C.int(0)
	if direct {
		d = 1
	}
	var rep report
	rr := &readerRules{}
	if C.sp_reader_new(cw, cn, cs, d, &rr.r, &rep.c) != 0 {
		return nil, fmt.Errorf("%s: %v", who, rep.err())
	}
	runtime.SetFinalizer(rr, func(rr *readerRules) { C.sp_reader_free(rr.r) })
	return rr, nil
}

func readerStepOf(st *C.SpStep) readerStep {
	return readerStep{action: int(st.action),
		state:     C.GoString(C.sp_reader_state_name(st.state)),
		freshRing: st.fresh_ring != 0,
		wait:      time.Duration(float64(st.wait_seconds) * float64(time.Second))}
}

func (rr *readerRules) folder() string { return C.GoString(C.sp_reader_folder(rr.r)) }

// poll tells the core what a poll found. ticket: for foundTicket. why:
// the error for foundUnreadable.
func (rr *readerRules) poll(found int, ticket *Ticket, why error) readerStep {
	var ct *C.SpTicket
	if found == foundTicket && ticket != nil {
		var err error
		if ct, err = coreTicket(ticket); err != nil {
			found, why = foundUnreadable, err
		} else {
			defer C.sp_ticket_free(ct)
		}
	}
	var cw *C.char
	if why != nil {
		cw = C.CString(why.Error())
		defer C.free(unsafe.Pointer(cw))
	}
	var st C.SpStep
	var rep report
	C.sp_reader_poll(rr.r, C.int(found), ct, cw,
		C.double(pqc.DeadAfter(0).Seconds()), &st, &rep.c)
	rr.said = append(rr.said, rep.lines()...)
	return readerStepOf(&st)
}

func (rr *readerRules) folderIs(exists bool) readerStep {
	e := C.int(0)
	if exists {
		e = 1
	}
	var st C.SpStep
	var rep report
	C.sp_reader_folder_is(rr.r, e, &st, &rep.c)
	rr.said = append(rr.said, rep.lines()...)
	return readerStepOf(&st)
}

func (rr *readerRules) connected() { C.sp_reader_connected(rr.r) }

// lost: the connection ended (why nil: without an error, or the local
// stream closed).
func (rr *readerRules) lost(why error) readerStep {
	var cw *C.char
	if why != nil {
		cw = C.CString(why.Error())
		defer C.free(unsafe.Pointer(cw))
	}
	var st C.SpStep
	var rep report
	C.sp_reader_lost(rr.r, cw, &st, &rep.c)
	rr.said = append(rr.said, rep.lines()...)
	return readerStepOf(&st)
}

func (rr *readerRules) close()         { C.sp_reader_close(rr.r) }
func (rr *readerRules) reason() string { return C.GoString(C.sp_reader_reason(rr.r)) }

// messages are the lines for the log that the steps since the last
// call made.
func (rr *readerRules) messages() []coreLine {
	out := rr.said
	rr.said = nil
	return out
}
