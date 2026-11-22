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

const coreErrLen = 256

// coreTicket builds the core's ticket from t. The caller frees it.
func coreTicket(t *Ticket) (*C.SpTicket, error) {
	ct := C.sp_ticket_new()
	if ct == nil {
		return nil, fmt.Errorf("ticket: out of memory")
	}
	var eb [coreErrLen]C.char
	fail := func() (*C.SpTicket, error) {
		C.sp_ticket_free(ct)
		return nil, fmt.Errorf("%s", C.GoString(&eb[0]))
	}
	text := func(field C.int, s string) bool {
		if s == "" {
			return true
		}
		cs := C.CString(s)
		defer C.free(unsafe.Pointer(cs))
		return C.sp_ticket_set_text(ct, field, cs, &eb[0], coreErrLen) == 0
	}
	integer := func(field C.int, v *int64) bool {
		return v == nil || C.sp_ticket_set_integer(ct, field, C.int64_t(*v), &eb[0], coreErrLen) == 0
	}
	number := func(field C.int, v float64) bool {
		return v == 0 || C.sp_ticket_set_number(ct, field, C.double(v), &eb[0], coreErrLen) == 0
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
		number(C.SP_T_SHIP_SECONDS, t.ShipSeconds) &&
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
			ptr(f.Rows), &eb[0], coreErrLen)
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
	var n C.size_t
	p := C.sp_ticket_write(ct, &n)
	if p == nil {
		return nil, fmt.Errorf("ticket: out of memory")
	}
	defer C.sp_free(unsafe.Pointer(p))
	return C.GoBytes(unsafe.Pointer(p), C.int(n)), nil
}

// ticketFromText reads a ticket's JSON text, as the core reads it. The
// core refuses a known member of the wrong kind, naming it.
func ticketFromText(b []byte) (*Ticket, error) {
	var eb [coreErrLen]C.char
	var p *C.char
	if len(b) > 0 {
		p = (*C.char)(unsafe.Pointer(&b[0]))
	}
	ct := C.sp_ticket_read(p, C.size_t(len(b)), &eb[0], coreErrLen)
	if ct == nil {
		return nil, fmt.Errorf("%s", C.GoString(&eb[0]))
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
		ShipSeconds:       number(C.SP_T_SHIP_SECONDS),
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

// isStale reports whether, at now (seconds, the ticket's own clock),
// the ticket's writer is dead: the core's rule. A ticket that names no
// stale_after gets the rule for a writer with no beat.
func (t *Ticket) isStale(now float64) bool {
	ct, err := coreTicket(t)
	if err != nil {
		return true
	}
	defer C.sp_ticket_free(ct)
	return C.sp_ticket_is_stale(ct, C.double(now),
		C.double(pqc.DeadAfter(0).Seconds())) != 0
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

const coreMsgLen = 2048

// lockRecord is the text a lock holder writes into its lock file.
func lockRecord(pid int, host string, port int, run string) []byte {
	ch, cr := C.CString(host), C.CString(run)
	defer C.free(unsafe.Pointer(ch))
	defer C.free(unsafe.Pointer(cr))
	var n C.size_t
	p := C.sp_lock_record(C.int64_t(pid), ch, C.int64_t(port), cr, &n)
	if p == nil {
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
	var msg [coreMsgLen]C.char
	C.sp_lock_refusal(cw, cp, h, C.size_t(len(holder)), &msg[0], coreMsgLen)
	return C.GoString(&msg[0])
}

// ownershipDecision is the ticket check: own, take over (with the
// warning to log) or refused (with the error). ticket nil = none there.
func ownershipDecision(who, name, store string, ticket *Ticket, host string,
	now float64) (decision int, message string) {
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
	var msg [coreMsgLen]C.char
	d := C.sp_ownership(cw, cn, cs, ct, ch, C.double(now),
		C.double(pqc.DeadAfter(0).Seconds()), &msg[0], coreMsgLen)
	return int(d), C.GoString(&msg[0])
}

// ownershipUnread is the warning for a ticket that could not be read.
func ownershipUnread(name, store string, why error) string {
	cn, cs, cw := C.CString(name), C.CString(store), C.CString(why.Error())
	defer C.free(unsafe.Pointer(cn))
	defer C.free(unsafe.Pointer(cs))
	defer C.free(unsafe.Pointer(cw))
	var msg [coreMsgLen]C.char
	C.sp_ownership_unread(cn, cs, cw, &msg[0], coreMsgLen)
	return C.GoString(&msg[0])
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
type writePlan struct{ p *C.SpWrites }

var monoStart = time.Now()

// monoNow is the plan's clock: monotonic seconds.
func monoNow() float64 { return time.Since(monoStart).Seconds() }

// newWritePlan: seconds of 0 mean none (no row group by time, no
// checkpoints), as ParquetOptions says.
func newWritePlan(rowGroupRows int64, rowGroupSeconds, checkpointSeconds float64) (*writePlan, error) {
	w := &writePlan{p: C.sp_writes_new()}
	if w.p == nil {
		return nil, fmt.Errorf("write plan: out of memory")
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
	var eb [coreErrLen]C.char
	if C.sp_writes_set_options(w.p, C.int64_t(rowGroupRows), rgs, cs, &eb[0], coreErrLen) != 0 {
		return nil, fmt.Errorf("%s", C.GoString(&eb[0]))
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
func (w *writePlan) commit(committed int64, now float64) int {
	var begins C.int
	return int(C.sp_writes_commit(w.p, C.int64_t(committed), C.double(now), &begins))
}

// plan is the write for a pin that holds the rows [lo, hi); the plan
// moves with it. lost: rows before lo that the ring had reused.
func (w *writePlan) plan(forceRowGroup bool, lo, hi int64, now float64) (kind int, from, to, lost int64) {
	var out C.SpWrite
	force := C.int(0)
	if forceRowGroup {
		force = 1
	}
	C.sp_writes_plan(w.p, force, C.int64_t(lo), C.int64_t(hi), C.double(now), &out)
	return int(out.kind), int64(out.lo), int64(out.hi), int64(out.lost)
}

// ---- what the store holds (sp.h: WHAT THE STORE HOLDS) ----

// storeState is what the store holds of one stream's run, in the core:
// the ticket's stored row and files. It has its own lock: the stream's
// goroutine writes it and the ticket's goroutine reads it.
type storeState struct {
	mu sync.Mutex
	p  *C.SpStore
}

func newStoreState(keep time.Duration) *storeState {
	st := &storeState{p: C.sp_store_new(C.double(keep.Seconds()))}
	runtime.SetFinalizer(st, func(st *storeState) { C.sp_store_free(st.p) })
	return st
}

// begins: the file's first row has the stream row number firstRow.
func (st *storeState) begins(file string, firstRow int64) {
	cf := C.CString(file)
	defer C.free(unsafe.Pointer(cf))
	st.mu.Lock()
	defer st.mu.Unlock()
	C.sp_store_begins(st.p, cf, C.int64_t(firstRow))
}

// stored: the checkpoint version is written and in the store; it holds
// finalRows in finished row groups, and rows in all.
func (st *storeState) stored(file string, version int, checkpoint string, finalRows, rows int64) {
	cf, cc := C.CString(file), C.CString(checkpoint)
	defer C.free(unsafe.Pointer(cf))
	defer C.free(unsafe.Pointer(cc))
	wall := float64(time.Now().UnixNano()) / 1e9
	st.mu.Lock()
	defer st.mu.Unlock()
	C.sp_store_covers(st.p, cf, C.int64_t(version), cc, C.int64_t(finalRows), C.int64_t(rows))
	C.sp_store_stored(st.p, cf, C.int64_t(version), cc, C.int64_t(rows), C.double(wall))
}

// finished: the file is finished in the store; it leaves the ticket.
func (st *storeState) finished(file string) {
	cf := C.CString(file)
	defer C.free(unsafe.Pointer(cf))
	st.mu.Lock()
	defer st.mu.Unlock()
	C.sp_store_finished(st.p, cf)
}

// kept: a file an earlier run finished, found beside its checkpoint
// version; readers may have been told of it.
func (st *storeState) kept(file string, version int, checkpoint string) {
	cf, cc := C.CString(file), C.CString(checkpoint)
	defer C.free(unsafe.Pointer(cf))
	defer C.free(unsafe.Pointer(cc))
	wall := float64(time.Now().UnixNano()) / 1e9
	st.mu.Lock()
	defer st.mu.Unlock()
	C.sp_store_stored(st.p, cf, C.int64_t(version), cc, 0, C.double(wall))
}

// awaits: the file is finished in the store with its checkpoints kept;
// it stays in the ticket until the catalog lists it.
func (st *storeState) awaits(file string) {
	cf := C.CString(file)
	defer C.free(unsafe.Pointer(cf))
	st.mu.Lock()
	defer st.mu.Unlock()
	C.sp_store_awaits(st.p, cf)
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
	if version == "" {
		C.sp_store_catalog(st.p, nil)
	} else {
		C.sp_store_catalog(st.p, cv)
	}
	return left
}

// dropped: the ticket without the listed files is written.
func (st *storeState) dropped() {
	st.mu.Lock()
	defer st.mu.Unlock()
	C.sp_store_dropped(st.p, C.double(monoNow()))
}

// due: the files whose checkpoints are due for deletion now.
func (st *storeState) due() []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	var out []string
	now := C.double(monoNow())
	for i := 0; ; i++ {
		p := C.sp_store_due(st.p, now, C.int32_t(i))
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
	C.sp_store_discarded(st.p, cf)
}

// fill puts what the store holds into t: the stored row and when, the
// keep time, the files. where is the stream's folder, ending in "/".
func (st *storeState) fill(t *Ticket, where string) error {
	ct := C.sp_ticket_new()
	if ct == nil {
		return fmt.Errorf("ticket: out of memory")
	}
	defer C.sp_ticket_free(ct)
	cw := C.CString(where)
	defer C.free(unsafe.Pointer(cw))
	var eb [coreErrLen]C.char
	st.mu.Lock()
	rc := C.sp_store_fill(st.p, ct, cw, &eb[0], coreErrLen)
	st.mu.Unlock()
	if rc != 0 {
		return fmt.Errorf("%s", C.GoString(&eb[0]))
	}
	var n C.size_t
	p := C.sp_ticket_write(ct, &n)
	if p == nil {
		return fmt.Errorf("ticket: out of memory")
	}
	defer C.sp_free(unsafe.Pointer(p))
	got, err := ticketFromText(C.GoBytes(unsafe.Pointer(p), C.int(n)))
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
type readerRules struct{ r *C.SpReader }

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
	r := C.sp_reader_new(cw, cn, cs, d)
	if r == nil {
		return nil, fmt.Errorf("%s: out of memory", who)
	}
	rr := &readerRules{r: r}
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
	C.sp_reader_poll(rr.r, C.int(found), ct, cw,
		C.double(float64(time.Now().UnixNano())/1e9), C.double(monoNow()),
		C.double(pqc.DeadAfter(0).Seconds()), &st)
	return readerStepOf(&st)
}

func (rr *readerRules) folderIs(exists bool) readerStep {
	e := C.int(0)
	if exists {
		e = 1
	}
	var st C.SpStep
	C.sp_reader_folder_is(rr.r, e, C.double(monoNow()), &st)
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
	C.sp_reader_lost(rr.r, cw, C.double(monoNow()), &st)
	return readerStepOf(&st)
}

func (rr *readerRules) close()         { C.sp_reader_close(rr.r) }
func (rr *readerRules) reason() string { return C.GoString(C.sp_reader_reason(rr.r)) }

// coreLine is a line the core has for the log, with its level.
type coreLine struct {
	level slog.Level
	text  string
}

// messages are the lines for the log since the last call.
func (rr *readerRules) messages() []coreLine {
	var out []coreLine
	var msg [coreMsgLen]C.char
	for {
		level := C.sp_reader_message(rr.r, &msg[0], coreMsgLen)
		if level == 0 {
			return out
		}
		l := slog.LevelInfo
		switch level {
		case C.SP_LOG_WARNING:
			l = slog.LevelWarn
		case C.SP_LOG_ERROR:
			l = slog.LevelError
		}
		out = append(out, coreLine{l, C.GoString(&msg[0])})
	}
}
