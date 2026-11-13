package streaming

// sp_core.c and sp.h are CHECKED-IN COPIES of ../../core/sp.{c,h}, the
// stream core (copies, not a symlink or an include from outside the
// package, so the Go build cache tracks their content).
// run_go_streaming_compat.py refreshes them and refuses to pass while
// they differ from the core.
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
	"unsafe"

	"github.com/neofactory/streaming-parquet/parquet_checkpoint/go/pqc"
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
