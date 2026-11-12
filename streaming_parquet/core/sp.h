/* sp.h -- the stream core: the rules of streaming-parquet that every
 * host language shares, written once.
 *
 * The core holds formats and rules. It does no input and no output: no
 * disk, no network, no clock. A host (Python through cffi, Go through
 * cgo) reads and writes the store, runs the threads, and tells the core
 * the time. A rule that is here is in no host.
 *
 * This file is the spec of what it declares. First in it: the ticket.
 *
 * THE TICKET
 *
 * A stream's ticket is one small JSON object in the store, at
 * tickets/<name>.json. The writer writes it when what it says changes,
 * and at least once a beat. A reader finds the writer through it. No
 * ticket means no writer; a ticket older than its stale_after means a
 * writer that died.
 *
 * Its members, in the order the core writes them. Every one is written,
 * as null when the writer does not know it:
 *
 *   name                text    the stream's whole path under the store
 *   host                text    where the writer's Flight server listens
 *   port                integer
 *   run                 text    the writer's start time, UTC to the
 *                               microsecond; every file it writes
 *                               records it (sp_run); row numbers start
 *                               again at 0 with each run
 *   beat_seconds        number  the longest gap between two writes of
 *                               the ticket
 *   stale_after         number  seconds after ts at which a reader
 *                               calls the writer dead
 *   checkpoint_seconds  number  the writer's checkpoint cadence
 *   ship_seconds        number  the cadence of its uploads from a spool
 *   stored_row          integer the newest row of this run the store
 *                               holds; -1 for none
 *   stored_at           number  when that row got there
 *   unfinished          array   the checkpoint paths of files, below,
 *                               alone: what a reader from before
 *                               `files` knows. The core writes it from
 *                               `files`; a host does not set it.
 *   files               array   the files not finished in the store,
 *                               each {checkpoint, first_row, final_rows,
 *                               rows}: the path of its newest checkpoint
 *                               version, the stream row number of its
 *                               first row, the rows in its finished row
 *                               groups, and its rows
 *   keep_seconds        number  how long a superseded checkpoint stays
 *   catalog_etag        text    the version of the catalog of finished
 *                               files that the store holds
 *   revision            integer counts this run's writes of the ticket
 *   ts                  number  when the ticket was written
 *
 * One more member is read and never written with a value unless a host
 * sets it: `archive` (text), which writers before 2026-11 wrote.
 *
 * A reader ignores members it does not know. A known member of the
 * wrong kind is an error: the ticket is refused, with the member named.
 */
#ifndef SP_H
#define SP_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SpTicket SpTicket;

/* The ticket's members that hold one value. */
typedef enum {
    SP_T_NAME = 0,
    SP_T_HOST,
    SP_T_PORT,
    SP_T_RUN,
    SP_T_BEAT_SECONDS,
    SP_T_STALE_AFTER,
    SP_T_CHECKPOINT_SECONDS,
    SP_T_SHIP_SECONDS,
    SP_T_STORED_ROW,
    SP_T_STORED_AT,
    SP_T_KEEP_SECONDS,
    SP_T_CATALOG_ETAG,
    SP_T_REVISION,
    SP_T_TS,
    SP_T_ARCHIVE,
    SP_T_FIELDS            /* the count */
} SpTicketField;

/* What a member holds. */
typedef enum { SP_TEXT = 0, SP_INTEGER = 1, SP_NUMBER = 2 } SpKind;

/* The JSON name of a member, and its kind. NULL / -1 for no such member. */
const char *sp_ticket_field_name(int field);
int sp_ticket_field_kind(int field);

/* An empty ticket: every member unknown, no files. NULL when memory is
 * out. */
SpTicket *sp_ticket_new(void);
void sp_ticket_free(SpTicket *t);

/* Set a member. Each returns 0, or -1 with the reason in err: no such
 * member, the wrong kind for it, text that is not UTF-8, a number that
 * is not finite. sp_ticket_clear makes a member unknown again. */
int sp_ticket_set_text(SpTicket *t, int field, const char *text,
                       char *err, size_t errlen);
int sp_ticket_set_integer(SpTicket *t, int field, int64_t value,
                          char *err, size_t errlen);
int sp_ticket_set_number(SpTicket *t, int field, double value,
                         char *err, size_t errlen);
int sp_ticket_clear(SpTicket *t, int field);

/* Add an unfinished file. A NULL number is one the writer does not
 * know. Returns 0, or -1 with the reason in err. */
int sp_ticket_add_file(SpTicket *t, const char *checkpoint,
                       const int64_t *first_row, const int64_t *final_rows,
                       const int64_t *rows, char *err, size_t errlen);

/* Read a member. sp_ticket_text returns NULL for an unknown member; the
 * pointer is the ticket's. The two others return 1 and fill *out, or 0
 * for an unknown member. A member of kind integer is read by either; a
 * member of kind number only by sp_ticket_number. */
const char *sp_ticket_text(const SpTicket *t, int field);
int sp_ticket_integer(const SpTicket *t, int field, int64_t *out);
int sp_ticket_number(const SpTicket *t, int field, double *out);

/* The unfinished files. sp_ticket_file fills the pointers given; each
 * has_* is 1 when the writer knew that number. Returns 0, or -1 for no
 * such index. */
int32_t sp_ticket_file_count(const SpTicket *t);
int sp_ticket_file(const SpTicket *t, int32_t index, const char **checkpoint,
                   int64_t *first_row, int *has_first_row,
                   int64_t *final_rows, int *has_final_rows,
                   int64_t *rows, int *has_rows);

/* The ticket as JSON text, for the host to store: sp_free it. *out_len
 * is its length without the final 0. NULL when memory is out. */
char *sp_ticket_write(const SpTicket *t, size_t *out_len);

/* A ticket from the text a host read from the store. NULL with the
 * reason in err when the text is not a JSON object, or a known member
 * is of the wrong kind. The text needs no final 0. */
SpTicket *sp_ticket_read(const char *text, size_t len,
                         char *err, size_t errlen);

/* The seconds after its ts at which this ticket means a dead writer:
 * what the ticket says (stale_after). A ticket that does not say gets
 * if_absent, which the host takes from the one rule for a dead writer
 * (pqc_dead_after(0), in the checkpoint core). */
double sp_ticket_stale_after(const SpTicket *t, double if_absent);

/* 1 when, at the time `now` (the host's clock, in the ticket's own
 * seconds), the ticket's writer is dead: now is stale_after or more
 * past ts. A ticket with no ts is stale. */
int sp_ticket_is_stale(const SpTicket *t, double now, double if_absent);

void sp_free(void *p);

#ifdef __cplusplus
}
#endif
#endif
