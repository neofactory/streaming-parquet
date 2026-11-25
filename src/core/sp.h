/* sp.h -- the stream core: the rules of streaming-parquet that every
 * host language shares, written once.
 *
 * The core holds formats and rules. It does no input and no output: no
 * disk, no network. A host (Python through cffi, Go through cgo) reads
 * and writes the store, runs the threads, does the waiting and writes
 * the log. A rule that is here is in no host.
 *
 * How a function reports is in report.h, the same for the three cores:
 * a function that can fail, or that can have a line for the host's
 * log, returns a status and takes a Report as its last parameter; what
 * it makes comes back through an output parameter. The core reads the
 * clock itself (report.h: TIME); sp_test_set_clock, at the end of this
 * file, is for tests.
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

#include "report.h"

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

/* An empty ticket: every member unknown, no files. */
int sp_ticket_new(SpTicket **out, Report *report);
void sp_ticket_free(SpTicket *t);

/* Set a member. Refused: no such member, the wrong kind for it, text
 * that is not UTF-8, a number that is not finite. A NULL text makes
 * the member unknown. sp_ticket_clear makes any member unknown again;
 * a number that is no member changes nothing. */
int sp_ticket_set_text(SpTicket *t, int field, const char *text,
                       Report *report);
int sp_ticket_set_integer(SpTicket *t, int field, int64_t value,
                          Report *report);
int sp_ticket_set_number(SpTicket *t, int field, double value,
                         Report *report);
void sp_ticket_clear(SpTicket *t, int field);

/* Add an unfinished file. A NULL number is one the writer does not
 * know. */
int sp_ticket_add_file(SpTicket *t, const char *checkpoint,
                       const int64_t *first_row, const int64_t *final_rows,
                       const int64_t *rows, Report *report);

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

/* The ticket as JSON text in *out, for the host to store: sp_free it.
 * *out_len is its length without the final 0. */
int sp_ticket_write(const SpTicket *t, char **out, size_t *out_len,
                    Report *report);

/* A ticket in *out from the text a host read from the store. Refused
 * (REPORT_REFUSED, with the member named) when the text is not a JSON
 * object, or a known member is of the wrong kind. The text needs no
 * final 0. */
int sp_ticket_read(const char *text, size_t len, SpTicket **out,
                   Report *report);

/* The seconds after its ts at which this ticket means a dead writer:
 * what the ticket says (stale_after). A ticket that does not say gets
 * if_absent, which the host takes from the one rule for a dead writer
 * (pqc_dead_after(0), in the checkpoint core). */
double sp_ticket_stale_after(const SpTicket *t, double if_absent);

/* 1 when the ticket's writer is dead: the wall clock is stale_after or
 * more past ts. A ticket with no ts is stale. */
int sp_ticket_is_stale(const SpTicket *t, double if_absent);

/* THE OWNERSHIP DECISION
 *
 * One writer per stream. A host that is about to write a stream (it
 * publishes it, or writes its files) makes two checks, in this order,
 * and the core decides each from the facts the host brings:
 *
 *   the lock file   a second process on THIS server. The host takes an
 *                   exclusive lock on <dir>/streams/<name>/writer.lock
 *                   and holds it for the stream's life; the operating
 *                   system releases it when the process ends, a crash
 *                   included. The holder writes its lock record into
 *                   the file (sp_lock_record). A host that cannot take
 *                   the lock is refused, and sp_lock_refusal names the
 *                   holder from that record.
 *   the ticket      another SERVER. The host reads the stream's ticket
 *                   from the store and asks sp_ownership. No ticket, or
 *                   a ticket with this host's address: own. A ticket of
 *                   another host that is not stale: refused, naming it.
 *                   A stale one: that server is gone, take over.
 *
 * A host that could not read the ticket still owns the stream on its
 * lock alone, and logs the line of sp_ownership_unread: an unreachable
 * store at start delays the uploads and never the stream.
 *
 * `who` in each message is the host's own call, as its user wrote it:
 * create_stream('a/b') in Python, CreateStream("a/b") in Go.
 */

/* The lock record: {"pid", "host", "port", "run"} as JSON text in *out,
 * for the host to write into the lock file it holds: sp_free it. */
int sp_lock_record(int64_t pid, const char *host, int64_t port,
                   const char *run, char **out, size_t *out_len,
                   Report *report);

/* The refusal for a lock another process holds: it always answers
 * REPORT_REFUSED, and the report's error names the holder. `holder` is
 * the text of the lock file (its lock record; any text is taken, and
 * what cannot be read from it shows as "?"). */
int sp_lock_refusal(const char *who, const char *lock_path,
                    const char *holder, size_t holder_len, Report *report);

typedef enum {
    SP_OWN = 0,        /* this writer owns the stream */
    SP_TAKE_OVER = 1,  /* it owns it, from a writer that died: the
                          report has a warning line that says so */
    SP_REFUSED = 2     /* another server writes it: the call answers
                          REPORT_REFUSED and the error names it */
} SpOwnership;

/* The ticket check: *decision is the SpOwnership. ticket: what the
 * store holds, NULL for none. host: this writer's address. if_absent:
 * as sp_ticket_is_stale. */
int sp_ownership(const char *who, const char *name, const char *store,
                 const SpTicket *ticket, const char *host,
                 double if_absent, int *decision, Report *report);

/* A ticket the host could not read (`why`: its error): the report gets
 * the warning line to log. */
int sp_ownership_unread(const char *name, const char *store,
                        const char *why, Report *report);

/* THE WRITE PLAN
 *
 * Which committed rows go to the stream's files, and when. One SpWrites
 * per stream holds the plan's state; the host tells it what was
 * committed, pins the rows, and does the writes the plan names. The
 * plan does no I/O; its times are the monotonic clock's.
 *
 * Rows have stream row numbers. The plan keeps:
 *
 *   cursor    the first row not yet planned as a row group
 *   planned   the first row not yet planned as a row group or a
 *             checkpoint (>= cursor)
 *   lost      rows the ring reused before they were planned
 *
 * While the plan is on (between a start and a stop):
 *
 *   A ROW GROUP is due when the rows from cursor reach row_group_rows,
 *   or, with row_group_seconds, when that long has passed since the
 *   first of them was committed. Its rows are final in the file.
 *
 *   A CHECKPOINT is due when rows past `planned` are committed and
 *   checkpoint_seconds have passed since the last planned write (or,
 *   before any, since the first pending row). It makes the rows from
 *   cursor durable and leaves cursor where it is: the next row group
 *   holds the same rows. So an idle stream plans nothing.
 *
 *   A row group is also planned, due or not, when the host forces one:
 *   at a cut, at a start that changes the options, at a stop. That is
 *   how a file ends with every row committed before the call.
 *
 * The host pins the rows [cursor, committed) and tells the plan what
 * the pin holds. Rows of it that the ring had already reused are lost:
 * a row group counts them and goes on from the first row still there.
 * A checkpoint is not planned over lost rows; the next row group counts
 * them.
 *
 * The order of a host's calls at a commit:
 *   sp_writes_commit    is a write due? (and: do the pending rows begin
 *                       now -- the moment that names the next file)
 *   pin [sp_writes_cursor, committed)
 *   sp_writes_plan      the write to do, and the plan moves
 */
typedef struct SpWrites SpWrites;

typedef enum {
    SP_W_NOTHING = 0,
    SP_W_ROW_GROUP = 1,
    SP_W_CHECKPOINT = 2
} SpWriteKind;

/* One planned write: the rows [lo, hi). lost: the rows before lo that
 * this step found reused, 0 for none. kind SP_W_NOTHING: no write (lost
 * may still be more than 0). */
typedef struct {
    int kind;
    int64_t lo, hi;
    int64_t lost;
} SpWrite;

/* A plan that is off, at row 0, with row_group_rows 1 and no cadences. */
int sp_writes_new(SpWrites **out, Report *report);
void sp_writes_free(SpWrites *w);

/* The cadences, from now on. A NULL seconds is none: no row group by
 * time, no checkpoints. REPORT_INVALID: row_group_rows under 1,
 * seconds negative or not finite. New options start the checkpoint
 * cadence again. */
int sp_writes_set_options(SpWrites *w, int64_t row_group_rows,
                          const double *row_group_seconds,
                          const double *checkpoint_seconds,
                          Report *report);

/* Turn the plan on at the row `committed` (the first row not yet
 * committed): earlier rows are not planned. No change when it is on. */
void sp_writes_start(SpWrites *w, int64_t committed);

/* Turn the plan off. A host that wants the pending rows in the file
 * forces a row group first. */
void sp_writes_stop(SpWrites *w);

/* The kind of write due for `committed` now; nothing moves. */
int sp_writes_due(const SpWrites *w, int64_t committed);

/* sp_writes_due, and the pending rows' clock starts when it was not
 * running: then *begins is 1 (the pending rows begin at this commit),
 * else 0. The answer is from before the clock started. */
int sp_writes_commit(SpWrites *w, int64_t committed, int *begins);

/* Plan the write for a pin that holds the rows [lo, hi): hi is the
 * committed row count the host pinned to, lo is cursor or, after a
 * loss, later. force_row_group: plan a row group whatever is due.
 * *out is the write; the plan moves with it. */
void sp_writes_plan(SpWrites *w, int force_row_group, int64_t lo,
                    int64_t hi, SpWrite *out);

int sp_writes_on(const SpWrites *w);
int64_t sp_writes_cursor(const SpWrites *w);
int64_t sp_writes_planned(const SpWrites *w);
int64_t sp_writes_lost(const SpWrites *w);

/* WHAT THE STORE HOLDS
 *
 * The part of the ticket that says what a reader can take from the
 * store, and the rule for when a file's old checkpoints are deleted.
 * One SpStore per stream; the host tells it each thing that lands in
 * the store, and asks it for the ticket's values. The host does the
 * writes and the deletes.
 *
 * A file of this run is named by its object name in the stream's
 * folder (20261114-174256.parquet). While it is written it is in the
 * store as checkpoint versions, numbered from 1; a version's object
 * name is `checkpoint` here. The host takes a version's number from
 * its name by the checkpoint core's rule and gives both.
 *
 * The life of a file, as the host tells it:
 *
 *   sp_store_begins     the file's first row has a stream row number
 *   sp_store_covers     a checkpoint version was written: the rows of
 *                       its finished row groups, and its rows
 *   sp_store_stored     a version is in the store (at once when the
 *                       first copy is the store; after its upload when
 *                       the store is a mirror). The file is in the
 *                       ticket's `files` with this version. What the
 *                       plan knew of older versions is dropped: no
 *                       later ticket names them. The newest stored row
 *                       moves up when this version holds a later one.
 *   then one of:
 *   sp_store_finished   the file is finished in the store and nothing
 *                       lists finished files: it leaves the ticket.
 *   sp_store_awaits     the file is finished in the store and a catalog
 *                       will list it: it STAYS in the ticket, with its
 *                       last checkpoint version, until the catalog
 *                       lists it, so no reader is between the two.
 *   sp_store_listed     a published catalog lists the file: it leaves
 *                       the ticket. The host writes the ticket, and
 *   sp_store_dropped    when that write worked: the file's checkpoints
 *                       are superseded from that moment.
 *   sp_store_due        the files whose checkpoints were superseded
 *                       keep_seconds ago or more: a reader told of them
 *                       has had that long. The host deletes them and
 *   sp_store_discarded  says so.
 *
 * Two clocks: the time the ticket shows (stored_at) is the wall
 * clock's; the keep time is counted on the monotonic clock.
 */
typedef struct SpStore SpStore;

/* keep_seconds: how long a superseded checkpoint stays.
 * REPORT_INVALID when it is negative or not finite. */
int sp_store_new(double keep_seconds, SpStore **out, Report *report);
void sp_store_free(SpStore *s);

/* The keep time from now on. REPORT_INVALID for a value that is
 * negative or not finite. */
int sp_store_set_keep_seconds(SpStore *s, double keep_seconds,
                              Report *report);
double sp_store_keep_seconds(const SpStore *s);

/* Each fails only when a name is NULL or memory is out. */
int sp_store_begins(SpStore *s, const char *file, int64_t first_row,
                    Report *report);
int sp_store_covers(SpStore *s, const char *file, int64_t version,
                    const char *checkpoint, int64_t final_rows, int64_t rows,
                    Report *report);
int sp_store_stored(SpStore *s, const char *file, int64_t version,
                    const char *checkpoint, int64_t rows, Report *report);
int sp_store_finished(SpStore *s, const char *file, Report *report);
int sp_store_awaits(SpStore *s, const char *file, Report *report);

/* 1 when the file was awaiting its catalog (it now leaves the ticket,
 * and waits for sp_store_dropped); 0 when it was not. */
int sp_store_listed(SpStore *s, const char *file);

/* The catalog version the ticket names from now on (NULL: none);
 * sp_store_fill puts it in the ticket. The host calls it with the
 * version a pass published, in the same step (under the same lock) as
 * sp_store_listed for the files that version lists. A ticket then never
 * names a catalog version together with a file that version lists: a
 * view would read that file's rows twice. */
int sp_store_catalog(SpStore *s, const char *version, Report *report);

/* The ticket without the listed files is written: their checkpoints are
 * superseded from now. Returns how many files that is. */
int sp_store_dropped(SpStore *s);

/* The index-th file (from 0) whose checkpoints are due for deletion
 * now; NULL past the last. The pointer holds until the next call that
 * changes the store. */
const char *sp_store_due(const SpStore *s, int32_t index);
int sp_store_discarded(SpStore *s, const char *file, Report *report);

/* The newest row of this run the store holds, -1 for none; 1 and *wall
 * filled when one is there, else 0. */
int64_t sp_store_stored_row(const SpStore *s);
int sp_store_stored_at(const SpStore *s, double *wall);

/* Put what the store holds into a ticket: stored_row, stored_at,
 * keep_seconds, and `files` (replaced), in the order of the files'
 * names. `where` is the stream's folder in the store, ending in '/':
 * each checkpoint is written as where + its object name. */
int sp_store_fill(const SpStore *s, SpTicket *t, const char *where,
                  Report *report);

/* How many files the store's state holds (in the ticket, awaiting, or
 * with checkpoints to delete): what a long run must not let grow. */
int32_t sp_store_files(const SpStore *s);

/* THE READER
 *
 * A reader of a stream by name: where it finds the writer, when it
 * waits, when it starts again. One SpReader per reader holds the state;
 * the host runs the thread, reads the store, makes the connection, and
 * tells the reader what each of those found. The reader answers with
 * the next step. It does no I/O and no waiting: a step says how long
 * to wait, and the host waits.
 *
 * Where a reader looks, at every poll, in this order:
 *
 *   the process's own streams   a stream of that name in this process
 *                               is read in place: no connection
 *   the store                   the stream's ticket names the writer
 *
 * What a poll finds, and the step:
 *
 *   a local stream     read it (SP_DO_LOCAL)
 *   a ticket           not stale: connect to its writer (SP_DO_CONNECT).
 *                      Stale: the writer died; wait. A ticket with no
 *                      ts is not stale, and a ticket the user gave the
 *                      reader directly is never stale: nothing writes
 *                      it again.
 *   no ticket          wait for a writer -- when the folder above the
 *                      name exists. At the first poll of a name with a
 *                      folder ("a/b/c": the folder is "a/b") the host
 *                      is asked to look (SP_DO_LIST): does the store
 *                      hold anything under tickets/<folder>/ or
 *                      streams/<folder>/, or has this process a stream
 *                      in the folder? If not, the path is wrong and the
 *                      host's connect call fails (SP_DO_FAIL) with the
 *                      reader's reason. A wrong last part of a name
 *                      still waits: the folder is there. A reader that
 *                      has waited once is not asked again.
 *   an unreadable store  wait; the reader cannot tell
 *
 * A ticket with another run than the one this reader last connected to:
 * the writer started again, and its row numbers start again at 0. The
 * step says fresh_ring: the host drops the ring it has and makes a new
 * one, so rows of two runs never share a ring.
 *
 * A lost connection, or a connection that could not be made: wait
 * 0.5 s, then twice as long each time up to 10 s, and poll again. A
 * connection that was made sets the wait back to 0.5 s.
 *
 * What a reader says (the lines of a step's report, for the host's
 * log): one line when a wait for a writer starts, one warning when the wait has lasted
 * SP_READER_WARN_SECONDS, an error when the store cannot be read and a
 * line when it can again, an error for each failed connection, and an
 * error when the writer started again.
 */
typedef struct SpReader SpReader;

#define SP_READER_POLL_SECONDS 1.0
#define SP_READER_WARN_SECONDS 30.0

typedef enum {
    SP_R_NO_TICKET = 0,       /* "no-ticket" */
    SP_R_STORE_UNREACHABLE,    /* "store-unreachable": the store cannot be read */
    SP_R_WAITING,             /* "waiting": a stale ticket */
    SP_R_CONNECTING,          /* "connecting" */
    SP_R_STREAMING,           /* "streaming" */
    SP_R_RECONNECTING,        /* "reconnecting" */
    SP_R_CLOSED               /* "closed" */
} SpReaderState;

/* What a poll found. */
typedef enum {
    SP_FOUND_NONE = 0,        /* the store was read; it has no ticket */
    SP_FOUND_TICKET,
    SP_FOUND_LOCAL,           /* a stream of this process */
    SP_FOUND_UNREADABLE       /* the store could not be read */
} SpFound;

typedef enum {
    SP_DO_WAIT = 0,           /* wait wait_seconds, then poll again */
    SP_DO_CONNECT,            /* connect to the ticket's writer */
    SP_DO_LOCAL,              /* read the local stream */
    SP_DO_LIST,               /* look for the folder; sp_reader_folder_is */
    SP_DO_FAIL                /* the connect call fails: sp_reader_reason */
} SpDo;

typedef struct {
    int action;               /* SpDo */
    int state;                /* SpReaderState, after this step */
    int fresh_ring;           /* drop the ring in hand first */
    double wait_seconds;      /* for SP_DO_WAIT */
} SpStep;

/* who: the host's own call, as its user wrote it (connect('a/b')).
 * store: where the reader looks, for the messages; NULL for a reader
 * with a direct ticket. */
int sp_reader_new(const char *who, const char *name, const char *store,
                  int direct, SpReader **out, Report *report);
void sp_reader_free(SpReader *r);

/* The folder above the name: "a/b" for "a/b/c", "" for a name with no
 * folder. */
const char *sp_reader_folder(const SpReader *r);

/* What a poll found: *step is the next step, and the report has the
 * lines the step made. ticket: for SP_FOUND_TICKET. why: the host's
 * error for SP_FOUND_UNREADABLE. if_absent: as sp_ticket_is_stale. */
int sp_reader_poll(SpReader *r, int found, const SpTicket *ticket,
                   const char *why, double if_absent, SpStep *step,
                   Report *report);

/* The answer to SP_DO_LIST: 1 when the folder exists (in the store or
 * among this process's streams), 0 when not. A host that could not
 * list polls with SP_FOUND_UNREADABLE instead. */
int sp_reader_folder_is(SpReader *r, int exists, SpStep *step,
                        Report *report);

/* The connection was made and rows flow. */
void sp_reader_connected(SpReader *r);

/* The connection was lost or could not be made (why: the host's error),
 * or the local stream closed (why: NULL). The step is a wait. */
int sp_reader_lost(SpReader *r, const char *why, SpStep *step,
                   Report *report);

void sp_reader_close(SpReader *r);

int sp_reader_state(const SpReader *r);
const char *sp_reader_state_name(int state);

/* Why the reader is not streaming, in words; "" while it is. */
const char *sp_reader_reason(const SpReader *r);

/* The run the reader last connected to; NULL for none (or a local
 * stream). */
const char *sp_reader_run(const SpReader *r);

/* THE VIEW
 *
 * One SQL view over a stream: the store's rows up to one row, the
 * ring's rows after it. One SpView per view holds the rules' state.
 * The host reads the ticket, fetches and attaches the catalog, runs
 * the SQL and the thread; the view decides what the text names, writes
 * the text, and says when to look again. It does no I/O and no
 * waiting.
 *
 * What the text reads, all named by one ticket, so a query never mixes
 * two states:
 *
 *   finished files    the catalog version the ticket names
 *   unfinished files  the checkpoint versions the ticket lists, each up
 *                     to the seam
 *   the ring          the rows after the seam (a view with a ring, of
 *                     the ticket's run)
 *
 * THE SEAM is the row where the view changes from the store to the
 * ring. To the extent that rows are in the ring, the view uses them:
 * the seam is the oldest row boundary the store has (the start of an
 * unfinished file, the end of its finished row groups, the stored row)
 * that the ring covers and that is above the ring's oldest quarter.
 * The oldest quarter is the time a running query has before the writer
 * reuses the rows after its seam. When no boundary is there:
 *
 *   SP_SEAM_TIGHT  the ring reaches the stored row: the seam is the
 *                  stored row, and a running query has less than a
 *                  quarter of the ring
 *   SP_SEAM_SHORT  the ring is full and begins after the stored row:
 *                  too short for the store's lag. No ring piece.
 *   SP_SEAM_LATE   the ring is not full and begins after the stored row
 *                  (a reader that connected after the ticket's
 *                  checkpoint). No ring piece; a later ticket ends it.
 *
 * A view with no ring, or of another run than the ring's, or of a
 * writer that is not live, has its seam at the stored row.
 *
 * A LIVE writer's ticket (not stale, with a revision) names the catalog
 * version to read (SP_CATALOG_EXACT) or none (SP_CATALOG_NONE). No
 * ticket, or one that is not live, vouches for none: the view reads the
 * store's current catalog (SP_CATALOG_CURRENT), and is made again at
 * every look.
 *
 * THE TEXT changes when what it names changes: the seam, the catalog,
 * the run, or a checkpoint version. A file keeps the checkpoint version
 * the text names until the seam moves, or until a newer version has
 * been seen for SP_VIEW_REPOINT_SHARE of the ticket's keep_seconds (a
 * replaced version is deleted keep_seconds after).
 *
 * A host's look:
 *   read the ticket
 *   sp_view_plan        what the text must name; build = 0: nothing to do
 *   get the catalog     the version sp_view_catalog_etag names, or the
 *                       current one. Gone: read the ticket, plan again.
 *   sp_view_sql         the statement; run it
 *   sp_view_built       the view's text is the plan's
 *   sp_view_followed    the look worked (sp_view_failed: it did not)
 *   sp_view_wait_seconds  until the next look, or sooner when the
 *                       connection shows a new ticket revision
 *
 * The ring's relation, as the host registers it under `memory`, has the
 * stream's columns and two more: _seq, each row's number, and _first,
 * the number of the first row the scan delivered. A query whose scan of
 * the ring begins past the row after the seam fails; it does not answer
 * short.
 */
typedef struct SpView SpView;

#define SP_VIEW_POLL_DEFAULT_SECONDS 15.0  /* no beat, or no live writer */
#define SP_VIEW_POLL_MIN_SECONDS 0.5
#define SP_VIEW_POLL_MARGIN_SECONDS 1.0    /* after the ticket's next write */
#define SP_VIEW_RING_MARGIN_SHARE 0.25
#define SP_VIEW_REPOINT_SHARE 0.5

typedef enum {
    SP_SEAM_AS_WANTED = 0, SP_SEAM_TIGHT, SP_SEAM_SHORT, SP_SEAM_LATE
} SpSeam;

typedef enum {
    SP_CATALOG_NONE = 0, SP_CATALOG_EXACT, SP_CATALOG_CURRENT
} SpViewCatalog;

typedef struct {
    int build;                /* 1: the text must change */
    int catalog;              /* SpViewCatalog */
    int64_t seam;
    int use_ring;
} SpViewPlan;

/* The seam for a ticket (NULL: none) and a ring's window: {oldest row,
 * newest row, capacity}, NULL for no ring. *use_ring: the view reads
 * the ring after the seam. *why: SpSeam. */
int64_t sp_view_seam(const SpTicket *t, const int64_t *window, int *use_ring,
                     int *why);

/* view: the view's name in SQL. stream: the stream's name, for the
 * messages. memory: the name the ring's relation is registered under,
 * NULL for a view with no ring. empty: the name of a relation with the
 * stream's columns and no rows, NULL when the host does not know the
 * columns. */
int sp_view_new(const char *view, const char *stream, const char *memory,
                const char *empty, SpView **out, Report *report);
void sp_view_free(SpView *v);

/* Plan the text for a ticket (NULL: the store has none); the report has
 * the lines the look made. files: for each of the ticket's files, in
 * order, the path of the file its checkpoint belongs to (the checkpoint
 * core's rule); NULL when the ticket has none. ring_run: the run of the
 * ring's rows, NULL for none. window: as sp_view_seam. if_absent: as
 * sp_ticket_is_stale. keep_if_absent: the keep time of a ticket that
 * names none. */
int sp_view_plan(SpView *v, const SpTicket *t, const char *const *files,
                 const char *ring_run, const int64_t *window,
                 double if_absent, double keep_if_absent, SpViewPlan *out,
                 Report *report);

/* ROW IDS. A view made with row ids has two more columns, which
 * together name a row of the stream for good:
 *   sp_run_start_time   TIMESTAMPTZ  when the writer that wrote the row
 *                                    started (the ticket's run)
 *   sp_run_row_number   BIGINT       the row's number in that run, from 0
 * A finished file's rows get them from the catalog's table sp_file and
 * the row's place in its file; an unfinished file's rows from the
 * file's own footer keys; the ring's rows from the ring. With row ids
 * the ring's relation has, besides _seq and _first, a column
 * sp_run_start_time. Rows of a file that does not record its run read
 * as null in both. Call before the first sp_view_plan. */
#define SP_ROW_ID_RUN "sp_run_start_time"
#define SP_ROW_ID_ROW "sp_run_row_number"
void sp_view_set_row_ids(SpView *v, int on);

/* The statement, in *out, that makes a view named `view` of the ring
 * alone, with row ids, from the ring's relation `memory` (as above):
 * sp_free it. */
int sp_view_live_sql(const char *view, const char *memory, char **out,
                     Report *report);

/* The catalog version the plan names (SP_CATALOG_EXACT), else NULL. */
const char *sp_view_catalog_etag(const SpView *v);

/* The statement, in *out, that makes the planned view: sp_free it.
 * catalog: the name the host attached the catalog under, NULL for none.
 * REPORT_REFUSED when there is nothing to read the columns from. */
int sp_view_sql(const SpView *v, const char *catalog, char **out,
                Report *report);

/* The host ran the statement. */
void sp_view_built(SpView *v);

/* The seam of the view as built; -1 before. */
int64_t sp_view_seam_row(const SpView *v);

/* Why the ring is not used as wanted, in words; "" when it is. */
const char *sp_view_ring_note(const SpView *v);

/* Seconds until the next look: a live writer's next write of the
 * ticket and a margin, never under SP_VIEW_POLL_MIN_SECONDS; with no
 * live writer, SP_VIEW_POLL_DEFAULT_SECONDS. */
double sp_view_wait_seconds(const SpView *v);

/* A look failed (why: the host's error): *wait_seconds is how long to
 * wait before the next, and the first failure of a row of them puts an
 * error line in the report. sp_view_followed: a look worked; after
 * failures the report gets one line that says so. */
int sp_view_failed(SpView *v, const char *why, double *wait_seconds,
                   Report *report);
int sp_view_followed(SpView *v, Report *report);



/* THE CATALOG
 *
 * The list of a stream's finished files: one object in the store,
 * <store>/streams/<name>/catalog.sqlite. It is a SQLite database that
 * holds a DuckLake 1.0 catalog with one table, `data`, whose files are
 * the stream's finished parquet files, each by its full path. DuckDB
 * reads it with ATTACH 'ducklake:sqlite:<file>' and opens only the
 * files a query needs: the catalog holds each file's row count and each
 * column's minimum and maximum. The core writes it; no writer needs
 * DuckDB.
 *
 * The host does the reads and writes: it brings the catalog's bytes
 * (none: a new catalog), brings each new file's footer, takes the new
 * bytes, and stores them (on a bucket: only if the stored object is
 * still the version it read). The files are the source of truth: a pass
 * adds every finished file the catalog does not list.
 *
 * A pass:
 *   sp_catalog_open     from the stored bytes
 *   sp_catalog_lists    for each finished file in the store: listed?
 *   read the file's last 8 bytes; sp_parquet_footer_length; read the
 *   footer; sp_catalog_add
 *   sp_catalog_bytes    the new catalog, with one snapshot for the pass
 *
 * What a file adds: its row in the list (path, rows, size), each
 * column's value count, null count, minimum and maximum as text DuckDB
 * casts back to the column's type, the table's totals, and, in the
 * core's own table sp_file, the file's run and the stream row number of
 * its first row (the footer's keys sp_run and sp_base_seq).
 *
 * The columns of `data` are those of the files: a file with a column
 * the catalog lacks adds it (earlier files read as null in it); a file
 * that lacks a column reads as null in it. A column whose type differs
 * from the catalog's refuses the file, with both types in the error.
 * Types taken: boolean, the integers, float32, float64, varchar, blob,
 * date, time, the timestamps, decimal, and lists and structs of them.
 */
typedef struct SpCatalog SpCatalog;

/* The length, in *out, of the footer of a parquet file, from the file's
 * last 8 bytes or more (`tail`, `len`): the footer is that many bytes
 * before those 8. REPORT_REFUSED for bytes that do not end a parquet
 * file. */
int sp_parquet_footer_length(const uint8_t *tail, size_t len, int64_t *out,
                             Report *report);

/* A catalog, in *out, from the bytes the store holds; len 0: a new one.
 * data_path: the stream's folder in the store, ending in '/'.
 * REPORT_REFUSED for bytes that are not a stream's catalog. */
int sp_catalog_open(const uint8_t *bytes, size_t len, const char *data_path,
                    SpCatalog **out, Report *report);
void sp_catalog_free(SpCatalog *c);

/* 1 when the catalog lists the file of this full path. */
int sp_catalog_lists(const SpCatalog *c, const char *path);

/* How many files the catalog lists, and the path, in *out, of the
 * index-th (from 0): sp_free it. REPORT_INVALID past the last. */
int64_t sp_catalog_files(const SpCatalog *c);
int sp_catalog_file(const SpCatalog *c, int64_t index, char **out,
                    Report *report);

/* List a finished file. footer: the footer's bytes (not the 8 after
 * it). A file already listed is left as it is. A refused file
 * (REPORT_REFUSED, REPORT_UNSUPPORTED) changes nothing. */
int sp_catalog_add(SpCatalog *c, const char *path, int64_t file_size,
                   const uint8_t *footer, size_t footer_len,
                   Report *report);

/* Files added since the last sp_catalog_bytes. */
int64_t sp_catalog_staged(const SpCatalog *c);

/* The catalog as bytes in *out, for the host to store: sp_free them.
 * The files added since the last call are in one snapshot, of the time
 * of this call. */
int sp_catalog_bytes(SpCatalog *c, uint8_t **out, size_t *out_len,
                     Report *report);

/* DATED COPIES OF THE CATALOG. Beside the published catalog a writer
 * keeps copies of it, catalog.<YYYYMMDD-HHMMSS>.sqlite, the history a
 * mistake can be undone from. A copy is due when the catalog changed
 * since the last copy or check, and copy_seconds have passed since the
 * newest copy (always, when there is none). The interval counts from
 * the time in the newest copy's own name, so a writer that starts
 * again makes no extra copy. has_copy: there is a copy; copy_wall: the
 * time its name says, as wall-clock seconds. changed: the published
 * catalog is not the version of the last copy or check. Returns 1 when
 * a copy is due. The host then makes it only if the catalog's bytes
 * differ from the newest copy's. Reads the wall clock. */
int sp_catalog_copy_due(double copy_seconds, int has_copy, double copy_wall,
                        int changed);

void sp_free(void *p);

/* FOR TESTS ONLY (report.h: TIME). fixed 1: from now on the core's wall
 * clock is `wall` and its monotonic clock `mono`, until the next call.
 * fixed 0: the system's clocks again. It changes the time for every
 * thread of the process; library code does not call it. */
void sp_test_set_clock(int fixed, double wall, double mono);

#ifdef __cplusplus
}
#endif
#endif
