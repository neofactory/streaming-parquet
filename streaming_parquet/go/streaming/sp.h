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
 * lock alone, and logs sp_ownership_unread: an unreachable store at
 * start delays the uploads and never the stream.
 *
 * `who` in each message is the host's own call, as its user wrote it:
 * create_stream('a/b') in Python, CreateStream("a/b") in Go.
 */

/* The lock record: {"pid", "host", "port", "run"} as JSON text, for the
 * host to write into the lock file it holds: sp_free it. NULL when
 * memory is out. */
char *sp_lock_record(int64_t pid, const char *host, int64_t port,
                     const char *run, size_t *out_len);

/* The refusal for a lock another process holds. `holder` is the text of
 * the lock file (its lock record; any text is taken, and what cannot be
 * read from it shows as "?"). */
void sp_lock_refusal(const char *who, const char *lock_path,
                     const char *holder, size_t holder_len,
                     char *msg, size_t msglen);

typedef enum {
    SP_OWN = 0,        /* this writer owns the stream; msg is empty */
    SP_TAKE_OVER = 1,  /* it owns it, from a writer that died; msg is the
                          warning to log */
    SP_REFUSED = 2     /* another server writes it; msg is the error */
} SpOwnership;

/* The ticket check. ticket: what the store holds, NULL for none. host:
 * this writer's address. now, if_absent: as sp_ticket_is_stale. */
int sp_ownership(const char *who, const char *name, const char *store,
                 const SpTicket *ticket, const char *host,
                 double now, double if_absent, char *msg, size_t msglen);

/* The warning for a ticket the host could not read (`why`: its error). */
void sp_ownership_unread(const char *name, const char *store,
                         const char *why, char *msg, size_t msglen);

/* THE WRITE PLAN
 *
 * Which committed rows go to the stream's files, and when. One SpWrites
 * per stream holds the plan's state; the host tells it what was
 * committed and what time it is, pins the rows, and does the writes the
 * plan names. The plan does no I/O and has no clock: `now` is the
 * host's monotonic clock, in seconds.
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

/* A plan that is off, at row 0, with row_group_rows 1 and no cadences.
 * NULL when memory is out. */
SpWrites *sp_writes_new(void);
void sp_writes_free(SpWrites *w);

/* The cadences, from now on. A NULL seconds is none: no row group by
 * time, no checkpoints. Returns 0, or -1 with the reason in err:
 * row_group_rows under 1, seconds negative or not finite. New options
 * start the checkpoint cadence again. */
int sp_writes_set_options(SpWrites *w, int64_t row_group_rows,
                          const double *row_group_seconds,
                          const double *checkpoint_seconds,
                          char *err, size_t errlen);

/* Turn the plan on at the row `committed` (the first row not yet
 * committed): earlier rows are not planned. No change when it is on. */
void sp_writes_start(SpWrites *w, int64_t committed);

/* Turn the plan off. A host that wants the pending rows in the file
 * forces a row group first. */
void sp_writes_stop(SpWrites *w);

/* The kind of write due for `committed` at `now`; nothing moves. */
int sp_writes_due(const SpWrites *w, int64_t committed, double now);

/* sp_writes_due, and the pending rows' clock starts when it was not
 * running: then *begins is 1 (the pending rows begin at this commit),
 * else 0. The answer is from before the clock started. */
int sp_writes_commit(SpWrites *w, int64_t committed, double now,
                     int *begins);

/* Plan the write for a pin that holds the rows [lo, hi): hi is the
 * committed row count the host pinned to, lo is cursor or, after a
 * loss, later. force_row_group: plan a row group whatever is due.
 * *out is the write; the plan moves with it. */
void sp_writes_plan(SpWrites *w, int force_row_group, int64_t lo,
                    int64_t hi, double now, SpWrite *out);

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
 *                       are superseded from `now`.
 *   sp_store_due        the files whose checkpoints were superseded
 *                       keep_seconds ago or more: a reader told of them
 *                       has had that long. The host deletes them and
 *   sp_store_discarded  says so.
 *
 * Two clocks, both the host's: `wall` is the time the ticket shows
 * (stored_at), `now` is monotonic seconds.
 */
typedef struct SpStore SpStore;

/* keep_seconds: how long a superseded checkpoint stays. NULL when
 * memory is out or keep_seconds is negative or not finite. */
SpStore *sp_store_new(double keep_seconds);
void sp_store_free(SpStore *s);

/* The keep time from now on. Returns 0, or -1 for a value that is
 * negative or not finite. */
int sp_store_set_keep_seconds(SpStore *s, double keep_seconds);
double sp_store_keep_seconds(const SpStore *s);

/* Each returns 0, or -1 when memory is out or the name is NULL. */
int sp_store_begins(SpStore *s, const char *file, int64_t first_row);
int sp_store_covers(SpStore *s, const char *file, int64_t version,
                    const char *checkpoint, int64_t final_rows, int64_t rows);
int sp_store_stored(SpStore *s, const char *file, int64_t version,
                    const char *checkpoint, int64_t rows, double wall);
int sp_store_finished(SpStore *s, const char *file);
int sp_store_awaits(SpStore *s, const char *file);

/* 1 when the file was awaiting its catalog (it now leaves the ticket,
 * and waits for sp_store_dropped); 0 when it was not. */
int sp_store_listed(SpStore *s, const char *file);

/* The ticket without the listed files is written: their checkpoints are
 * superseded from now. Returns how many files that is. */
int sp_store_dropped(SpStore *s, double now);

/* The index-th file (from 0) whose checkpoints are due for deletion at
 * `now`; NULL past the last. The pointer holds until the next call that
 * changes the store. */
const char *sp_store_due(const SpStore *s, double now, int32_t index);
int sp_store_discarded(SpStore *s, const char *file);

/* The newest row of this run the store holds, -1 for none; 1 and *wall
 * filled when one is there, else 0. */
int64_t sp_store_stored_row(const SpStore *s);
int sp_store_stored_at(const SpStore *s, double *wall);

/* Put what the store holds into a ticket: stored_row, stored_at,
 * keep_seconds, and `files` (replaced), in the order of the files'
 * names. `where` is the stream's folder in the store, ending in '/':
 * each checkpoint is written as where + its object name. Returns 0, or
 * -1 with the reason in err. */
int sp_store_fill(const SpStore *s, SpTicket *t, const char *where,
                  char *err, size_t errlen);

/* How many files the store's state holds (in the ticket, awaiting, or
 * with checkpoints to delete): what a long run must not let grow. */
int32_t sp_store_files(const SpStore *s);

/* THE READER
 *
 * A reader of a stream by name: where it finds the writer, when it
 * waits, when it starts again. One SpReader per reader holds the state;
 * the host runs the thread, reads the store, makes the connection, and
 * tells the reader what each of those found. The reader answers with
 * the next step. It does no I/O and has no clock: `now` is the host's
 * wall clock (the ticket's ts is on it), `mono` its monotonic clock,
 * both in seconds.
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
 * What a reader says (sp_reader_message, for the host's log): one line
 * when a wait for a writer starts, one warning when the wait has lasted
 * SP_READER_WARN_SECONDS, an error when the store cannot be read and a
 * line when it can again, an error for each failed connection, and an
 * error when the writer started again.
 */
typedef struct SpReader SpReader;

#define SP_READER_POLL_SECONDS 1.0
#define SP_READER_WARN_SECONDS 30.0

typedef enum {
    SP_R_NO_TICKET = 0,       /* "no-ticket" */
    SP_R_ROOT_UNREACHABLE,    /* "root-unreachable": the store cannot be read */
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

typedef enum {
    SP_LOG_INFO = 1, SP_LOG_WARNING = 2, SP_LOG_ERROR = 3
} SpLog;

typedef struct {
    int action;               /* SpDo */
    int state;                /* SpReaderState, after this step */
    int fresh_ring;           /* drop the ring in hand first */
    double wait_seconds;      /* for SP_DO_WAIT */
} SpStep;

/* who: the host's own call, as its user wrote it (connect('a/b')).
 * store: where the reader looks, for the messages; NULL for a reader
 * with a direct ticket. NULL when memory is out or name is NULL. */
SpReader *sp_reader_new(const char *who, const char *name, const char *store,
                        int direct);
void sp_reader_free(SpReader *r);

/* The folder above the name: "a/b" for "a/b/c", "" for a name with no
 * folder. */
const char *sp_reader_folder(const SpReader *r);

/* What a poll found. ticket: for SP_FOUND_TICKET. why: the host's error
 * for SP_FOUND_UNREADABLE. if_absent: as sp_ticket_is_stale. */
void sp_reader_poll(SpReader *r, int found, const SpTicket *ticket,
                    const char *why, double now, double mono,
                    double if_absent, SpStep *step);

/* The answer to SP_DO_LIST: 1 when the folder exists (in the store or
 * among this process's streams), 0 when not. A host that could not
 * list polls with SP_FOUND_UNREADABLE instead. */
void sp_reader_folder_is(SpReader *r, int exists, double mono, SpStep *step);

/* The connection was made and rows flow. */
void sp_reader_connected(SpReader *r);

/* The connection was lost or could not be made (why: the host's error),
 * or the local stream closed (why: NULL). The step is a wait. */
void sp_reader_lost(SpReader *r, const char *why, double mono, SpStep *step);

void sp_reader_close(SpReader *r);

int sp_reader_state(const SpReader *r);
const char *sp_reader_state_name(int state);

/* Why the reader is not streaming, in words; "" while it is. */
const char *sp_reader_reason(const SpReader *r);

/* The run the reader last connected to; NULL for none (or a local
 * stream). */
const char *sp_reader_run(const SpReader *r);

/* The next line for the host's log: its level (SpLog) with the text in
 * msg, or 0 when there is none. Call until 0 after each step. */
int sp_reader_message(SpReader *r, char *msg, size_t msglen);

/* THE VIEW
 *
 * One SQL view over a stream: the store's rows up to one row, the
 * ring's rows after it. One SpView per view holds the rules' state.
 * The host reads the ticket, fetches and attaches the catalog, runs
 * the SQL and the thread; the view decides what the text names, writes
 * the text, and says when to look again. It does no I/O and has no
 * clock: `now` is the host's wall clock, `mono` its monotonic clock.
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
 * columns. NULL when memory is out or a name is missing. */
SpView *sp_view_new(const char *view, const char *stream, const char *memory,
                    const char *empty);
void sp_view_free(SpView *v);

/* Plan the text for a ticket (NULL: the store has none). files: for
 * each of the ticket's files, in order, the path of the file its
 * checkpoint belongs to (the checkpoint core's rule); NULL when the
 * ticket has none. ring_run: the run of the ring's rows, NULL for none.
 * window: as sp_view_seam. if_absent: as sp_ticket_is_stale.
 * keep_if_absent: the keep time of a ticket that names none. Returns
 * 0, or -1 when memory is out. */
int sp_view_plan(SpView *v, const SpTicket *t, const char *const *files,
                 const char *ring_run, const int64_t *window, double now,
                 double mono, double if_absent, double keep_if_absent,
                 SpViewPlan *out);

/* The catalog version the plan names (SP_CATALOG_EXACT), else NULL. */
const char *sp_view_catalog_etag(const SpView *v);

/* The statement that makes the planned view: sp_free it. catalog: the
 * name the host attached the catalog under, NULL for none. NULL with
 * the reason in err when there is nothing to read the columns from. */
char *sp_view_sql(const SpView *v, const char *catalog, char *err,
                  size_t errlen);

/* The host ran the statement. */
void sp_view_built(SpView *v);

/* The seam of the view as built; -1 before. */
int64_t sp_view_seam_row(const SpView *v);

/* Why the ring is not used as wanted, in words; "" when it is. */
const char *sp_view_ring_note(const SpView *v);

/* Seconds until the next look: a live writer's next write of the
 * ticket and a margin, never under SP_VIEW_POLL_MIN_SECONDS; with no
 * live writer, SP_VIEW_POLL_DEFAULT_SECONDS. */
double sp_view_wait_seconds(const SpView *v, double now);

/* A look failed (why: the host's error): returns the seconds to wait
 * before the next. sp_view_followed: a look worked. */
double sp_view_failed(SpView *v, const char *why);
void sp_view_followed(SpView *v);

/* As sp_reader_message. */
int sp_view_message(SpView *v, char *msg, size_t msglen);

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

/* The length of the footer of a parquet file, from the file's last 8
 * bytes or more (`tail`, `len`): the footer is the `length` bytes
 * before those 8. -1 with the reason for bytes that do not end a
 * parquet file. */
int64_t sp_parquet_footer_length(const uint8_t *tail, size_t len,
                                 char *err, size_t errlen);

/* A catalog from the bytes the store holds; len 0: a new one. data_path:
 * the stream's folder in the store, ending in '/'. now: the host's wall
 * clock. NULL with the reason for bytes that are not a stream's catalog. */
SpCatalog *sp_catalog_open(const uint8_t *bytes, size_t len,
                           const char *data_path, double now,
                           char *err, size_t errlen);
void sp_catalog_free(SpCatalog *c);

/* 1 when the catalog lists the file of this full path. */
int sp_catalog_lists(const SpCatalog *c, const char *path);

/* How many files the catalog lists, and the path of the index-th (from
 * 0): sp_free it; NULL past the last. */
int64_t sp_catalog_files(const SpCatalog *c);
char *sp_catalog_file(const SpCatalog *c, int64_t index);

/* List a finished file. footer: the footer's bytes (not the 8 after
 * it). A file already listed is left as it is. Returns 0, or -1 with
 * the reason; a refused file changes nothing. */
int sp_catalog_add(SpCatalog *c, const char *path, int64_t file_size,
                   const uint8_t *footer, size_t footer_len,
                   char *err, size_t errlen);

/* Files added since the last sp_catalog_bytes. */
int64_t sp_catalog_staged(const SpCatalog *c);

/* The catalog as bytes, for the host to store: sp_free them. The files
 * added since the last call are in one snapshot, of the time `now`.
 * NULL with the reason on failure. */
uint8_t *sp_catalog_bytes(SpCatalog *c, double now, size_t *out_len,
                          char *err, size_t errlen);

void sp_free(void *p);

#ifdef __cplusplus
}
#endif
#endif
