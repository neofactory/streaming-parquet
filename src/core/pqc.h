/* pqc.h — parquet_checkpoint core: the format engine. THIS HEADER IS
 * THE SPEC.
 *
 * Language-neutral by construction: the core never encodes parquet
 * pages and never touches storage. A host (Python/pyarrow, Go, ...)
 * encodes rows as a complete MINI PARQUET FILE (PAR1 ... footer ...
 * PAR1, sink position starting at 0) and hands the bytes here; the
 * core does the format work and hands back the bytes of each object
 * for the host to store.
 *
 * THE FILE WHILE IT IS WRITTEN is a list of objects, in file order,
 * and a footer. Each object is written once and never changed:
 *
 *   chunks       >= chunk_bytes of final row groups each (the first
 *                object of the file carries the leading PAR1). A chunk
 *                is a part S3 can copy into the finished file.
 *   loose groups final row groups that do not yet fill a chunk, one
 *                object each. A chunk replaces them when they do.
 *   unfinished   the rows of the row group still being filled. Each
 *                checkpoint adds one object with only its new rows.
 *                A final row group replaces them all.
 *
 * THE CHECKPOINT object (PQC2: header CRC-32, final file name,
 * watermark, manifest = that list with a byte length and CRC-32 for
 * each object, data region = the footer of the whole) says what the
 * file is at one moment. A reader takes the objects in list order,
 * then the data region: that is an ordinary parquet file. A file with
 * no rows has no objects, and its data region is a whole empty parquet
 * file.
 *
 * THE COST OF A CHECKPOINT FOLLOWS THE NEW ROWS, not the size of the
 * file: it stores one unfinished object and the checkpoint object, and
 * no row a stored object already holds.
 *
 * MERGE IN STEPS. Every unfinished object is a row group, and the
 * footer, written at every checkpoint, has an entry for each. So a
 * small object left in place costs footer bytes at every checkpoint,
 * and a merge costs its rows once more. The objects of one size are a
 * run. When the footer bytes a run has cost reach the bytes of its
 * objects, or the run reaches PQC_RUN_LIMIT objects, the run and
 * every object after it are replaced by one object of the same rows,
 * one size step up. A merge is the host encoding those rows again, not
 * the store joining objects (S3 cannot). It replaces only the END of
 * the list, so no object that stays ever moves, and each row is
 * written once for each step.
 *
 * The core also
 *   - rebases each mini-file's thrift footer offsets to its logical
 *     position in the final file (Thrift Compact Protocol streaming
 *     rewrite of the eight offset fields parquet.thrift defines),
 *   - combines every row group's rebased metadata into one footer,
 *   - parses checkpoint objects back (reader + assembly plan).
 *
 * The host stores objects and checkpoints wherever it likes (local
 * dir, S3). Durability ordering is the host's contract: store an
 * object BEFORE fetching the next one or building a checkpoint that
 * names it (pqc_object_stored is that acknowledgment; an
 * unacknowledged object is given again, so a failed store retries
 * naturally). An object no kept checkpoint names any more is the
 * host's to delete.
 *
 * Dependencies: libc only. Not thread-safe per writer/reader (one
 * owner each, like a file handle). All returned buffers are malloc'd
 * and freed with pqc_free(). All multi-byte integers in the formats
 * are little-endian.
 *
 * How a function reports is in report.h, the same for the three cores:
 * a function that can fail returns a status and takes a Report as its
 * last parameter; what it makes comes back through an output parameter.
 * Errors are LOUD: nothing is guessed. REPORT_REFUSED: bytes that are
 * not what they must be (a mini file, a checkpoint). REPORT_INVALID: a
 * call the writer's state does not allow, or a NULL argument. This core
 * has no lines for the log. One rule of it reads the clock: how long a
 * replaced checkpoint version is kept (CHECKPOINT VERSIONS, below).
 */

#ifndef PQC_H
#define PQC_H

#include <stddef.h>
#include <stdint.h>

#include "report.h"

#ifdef __cplusplus
extern "C" {
#endif

#define PQC_VERSION 2            /* PQC2 checkpoint format */
#define PQC_NAME_MAX 256         /* final-name and chunk-name limit  */

/* ---- writer ------------------------------------------------------------- */

typedef struct PqcWriter PqcWriter;

/* The most unfinished objects of one size before they merge. It is
 * also the bound on what a reader that opens the file cold must
 * fetch for the unfinished row group: this many for each size step. */
#define PQC_RUN_LIMIT 16

/* final_name must end in ".parquet". chunk_bytes is the cut threshold
 * (>= 5 MiB when the host stores on S3 -- the host enforces its sink's
 * part minimum, the core doesn't know the sink). */
int pqc_writer_new(const char *final_name, uint64_t chunk_bytes,
                   PqcWriter **out, Report *report);
void pqc_writer_free(PqcWriter *w);

/* Append one final row group (or several: the mini file may hold more
 * than one). mini = a complete parquet file whose offsets count from
 * 0. It replaces every unfinished object: the final group holds their
 * rows. *rows: the rows added. */
int pqc_add_group(PqcWriter *w, const uint8_t *mini, size_t len,
                  int64_t *rows, Report *report);

/* A checkpoint is due, and new_rows rows are in no object yet. Fills
 * the rows the host must encode as ONE row group, as rows of the file:
 * [*first_row, *first_row + *rows). They are the new rows alone, or,
 * when a run merges, the rows of that run and of every object after it
 * together with the new rows. *first_row - pqc_rows_final() is where
 * the span begins among the rows after the last final row group. */
void pqc_next_span(PqcWriter *w, uint64_t new_rows,
                   uint64_t *first_row, uint64_t *rows);

/* The span of the last pqc_next_span, encoded: a complete parquet
 * file of exactly those rows. The object replaces the unfinished
 * objects from the span's first row on, once the host has stored it.
 * *rows: its rows. */
int pqc_add_unfinished(PqcWriter *w, const uint8_t *mini, size_t len,
                       int64_t *rows, Report *report);

/* Forget the unfinished objects: the next checkpoint claims the final
 * rows only. */
void pqc_drop_unfinished(PqcWriter *w);

/* Object drain loop. pqc_next_object presents the next object to
 * store: *have is 1 and name/data/len/crc are filled, or *have is 0
 * when none is owed. The SAME object is presented again until the
 * host acknowledges the store with pqc_object_stored (retry after a
 * failed PUT = just call pqc_next_object again). data is valid until
 * the acknowledgment. The object is a chunk when the loose final
 * groups reach chunk_bytes (a group not yet stored alone is then
 * written only in the chunk), else a final group alone, else the
 * unfinished object pqc_add_unfinished took. Object names are
 * "<final_name>.chunks/NNNNNN", numbered in the order made. */
int pqc_next_object(PqcWriter *w, char name[PQC_NAME_MAX],
                    const uint8_t **data, uint64_t *len, uint32_t *crc,
                    int *have, Report *report);
void pqc_object_stored(PqcWriter *w);

/* Build THE checkpoint object: the list of the stored objects and the
 * footer of the whole. empty_mini is used only by a writer with no
 * rows at all: a complete (empty) parquet file, taken verbatim as the
 * data region; pass NULL otherwise. *out: the malloc'd checkpoint
 * bytes (caller: pqc_free). out_watermark = rows covered. Calling this
 * while pqc_next_object still owes an object is an error
 * (REPORT_INVALID): claims never precede durability. */
int pqc_build_checkpoint(PqcWriter *w, const uint8_t *empty_mini,
                         size_t empty_len, uint8_t **out, uint64_t *out_len,
                         uint64_t *out_watermark, Report *report);

/* Bookkeeping views. */
uint64_t pqc_rows_final(const PqcWriter *w);      /* rows in final groups     */
uint64_t pqc_rows_unfinished(const PqcWriter *w); /* rows in unfinished objects */
uint32_t pqc_chunks_cut(const PqcWriter *w);      /* chunks stored            */
uint64_t pqc_logical_end(const PqcWriter *w);     /* next final group's position */

/* ---- reader ------------------------------------------------------------- */

typedef struct PqcReader PqcReader;

/* Parse + validate (magic, version, CRC-32). REPORT_REFUSED for bytes
 * that are not a checkpoint. */
int pqc_reader_open(const uint8_t *ckpt, size_t len, PqcReader **out,
                    Report *report);
void pqc_reader_free(PqcReader *r);

const char *pqc_reader_name(const PqcReader *r);
uint64_t pqc_reader_watermark(const PqcReader *r);
uint32_t pqc_reader_n_chunks(const PqcReader *r);
/* idx in [0, n_chunks). Returns 0, or -1 on bad idx. */
int pqc_reader_chunk(const PqcReader *r, uint32_t idx, const char **name,
                     uint64_t *length, uint32_t *crc);
/* The data region's [offset, offset+length) within the checkpoint
 * object's own bytes -- the assembly plan's final entry (range copy /
 * concat after the chunks). */
void pqc_reader_data(const PqcReader *r, uint64_t *offset,
                     uint64_t *length);

/* ---- checkpoint versions ------------------------------------------------- */

/* A checkpoint of file <name> is stored as VERSIONS: objects named
 * <name>.NNNNNN.ckpt, numbered up from 1 (six digits or more), each
 * written once and never changed. A reader is told of one version (a
 * stream's ticket names it) and may open it some time later. So a
 * version that a later one replaced is KEPT keep_seconds more, and
 * then deleted, and with it each object that no kept version names.
 * Nothing runs on a timer: the deletes are found in the step that
 * stores a later version. The time is the monotonic clock's
 * (report.h: TIME).
 *
 * One PqcVersions for the versions of one file in one place (a
 * directory, a bucket). The host stores and deletes; this says what. */
typedef struct PqcVersions PqcVersions;

#define PQC_OBJECT_MAX 320       /* an object's name: a chunk's or a version's */
#define PQC_SUPERSEDED_KEEP_S 600.0

/* The object name of version seq of file name's checkpoint.
 * REPORT_INVALID for a name that does not fit, or a seq below 0. */
int pqc_ckpt_object(const char *name, int64_t seq, char out[PQC_OBJECT_MAX],
                    Report *report);

/* 1 when `object` is the name of a checkpoint version, with the file's
 * name and the version filled; 0 when it is not one. A checkpoint
 * named without a version (<name>.ckpt, as written before versions) is
 * version 0. */
int pqc_ckpt_object_parse(const char *object, char name[PQC_OBJECT_MAX],
                          int64_t *seq);

/* REPORT_INVALID when keep_seconds is negative or not finite. */
int pqc_versions_new(double keep_seconds, PqcVersions **out, Report *report);
void pqc_versions_free(PqcVersions *v);

/* The version `object`, with the bytes ckpt, is stored (whole, before
 * this call: a version is stored before any delete). It replaces the
 * newest version this holds, from now. REPORT_REFUSED when the bytes
 * are not a checkpoint. After it, pqc_versions_due lists the deletes. */
int pqc_versions_stored(PqcVersions *v, const char *object,
                        const uint8_t *ckpt, size_t len, Report *report);

/* The index-th object (from 0) to delete now, NULL past the last: each
 * version replaced keep_seconds ago or more, the version first and
 * then the objects that only it named. The pointer holds until the
 * next call that changes v. */
const char *pqc_versions_due(const PqcVersions *v, int32_t index);

/* The host deleted `object`: it leaves the list. An object whose
 * delete failed stays, and is listed again. REPORT_INVALID for an
 * object that is not in the list. */
int pqc_versions_deleted(PqcVersions *v, const char *object, Report *report);

/* 1 when a version this keeps names `object`. */
int pqc_versions_names(const PqcVersions *v, const char *object);

/* ---- the mirror --------------------------------------------------------- */

/* THE TWO-COPIES MODEL. A writer writes its objects and checkpoint
 * versions to a LOCAL copy (a disk, the spool) at its own cadence,
 * never held up by the network. A mirror copies them to a REMOTE copy
 * (a bucket) at the cadence the link allows. The local disk's loss
 * window is the rows since the last pass that worked, by design.
 *
 * One PqcMirror for one file. It does no input and no output: in each
 * PASS the host says what the local copy has (pqc_mirror_local), then
 * asks for the next step (pqc_mirror_next), does it, and says how it
 * went, until the step is NOTHING or DONE. The rules are here:
 *
 *   WHAT IS SENT. Only the newest local version, and of the objects it
 *   names only those the remote lacks. An object never changes, so one
 *   that was sent or seen there is not looked at again. Every version
 *   an outage swallowed, and every object only such a version named,
 *   is never sent.
 *
 *   THE ORDER. Every object a version names is on the remote before
 *   that version: a reader told of a version finds what it names.
 *
 *   WHAT IS DELETED. The remote versions this mirror stored, by the
 *   rule above (CHECKPOINT VERSIONS).
 *
 *   THE END OF A FILE, in one order: the writer's last checkpoint
 *   locally; that version on the remote; the file assembled locally;
 *   the file assembled on the remote from the remote's own objects
 *   (the same bytes as the local file). While the remote cannot be
 *   reached the file stays where it is in that order, readable and
 *   crash-durable locally; the writer goes on to its next file, and
 *   every later pass tries the next step again.
 *
 * A step that fails ends the pass (pqc_mirror_failed); what was done
 * before it stays done, and the next pass goes on from there. A new
 * process makes a new PqcMirror for a file an earlier one left
 * unfinished (THE SWEEP AT START, below): with nothing remembered it
 * asks the remote again, and finishes the file from what is there. */
typedef struct PqcMirror PqcMirror;

typedef enum {
    PQC_MIRROR_NOTHING = 0,       /* the pass is over; nothing more now */
    PQC_MIRROR_STAT = 1,          /* does the remote hold object `name`,
                                     and how many bytes? Answer with
                                     pqc_mirror_remote_has. `size`: the
                                     bytes it must have. */
    PQC_MIRROR_UPLOAD = 2,        /* copy object `name` (`size` bytes)
                                     from the local copy to the remote */
    PQC_MIRROR_UPLOAD_VERSION = 3,/* store the version `name` on the
                                     remote: the bytes given to
                                     pqc_mirror_local */
    PQC_MIRROR_STORED = 4,        /* the remote now holds the file's first
                                     `size` rows in the version `name`: what
                                     a ticket may say. Nothing to do on
                                     the remote. */
    PQC_MIRROR_DELETE = 5,        /* delete object `name` on the remote */
    PQC_MIRROR_ASSEMBLE_LOCAL = 6,/* assemble the file in the local copy
                                     from its newest version there */
    PQC_MIRROR_ASSEMBLE_REMOTE = 7,/* assemble the file on the remote.
                                     flag 1: from the version this mirror
                                     stored there (pqc_mirror_shipped has
                                     its bytes: no read-back). flag 0:
                                     from the newest version the remote
                                     holds, or nothing to do when the file
                                     is there already. */
    PQC_MIRROR_DONE = 8           /* the file is finished in both copies:
                                     free the mirror */
} PqcMirrorAction;

typedef struct {
    int32_t action;               /* PqcMirrorAction */
    int32_t flag;
    uint64_t size;
    char name[PQC_OBJECT_MAX];
} PqcMirrorStep;

/* name: the file's name (it ends in ".parquet"). keep_seconds: how
 * long a replaced remote version stays. */
int pqc_mirror_new(const char *name, double keep_seconds, PqcMirror **out,
                   Report *report);
void pqc_mirror_free(PqcMirror *m);

/* A pass begins. What the local copy has now:
 *   object, ckpt, len  the newest checkpoint version of the file there
 *                      and its bytes; NULL and 0 when it has none
 *   final_exists       1 when the finished file is there
 *   live               1 while the writer has the file open: the pass
 *                      mirrors it and stops; 0 when the writer ended
 *                      it: the pass also takes the steps of THE END OF
 *                      A FILE
 * REPORT_REFUSED when the bytes are not a checkpoint, or name another
 * file. The bytes are read during the call, and not kept after it
 * unless the version is stored on the remote. */
int pqc_mirror_local(PqcMirror *m, const char *object, const uint8_t *ckpt,
                     size_t len, int final_exists, int live, Report *report);

/* The next step of the pass. The same step is given again until the
 * host answers it: pqc_mirror_remote_has for STAT, pqc_mirror_did for
 * the others (STORED too), pqc_mirror_failed for one that failed.
 * NOTHING and DONE need no answer. REPORT_INVALID before a pass
 * begins. */
int pqc_mirror_next(PqcMirror *m, PqcMirrorStep *step, Report *report);

/* The answer to STAT: whether the remote holds the object, and its
 * bytes there. */
int pqc_mirror_remote_has(PqcMirror *m, int exists, uint64_t size,
                          Report *report);
/* The step was done. */
int pqc_mirror_did(PqcMirror *m, Report *report);
/* The step failed: the pass is over. */
void pqc_mirror_failed(PqcMirror *m);

/* The newest version this mirror stored on the remote: its bytes (they
 * hold until the next pqc_mirror_did, and the mirror owns them) and its
 * object name. 1 when there is one, else 0. */
int pqc_mirror_shipped(const PqcMirror *m, const uint8_t **ckpt, size_t *len,
                       const char **object);

/* What the mirror is doing on the remote right now, in words for a
 * person who waits ("uploading object x (3.4 MB)"); "" between steps. */
const char *pqc_mirror_doing(const PqcMirror *m);

/* THE SWEEP AT START. A writer that starts owns its stream, so every
 * checkpoint version it finds is a leftover of an earlier run. For
 * each file that has versions, what to do with it. */
typedef enum {
    PQC_SWEEP_NOTHING = 0,
    PQC_SWEEP_FINISH = 1,         /* assemble the file here, now */
    PQC_SWEEP_MIRROR = 2,         /* give it a mirror: its passes finish it */
    PQC_SWEEP_AWAIT_CATALOG = 3,  /* it is finished; its versions stay until
                                     a catalog lists the file */
    PQC_SWEEP_DISCARD = 4         /* it is finished; delete its versions and
                                     their objects */
} PqcSweep;

/* In the local copy. final_exists: the finished file is beside the
 * versions (it finished before the crash). has_remote: the local copy
 * is mirrored. keeps_checkpoints: readers are told of the store's
 * unfinished files, so a finished file's versions stay until a catalog
 * lists it. */
int pqc_sweep_local(int final_exists, int has_remote, int keeps_checkpoints);

/* On the remote, in the first pass. in_flight: the file already has a
 * mirror (the local sweep found it). */
int pqc_sweep_remote(int final_exists, int keeps_checkpoints, int in_flight);

/* ---- the assembly of a file ---------------------------------------------- */

/* THE ASSEMBLY OF A FILE. A file's checkpoint becomes the ordinary
 * .parquet file in one place (a directory, a bucket), from the objects
 * that place holds. Anyone can do it, at any time, any number of
 * times: the writer at its close, a mirror, the sweep at start. One
 * PqcFinish for one assembly. The host lists, reads, assembles and
 * deletes; the rules are here:
 *
 *   WHICH VERSION. The newest version the place holds whose bytes are
 *   a checkpoint (the magic, the format's version, the CRC). A newer
 *   version that is not one is passed over, with a line for the log. A
 *   version the host could not READ is not passed over: the host ends
 *   the assembly, and tries again later. (An older version has fewer
 *   rows, and the deletes below would take the newer one away.)
 *
 *   THE CHECKS, each a refusal:
 *     - no version, and no finished file: nothing is there
 *     - a version and the finished file, both: someone must look
 *     - the version names another file
 *     - an object the version names is not there, or not whole
 *
 *   THE DELETES, after the file is assembled, in this order: every
 *   object under <name>.chunks/ (also one that no version names: a
 *   crash left it); every version but the one in use; the version in
 *   use, last. So while any of them is left, a version that can make
 *   the file again is left. With keep_checkpoints nothing is deleted:
 *   readers were told of the versions; PQC_FINISH_DISCARD deletes them
 *   later, in the same order, and refuses when the finished file is
 *   not there.
 *
 * The calls, in order: pqc_finish_new; pqc_finish_version for each
 * checkpoint version the place holds (any order); pqc_finish_given
 * when the caller has the newest version's bytes (it stored them
 * there); then pqc_finish_next and the answer to each step, until
 * DONE. A step that fails in the host, or an answer that is refused,
 * ends the assembly: free it. What was done stays done. */
typedef struct PqcFinish PqcFinish;

enum { PQC_FINISH_ASSEMBLE_FILE = 0, PQC_FINISH_DISCARD = 1 };  /* mode */

typedef enum {
    PQC_FINISH_READ = 1,          /* read the version `name`; give its
                                     bytes to pqc_finish_read */
    PQC_FINISH_STAT = 2,          /* does the place hold `name`, and how
                                     many bytes? Answer with
                                     pqc_finish_has. flag 1: the finished
                                     file; only whether it is there
                                     matters. flag 0: an object, `size`
                                     the bytes it must have. */
    PQC_FINISH_ASSEMBLE = 3,      /* make the file `name` from `size`
                                     sources, in order
                                     (pqc_finish_source); it appears
                                     whole or not at all */
    PQC_FINISH_LIST = 4,          /* list the objects whose names begin
                                     with `name`: pqc_finish_listed for
                                     each, then pqc_finish_did */
    PQC_FINISH_DELETE = 5,        /* delete the object `name` */
    PQC_FINISH_DONE = 6           /* over. flag 1: the file was assembled
                                     now; flag 0: it was not (it was
                                     there already, or this was a
                                     discard) */
} PqcFinishAction;

typedef PqcMirrorStep PqcFinishStep;

/* name: the file's name in the place. REPORT_INVALID for a mode that
 * is not one, or a name that does not fit. */
int pqc_finish_new(const char *name, int mode, int keep_checkpoints,
                   PqcFinish **out, Report *report);
void pqc_finish_free(PqcFinish *f);

/* The place holds the checkpoint version `object`. REPORT_INVALID
 * when it is not a version of this file, or after the first step. */
int pqc_finish_version(PqcFinish *f, const char *object, Report *report);

/* The newest version, `object`, has the bytes ckpt: the caller stored
 * it. The assembly uses it, reads no version, and does not ask
 * whether the finished file is there. REPORT_REFUSED when the bytes
 * are not a checkpoint. */
int pqc_finish_given(PqcFinish *f, const char *object, const uint8_t *ckpt,
                     size_t len, Report *report);

/* The next step. The same step is given again until the host answers
 * it: pqc_finish_read for READ, pqc_finish_has for STAT, pqc_finish_did
 * for the others. DONE needs no answer. REPORT_REFUSED when a check
 * fails. */
int pqc_finish_next(PqcFinish *f, PqcFinishStep *step, Report *report);

/* The answer to STAT. REPORT_REFUSED when a check fails on it. */
int pqc_finish_has(PqcFinish *f, int exists, uint64_t size, Report *report);
/* The answer to READ: the version's bytes. Bytes that are not a
 * checkpoint are no failure while an older version is left: the next
 * step reads that one. REPORT_REFUSED when no version holds, or the
 * version names another file. */
int pqc_finish_read(PqcFinish *f, const uint8_t *ckpt, size_t len,
                    Report *report);
/* During LIST: one object of the list. */
int pqc_finish_listed(PqcFinish *f, const char *object, Report *report);
/* The step was done. */
int pqc_finish_did(PqcFinish *f, Report *report);

/* The index-th source (from 0) of the ASSEMBLE step: the object, and
 * the bytes of it to take. offset -1: the whole object. 1 and the
 * values, or 0 past the last. The name holds until the assembly is
 * freed. */
int pqc_finish_source(const PqcFinish *f, uint32_t index, const char **name,
                      int64_t *offset, int64_t *length);

/* ---- the parts of an assembly -------------------------------------------- */

/* THE PARTS OF AN ASSEMBLY. A bucket makes one object from others as a
 * multipart upload, under two rules of S3: every part but the last is
 * at least part_min bytes, and a part copied on the server comes from
 * ONE object (S3 cannot join small objects). So: a source of at least
 * part_min is copied as a part of its own, and no byte of it travels.
 * Smaller sources that follow one another are fetched and uploaded
 * together as one part; such a run takes in sources until it reaches
 * part_min or the sources end. A file is its chunks, each at least
 * part_min, then its smaller objects and its footer: the chunks are
 * copied, and what follows them travels once, as the last part. */
#define PQC_PART_MIN (5u * 1024u * 1024u)   /* S3's */
#define PQC_MAX_PARTS 10000                 /* S3's */

typedef struct {
    uint32_t first, count;        /* the sources first .. first+count-1 */
    int32_t copy;                 /* 1: one source, copied on the server;
                                     0: fetched, and uploaded as one part */
} PqcPart;

/* The parts for n sources of these sizes, in order. *out: malloc'd,
 * *n_out entries; free it with pqc_free. REPORT_REFUSED for more than
 * PQC_MAX_PARTS parts. */
int pqc_plan_parts(const uint64_t *sizes, uint32_t n, uint64_t part_min,
                   PqcPart **out, uint32_t *n_out, Report *report);

/* ---- a dead writer ------------------------------------------------------ */

/* A writer is dead when nothing has come from it for
 * PQC_DEAD_WRITER_BEATS of its beats, and never sooner than
 * PQC_DEAD_WRITER_MIN_S seconds: one write to S3 has taken 5 seconds.
 * This is the one rule for every place that asks: a reader of a
 * stream's ticket, a writer that takes a stream over, and the start-up
 * sweep that finishes a dead writer's files. beat_seconds is how often
 * the writer shows that it lives, 0 when it does not say. Returns
 * seconds. */
#define PQC_DEAD_WRITER_BEATS 3
#define PQC_DEAD_WRITER_MIN_S 30.0
double pqc_dead_after(double beat_seconds);

/* ---- utilities ---------------------------------------------------------- */

/* CRC-32 (IEEE, zlib-compatible) for host-side chunk verification. */
uint32_t pqc_crc32(const uint8_t *data, size_t len);

/* Rebase a bare parquet footer's offsets by delta (exposed for tests:
 * byte-parity with the Python reference). *out: malloc'd bytes. */
int pqc_shift_footer(const uint8_t *thrift, size_t len, int64_t delta,
                     uint8_t **out, size_t *out_len, Report *report);

void pqc_free(void *p);

/* For tests only: the time this core sees (report.h: TIME). fixed 0 =
 * the system's clocks again. Library code does not call it. */
void pqc_test_set_clock(int fixed, double wall, double mono);

#ifdef __cplusplus
}
#endif
#endif /* PQC_H */
