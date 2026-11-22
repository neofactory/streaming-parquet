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
 * are little-endian. Errors are LOUD: calls return NULL/negative and
 * write a message into the caller's err buffer; nothing is guessed.
 */

#ifndef PQC_H
#define PQC_H

#include <stddef.h>
#include <stdint.h>

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
PqcWriter *pqc_writer_new(const char *final_name, uint64_t chunk_bytes,
                          char *err, size_t errlen);
void pqc_writer_free(PqcWriter *w);

/* Append one final row group (or several: the mini file may hold more
 * than one). mini = a complete parquet file whose offsets count from
 * 0. It replaces every unfinished object: the final group holds their
 * rows. Returns rows added, or -1 (err filled). */
int64_t pqc_add_group(PqcWriter *w, const uint8_t *mini, size_t len,
                      char *err, size_t errlen);

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
 * Returns its rows, or -1 (err filled). */
int64_t pqc_add_unfinished(PqcWriter *w, const uint8_t *mini, size_t len,
                           char *err, size_t errlen);

/* Forget the unfinished objects: the next checkpoint claims the final
 * rows only. */
void pqc_drop_unfinished(PqcWriter *w);

/* Object drain loop. pqc_next_object presents the next object to
 * store: returns 1 and fills name/data/len/crc, or 0 when none is
 * owed, or -1 on error. The SAME object is presented again until the
 * host acknowledges the store with pqc_object_stored (retry after a
 * failed PUT = just call pqc_next_object again). data is valid until
 * the acknowledgment. The object is a chunk when the loose final
 * groups reach chunk_bytes (a group not yet stored alone is then
 * written only in the chunk), else a final group alone, else the
 * unfinished object pqc_add_unfinished took. Object names are
 * "<final_name>.chunks/NNNNNN", numbered in the order made. */
int pqc_next_object(PqcWriter *w, char name[PQC_NAME_MAX],
                    const uint8_t **data, uint64_t *len, uint32_t *crc);
void pqc_object_stored(PqcWriter *w);

/* Build THE checkpoint object: the list of the stored objects and the
 * footer of the whole. empty_mini is used only by a writer with no
 * rows at all: a complete (empty) parquet file, taken verbatim as the
 * data region; pass NULL otherwise. Returns malloc'd checkpoint bytes
 * (caller: pqc_free), or NULL (err filled). out_watermark = rows
 * covered. Calling this while pqc_next_object still owes an object is
 * an error: claims never precede durability. */
uint8_t *pqc_build_checkpoint(PqcWriter *w, const uint8_t *empty_mini,
                              size_t empty_len, uint64_t *out_len,
                              uint64_t *out_watermark,
                              char *err, size_t errlen);

/* Bookkeeping views. */
uint64_t pqc_rows_final(const PqcWriter *w);      /* rows in final groups     */
uint64_t pqc_rows_unfinished(const PqcWriter *w); /* rows in unfinished objects */
uint32_t pqc_chunks_cut(const PqcWriter *w);      /* chunks stored            */
uint64_t pqc_logical_end(const PqcWriter *w);     /* next final group's position */

/* ---- reader ------------------------------------------------------------- */

typedef struct PqcReader PqcReader;

/* Parse + validate (magic, version, CRC-32). NULL on refusal. */
PqcReader *pqc_reader_open(const uint8_t *ckpt, size_t len,
                           char *err, size_t errlen);
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
 * byte-parity with the Python reference). Returns malloc'd bytes. */
uint8_t *pqc_shift_footer(const uint8_t *thrift, size_t len,
                          int64_t delta, size_t *out_len,
                          char *err, size_t errlen);

void pqc_free(void *p);

#ifdef __cplusplus
}
#endif
#endif /* PQC_H */
