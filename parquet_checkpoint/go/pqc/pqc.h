/* pqc.h — parquet_checkpoint core: the format engine. THIS HEADER IS
 * THE SPEC.
 *
 * Language-neutral by construction: the core never encodes parquet
 * pages and never touches storage. A host (Python/pyarrow, Go, ...)
 * encodes each row group as a complete MINI PARQUET FILE (PAR1 ...
 * footer ... PAR1, sink position starting at 0) and hands the bytes
 * here; the core does the format work:
 *
 *   - rebases the mini-file's thrift footer offsets to the group's
 *     logical position in the final file (Thrift Compact Protocol
 *     streaming rewrite of the eight offset fields parquet.thrift
 *     defines),
 *   - buffers page bytes and cuts immutable CHUNKS at row-group
 *     boundaries once chunk_bytes accumulate (first chunk carries the
 *     leading PAR1),
 *   - combines every group's rebased metadata into one footer and
 *     builds THE CHECKPOINT object (PQC2: header CRC-32, final file
 *     name, watermark, per-chunk manifest with CRC-32s, data region =
 *     [magic if no chunks][buffered final groups][provisional tail]
 *     [footer]),
 *   - parses checkpoint objects back (reader + assembly plan).
 *
 * The host stores chunks and checkpoints wherever it likes (local
 * dir, S3). Durability ordering is the host's contract: store a chunk
 * BEFORE fetching the next one or building a checkpoint that claims
 * it (pqc_chunk_stored is that acknowledgment; an unacknowledged
 * chunk is re-presented, so a failed store retries naturally).
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

/* final_name must end in ".parquet". chunk_bytes is the cut threshold
 * (>= 5 MiB when the host stores on S3 -- the host enforces its sink's
 * part minimum, the core doesn't know the sink). */
PqcWriter *pqc_writer_new(const char *final_name, uint64_t chunk_bytes,
                          char *err, size_t errlen);
void pqc_writer_free(PqcWriter *w);

/* Append one final row group (or several: the mini file may hold more
 * than one). mini = a complete parquet file whose offsets count from
 * 0. Returns rows added, or -1 (err filled). */
int64_t pqc_add_group(PqcWriter *w, const uint8_t *mini, size_t len,
                      char *err, size_t errlen);

/* Chunk drain loop. pqc_next_chunk presents the next cut chunk:
 * returns 1 and fills name/data/len/crc, or 0 when no chunk is ready,
 * or -1 on error. The SAME chunk is presented again until the host
 * acknowledges the store with pqc_chunk_stored (retry after a failed
 * PUT = just call pqc_next_chunk again). data is valid until the
 * acknowledgment. Chunk names are "<final_name>.chunks/NNNNNN". */
int pqc_next_chunk(PqcWriter *w, char name[PQC_NAME_MAX],
                   const uint8_t **data, uint64_t *len, uint32_t *crc);
void pqc_chunk_stored(PqcWriter *w);

/* Build THE checkpoint object. tail_mini (may be NULL) is a complete
 * parquet file of provisional rows -- rows not yet added as final
 * groups; they ride this checkpoint and are superseded by a later
 * pqc_add_group of the same rows. If NO groups have ever been added
 * and no chunks exist, tail_mini is REQUIRED (pass an empty parquet
 * file for a zero-row checkpoint) and is used verbatim as the data
 * region. Returns malloc'd checkpoint bytes (caller: pqc_free), or
 * NULL (err filled). out_watermark = rows covered. Calling this with
 * an unacknowledged pending chunk is an error: claims never precede
 * durability. */
uint8_t *pqc_build_checkpoint(PqcWriter *w, const uint8_t *tail_mini,
                              size_t tail_len, uint64_t *out_len,
                              uint64_t *out_watermark,
                              char *err, size_t errlen);

/* Bookkeeping views. */
uint64_t pqc_rows_final(const PqcWriter *w);   /* rows in added groups   */
uint32_t pqc_chunks_cut(const PqcWriter *w);   /* acknowledged chunks    */
uint64_t pqc_logical_end(const PqcWriter *w);  /* next group's position  */

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
