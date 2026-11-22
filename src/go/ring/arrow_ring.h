/* arrow_ring.h — the arrow-ring C core. This header IS the spec.
 *
 * A recycling builder over column-major ring storage. Writers fill
 * mutable slots; commit freezes them; frozen regions are honest
 * zero-copy Arrow data, because pins prevent the write cursor from
 * recycling a region while anyone holds it.
 *
 * Boundary language: the Arrow C Data Interface. Schemas come in as
 * ArrowSchema; batches come in as ArrowArray; record sets go out as
 * ArrowArrayStream. No types are invented at this boundary, which is
 * what lets Python/Go/Rust/C++ bindings stay thin and lets DuckDB,
 * Flight, IPC, and parquet writers consume the ring directly.
 *
 * Threading contract: ONE writer thread per ring (ar_row_*, ar_set_*,
 * ar_commit, ar_append_*). Any number of reader threads (ar_get_*,
 * ar_pin, record sets). Pins are internally synchronized.
 *
 * How a function reports is in report.h, the same for the three cores:
 * a function that can fail, or that can have a line for the host's
 * log, returns a status (a ReportStatus) and takes a Report as its last
 * parameter. A status that is not REPORT_OK leaves a detailed, loud
 * reason in the report's error. There are no silent fallbacks anywhere
 * in the core. The core writes nowhere, calls no function of the host,
 * and never waits: where the writer must wait (a pin holds the row it
 * needs) the call answers REPORT_BLOCKED and the host waits. It reads
 * the monotonic clock itself (a pin's age); ar_test_set_clock, at the
 * end of this file, is for tests.
 *
 * The statuses the ring gives:
 *   REPORT_INVALID       bad argument / misuse
 *   REPORT_UNSUPPORTED   schema field outside the type surface
 *   REPORT_STALE         seq recycled or not yet committed
 *   REPORT_BLOCKED       writer reached a pinned region (see ar_row_begin)
 *   REPORT_NOT_NULLABLE  null write to a non-nullable leaf
 *   REPORT_WIDTH         wrong byte/element count for fixed-width
 *   REPORT_OVERFLOW      chunk exceeds int32 offsets (use large_*)
 *   REPORT_REFUSED       a stream given to ar_append_stream failed
 *   REPORT_NO_MEMORY
 *
 * The lines the ring makes (warnings): an oversized value, and a jump
 * in the row numbers at ar_append_batch_at. Both are made on the
 * writer's thread: the host hands them to a thread that may do output.
 *
 * Ownership: the core allocates all storage itself (64-byte aligned).
 * ArrowSchema/ArrowArray/ArrowArrayStream ownership follows the Arrow C
 * Data Interface rules (release callbacks) exactly. An exported
 * ArrowArrayStream reports by that interface (get_last_error), not by a
 * Report: its consumer is an Arrow library.
 */

#ifndef ARROW_RING_H
#define ARROW_RING_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "report.h"

#ifdef __cplusplus
extern "C" {
#endif

#define AR_ABI_VERSION 1

/* Arrow C Data Interface structs (canonical definitions verbatim from
 * https://arrow.apache.org/docs/format/CDataInterface.html; the guards
 * let this header coexist with arrow/abi.h or nanoarrow.h). */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  const char *format;
  const char *name;
  const char *metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema **children;
  struct ArrowSchema *dictionary;
  void (*release)(struct ArrowSchema *);
  void *private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void **buffers;
  struct ArrowArray **children;
  struct ArrowArray *dictionary;
  void (*release)(struct ArrowArray *);
  void *private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
  int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *out);
  int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *out);
  const char *(*get_last_error)(struct ArrowArrayStream *);
  void (*release)(struct ArrowArrayStream *);
  void *private_data;
};

#endif /* ARROW_C_STREAM_INTERFACE */

/* ---- handles ---------------------------------------------------------- */

typedef struct ArRing ArRing;           /* opaque */
typedef struct ArRecordSet ArRecordSet; /* opaque */

typedef struct ArRingOptions {
  int64_t segment_bytes;    /* var-width pool segment size; default 1<<20 */
} ArRingOptions;

/* ---- lifecycle -------------------------------------------------------- */

/* Create a ring for `schema` (a struct-typed ArrowSchema whose children
 * are the columns). capacity = rows; it is the total loss-free budget
 * for every consumer — size it 2x the working need. The schema is
 * deep-copied; the caller's ArrowSchema is released per C-interface
 * rules. opts may be NULL for defaults.
 *
 * Type surface (REPORT_UNSUPPORTED otherwise, naming the field):
 *   fixed-width  int8..64, uint8..64, float16/32/64, bool, timestamp,
 *                date32/64, time32/64, duration, decimal128/256,
 *                fixed_size_binary, fixed_size_list<fixed primitive>
 *   nested       struct (flattened to leaves; structs non-nullable,
 *                leaves may be nullable)
 *   var-width    utf8/large_utf8, binary/large_binary,
 *                list/large_list<fixed primitive>
 * Rejected: map, union, dictionary, run-end, interval, nested lists. */
int ar_ring_new(const struct ArrowSchema *schema, int64_t capacity,
                const ArRingOptions *opts, ArRing **out, Report *report);

void ar_ring_free(ArRing *ring);   /* outstanding record sets keep their
                                      buffers alive via refcount        */

/* ---- leaves ------------------------------------------------------------ */

/* Columns are addressed by leaf index, resolved once at startup from the
 * flattened dotted path ("tv", "servo.pos"). Bindings build their row
 * ergonomics (MutableRow / ReadRow descriptors) on top of these. */
int32_t ar_leaf_index(const ArRing *ring, const char *path); /* -1 = none */
int32_t ar_leaf_count(const ArRing *ring);
const char *ar_leaf_path(const ArRing *ring, int32_t leaf);

/* ---- write side (single writer thread) --------------------------------- */

/* Allocate the next slot ("the open row"). Allocating seq S recycles
 * seq S - capacity: if a pin holds that seq, the call answers
 * REPORT_BLOCKED, with the pin and its age named in the report's
 * error, and nothing has changed: the host waits and calls again (the
 * core does not wait). Nullable leaves reset to null; var-width leaves
 * reset to empty. out_seq may be NULL. */
int ar_row_begin(ArRing *ring, int64_t *out_seq, Report *report);

/* Setters write the open row. Integer setters range-check against the
 * leaf's storage width (REPORT_WIDTH on overflow). Temporal leaves take
 * raw ticks via ar_set_i64. decimal128/256 and fixed_size_binary take
 * raw little-endian bytes of exactly the leaf width. fixed_size_list
 * takes one row of raw elements; list takes n_elems raw elements;
 * utf8/binary take bytes. ar_set_null is REPORT_NOT_NULLABLE on
 * non-nullable leaves. A var-width value larger than a pool segment is
 * taken, and the report has a warning line that says so. */
int ar_set_i64  (ArRing *ring, int32_t leaf, int64_t v, Report *report);
int ar_set_u64  (ArRing *ring, int32_t leaf, uint64_t v, Report *report);
int ar_set_f64  (ArRing *ring, int32_t leaf, double v, Report *report);
int ar_set_bool (ArRing *ring, int32_t leaf, bool v, Report *report);
int ar_set_bytes(ArRing *ring, int32_t leaf,
                 const void *data, int64_t nbytes, Report *report);
int ar_set_list (ArRing *ring, int32_t leaf,
                 const void *elems, int64_t n_elems, Report *report);
int ar_set_null (ArRing *ring, int32_t leaf, Report *report);

/* Freeze every allocated row (there may be several open via repeated
 * ar_row_begin). Cheap: a pointer advance plus the rate observation.
 * Frozen rows must never be written again. */
int ar_commit(ArRing *ring, int64_t *out_committed_count, Report *report);

/* ---- bulk ingest: Arrow streams in ------------------------------------- */

/* Append a whole batch (schema must match the ring's; REPORT_INVALID
 * otherwise). This is the memcpy path: per fixed-width leaf one or two
 * memcpys (ring wrap), per var-width leaf one data memcpy plus an O(n)
 * offset rebase, bitmaps memcpy when the landing slot is 8-aligned and
 * shift-copied when not. Auto-commits. THIS is how a received Flight/
 * IPC stream enters the ring. A row whose slot a pin holds: REPORT_BLOCKED,
 * and no row of the batch is taken. */
int ar_append_batch(ArRing *ring, struct ArrowArray *batch, Report *report);

/* Append a batch AT an absolute sequence number -- the attach-side
 * seq-alignment primitive. Each wire batch carries the origin seq of
 * its first row; appending at that seq keeps the subscriber ring on
 * the PUBLISHER's seq axis, so archives (whose files record their
 * base seq) and the hot ring share one seam.
 *   first_seq == next expected  normal append
 *   first_seq  > next expected  a GAP (reconnect / upstream drop):
 *       jump forward loudly (a warning line in the report) -- counters
 *       advance, the readable floor rises to first_seq, pre-gap rows
 *       become REPORT_STALE.
 *       Requires no open (uncommitted) rows.
 *   first_seq  < next expected  REPORT_INVALID (seq regression)
 * first_seq == -1 means "next expected" (identical to
 * ar_append_batch). An empty fresh ring jump-starts at any seq. */
int ar_append_batch_at(ArRing *ring, struct ArrowArray *batch,
                       int64_t first_seq, Report *report);

/* Drain an ArrowArrayStream to exhaustion, batch by batch as
 * ar_append_batch. REPORT_REFUSED when the stream itself fails. */
int ar_append_stream(ArRing *ring, struct ArrowArrayStream *stream,
                     Report *report);

/* ---- wire frames: packed row-major packets in --------------------------- */

/* Precompiled scatter map from a packed wire frame (what PLCs/probes
 * send) to fixed-width leaves: leaf[i] takes size[i] bytes at byte
 * offset[i] of the frame. Built once at startup, applied per packet
 * with no per-field dispatch. */
typedef struct ArWireMap ArWireMap;
int  ar_wiremap_new(const ArRing *ring, const int32_t *leaves,
                    const int32_t *offsets, const int32_t *sizes,
                    int32_t n, ArWireMap **out, Report *report);
void ar_wiremap_free(ArWireMap *map);
/* Scatter one frame into the open row (ar_row_begin first). */
int ar_from_wire(ArRing *ring, const ArWireMap *map,
                 const void *frame, int64_t frame_len, Report *report);
/* Gather leaves of a committed row back into a frame (TX path). */
int ar_to_wire(const ArRing *ring, const ArWireMap *map, int64_t seq,
               void *frame, int64_t frame_len, Report *report);

/* ---- read side: single rows (hot path, no pin) -------------------------- */

/* Read one value of a committed row. REPORT_STALE outside the readable
 * window [ar_oldest_seq, ar_newest_seq]. is_null distinguishes null
 * from a real value on nullable leaves (may be NULL for non-nullable).
 * Getters mirror the setters: temporal = raw ticks, decimal/fsb = raw
 * bytes. ar_get_var returns a BORROWED pointer into ring/pool storage
 * (var-width leaves AND fixed_size_list / decimal / fixed_size_binary,
 * where n_elems = the leaf width): valid only until the slot can be
 * recycled — consume immediately (this is the ~100ns peek; take a
 * record set for anything held). */
int ar_get_i64 (const ArRing *ring, int64_t seq, int32_t leaf,
                int64_t *out, bool *is_null, Report *report);
int ar_get_u64 (const ArRing *ring, int64_t seq, int32_t leaf,
                uint64_t *out, bool *is_null, Report *report);
int ar_get_f64 (const ArRing *ring, int64_t seq, int32_t leaf,
                double *out, bool *is_null, Report *report);
int ar_get_bool(const ArRing *ring, int64_t seq, int32_t leaf,
                bool *out, bool *is_null, Report *report);
int ar_get_var (const ArRing *ring, int64_t seq, int32_t leaf,
                const void **data, int64_t *n_elems, bool *is_null,
                Report *report);

int64_t ar_oldest_seq(const ArRing *ring);
int64_t ar_newest_seq(const ArRing *ring);   /* -1 when empty */

/* Diagnostics (tests, debuggers, watchdogs — not for data access). */
uint64_t ar_leaf_data_address(const ArRing *ring, int32_t leaf);
                              /* base address of the leaf's ring buffer:
                                 a host may store fixed-width values
                                 into the open row's slot directly,
                                 with no call per field (the slot is
                                 seq % capacity; set the validity bit
                                 for a nullable leaf)                   */
uint64_t ar_leaf_valid_address(const ArRing *ring, int32_t leaf);
                              /* the validity bitmap (Arrow LSB order),
                                 0 for a non-nullable leaf             */
uint64_t ar_var_data_address(const ArRing *ring, int32_t leaf,
                             int64_t elem_seq);   /* address of a pool
                                 element, 0 if not live               */
int32_t  ar_pool_segment_count(const ArRing *ring, int32_t leaf);
int64_t  ar_pool_oversized_count(const ArRing *ring, int32_t leaf);

/* ---- read side: record sets (pinned, immutable, zero-copy) -------------- */

/* Pin a record set over the rows the caller names, [lo, hi), and no
 * others. The pin is the guarantee: a set that is held never becomes
 * invalid, and the var-width segments it references are held too. The
 * writer meets this pin only when it is a whole ring past the pin's
 * first row (REPORT_BLOCKED names the range): a pin on the oldest
 * row of a full ring makes the writer wait at once. lo below the
 * oldest readable row is raised to it (the set's lo says so: rows
 * recycled); hi above committed is lowered to it; an empty range
 * yields an empty set that pins nothing. The publish and archive
 * paths pin the rows they send or write, usually a few.
 * REPORT_INVALID when hi < lo. */
int ar_pin(ArRing *ring, int64_t lo, int64_t hi, ArRecordSet **out,
           Report *report);

int64_t ar_recordset_lo (const ArRecordSet *rs);  /* first seq          */
int64_t ar_recordset_hi (const ArRecordSet *rs);  /* one past the last  */

/* Export the set as an ArrowArrayStream of zero-copy record batches.
 * Chunk boundaries: the physical ring wrap plus var-width segment
 * boundaries / rewrite discontinuities. THIS is the universal egress:
 * hand the stream to DuckDB's C API, a Flight DoGet writer, an IPC
 * file writer, a parquet writer, or import it into pyarrow/arrow-go/
 * arrow-rs. The stream (and each batch it yields) holds the pin's
 * references; releasing the stream after the record set is safe.
 * Bindings that want "iterate rows of a record set with a read-only
 * row handle" build that on these batches — row iteration is sugar,
 * not core. */
int ar_recordset_stream(ArRecordSet *rs, struct ArrowArrayStream *out,
                        Report *report);

/* Drop the pin. Idempotent. Exported streams/batches stay memory-safe
 * afterwards (refcounts), but the REGION may be recycled: do not treat
 * post-release reads as current data. Release means done. */
void ar_recordset_release(ArRecordSet *rs);

/* Hand off: drop the CALLER's reference without dropping the pin. The
 * pin then lives exactly as long as the exported stream/batches do and
 * releases itself when the last consumer releases (e.g. when a Flight
 * push or parquet write task finishes with the batch). For fire-and-
 * forget publish paths. Call exactly one of release/detach. */
void ar_recordset_detach(ArRecordSet *rs);

/* ---- read side: chunk cursor (a pin per chunk, nothing between) --------- */

/* A chunkwise read of [lo, hi), for a consumer that pulls batches one at
 * a time (a DuckDB scan). Nothing is pinned at open. The range is fixed
 * at the FIRST next, not at open: hi is lowered to the committed row
 * then, lo raised to the oldest row then, so a consumer that reaches
 * memory late reads memory as it is then. Each next pins up to
 * chunk_rows rows, builds one zero-copy batch (the first physical run
 * in that span: the ring wrap and var-width segment boundaries cut as
 * ar_recordset_stream cuts), and hands the pin to the batch: the pin
 * drops when the consumer releases the batch. So a scan holds only the
 * chunk in hand, however long the query around it runs. A row the
 * writer recycled before the read reached it ends the read with
 * REPORT_STALE naming the rows: the read was lapped. At the end `out`
 * has release == NULL. *first_seq is the batch's first row. */
typedef struct ArChunkCursor ArChunkCursor;
int  ar_chunks_open(ArRing *ring, int64_t lo, int64_t hi,
                    int64_t chunk_rows, ArChunkCursor **out, Report *report);
int  ar_chunks_next(ArChunkCursor *cursor, struct ArrowArray *out,
                    int64_t *first_seq, Report *report);
void ar_chunks_free(ArChunkCursor *cursor);

/* ---- for tests only (report.h: TIME) ------------------------------------ */

/* fixed 1: from now on the core's wall clock is `wall` and its
 * monotonic clock `mono`, until the next call. fixed 0: the system's
 * clocks again. It changes the time for every thread of the process;
 * library code does not call it. */
void ar_test_set_clock(int fixed, double wall, double mono);

/* The next `allow` allocations of ring storage work and the ones after
 * them fail (REPORT_NO_MEMORY); under 0: all work again. For the test
 * that every such failure is loud and leaks nothing. */
void ar_test_set_alloc_budget(int64_t allow);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* ARROW_RING_H */
