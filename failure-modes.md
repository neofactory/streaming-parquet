# Failure modes and coverage — findings

STATUS 2026-08-23: test environment hardened in response to this doc.
Done: hostile-data testkit through all 3 Python suites; cross-language
type matrix (nulls/NaN/unicode/ts/lists both directions); checkpoint
mangler (400 corruptions); C allocator fault injection (found + fixed
a REAL crash: ar_ring_new never checked its allocations); the two
untrusted parsers now fuzzed (corpus-seeded, ASan/UBSan clean);
torture rig gained idle + bursty rate profiles. UPDATE 2026-08-23 (Paul's design
calls made; all fixed + tested): #1 publisher restart -> random epoch
ID in the ticket, feeder resyncs on a FRESH ring on epoch change
(loud; old ring closed; relations must be re-created); #2 boot sweep
refuses checkpoints modified < 30s ago (now `dead_after()`, the one rule for a dead writer; file
mtime = writer liveness, sinks grew mtime()); #3 Stream.close now
unregisters (name reusable) AND unregister_stream closes subscriber
do_get streams (a closed publisher looks closed to feeders); #4 exit
hang fixed via threading._register_atexit(close) — plain atexit is
too late, daemon threads can no longer service gRPC teardown;
#5 malformed app_metadata (len != 8) drops the connection loudly both
languages; #6 ar_append_batch_at refuses first_seq < -1; #7 archive
seq-axis overlap (respawn without archive epochs) detected at scan,
refuses the relation loudly — archive-side epoch is spec work.

Measured 2026-08-23 (coverage.py --branch; gcov; go -cover; live
probes). Findings and test ideas, not a plan.

## Confirmed by probe (reproduced, not speculative)

1. **Publisher restart wedges every persistent subscriber (livelock).**
   New publisher generation restarts the seq axis at 0; the attached
   ring refuses the regression (correct, loud), the feeder tears down,
   reconnects, hits the same refusal on the first batch, forever.
   Subscriber stuck at the old head while the new generation publishes.
   Design gap: the feeder has no EPOCH policy — a regression on a
   fresh connection means "new generation" and should rebuild the
   ring, not retry into a wall. (Ring behavior itself is right.)
2. **A second writer with the same stream name silently corrupts the
   first.** Daemon B's boot sweep cannot distinguish a crashed
   checkpoint from a LIVE one: it finalizes A's open file. A keeps
   writing WITHOUT ANY ERROR, recreating its checkpoint beside the
   finalized file — the both-exist poisoned state. Readers see the
   finalized prefix forever; A believes it is archiving. Needs a
   liveness signal before sweeping (ckpt freshness vs heartbeat, or a
   writer lease) and a loud refusal on the second writer.
3. **Python Stream.close() leaks the registry entry** — a closed
   stream's name can never be reused in-process ("already exists").
   The Go implementation removes it; Python doesn't.
4. **Unclean interpreter exit hangs** (~forever) when streaming_parquet.close()
   was not called: Flight generator threads block in queue.get while
   finalizers run. A crashing daemon hangs instead of dying loudly.
   (Observed once even after a clean close.)

## Confirmed by code-read (test still owed)

5. **Malformed wire seq metadata is swallowed silently.** app_metadata
   of any length != 8 -> at_seq=None -> the ring silently continues on
   its LOCAL axis: misalignment with no diagnostic. Violates the loud
   principle; on a public stream this is attacker-adjacent input.
6. **at_seq < -1 is treated as "next expected"** — garbage negative
   seq from a bad peer is silently accepted (-1 is the only legal
   sentinel).
7. **Overlapping sp_base_seq ranges across archive files double-count
   silently** in the timeline relations (two crashed generations can
   produce overlapping [base, base+rows) ranges; only the clamp is
   checked). An overlap check in the archive index would make it loud.

## Branch coverage, measured

| layer | result |
|---|---|
| Python packages (suite-driven) | 80–95% per module, EXCEPT: `_thrift_shift` 79%, `_attach` 68%, `_sinks` 67% |
| arrow_ring.c | 88.7% lines; **68.7% of branch DIRECTIONS taken** |
| pqc.c | 84.5% lines; **67.2% of branch directions taken** |
| Go pqc | 79.5% statements (ring/streaming Go exercised mostly via Python drivers, uninstrumented) |

The uncovered branches cluster exactly where the risk is:
- `_thrift_shift` / pqc shift: the rare-type paths (maps, bool struct
  fields, long-form field headers, unsupported-type refusals) that
  pyarrow output never produces but MALICIOUS footers would. This is
  the fuzzing argument with numbers attached.
- `_attach`: the lifecycle machinery — stale tickets, broker-down,
  reconnect backoff, connect errors. Finding #1 lived here.
- `_sinks`: S3Sink paths (covered only by the real-S3 contract suite,
  run separately) and some fault-injection branches.
- The C third-of-branches-untaken is mostly error paths (alloc
  failure, refusals) and boundary directions.
- Dead API: `ar_append_stream`, `ar_recordset_write_ipc` — no host
  calls them; untested by construction.

## Test-data uniformity (the suites' blind spot)

Everything we generate is friendly: monotone ints, x = n/2, ASCII
strings, constant rates, loopback network, always-flowing streams.
Never exercised end to end:
- NaN / ±Inf floats (parquet stats behave oddly with NaN — the
  pruning proof itself could differ), int64 extremes, huge single
  values over the WIRE, empty-vs-null strings, multi-byte UTF-8.
- Nulls across the FULL path: Go publisher -> wire -> Python ring ->
  archive -> timeline relation (nulls are tested in the ring suite
  only).
- Cross-language suites use the n/x/s schema exclusively: struct,
  timestamp, decimal, binary, list types have never crossed the wire
  or the archive between languages.
- Adversarial schemas: SQL-hostile column names (the relation view
  text embeds identifiers), 500-column width, deep struct nesting.
- Idle streams (the pool-pin leak was found by idleness; the torture
  rig never idles), bursty/pausing rates, clock skew between hosts
  (ticket staleness math), slow disk / ENOSPC (a ramdisk makes this
  testable), schema drift across publisher restarts, gRPC message
  limits with huge rows.

## Test ideas (each maps to a finding above)

- Fuzz harnesses (libFuzzer, ASan) for `pqc_reader_open` and
  `pqc_shift_footer` + a Python Hypothesis-style byte mangler for
  `_thrift_shift` and `CheckpointReader`. [#thrift branches, errata]
- A hostile-data generator shared by all suites: random nulls, NaN,
  unicode, empty/huge values, extreme ints — parameterizing the
  existing tests rather than new ones. [uniformity]
- Cross-language type-surface matrix: every supported type through
  Go->wire->Python->archive->relation and the reverse. [uniformity]
- Torture rig additions: a publisher-restart-same-name chaos event
  (catches #1 forever), an idle-stream worker, a malformed-metadata
  adversarial peer, rate burst/pause patterns. [#1, #5, idleness]
- Same-name-twice test asserting a LOUD refusal once the liveness
  design exists. [#2]
- ENOSPC/slow-disk suite on a small ramdisk. [sinks error paths]
- Ticket clock-skew tests (sender ts vs receiver clock ±minutes).
