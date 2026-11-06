# TODO

Status recorded 2026-08-27.

## Done recently (all committed, pushed, suites green)

- **Discovery migration**: MQTT is gone from v2 entirely. Tickets are
  JSON objects at `<root>/streams/<name>.json` (file:// and s3://
  backends); root resolution: call site > `set_root()` >
  `$STREAMING_PARQUET_ROOT` > public bucket (reads only). Go:
  `Config.Root`; paho dependency dropped both languages.
- **Failure-modes ledger closed**: publisher-restart epoch ID in the
  ticket (feeder resyncs on a fresh ring, loudly); boot sweep refuses
  checkpoints touched <30 s ago (mtime = liveness); `Stream.close()`
  unregisters its name; unregister ends subscriber do_gets; unclean
  exit no longer hangs (`threading._register_atexit`); malformed wire
  metadata drops the connection; `first_seq < -1` refused in the core;
  archive seq-overlap detected loudly.
- **CheckpointFile (virtual file)**: `CheckpointReader.open()` — the
  live checkpoint as a read-only seekable file object. Stock
  pyarrow/pandas read live files; column pruning reaches storage
  (byte-metered in tests). `live_file(path)` convenience.
- **DuckDB extension (duckdb-checkpoint/)**: `parquet_checkpoint` →
  stock `read_parquet()` over live checkpoints. Bare local `.ckpt`
  claimed by suffix; scheme'd paths wrapped as `pqc://s3://…` (first
  claimer wins in DuckDB's filesystem dispatch and order is
  unknowable, so the marker makes it unambiguous; raw bytes flow
  through httpfs etc.). Shared fuzz-tested C parser (pqc.c). 8/8.

## Open — needs Paul's decision

- **Track A (restart continuation), PARKED**: subscriber continuing
  across publisher restarts on one continuous local count (v1-style
  drain-and-continue) instead of today's fresh-ring resync; file
  open-timestamps as run identity; lifecycle test suite (attach
  before create, dead-ticket revive, restart storms, v1-via-bridge).
  Paul: not settled — wants the Iceberg/DuckLake higher-order
  discussion first. Settled so far: catalog lists closed files only;
  checkpoint stays outside it; run identity by start timestamp (not
  random UUID); rows scoped per run only for the live seam
  (`sp_base_seq` = one anchor per file); restarted publisher = new
  file, never continue the old one.
- **Orphan rows after a publisher crash**: the last seconds may exist
  only in subscribers' rings — no archive file will ever hold them.
  What should a subscriber's history view claim? Could a subscriber
  contribute them back? (Complicated; raised 2026-08-24.)
- **R2 chunk sizing**: R2 multipart requires all parts equal size
  except the last; our chunks are variable (cut at whole row groups).
  Approach settled 2026-08-27: cut parts at a fixed size S, splitting
  row groups across part boundaries. No padding mode — a row group
  larger than S must split anyway, so splitting machinery is required
  regardless and padding wastes ~half a row group per part forever in
  the archived file. Splitting leaves the final object byte-identical
  to whole-row-group cutting, so the format is unchanged; the cutting
  rule is transport-only. assemble() then works as spec'd on R2 (all
  copied ranges = S except the last). Open: do fixed cutting on R2
  only or on both backends — one code path vs. a cutting rule readers
  of the code won't expect. Verify the equal-parts rule (including
  UploadPartCopy) with run_s3_contract.py against a real R2 bucket
  (needs the Cloudflare account — Paul at the keyboard).

## Open — agreed direction, not yet built

- **assemble() as a named module**: spec the four-item contract
  (endpoint, credentials, dest, [(source, offset, length)]). Homes:
  our sinks (exists); later a `checkpoint_finalize()` SQL function in
  the DuckDB extension using DuckDB secrets + a minimal sigv4 signer
  (~300 lines, no httpfs dependency — extensions can't share code);
  eventually an httpfs upstream PR once there's a public consumer.
- **Small-row-group torture test**: thousands of one-row groups vs
  5 MB chunks — data-region residency assertions, checkpoint-cost
  growth curve, reads mid-churn. Capability exists and is covered
  incidentally; the dedicated test isn't.
- **Warm tier of the relation via the extension** (optional): the
  Python relation's checkpoint piece could become
  `read_parquet('….ckpt')` instead of assemble-in-Python — makes the
  extension a dependency of the relation path, Paul's call.
- **Format spec**: checkpoint object layout, the virtual-file offset
  mapping, `.ckpt` suffix convention, ticket schema, version field
  (must land before first public byte), trust model for attaching to
  third-party streams.
