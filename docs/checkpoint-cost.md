# What a checkpoint costs

A checkpoint is a promise in seconds. If the writer dies, the store
holds every row that is older than the checkpoint interval. This
document gives the price of that promise in bytes: for the package as
it is today, and for the design that replaces it.

**Status.** "Today" describes the package as it is. "The design" was
worked out on 2026-11-09 and is not built. The numbers for today agree
with the sizes measured in a store. The numbers for the design come
from a model
([checkpoint_cost_model.py](checkpoint_cost_model.py)).

## The answer

KB put in the store in each 15-second checkpoint interval, for one
stream of 55 columns at different row rates:

| Rows/s | New rows | Today | Steps, by time | Steps, by time or bytes | Row groups only |
|---|---|---|---|---|---|
| 1 | 0.1 | 51 | 35 | 35 | 0.1 |
| 10 | 1.0 | 381 | 44 | 44 | 1.0 |
| 100 | 10 | 2,513 | 101 | 101 | 19 |
| 1,000 | 101 | 3,194 | 542 | 542 | 226 |
| 10,000 | 1,014 | 6,056 | 4,637 | 3,180 | 2,753 |
| 100,000 | 10,144 | 32,412 | 42,575 | 11,432 | 11,018 |

The four designs:

- **Today.** Every checkpoint writes one checkpoint object with every
  finished row group not yet in a 5 MiB chunk, the whole unfinished row
  group, and the footer.
- **Steps, by time.** Every checkpoint writes only the new rows. Small
  objects merge in steps. The file is cut by time.
- **Steps, by time or bytes.** The same, plus two byte rules. The file
  is also cut at 256 MB. A checkpoint with 5 MiB of new rows writes
  them straight into a chunk.
- **Row groups only.** No checkpoints. A row group is written once,
  when it is full, with the same byte rules.

Rows lost if the writer dies, as time. This is also how stale the store
is for a reader that has no live connection:

| Rows/s | With checkpoints | Row groups only |
|---|---|---|
| 1 | 15 s | 3 h |
| 10 | 15 s | 3 h |
| 100 | 15 s | 20 min |
| 1,000 | 15 s | 2 min |
| 10,000 | 15 s | 12 s |
| 100,000 | 15 s | 1 s |

At 1 and 10 rows per second, a row group does not fill in 3 hours. With
row groups only, nothing is stored until the file is cut.

What it shows:

- **Checkpoints become cheap.** At 100 rows per second, the 15-second
  promise costs 101 KB, against 19 KB with no checkpoints. Today it
  costs 2,513 KB.
- **Dropping checkpoints saves little and loses much on slow streams.**
  It saves under 100 KB per 15 seconds, and the loss in a crash goes
  from 15 seconds to minutes or hours.
- **On fast streams checkpoints are free.** From 10,000 rows per
  second, a row group fills in less time than one checkpoint interval,
  so the last two columns meet.
- **The byte rules are needed at the top.** At 100,000 rows per second,
  steps by time alone is worse than today, because each row is written
  twice and the footer grows for 3 hours.

## Today

Every checkpoint writes one checkpoint object. A checkpoint object is
never changed, so the next checkpoint writes a new one. The object
holds three things, and all three repeat from one checkpoint to the
next:

| Part of the checkpoint object, at 100 rows/s | Mean KB per checkpoint | Share |
|---|---|---|
| Finished row groups that wait for a 5 MiB chunk | 2,062 | 82% |
| The rows of the unfinished row group | 411 | 16% |
| The footer | 29 | 1% |

The first part is there because of S3. S3 builds the finished file from
parts, by server-side copy, and it refuses a part under 5 MiB unless it
is the last part. So a chunk is at least 5 MiB, and every row
group that does not yet fill a chunk rides in the checkpoint object,
again and again. The stream above takes 2 hours and 24 minutes to make
5 MiB. For that time, each checkpoint writes most of the unfinished
file.

The second part is there because the rows of the unfinished row group
are encoded again, all of them, at every checkpoint.

| Rows/s | New rows per checkpoint, KB | Written per checkpoint, KB | Largest checkpoint object, KB |
|---|---|---|---|
| 1 | 0.1 | 51 | 87 |
| 10 | 1.0 | 381 | 744 |
| 100 | 10 | 2,513 | 5,856 |
| 1,000 | 101 | 3,194 | 6,229 |
| 10,000 | 1,014 | 6,056 | 9,984 |
| 100,000 | 10,144 | 32,412 | 44,166 |

A reader that follows the stream reads the same object: each new ticket
names a new checkpoint object, and the reader downloads it whole.

What a user can set today:

- `Archive(checkpoint_seconds=...)` is the promise. `None`, the
  default, is row groups only: the store gets a row group when it is
  full, and not before.
- `Archive(row_group_seconds=...)` ends a row group after that long.
  A shorter row group makes the second part smaller. It does not
  change the first part.
- A stream that commits no rows writes no checkpoint. The cost is for
  intervals that have new rows.

## The design

The cost of a checkpoint follows the new rows, not the size of the
file.

- **Each checkpoint writes one small object** with only the rows since
  the last checkpoint. It is never rewritten.
- **The checkpoint object keeps only the list of objects and the
  footer.**
- **A full row group is written once,** as its own object, when it is
  complete. It replaces the small objects.
- **The 5 MiB chunk is still written,** but only as a part for S3 to
  build the finished file from. The finished file is the same as today.

Nothing is merged in the store. S3 cannot append to an object, and it
cannot join small objects. When objects merge, the writer encodes those
rows again from its memory and writes one new object. The objects it
replaces are deleted 10 minutes later, by the rule already used for old
checkpoint versions.

### Merge in steps

Each small object is a row group, and the footer has an entry for each
row group and each column: about 5 KB for 55 columns. The footer is
written at every checkpoint. So a small object left in place costs
footer bytes at every checkpoint, and a merge costs row bytes once.

Small objects merge into one middle object, and middle objects merge
into one large object. The test is the same at each size:

> Merge a run of objects when the footer bytes they have cost reach the
> size of the merged object. A run never passes 16 objects.

Each row is then written once per step, not once per checkpoint. The
writer counts both numbers itself, so there is nothing to set.

The 16 bounds what a reader meets when it opens the stream for the
first time: it makes one request for each object. In the model the
unfinished row group never held more than 10 small objects.

### Time or bytes, whichever comes first

Each decision in the writer is taken by time or by bytes, whichever
comes first. A slow stream is decided by time. A fast stream is decided
by bytes. Bytes, not rows: a row is 7 bytes in one stream and kilobytes
in another, and the S3 part limit and the network both count bytes.
Rows stay in one place only: the row group size that DuckDB expects.

| Decision | Slow stream | Fast stream |
|---|---|---|
| Write the new rows | The checkpoint interval | The same. It is a promise in seconds. |
| Merge small objects | The footer-bytes rule | Never needed. A row group fills first. |
| Cut a 5 MiB chunk | Rarely reached | Reached in seconds |
| Cut the file | By time | By bytes. Today it is by time only. |

Two byte rules are new:

- **Cut the file by bytes.** At 10,000 rows per second a 3-hour file
  has 879 row groups and a footer of 4.4 MB, and the footer is written
  at every checkpoint. A cut by bytes bounds the footer. The 256 MB in the
  model is an example. A smaller cut gives a smaller footer and more
  files in the catalog.
- **Write straight into a chunk.** When the new rows of one checkpoint
  already reach 5 MiB, they go into a chunk and are written once.

What remains on a fast stream is about twice the new rows. Each row is
written once at its checkpoint, and once again inside the 5 MiB chunk
that S3 needs.

### What a user sets

The checkpoint interval, as today. The design adds no setting.

## Readers

- **A reader that follows every ticket** reads about the bytes the
  writer writes, so it gains by the same factor.
- **An object never changes,** so a reader can keep each one and read
  it once.
- **A reader that opens the stream for the first time** meets the
  objects of the unfinished file:

| Rows/s | Most small objects in the unfinished row group | Largest checkpoint object, KB | Files in the run |
|---|---|---|---|
| 1 | 9 | 50 | 1 |
| 10 | 9 | 50 | 1 |
| 100 | 10 | 89 | 1 |
| 1,000 | 9 | 476 | 1 |
| 10,000 | 1 | 1,537 | 3 |
| 100,000 | 1 | 1,580 | 28 |

  It also meets the finished row groups that are not yet in a chunk, at
  most 6 for these sizes, and the chunks.

## How the numbers were made

The model is [checkpoint_cost_model.py](checkpoint_cost_model.py). It
is arithmetic: it reads no store and writes no file.

    python3 docs/checkpoint_cost_model.py

prints every table in this document. One run is one unfinished file,
written for 3 hours at a constant row rate, with a checkpoint every 15
seconds.

**Measured sizes.** They come from the store of one stream on
2026-11-09: 55 columns, 100 rows per second, a checkpoint every 15
seconds, DuckDB's row group of 122,880 rows.

- A chunk of 7 row groups was 5.82 MB: 831 KB for a row group, 6.8
  bytes for a row.
- The footers of four finished files with 1, 2, 3 and 9 row groups were
  9.6, 14.5, 19.4 and 49.1 KB: 4.7 KB and 4.95 KB for each row group.

**The check of the model.** For today's design the model gives a mean
of 2.5 MB for each checkpoint and a largest object of 5.9 MB. In the
store, the chunk was cut 2 hours and 24 minutes after the file began,
at 5.82 MB. 27 checkpoint objects in a row grew from 0.61 MB to 0.89
MB, 11 KB for each checkpoint. The model has 10 KB.

**Estimated sizes.** The headers of one small object are taken as 80
bytes for each column, and one line of the list as 50 bytes. The result
is not sensitive to the first number:

| Overhead per column, bytes | Steps, KB per checkpoint at 100 rows/s |
|---|---|
| 0 | 97 |
| 80 | 101 |
| 160 | 106 |
| 320 | 115 |

**Limits.**

- The rows of the table scale the row rate only. They show the trend.
  They are not measurements of other streams.
- The measured stream was idle most of the time, and idle rows compress
  well. In one short file written while the source was busy, a row was
  about 37 bytes.
- The model counts bytes. It does not count requests. The design makes
  two writes for each checkpoint, the small object and the checkpoint
  object, where today makes one.
- The design is not built. Its numbers are what the rules give on
  paper.
