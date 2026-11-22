"""Measure what a live stream's writer puts in an S3 store.

    python3 docs/checkpoint_cost_measure.py BUCKET STREAM MINUTES [CHECKPOINT_SECONDS]

    BUCKET   the store's bucket
    STREAM   the stream's name (its files are at streams/STREAM/)
    MINUTES  how long to watch

It reads the store and writes nothing. Every 5 minutes it lists the
stream's objects and notes each row object (`*.parquet.chunks/NNNNNN`)
and each checkpoint object (`*.ckpt`) it has not seen. The store keeps
an object for 10 minutes after a newer checkpoint stops naming it, so a
listing every 5 minutes sees every object the writer stores. At the end
it prints the bytes stored in each checkpoint interval (default 15 s),
the number docs/checkpoint-cost.md gives from its model.

Finished files (`*.parquet`) are not counted: S3 builds them by copy.
The last part of a finished file, which the writer sends once more, is
therefore not in this number.
"""

import datetime
import sys
import time

import boto3

LIST_EVERY_S = 300


def listing(s3, bucket, stream):
    found = {}
    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"streams/{stream}/")
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".ckpt") or ".parquet.chunks/" in key:
                found[key] = (obj["LastModified"], obj["Size"])
    return found


def main():
    bucket, stream, minutes = sys.argv[1], sys.argv[2], float(sys.argv[3])
    checkpoint_s = float(sys.argv[4]) if len(sys.argv) > 4 else 15.0
    s3 = boto3.client("s3")
    begin = datetime.datetime.now(datetime.timezone.utc)
    end = begin + datetime.timedelta(minutes=minutes)
    seen = {}
    while True:
        seen.update(listing(s3, bucket, stream))
        now = datetime.datetime.now(datetime.timezone.utc)
        if now >= end:
            break
        time.sleep(min(LIST_EVERY_S, (end - now).total_seconds()))
    stored = [(when, size, key) for key, (when, size) in seen.items()
              if begin <= when < end]
    rows = [size for _when, size, key in stored if ".parquet.chunks/" in key]
    checkpoints = [size for _when, size, key in stored if key.endswith(".ckpt")]
    intervals = minutes * 60 / checkpoint_s
    print(f"stream {stream}: {minutes:g} minutes from "
          f"{begin:%Y-%m-%d %H:%M:%S} UTC, {intervals:.0f} checkpoint intervals")
    if not checkpoints:
        print("no checkpoint object was stored: the stream had no new rows")
        return
    print(f"row objects:        {len(rows):5d}, {sum(rows) / 1000:9.0f} KB, "
          f"largest {max(rows, default=0) / 1000:.0f} KB")
    print(f"checkpoint objects: {len(checkpoints):5d}, {sum(checkpoints) / 1000:9.0f} KB, "
          f"{min(checkpoints) / 1000:.0f} to {max(checkpoints) / 1000:.0f} KB")
    print(f"KB stored per checkpoint interval: "
          f"{(sum(rows) + sum(checkpoints)) / 1000 / intervals:.0f}")


if __name__ == "__main__":
    main()
