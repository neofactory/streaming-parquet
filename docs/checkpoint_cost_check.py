"""The check of checkpoint_cost_model.py against the package's writer.

    python3 docs/checkpoint_cost_check.py

It runs the writer (parquet_checkpoint.CheckpointWriter, with the
encodings a stream gets by default) on a stream made here, into a store
in memory that keeps S3's rules, and counts every byte the writer
sends. Then it gives the model the sizes of that stream and prints the
two side by side.

The stream: 55 columns, 1,500 new rows at each checkpoint (100 rows per
second, a checkpoint every 15 seconds), row groups of 122,880 rows, 240
checkpoints (one hour), then the file is cut. Its values do not change,
so a row is well under one byte and nearly every byte is footer. That
is the part of the cost that the merge rule decides.
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import pyarrow as pa

import checkpoint_cost_model as model
from parquet_checkpoint import CheckpointReader, CheckpointWriter, FakeS3
from parquet_checkpoint._sinks import plan_parts
from streaming_parquet._parquet_writer import default_encodings

COLUMNS = 55
NEW_ROWS = 1_500                    # 100 rows per second, 15 seconds
ROW_GROUP_ROWS = 122_880
CHECKPOINTS = 240                   # one hour


class CountingStore(FakeS3):
    """FakeS3 that counts the bytes sent to it: every object stored,
    and every part of an assembly that S3 cannot copy for itself."""

    def __init__(self):
        super().__init__()
        self.sent = 0

    def put(self, name, data):
        self.sent += len(data)
        return super().put(name, data)

    def assemble(self, dest, sources):
        sizes = [len(self.get(name)) if off < 0 else length
                 for name, off, length in sources]
        size_of = dict(zip(sources, sizes))
        for kind, group in plan_parts(sources, sizes, self.part_min):
            if kind == 'upload':
                self.sent += sum(size_of[source] for source in group)
        return super().assemble(dest, sources)


def make_schema():
    fields = [pa.field("tm", pa.float64(), nullable=False)]
    fields += [pa.field(f"i{k}", pa.int32(), nullable=False) for k in range(18)]
    fields += [pa.field(f"f{k}", pa.float64(), nullable=False)
               for k in range(COLUMNS - 19)]
    return pa.schema(fields)


def rows(schema, first, count):
    """Rows first .. first+count of a machine that does nothing: a
    clock, counters that do not step, positions that do not move."""
    columns = {"tm": [1.7e9 + (first + j) * 0.01 for j in range(count)]}
    for k in range(18):
        columns[f"i{k}"] = [1000 + k] * count
    for k in range(COLUMNS - 19):
        columns[f"f{k}"] = [round(12.5 + k * 0.25, 4)] * count
    return pa.table(columns, schema=schema)


def run_writer():
    """Returns what the writer sent and the sizes the model needs."""
    schema = make_schema()
    store = CountingStore()
    writer = CheckpointWriter(
        "check.parquet", schema, sink=store, checkpoint_every_write=False,
        compression="zstd", compression_level=9, use_dictionary=False,
        column_encoding=default_encodings(schema))
    final_rows, tail_rows = 0, 0
    first_object_kb = row_group_kb = None
    listed, checkpoint_kb, most_small = [], [], 0
    for _ in range(CHECKPOINTS):
        tail_rows += NEW_ROWS
        if tail_rows >= ROW_GROUP_ROWS:
            before = store.sent
            writer.write_table(rows(schema, final_rows, ROW_GROUP_ROWS))
            final_rows += ROW_GROUP_ROWS
            tail_rows -= ROW_GROUP_ROWS
            if row_group_kb is None:
                row_group_kb = (store.sent - before) / 1000
        before = store.sent
        writer.checkpoint(rows(schema, final_rows, tail_rows) if tail_rows else None)
        reader = CheckpointReader(writer.ckpt_name, store)
        size = len(store.get(writer.ckpt_name)) / 1000
        if first_object_kb is None:
            first_object_kb = (store.sent - before) / 1000 - size
        listed.append(len(reader.manifest))
        checkpoint_kb.append(size)
        groups = final_rows // ROW_GROUP_ROWS
        most_small = max(most_small, len(reader.manifest) - groups)
    writer.close()
    return dict(sent_kb=store.sent / 1000, first_object_kb=first_object_kb,
                row_group_kb=row_group_kb, listed=listed,
                checkpoint_kb=checkpoint_kb, most_small=most_small)


def fit_line(xs, ys):
    """Least squares: ys = base + slope * xs."""
    n = len(xs)
    mean_x, mean_y = sum(xs) / n, sum(ys) / n
    slope = (sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
             / sum((x - mean_x) ** 2 for x in xs))
    return mean_y - slope * mean_x, slope


def main():
    got = run_writer()
    # The model, with this stream's sizes in place of the measured stream's.
    model.ROW_GROUP_ROWS = ROW_GROUP_ROWS
    model.ROW_GROUP_KB = got["row_group_kb"]
    model.KB_PER_ROW = model.ROW_GROUP_KB / ROW_GROUP_ROWS
    model.OBJECT_OVERHEAD_KB = got["first_object_kb"] - NEW_ROWS * model.KB_PER_ROW
    base, entry = fit_line(got["listed"], got["checkpoint_kb"])
    model.FOOTER_BASE_KB, model.FOOTER_ENTRY_KB = base, entry
    model.LIST_ENTRY_KB = 0.0       # the fit puts an object's line in its entry
    model.CHECKPOINT_S = 15.0
    model.RUN_HOURS = CHECKPOINTS * model.CHECKPOINT_S / 3600
    mean, largest, most, _files = model.steps(NEW_ROWS / model.CHECKPOINT_S, True)

    print(f"The stream: {COLUMNS} columns, {NEW_ROWS:,} new rows at each of "
          f"{CHECKPOINTS} checkpoints, {model.KB_PER_ROW * 1000:.2f} bytes per row.")
    print(f"Its sizes: a row group {model.ROW_GROUP_KB:.1f} KB, an object of "
          f"{NEW_ROWS:,} rows {got['first_object_kb']:.1f} KB, the footer "
          f"{base:.1f} KB and {entry:.2f} KB for each object.\n")
    print("| | The writer | The model |")
    print("|---|---|---|")
    print(f"| KB sent per checkpoint | {got['sent_kb'] / CHECKPOINTS:.1f} | {mean:.1f} |")
    print(f"| Largest checkpoint object, KB | {max(got['checkpoint_kb']):.1f} | {largest:.1f} |")
    print(f"| Most small objects in the unfinished row group | {got['most_small']} | {most} |")
    sys.stdout.flush()
    os._exit(0)     # pyarrow's threads can hang an exit on some Python versions


if __name__ == "__main__":
    main()
