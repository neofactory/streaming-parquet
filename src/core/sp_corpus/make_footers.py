"""Make the footer seeds of `make fuzz`: the footers of two small
parquet files, one flat and one with lists, structs and decimals.

    python3 src/core/sp_corpus/make_footers.py
"""
import datetime as dt
import decimal
import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "python"))
from streaming_parquet import _core  # noqa: E402

flat = pa.table({
    "n": pa.array(range(50), pa.int64()),
    "s": pa.array([f"x{k}" for k in range(50)]),
    "ts": pa.array([k * 1000 for k in range(50)], pa.timestamp("us", "UTC")),
    "f": pa.array([k / 7 for k in range(50)])})
nested = pa.table({
    "l": pa.array([[1, 2], [3], None], pa.list_(pa.int64())),
    "st": pa.array([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, None]),
    "dec": pa.array([decimal.Decimal("-12.345"), decimal.Decimal("99.999"),
                     None], pa.decimal128(10, 3)),
    "u": pa.array([0, 2**64 - 1, 7], pa.uint64()),
    "b": pa.array([b"\x00", b"\xff", b"A"]),
    "d": pa.array([dt.date(1999, 12, 31), dt.date(2026, 11, 1), None]),
    "ns": pa.array([1, 2, 3], pa.timestamp("ns"))})
work = tempfile.mkdtemp()
for name, table, rows in (("flat", flat, 20), ("nested", nested, 2)):
    path = os.path.join(work, name + ".parquet")
    pq.write_table(table.replace_schema_metadata(
        {"sp_run": "2026-11-21T00:00:00.000000Z", "sp_base_seq": "1200"}),
        path, row_group_size=rows)
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        f.seek(size - 8)
        n = _core.footer_length(f.read(8))
        f.seek(size - 8 - n)
        footer = f.read(n)
    with open(os.path.join(HERE, name + ".footer"), "wb") as f:
        f.write(footer)
    print(name, len(footer), "bytes")
