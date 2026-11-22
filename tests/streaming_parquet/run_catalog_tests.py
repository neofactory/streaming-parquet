"""The core's catalog (core/sp.h: THE CATALOG) against DuckDB's own.

    python3 run_catalog_tests.py

Six parquet files with a column of every type the catalog takes (nulls,
lists, structs, a column added later, a column dropped later) are
listed twice: by DuckDB (ducklake_add_data_files into a DuckLake 1.0
catalog in SQLite) and by the core. The two catalogs must hold the same
columns, the same numbers for every file and column, and the same
table totals; DuckDB must read the same rows through each, and open
only the files a query needs.

One known difference, kept on purpose: the minimum of a blob column.
DuckDB 2.0.0 writes the least of the values' TEXTS ('A...' sorts below
'\\x00'); the core writes the least value.
"""
import datetime as dt
import decimal
import os
import sqlite3
import sys
import tempfile
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from streaming_parquet import _core  # noqa: E402

here = tempfile.mkdtemp(prefix="cat-cmp-")
utc = dt.timezone.utc
def table(k, extra=False):
    cols = {
        "i8": pa.array([-5 + k, 7 + k, None], pa.int8()),
        "i16": pa.array([-500, 700 + k, 1], pa.int16()),
        "i32": pa.array([-70000 - k, 70000, 1], pa.int32()),
        "i64": pa.array([-2**62 + k, 2**62, 5], pa.int64()),
        "u8": pa.array([0, 255, 3], pa.uint8()),
        "u16": pa.array([k, 65535, 3], pa.uint16()),
        "u32": pa.array([k, 2**32 - 1, 7], pa.uint32()),
        "u64": pa.array([k, 2**64 - 1, 7], pa.uint64()),
        "f32": pa.array([-1.5 * (k + 1), 3.25e10, 0.1], pa.float32()),
        "f64": pa.array([-1e-300, 1.7976931348623157e308, 1.7e9 + k / 7], pa.float64()),
        "fnan": pa.array([float("nan"), 1.0, 2.5], pa.float64()),
        "b": pa.array([False, k % 2 == 0, None]),
        "s": pa.array(["a'b", "zé🎯" + str(k), ""]),
        "bin": pa.array([b"\x00\x01", b"\xff\xfe" + bytes([k]), b"A\"'\\ ~"]),
        "ts_us": pa.array([dt.datetime(2026, 11, 21, 1, 2, 3, 456789), dt.datetime(2027, 1, 1 + k), dt.datetime(1969, 12, 31, 23, 59, 59, 500000)], pa.timestamp("us")),
        "ts_ns": pa.array([1, 1763686923123456789 + k, -1], pa.timestamp("ns")),
        "ts_ms": pa.array([1, 1763686923123 + k, 1000], pa.timestamp("ms")),
        "ts_tz": pa.array([dt.datetime(2026, 11, 21, 1, 2, 3, 456789, utc), dt.datetime(2027, 1, 1 + k, tzinfo=utc), None], pa.timestamp("us", "UTC")),
        "ts_nstz": pa.array([1, 1763686923123456789 + k, 5], pa.timestamp("ns", "UTC")),
        "d": pa.array([dt.date(1999, 12, 31), dt.date(2026, 11, 1 + k), dt.date(1960, 2, 29)]),
        "t": pa.array([dt.time(0, 0, 1), dt.time(23, 59, 59, 999999), dt.time(12, 0, 0, 500)]),
        "t_ms": pa.array([1, 86399999, 5000 + k], pa.time32("ms")),
        "dec": pa.array([decimal.Decimal("-12.345"), decimal.Decimal("99.999"), decimal.Decimal("0.001")], pa.decimal128(10, 3)),
        "dec0": pa.array([decimal.Decimal("-12"), decimal.Decimal(str(99 + k)), decimal.Decimal("0")], pa.decimal128(5, 0)),
        "decbig": pa.array([decimal.Decimal("-" + "9" * 30 + ".12345678"), decimal.Decimal("1.5"), None], pa.decimal128(38, 8)),
        "nul": pa.array([None, None, None], pa.int64()),
        "lst": pa.array([[1, 2 + k], [3], None], pa.list_(pa.int64())),
        "st": pa.array([{"x": 1, "y": "a"}, {"x": 2 + k, "y": "b"}, None]),
        "lst_st": pa.array([[{"p": 1.5, "q": "u"}], [], [{"p": float(k), "q": None}]]),
    }
    if extra:
        cols["later"] = pa.array([1.0, 2.0, 3.0])
        del cols["u8"]
    return pa.table(cols)

paths = []
for k in range(6):
    p = f"{here}/f{k:02d}.parquet"
    pq.write_table(table(k, extra=k >= 4), p, row_group_size=2)
    paths.append(p)

# DuckDB's catalog
con = duckdb.connect()
con.execute(f"ATTACH 'ducklake:sqlite:{here}/duck.sqlite' AS lk (DATA_PATH '{here}/', DUCKLAKE_VERSION '1.0')")
for p in paths:
    cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall()
    have = {r[0] for r in con.execute("SELECT column_name FROM duckdb_columns() WHERE database_name='lk' AND table_name='data'").fetchall()}
    if not have:
        con.execute("CREATE TABLE lk.data (" + ", ".join(f'"{c}" {t}' for c, t, *_ in cols) + ")")
    else:
        for c, t, *_ in cols:
            if c not in have:
                con.execute(f'ALTER TABLE lk.data ADD COLUMN "{c}" {t}')
    con.execute(f"CALL ducklake_add_data_files('lk', 'data', '{p}', allow_missing => true)")
con.execute("DETACH lk"); con.close()

# the core's
cat = _core.Catalog(b"", here + "/", time.time())
blob = None
for i, p in enumerate(paths):
    size = os.path.getsize(p)
    with open(p, "rb") as f:
        f.seek(size - 8); n = _core.footer_length(f.read(8))
        f.seek(size - 8 - n); footer = f.read(n)
    cat.add(p, size, footer)
    if i == 2:                 # a pass ends here; the catalog is opened again from its bytes
        blob = cat.to_bytes(time.time())
        cat = _core.Catalog(blob, here + "/", time.time())
blob = cat.to_bytes(time.time())
assert cat.files() == paths, cat.files()
with open(f"{here}/core.sqlite", "wb") as f:
    f.write(blob)
print("core catalog:", len(blob), "bytes; duckdb's:", os.path.getsize(f"{here}/duck.sqlite"))

a = sqlite3.connect(f"{here}/duck.sqlite"); b = sqlite3.connect(f"{here}/core.sqlite")
def cols(db):
    out = {}
    for cid, name, typ, parent in db.execute("select column_id, column_name, column_type, parent_column from ducklake_column where end_snapshot is null order by column_id"):
        out[cid] = (name, typ, parent)
    def full(cid):
        n, t, p = out[cid]
        return (full(p)[0] + "." + n if p else n), t
    return {cid: full(cid) for cid in out}
ca, cb = cols(a), cols(b)
bad = 0
if sorted(ca.values()) != sorted(cb.values()):
    bad += 1; print("COLUMNS differ:\n  duck", sorted(set(ca.values()) - set(cb.values())), "\n  core", sorted(set(cb.values()) - set(ca.values())))
def fstats(db, cmap):
    out = {}
    for path, cid, size, vc, nc, lo, hi in db.execute("select f.path, s.column_id, s.column_size_bytes, s.value_count, s.null_count, s.min_value, s.max_value from ducklake_file_column_stats s join ducklake_data_file f using (data_file_id)"):
        out[(os.path.basename(path), cmap[cid][0])] = (size, vc, nc, lo, hi)
    return out
fa, fb = fstats(a, ca), fstats(b, cb)
for k in sorted(set(fa) | set(fb)):
    x, y = fa.get(k), fb.get(k)
    if k[1] == "bin" and x and y and x[:3] == y[:3] and x[4] == y[4] \
            and y[3] == "\\x00\\x01":
        continue                        # the blob minimum: see the docstring
    if x != y:
        bad += 1; print("FILE STATS", k, "\n   duck", x, "\n   core", y)
def tstats(db, cmap):
    return {cmap[cid][0]: (cn, lo, hi) for cid, cn, lo, hi in db.execute("select column_id, contains_null, min_value, max_value from ducklake_table_column_stats")}
ta, tb = tstats(a, ca), tstats(b, cb)
for k in sorted(set(ta) | set(tb)):
    x, y = ta.get(k), tb.get(k)
    if k == "bin" and x and y and x[0] == y[0] and x[2] == y[2] \
            and y[1] == "\\x00\\x01":
        continue
    if x != y:
        bad += 1; print("TABLE STATS", k, "\n   duck", x, "\n   core", y)
fl = "select path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start from ducklake_data_file order by path"
if a.execute(fl).fetchall() != b.execute(fl).fetchall():
    bad += 1; print("FILES differ\n", a.execute(fl).fetchall(), "\n", b.execute(fl).fetchall())
if a.execute("select * from ducklake_table_stats").fetchall() != b.execute("select * from ducklake_table_stats").fetchall():
    bad += 1; print("TABLE totals differ", a.execute("select * from ducklake_table_stats").fetchall(), b.execute("select * from ducklake_table_stats").fetchall())
print("sp_file:", b.execute("select * from sp_file limit 2").fetchall(), " snapshots:", b.execute("select snapshot_id, schema_version, next_catalog_id, next_file_id from ducklake_snapshot").fetchall(), b.execute("select changes_made from ducklake_snapshot_changes").fetchall())
print("mappings:", b.execute("select count(*) from ducklake_column_mapping").fetchall(), "duck:", a.execute("select count(*) from ducklake_column_mapping").fetchall())

# every row, read through each catalog
def rows(name):
    con = duckdb.connect()
    con.execute(f"ATTACH 'ducklake:sqlite:{here}/{name}.sqlite' AS r (READ_ONLY)")
    names = [r[0] for r in con.execute("select column_name from duckdb_columns() where database_name='r' and table_name='data' order by column_name").fetchall()]
    q = "select " + ", ".join(f'"{n}"' for n in names) + ", filename from r.data order by filename, i64"
    return names, con.execute(q).fetchall(), con
na, ra, cona = rows("duck"); nb, rb, conb = rows("core")
print("rows:", len(ra), len(rb), "columns same:", na == nb, "rows same:", repr(ra) == repr(rb))
if na != nb or repr(ra) != repr(rb): bad += 1
for q in ["select count(*) from r.data where i64 = 5", "select count(*) from r.data where ts_us >= TIMESTAMP '2027-01-04'",
          "select count(*) from r.data where s = 'zé🎯5'", "select min(u64), max(u64), min(f32), max(dec), min(d), max(ts_tz), min(later), count(u8) from r.data",
          "select count(*) from r.data where u16 between 4 and 5", "select count(*) from r.data where dec0 = 104",
          "select count(*) from r.data where bin = '\\x00\\x01'::BLOB", "select count(*) from r.data where bin >= '\\xFF\\xFE\\x03'::BLOB",
          "select count(*) from r.data where later is null", "select count(*) from r.data where u8 is null",
          "select count(*) from r.data where ts_nstz::TIMESTAMPTZ > TIMESTAMPTZ '2025-11-21 01:02:03.123456+00'",
          "select count(*) from r.data where f64 > 1.7e9 and f64 < 1.8e9", "select count(*) from r.data where t_ms = TIME '00:00:05.003'"]:
    x, y = cona.execute(q).fetchall(), conb.execute(q).fetchall()
    if x != y: bad += 1; print("QUERY", q, x, y)
plan = conb.execute("explain analyze select count(*) from r.data where u16 between 4 and 5 and i32 < -70003").fetchall()[0][1]
print([l.strip("│ ") for l in plan.splitlines() if "Total Files Read" in l])
files_read = [l for l in plan.splitlines() if "Total Files Read" in l]
if not files_read or "Total Files Read: 2" not in files_read[0]:
    bad += 1; print("PRUNING: wanted 2 files read of 6:", files_read)
snaps = b.execute("select count(*) from ducklake_snapshot").fetchall()[0][0]
if snaps != 3:
    bad += 1; print("SNAPSHOTS: wanted 3 (the new catalog and two passes), got", snaps)
print("DIFFERENCES:", bad)
print("All tests passed!" if bad == 0 else "FAILED")
sys.exit(1 if bad else 0)
