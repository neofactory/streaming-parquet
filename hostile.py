"""The hostile-data testkit: one generator every suite shares.

The suites' historical blind spot (failure-modes.md): friendly data —
monotone ints, ASCII, no nulls, no NaN. This module generates the
unfriendly version, DETERMINISTICALLY from (start, count), so exact
verification needs no stored expectation:

  n     int64, not null      start..start+count (the identity)
  f     float64, nullable    cycle: n*0.5, nan, +inf, -inf, null,
                             5e-324 (min subnormal), 1.8e308
  s     utf8, nullable       cycle: ascii, "", null, unicode
                             (CJK+emoji+combining), 3000-char run
  b     binary, nullable     cycle: bytes, b"", null, 0xFF-run
  t     timestamp[us], nn    n*1_000_000 + extremes at cycle points
  tags  list<int32>, nn      length n%4, elements (n, -n, i32 edges)
"""

import math

import pyarrow as pa

F = pa.field
HOSTILE_SCHEMA = pa.schema([
    F('n', pa.int64(), nullable=False),
    F('f', pa.float64(), nullable=True),
    F('s', pa.utf8(), nullable=True),
    F('b', pa.binary(), nullable=True),
    F('t', pa.timestamp('us'), nullable=False),
    F('tags', pa.list_(pa.int32()), nullable=False),
])

_UNI = "面积µ°→∑é🎯"          # CJK, micro, degree, combining, emoji
I32MAX, I32MIN = 2**31 - 1, -2**31


def _f(n):
    return [float(n) * 0.5, math.nan, math.inf, -math.inf, None,
            5e-324, 1.8e308][n % 7]


def _s(n):
    return [f"row-{n}", "", None, f"{_UNI}{n}", "x" * 3000][n % 5]


def _b(n):
    return [f"bin{n}".encode(), b"", None, b"\xff" * (n % 50 + 1)][n % 4]


def _t(n):
    if n % 97 == 0:
        return 0
    if n % 89 == 0:
        return 250_000_000_000_000_000    # year ~9889; datetime-safe max-ish
    return n * 1_000_000


def _tags(n):
    return [(n % 1000, -(n % 1000), I32MAX, I32MIN)[i] for i in range(n % 4)]


def hostile_table(start, count):
    ns = list(range(start, start + count))
    return pa.table({
        'n': ns,
        'f': [_f(n) for n in ns],
        's': [_s(n) for n in ns],
        'b': [_b(n) for n in ns],
        't': pa.array([_t(n) for n in ns], type=pa.timestamp('us')),
        'tags': [_tags(n) for n in ns],
    }, schema=HOSTILE_SCHEMA)


def hostile_batch(start, count):
    return hostile_table(start, count).to_batches()[0]


def assert_hostile_equal(table, start, count, cols=None):
    """Exact check of a table claiming to be rows [start, start+count),
    NaN-aware. cols limits which columns are verified."""
    assert table.num_rows == count, (table.num_rows, count)
    ns = table.column('n').to_pylist()
    assert ns == list(range(start, start + count)), "n wrong/misordered"
    want = hostile_table(start, count)
    for name in (cols or ('f', 's', 'b', 't', 'tags')):
        gc, ec = table.column(name), want.column(name)
        if name == 't':                      # compare raw ticks
            gc, ec = gc.cast(pa.int64()), ec.cast(pa.int64())
        got, exp = gc.to_pylist(), ec.to_pylist()
        for i, (g, e) in enumerate(zip(got, exp)):
            if isinstance(e, float) and math.isnan(e):
                ok = isinstance(g, float) and math.isnan(g)
            else:
                ok = g == e
            assert ok, f"{name}[{i}] (n={ns[i]}): {g!r} != {e!r}"
