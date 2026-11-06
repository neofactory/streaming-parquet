"""Write real seed artifacts (checkpoints + footers) for the fuzz
driver's corpus. Uses the pyarrow encoder the C core lacks, so the
standalone mutation fuzzer starts from valid inputs and explores the
accept boundary. Run: python3 parquet_checkpoint/make_corpus.py
"""
import io
import os
import struct
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from parquet_checkpoint import CheckpointWriter, FakeS3, ckpt_versions

F = pa.field
S = pa.schema([F('n', pa.int64(), nullable=False), F('s', pa.string())])
D = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'core', 'corpus')
os.makedirs(D, exist_ok=True)


def main():
    for i, (rows, tail, chunked) in enumerate(
            [(1, 0, False), (50, 0, False), (300, 7, False), (2000, 0, True)]):
        sink = FakeS3(part_min=4096)
        w = CheckpointWriter(f'c{i}.parquet', S, sink=sink,
                             chunk_bytes=16 * 1024 if chunked else 5 << 20,
                             compression='none', checkpoint_every_write=False)
        if rows:
            w.write_table(pa.table(
                {'n': list(range(rows)),
                 's': [f'r{j}' for j in range(rows)]}, schema=S))
        t = (pa.table({'n': list(range(rows, rows + tail)),
                       's': [f'r{j}' for j in range(rows, rows + tail)]},
                      schema=S) if tail else None)
        w.checkpoint(t)
        open(f'{D}/ckpt{i}.bin', 'wb').write(sink.get(ckpt_versions(sink, f'c{i}.parquet')[-1][1]))
        buf = io.BytesIO()
        pw = pq.ParquetWriter(buf, S, compression='none')
        pw.write_table(pa.table({'n': [1, 2, 3], 's': ['a', 'b', 'c']},
                                schema=S))
        pw.close()
        m = buf.getvalue()
        flen = struct.unpack('<I', m[-8:-4])[0]
        open(f'{D}/footer{i}.bin', 'wb').write(m[len(m) - 8 - flen:len(m) - 8])
    print("corpus written:", sorted(os.listdir(D)))


if __name__ == '__main__':
    main()
