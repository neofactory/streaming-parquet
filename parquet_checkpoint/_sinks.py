"""Storage sinks: where chunks, checkpoints, and finished files live.

One interface, three implementations:

  DirSink   a local directory. Atomic replace via os.replace; assembly
            is concatenation. The volume tier for tests and the local
            backup in production.
  FakeS3    in-process, memory-speed, implements S3 SEMANTICS: atomic
            whole-object replace, immutable objects, no append, and
            assembly through a real multipart-upload state machine that
            enforces the >=5MB-per-part-except-last rule and the
            10,000-part ceiling. Fault injection: fail or crash any
            upcoming operation. The tests' S3.
  S3Sink    the real thing (boto3). put/get/delete plus assembly via
            CreateMultipartUpload / UploadPartCopy (range) /
            CompleteMultipartUpload. The thin contract suite runs here.

Sink names are relative ('run.parquet.chunks/000001'); each sink roots
them (directory path / bucket+prefix).

Assembly sources are (name, offset, length) triples; offset/length of
-1 mean the whole object. The final triple may be any length; earlier
ones must satisfy the sink's part rules (FakeS3 and S3Sink enforce
5 MB; DirSink doesn't care).
"""

import io
import logging
import os
import threading
import time
import zlib

from . import _s3

#: S3Sink's assemble line (each call's own line is _s3's). The same
#: logger as _s3's: streaming_parquet attaches its diag file handler.
_log = logging.getLogger('parquet_checkpoint')


class SinkError(Exception):
    """A sink operation failed (loudly)."""


class FaultInjected(SinkError):
    """A deliberately injected failure (tests)."""


class CrashPoint(BaseException):
    """A deliberately injected crash: BaseException so ordinary
    error handling cannot swallow it -- like a real SIGKILL."""


# ---- local directory ---------------------------------------------------------

class SubSink:
    """A sink rooted deeper inside another: every name gets `prefix`
    in front on the way in and loses it on the way out of list(). The
    stream layout uses it: a root sink, and `streams/<name>/` under it
    per stream. Faults injected on the parent apply (FakeS3)."""

    def __init__(self, parent, prefix):
        self.parent = parent
        self.prefix = prefix.strip('/') + '/' if prefix.strip('/') else ''
        self.part_min = parent.part_min
        base = parent.uri if parent.uri.endswith('/') else parent.uri + '/'
        self.uri = base + self.prefix

    def sub(self, prefix):
        return SubSink(self.parent, self.prefix + prefix)

    def _k(self, name):
        return self.prefix + name

    def put(self, name, data):
        return self.parent.put(self._k(name), data)

    def get(self, name, offset=0, length=-1):
        return self.parent.get(self._k(name), offset, length)

    def size(self, name):
        return self.parent.size(self._k(name))

    def exists(self, name):
        return self.parent.exists(self._k(name))

    def delete(self, name):
        return self.parent.delete(self._k(name))

    def mtime(self, name):
        return self.parent.mtime(self._k(name))

    def list(self, prefix):
        n = len(self.prefix)
        return [name[n:] for name in self.parent.list(self._k(prefix))]

    def assemble(self, dest, sources):
        return self.parent.assemble(
            self._k(dest), [(self._k(name), off, length)
                            for name, off, length in sources])


class DirSink:
    part_min = 0        # concatenation has no part-size rules

    def __init__(self, root):
        self.root = root

    @property
    def uri(self):
        return os.path.abspath(self.root)

    def sub(self, prefix):
        return DirSink(os.path.join(self.root, prefix.strip('/')))

    def _p(self, name):
        return os.path.join(self.root, name)

    def _folder_for(self, path):
        """Make the folder of `path`. Only a write makes one: a store
        has no empty folders, and a question (exists, size, list) must
        not leave one behind."""
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def put(self, name, data):
        p = self._p(name)
        tmp = p + '.tmp'
        for attempt in (0, 1):
            self._folder_for(p)
            try:
                f = open(tmp, 'wb')
                break
            except FileNotFoundError:
                if attempt:             # a delete took the folder in between
                    raise
        with f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)

    def get(self, name, offset=0, length=-1):
        with open(self._p(name), 'rb') as f:
            f.seek(offset)
            return f.read() if length < 0 else f.read(length)

    def size(self, name):
        return os.path.getsize(self._p(name))

    def exists(self, name):
        return os.path.exists(self._p(name))

    def delete(self, name):
        p = self._p(name)
        if os.path.exists(p):
            os.remove(p)
        # an object's folder goes with its last object (a file's
        # <name>.chunks/): a store has no empty folders
        folder = os.path.dirname(p)
        if os.path.abspath(folder) != os.path.abspath(self.root):
            try:
                os.rmdir(folder)
            except OSError:
                pass                    # not empty, or gone already

    def list(self, prefix):
        out = []
        for dirpath, _, files in os.walk(self.root):
            for f in files:
                rel = os.path.relpath(os.path.join(dirpath, f), self.root)
                if rel.startswith(prefix) and not rel.endswith('.tmp'):
                    out.append(rel)
        return sorted(out)

    def mtime(self, name):
        """Last-modified epoch seconds — the object's liveness stamp
        (a live checkpoint is rewritten on its checkpoint cadence)."""
        return os.path.getmtime(self._p(name))

    def assemble(self, dest, sources):
        """Concatenate sources into dest. Local: no part rules."""
        p = self._p(dest)
        self._folder_for(p)
        tmp = p + '.tmp'
        with open(tmp, 'wb') as f:
            for name, off, length in sources:
                f.write(self.get(name, 0 if off < 0 else off, length))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)


# ---- fake S3 -----------------------------------------------------------------

MIN_PART = 5 * 1024 * 1024


def plan_parts(sources, sizes, part_min):
    """The parts of a multipart assembly of `sources` ((name, offset,
    length), `sizes` their byte counts), under S3's rule: every part
    but the last is at least part_min. S3 copies a part server-side
    from one object, and it cannot join small objects.

    Returns [('copy', [source]) | ('upload', [source, ...])]. A source
    of at least part_min is copied as a part of its own. Smaller
    sources that follow one another are fetched and uploaded together
    as one part; such a run takes in sources until it reaches part_min
    or the file ends. A file is its chunks, each at least part_min,
    then its smaller objects and its footer: so the chunks are copied
    and what follows them travels once, as the last part."""
    parts, run, run_bytes = [], [], 0
    for source, size in zip(sources, sizes):
        if not run and size >= part_min:
            parts.append(('copy', [source]))
            continue
        run.append(source)
        run_bytes += size
        if run_bytes >= part_min:
            parts.append(('upload', run))
            run, run_bytes = [], 0
    if run:
        parts.append(('upload', run))
    return parts
MAX_PARTS = 10_000


class FakeS3:
    """S3 semantics at memory speed, with fault injection.

    Faults: fail_next(op_substr, n=1) makes the next n matching
    operations raise FaultInjected AFTER doing nothing (a failed PUT
    stores nothing -- S3 PUTs are atomic). crash_next(op_substr) makes
    the next matching operation raise CrashPoint (simulates the process
    dying at exactly that moment; state reflects a real kill: nothing
    partial is ever visible).
    """

    def __init__(self, part_min=MIN_PART):
        self.part_min = part_min    # scaled down in tests; semantics real
        self.uri = "fake-s3://"
        self._objects = {}          # name -> bytes (immutable)
        self._mtimes = {}           # name -> epoch s of last put
        self._lock = threading.Lock()
        self._faults = []           # (op_substr, kind, remaining)
        self.op_log = []

    def sub(self, prefix):
        return SubSink(self, prefix)

    # -- fault injection --
    def fail_next(self, op_substr, n=1):
        self._faults.append([op_substr, 'fail', n])

    def crash_next(self, op_substr, n=1):
        self._faults.append([op_substr, 'crash', n])

    def clear_faults(self):
        """End an injected outage."""
        self._faults.clear()

    def _op(self, opname):
        self.op_log.append(opname)
        for f in self._faults:
            sub, kind, remaining = f
            if sub in opname and remaining > 0:
                f[2] -= 1
                if kind == 'fail':
                    raise FaultInjected(f"injected failure at {opname}")
                raise CrashPoint(f"injected crash at {opname}")

    # -- object ops (all atomic, all immutable-replace) --
    def put(self, name, data):
        self._op(f"put:{name}")
        import time
        with self._lock:
            self._objects[name] = bytes(data)
            self._mtimes[name] = time.time()

    def get(self, name, offset=0, length=-1):
        self._op(f"get:{name}")
        with self._lock:
            if name not in self._objects:
                raise SinkError(f"NoSuchKey: {name}")
            b = self._objects[name]
        b = b[offset:]
        return b if length < 0 else b[:length]

    def size(self, name):
        with self._lock:
            if name not in self._objects:
                raise SinkError(f"NoSuchKey: {name}")
            return len(self._objects[name])

    def exists(self, name):
        with self._lock:
            return name in self._objects

    def delete(self, name):
        self._op(f"delete:{name}")
        with self._lock:
            self._objects.pop(name, None)

    def list(self, prefix):
        with self._lock:
            return sorted(n for n in self._objects if n.startswith(prefix))

    def mtime(self, name):
        import time
        with self._lock:
            if name not in self._objects:
                raise SinkError(f"mtime: no object {name}")
            return self._mtimes.get(name, time.time())

    def assemble(self, dest, sources):
        """A real MPU state machine: enforces S3's part rules. Parts
        as plan_parts makes them: a copy for each source of at least
        the part minimum, one upload for a run of smaller ones."""
        self._op(f"assemble:{dest}")
        with self._lock:
            sizes = [len(self._objects[name]) if off < 0 else length
                     for name, off, length in sources]
        plan = plan_parts(sources, sizes, self.part_min)
        if len(plan) > MAX_PARTS:
            raise SinkError(f"assemble: {len(plan)} parts exceeds "
                            f"{MAX_PARTS}")
        parts = []
        for i, (kind, group) in enumerate(plan):
            self._op(f"upload_part_copy:{dest}:{i + 1}" if kind == 'copy'
                     else f"upload_part:{dest}:{i + 1}")
            data = b''.join(self.get(name, 0 if off < 0 else off, length)
                            for name, off, length in group)
            last = i == len(plan) - 1
            if not last and len(data) < self.part_min:
                raise SinkError(
                    f"assemble: part {i + 1} is {len(data)} bytes; parts "
                    f"before the last must be >= {self.part_min}")
            parts.append(data)
        self._op(f"complete:{dest}")
        with self._lock:
            self._objects[dest] = b''.join(parts)   # atomic materialize


# ---- real S3 ------------------------------------------------------------------

class S3Sink:
    """boto3-backed sink. Same interface; the contract suite's target.

    Credentials come from the standard AWS chain (the root only picks the
    bucket). Every call goes through _s3: its class (QUICK or TRANSFER)
    sets its timeouts and attempts, and it is timed and logged when
    slow. An unreachable S3 surfaces in seconds, not minutes: a Shipper
    poll on a dead WAN must fail fast and be retried by the host's next
    poll."""

    part_min = MIN_PART

    def __init__(self, bucket, prefix=''):
        self.bucket = bucket
        self.prefix = prefix.rstrip('/') + '/' if prefix else ''
        self._quick = _s3.client(_s3.QUICK)
        self._transfer = _s3.client(_s3.TRANSFER)
        self.uri = f"s3://{bucket}/{self.prefix}"

    def sub(self, prefix):
        child = S3Sink.__new__(S3Sink)
        child.bucket = self.bucket
        child.prefix = self.prefix + prefix.strip('/') + '/'
        child._quick, child._transfer = self._quick, self._transfer
        child.uri = f"s3://{self.bucket}/{child.prefix}"
        return child

    def _k(self, name):
        return self.prefix + name

    def put(self, name, data):
        data = bytes(data)
        _s3.timed('put', self._k(name), lambda: self._transfer.put_object(
            Bucket=self.bucket, Key=self._k(name), Body=data),
            detail=f" {len(data)}B")

    def get(self, name, offset=0, length=-1):
        kw = {}
        if offset or length >= 0:
            end = '' if length < 0 else str(offset + length - 1)
            kw['Range'] = f'bytes={offset}-{end}'
        r = _s3.timed('get', self._k(name), lambda: self._transfer.get_object(
            Bucket=self.bucket, Key=self._k(name), **kw))
        return r['Body'].read()

    def size(self, name):
        return self._head(name)['ContentLength']

    def exists(self, name):
        try:
            self.size(name)
            return True
        except Exception:
            return False

    def delete(self, name):
        _s3.timed('delete', self._k(name), lambda: self._quick.delete_object(
            Bucket=self.bucket, Key=self._k(name)))

    def mtime(self, name):
        return self._head(name)['LastModified'].timestamp()

    def _head(self, name):
        return _s3.timed('head', self._k(name), lambda: self._quick.head_object(
            Bucket=self.bucket, Key=self._k(name)))

    def list(self, prefix):
        return sorted(k[len(self.prefix):] for k in
                      _s3.list_keys(self.bucket, self._k(prefix)))

    def assemble(self, dest, sources):
        """dest as one object from `sources`, by a multipart upload.
        The parts are plan_parts': a server-side copy for each source of
        at least MIN_PART (a chunk: no bytes travel), and one uploaded
        part for a run of smaller sources (the objects after the last
        chunk and the footer: fetched and sent once)."""
        key = self._k(dest)
        t_all = time.monotonic()
        sizes = [self.size(name) if off < 0 else length
                 for name, off, length in sources]
        plan = plan_parts(sources, sizes, MIN_PART)
        if len(plan) > MAX_PARTS:
            raise SinkError(f"assemble: {len(plan)} parts exceeds "
                            f"{MAX_PARTS}")
        mpu = _s3.timed('create-mpu', key, lambda: self._quick.create_multipart_upload(
            Bucket=self.bucket, Key=key))
        uid = mpu['UploadId']
        try:
            parts = []
            for i, (kind, group) in enumerate(plan):
                n = i + 1
                if kind == 'copy':
                    name, off, length = group[0]
                    rng = (None if off < 0
                           else f'bytes={off}-{off + length - 1}')
                    kw = {'CopySourceRange': rng} if rng else {}
                    r = _s3.timed('copy-part', key, lambda: self._transfer.upload_part_copy(
                        Bucket=self.bucket, Key=key, UploadId=uid, PartNumber=n,
                        CopySource={'Bucket': self.bucket,
                                    'Key': self._k(name)}, **kw),
                        detail=f" part {n}")
                    etag = r['CopyPartResult']['ETag']
                else:
                    body = b''.join(
                        self.get(name, 0 if off < 0 else off, length)
                        for name, off, length in group)
                    r = _s3.timed('upload-part', key, lambda: self._transfer.upload_part(
                        Bucket=self.bucket, Key=key, UploadId=uid, PartNumber=n,
                        Body=body),
                        detail=f" part {n} {len(body)}B of {len(group)} object(s)")
                    etag = r['ETag']
                parts.append({'PartNumber': n, 'ETag': etag})
            _s3.timed('complete-mpu', key, lambda: self._transfer.complete_multipart_upload(
                Bucket=self.bucket, Key=key, UploadId=uid,
                MultipartUpload={'Parts': parts}),
                detail=f" {len(parts)} part(s)")
        except BaseException:
            try:
                _s3.timed('abort-mpu', key, lambda: self._quick.abort_multipart_upload(
                    Bucket=self.bucket, Key=key, UploadId=uid))
            except Exception:
                pass
            raise
        dt = time.monotonic() - t_all
        if _s3.trace or dt >= _s3.SLOW_CALL_S:
            _log.info(f"s3 assemble {key} {len(plan)} part(s) {dt:.3f}s"
                      f"{' SLOW' if dt >= _s3.SLOW_CALL_S else ''}")
