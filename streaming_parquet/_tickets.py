"""Ticket store: bucket/directory stream discovery (replaces MQTT).

A stream's ticket is one small JSON object at

    <root>/tickets/<name>.json

The publisher heartbeats it (rewrites it with a fresh `ts`); readers
poll it; a clean close deletes it. A missing ticket means "no
publisher"; a stale `ts` means "dead publisher". The ticket dict:
{"name","host","port","ts","run"}; "run" is the writer's start time
(UTC text, to the microsecond), the same value every file it writes
records as sp_run. The archive is not in it: it is
<root>/streams/<name>, the same name under the same root.

The store is a whole bucket or a whole directory:

    /data/streams  or  file:///data/streams      a directory
    s3://bucket                                  S3 (boto3, standard credential chain)

No prefix inside a bucket: the layout is the store's contents. Store
resolution when a call names none:

    1. the STREAMING_PARQUET_STORE environment variable
    2. READS ONLY: the public demo bucket (DEFAULT_PUBLIC_STORE)

Writers never fall back to the public store: create_stream with no store
disables the ticket, loudly, and keeps the stream local. A misconfigured
daemon must fail visibly, never publish somewhere surprising.
"""

import getpass
import json
import os
import threading

DEFAULT_PUBLIC_STORE = "s3://streaming-parquet"

_lock = threading.Lock()
_stores = {}          # store uri -> ticket backend (client reuse)

#: the spool a bucket store writes through when none is named
DEFAULT_SPOOL = os.path.join("/var/tmp", "streaming-parquet", getpass.getuser())


def resolve_store(for_write=False):
    """The store when a call names none: $STREAMING_PARQUET_STORE, or
    for reads the public demo bucket. Writes get None (the caller
    disables the ticket, loudly, and keeps the stream local)."""
    env = os.environ.get("STREAMING_PARQUET_STORE")
    if env:
        return env
    return None if for_write else DEFAULT_PUBLIC_STORE


def parse_store(store):
    """(kind, where) for a store: ('s3', bucket) for 's3://bucket',
    ('dir', path) for a bare path or 'file://path', ('sink', obj) for a
    parquet_checkpoint sink object (tests). A store is a WHOLE bucket or
    directory whose contents are the layout; a prefix inside a bucket is
    refused so every deployment has one shape."""
    if store is None:
        raise ValueError("no store")
    if not isinstance(store, str):
        return "sink", store
    if store.startswith("s3://"):
        rest = store[len("s3://"):].strip("/")
        bucket, _, prefix = rest.partition("/")
        if not bucket:
            raise ValueError(f"store {store!r}: no bucket in URI")
        if prefix:
            raise ValueError(
                f"store {store!r}: a store is a whole bucket; {prefix!r} is a "
                f"prefix inside it. Give the bucket alone; the layout "
                f"streams/<name>/ and tickets/<name>.json is its contents")
        return "s3", bucket
    if store.startswith("file://"):
        return "dir", store[len("file://"):]
    if "://" in store:
        scheme = store.split("://", 1)[0]
        raise ValueError(
            f"store {store!r}: unsupported scheme {scheme!r} "
            f"(supported: s3://bucket, file://path, a bare path)")
    return "dir", store


def parse_stream_uri(uri):
    """Split a fully-qualified stream URI into (store, name).

    'file:///data/streams/streams/probe/voltages' ->
    ('file:///data/streams', 'probe/voltages'). A stream URI names
    the stream's archive directory, <store>/streams/<name>/; its ticket
    is at <store>/tickets/<name>.json. The '/streams/' segment is the
    split point.
    """
    marker = "/streams/"
    idx = uri.find(marker)
    if idx < 0:
        raise ValueError(
            f"stream URI {uri!r} has no '/streams/' segment; expected "
            f"<store>/streams/<name>")
    return uri[:idx], uri[idx + len(marker):]


def archive_dir(store, name):
    """Where a stream's archive lives under a store: <store>/streams/<name>.
    The same name under the same store; nothing in the ticket says so. A
    local path for a directory store, a URI for a bucket store, None for
    no store."""
    if store is None:
        return None
    kind, where = parse_store(store)
    if kind == "s3":
        return f"s3://{where}/streams/{name}/"
    if kind == "dir":
        return os.path.join(where, "streams", name)
    return f"{where.uri}streams/{name}/" if hasattr(where, "uri") else None


def _store_for(store):
    key = store if isinstance(store, str) else id(store)
    with _lock:
        backend = _stores.get(key)
        if backend is None:
            backend = _make_backend(store)
            _stores[key] = backend
        return backend


def _make_backend(store):
    kind, where = parse_store(store)
    if kind == "s3":
        return _S3Store(where, "")
    if kind == "dir":
        return _FileStore(where)
    return _SinkStore(where)


# ---- store operations (module face) -----------------------------------

#: tickets without "stale_after" -- the Go writer's, rewritten once a
#: minute -- are stale after 2.25 of its beats
GO_TICKET_STALE_S = 135.0


def stale_after(ticket):
    """Seconds after its write a ticket means a dead writer: the writer's
    own promise (three beats, at least 30 s), else the Go writer's."""
    return float(ticket.get("stale_after", GO_TICKET_STALE_S))


def put_ticket(store, name, ticket):
    _store_for(store).put(name, ticket)


def get_ticket(store, name):
    """The ticket dict, or None when no publisher has one up."""
    return _store_for(store).get(name)


def delete_ticket(store, name):
    _store_for(store).delete(name)


def list_streams(store):
    """Names of all streams with a ticket under the store (sorted).
    Liveness is per-ticket (`ts`); a listed name may be stale."""
    return sorted(_store_for(store).list())


class _SinkStore:
    """Tickets as objects in a parquet_checkpoint sink (tests: FakeS3)."""

    def __init__(self, sink):
        self._sink = sink.sub("tickets")

    def put(self, name, ticket):
        self._sink.put(name + ".json", json.dumps(ticket).encode())

    def get(self, name):
        try:
            return json.loads(self._sink.get(name + ".json"))
        except Exception:
            return None

    def delete(self, name):
        try:
            self._sink.delete(name + ".json")
        except Exception:
            pass

    def list(self):
        return [n[:-len(".json")] for n in self._sink.list("") if n.endswith(".json")]


# ---- backends ---------------------------------------------------------

class _FileStore:
    """Tickets as files under <dir>/tickets/. Atomic writes via
    rename so a poller never reads a half-written ticket."""

    def __init__(self, base):
        self._base = os.path.join(base, "tickets")

    def _path(self, name):
        return os.path.join(self._base, name + ".json")

    def put(self, name, ticket):
        path = self._path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(ticket, f)
        os.replace(tmp, path)

    def get(self, name):
        try:
            with open(self._path(name)) as f:
                return json.load(f)
        except FileNotFoundError:
            return None

    def delete(self, name):
        try:
            os.remove(self._path(name))
        except FileNotFoundError:
            pass

    def list(self):
        names = []
        for dirpath, _dirs, files in os.walk(self._base):
            for fn in files:
                if fn.endswith(".json"):
                    full = os.path.join(dirpath, fn)
                    rel = os.path.relpath(full, self._base)
                    names.append(rel[:-len(".json")].replace(os.sep, "/"))
        return names


class _S3Store:
    """Tickets as objects at s3://bucket/<prefix>/tickets/<name>.json
    (boto3, standard credential chain — the root only picks the
    bucket, never the credentials)."""

    def __init__(self, bucket, prefix):
        import botocore.exceptions
        from parquet_checkpoint import _s3
        self._bucket = bucket
        base = prefix.strip("/")
        self._prefix = (base + "/tickets/") if base else "tickets/"
        # a ticket is a small object: QUICK, so an unreachable bucket
        # surfaces in seconds and never stalls a start
        self._timed = _s3.timed
        self._list_keys = _s3.list_keys
        self._s3 = _s3.client(_s3.QUICK)
        self._client_error = botocore.exceptions.ClientError

    def _key(self, name):
        return self._prefix + name + ".json"

    def put(self, name, ticket):
        key = self._key(name)
        self._timed("put", key, lambda: self._s3.put_object(
            Bucket=self._bucket, Key=key, Body=json.dumps(ticket).encode(),
            ContentType="application/json"))

    def get(self, name):
        try:
            key = self._key(name)
            obj = self._timed("get", key, lambda: self._s3.get_object(
                Bucket=self._bucket, Key=key))
        except self._client_error as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
        return json.loads(obj["Body"].read())

    def delete(self, name):
        key = self._key(name)
        self._timed("delete", key, lambda: self._s3.delete_object(
            Bucket=self._bucket, Key=key))

    def list(self):
        return [key[len(self._prefix):-len(".json")]
                for key in self._list_keys(self._bucket, self._prefix)
                if key.endswith(".json")]
