"""Every S3 call of the library goes through here: the clients, their
network behavior, and the timing line of each call.

Two named classes. A caller
picks the class by what the call does; nothing else sets a timeout or
a retry count:

  QUICK     small objects and metadata: tickets, HEAD, LIST, DELETE,
            the start and the abort of a multipart upload. S3 answers
            these in milliseconds; an attempt that waits 2 s with no
            byte arriving is broken, and the next attempt goes on a
            new connection.
  TRANSFER  calls whose time grows with bytes: chunks, checkpoint
            versions, the catalog's download and upload, part copies
            and the completion of a multipart upload. No cap on the
            attempt as a whole; 15 s with no byte arriving fails it.

Both classes bound the connection setup (TCP connect, then the TLS
handshake) at 1 s. A setup takes milliseconds; past 1 s a packet was
lost, and a new connection beats the kernel's SYN retransmit (1 s,
then 3 s, then 7 s). urllib3 tries each address DNS gave in turn, each
with this bound, so an address that does not answer costs 1 s, not 5.

Both make 3 attempts in botocore's standard retry mode: jittered
exponential backoff, and a retry quota that stops retrying when most
calls of a client fail (a dead WAN fails fast; the host's next poll
tries again).

botocore's read_timeout is the longest wait for one read on the
socket, not a limit on the attempt: a slow trickle of bytes is not
cut. A 2 s per-attempt deadline (Go's context deadline) has no
exact botocore counterpart.

One boto3 session per process (one credential chain; an assumed role
is renewed once for all callers) and one client per class: botocore
clients are thread-safe, sessions are not, so both are made under a
lock.
"""

import logging
import threading
import time

#: Timing lines for S3 calls. streaming_parquet attaches its diag file
#: handler to this logger.
_log = logging.getLogger('parquet_checkpoint')

#: A call at or over SLOW_CALL_S is logged (always); with `trace` on,
#: every call is (streaming_parquet turns it on at verbosity 2).
SLOW_CALL_S = 1.0
trace = False


class Class:
    """A named set of network settings for S3 calls."""

    def __init__(self, name, read_timeout_s):
        self.name = name
        self.connect_timeout_s = 1.0
        self.read_timeout_s = read_timeout_s
        self.attempts = 3


QUICK = Class('quick', read_timeout_s=2.0)
TRANSFER = Class('transfer', read_timeout_s=15.0)

_lock = threading.Lock()
_session = None
_clients = {}


def session():
    """The process's boto3 session (its credentials: the catalog gives
    them to DuckDB). Made at the first call: a host that picks a
    credential profile (AWS_PROFILE) does so before."""
    global _session
    with _lock:
        if _session is None:
            import boto3
            _session = boto3.session.Session()
        return _session


def client(cls):
    """The S3 client of class `cls` (QUICK or TRANSFER)."""
    s = session()
    with _lock:
        c = _clients.get(cls.name)
        if c is None:
            from botocore.config import Config
            c = s.client('s3', config=Config(
                connect_timeout=cls.connect_timeout_s,
                read_timeout=cls.read_timeout_s,
                retries={'mode': 'standard',
                         'total_max_attempts': cls.attempts}))
            # Ahead of botocore's own retry handler, which ends the
            # event when it decides to retry: a failed attempt is
            # logged with its cause (a connect timeout, a 503, ...).
            c.meta.events.register_first('needs-retry.s3',
                                         _log_failed_attempt)
            for ev in _PHASE_EVENTS:        # where a slow call's time went
                c.meta.events.register(f'{ev}.s3', _mark(ev))
            _clients[cls.name] = c
        return c


def timed(op, key, fn, detail=''):
    """Run one S3 call (`fn`), log it when slow (always) or traced,
    with how many attempts it took and where its time went."""
    t0 = time.monotonic()
    outcome = 'ok'
    meta = {}
    _marks.m = [('start', t0)]
    try:
        r = fn()
        if isinstance(r, dict):
            meta = r.get('ResponseMetadata', {})
        return r
    except Exception as e:
        outcome = type(e).__name__
        meta = (getattr(e, 'response', None) or {}).get(
            'ResponseMetadata', {})
        raise
    finally:
        t_end = time.monotonic()
        dt = t_end - t0
        marks, _marks.m = _marks.m, None
        if trace or dt >= SLOW_CALL_S:
            retries = meta.get('RetryAttempts')
            tries = ('' if retries is None else
                     f" {retries + 1} attempt{'s' if retries else ''}")
            _log.info(f"s3 {op} {key}{detail} {dt:.3f}s {outcome}{tries}"
                      f"{' SLOW' if dt >= SLOW_CALL_S else ''}"
                      f"{_breakdown(marks, t_end)}")


def list_keys(bucket, prefix, delimiter=None):
    """Every key under `prefix` (with `delimiter`: not below the next
    one), as one timed QUICK call."""
    def do_list():
        kw = {'Delimiter': delimiter} if delimiter else {}
        out = []
        pages = client(QUICK).get_paginator('list_objects_v2').paginate(
            Bucket=bucket, Prefix=prefix, **kw)
        for page in pages:
            out += [o['Key'] for o in page.get('Contents', ())]
        return out
    return timed('list', prefix, do_list)


#: Per-thread marks of the S3 call in progress: botocore's events fire
#: on the calling thread, so each call collects its own.
_marks = threading.local()

#: The botocore events a slow call's breakdown is measured between.
_PHASE_EVENTS = ('before-sign', 'before-send', 'response-received',
                 'after-call')


def _mark(event):
    def handler(**_):
        m = getattr(_marks, 'm', None)
        if m is not None:
            m.append((event, time.monotonic()))
    return handler


def _breakdown(marks, t_end):
    """Where a call's time went, from its marks: prep (parameters,
    endpoint) until the first signing; sign (credentials, a role
    renewal included, and signing) until the request goes out; http
    (connect, send, S3's answer) until the response is in; after
    (parsing) until the call returns. Summed over attempts."""
    t0 = marks[0][1]
    prep = sign = http = 0.0
    first_sign = last_resp = None
    pending = {}
    for ev, t in marks[1:]:
        if ev == 'before-sign':
            first_sign = t if first_sign is None else first_sign
            pending['sign'] = t
        elif ev == 'before-send' and 'sign' in pending:
            sign += t - pending.pop('sign')
            pending['send'] = t
        elif ev == 'response-received' and 'send' in pending:
            http += t - pending.pop('send')
            last_resp = t
    if first_sign is not None:
        prep = first_sign - t0
    after = t_end - last_resp if last_resp is not None else 0.0
    return (f" (prep {prep:.2f}s, sign {sign:.2f}s, http {http:.2f}s, "
            f"after {after:.2f}s)")


def _log_failed_attempt(attempts, response=None, caught_exception=None,
                        operation=None, request_dict=None, **_):
    """botocore's needs-retry event, before its retry handler: log an
    attempt that failed -- the exception (connect timeout, reset, ...)
    or the error status. Returns None: the retry decision stays
    botocore's."""
    if caught_exception is not None:
        # names the endpoint URL itself
        why = f"{type(caught_exception).__name__}: {caught_exception}"
    else:
        status = (response[1] if response else {}).get(
            'ResponseMetadata', {}).get('HTTPStatusCode', 0)
        if status < 500 and status != 429:
            return None
        url = (request_dict or {}).get('url', '').split('?')[0]
        why = f"HTTP {status} from {url}"
    _log.info(f"s3 attempt {attempts} of "
              f"{getattr(operation, 'name', operation)} failed: {why}")
    return None
