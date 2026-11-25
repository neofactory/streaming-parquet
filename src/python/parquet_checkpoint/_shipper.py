"""Shipper: mirror one live file's artifacts from a local sink to S3.

The two-copies model: the writer writes objects + checkpoint versions
to local disk at its own tight cadence (never blocked by the network);
the shipper copies them to S3 at whatever cadence the link allows.
After an outage, catch-up is automatic and cheap: an object never
changes, so the shipper sends each object of the NEWEST checkpoint
version that the remote lacks, then that version under its own name --
every intermediate version the outage swallowed, and every object only
such a version named, is skipped by construction. The shipper deletes a
remote version it shipped once it has been superseded
SUPERSEDED_KEEP_S, and with it the objects no version it keeps names.

The RULES are the C core's (src/core/pqc.h, THE MIRROR): what is sent,
the order, what is deleted, and the steps of the end of a file. This
class does what the core asks: it reads the local sink, and it puts,
deletes and assembles on the remote. The Go host does the same with
the same core.

Pollable, no threads: the host calls poll() from its own loop. A poll
that cannot reach S3 RAISES -- the host logs it loudly and keeps
polling; nothing is swallowed here, and the next poll goes on from what
was done. Ordering is preserved across the mirror: every chunk a
checkpoint references is shipped before that checkpoint.

    ship = Shipper('run.parquet', local=DirSink(d), remote=S3Sink(b))
    ship.poll()      # each host-loop tick; returns a status dict
    ...
    # shutdown protocol: write_table any remaining rows (so the last
    # checkpoint has no provisional tail), poll() until synced, then
    w.close()        # local file finalized by the writer
    ship.close()     # finalizes the REMOTE copy from its own
                     # artifacts -- byte-identical to the local file

A host that ends files itself (a stream's writer) calls
run(live=False): one pass that also takes the steps of the end of the
file, in the core's order.

Loss window if the local disk dies: rows since the last successful
poll -- by design (see the streaming-parquet README).
"""

import time

from . import (SUPERSEDED_KEEP_S, CheckpointError, ckpt_versions, finalize)
from . import _core


class Shipper:
    def __init__(self, name, local, remote,
                 superseded_keep_s=SUPERSEDED_KEEP_S):
        self.name = name
        self.local = local
        self.remote = remote
        try:
            self._mirror = _core.Mirror(name, superseded_keep_s)
        except _core.CoreError as e:
            raise CheckpointError(f"{name}: {e}") from None
        self._last_shipped = None       # bytes of the last shipped version
        self._last_object = None        # its object name
        self.done = False               # finished in both copies

    @property
    def doing(self):
        """What the shipper is doing on the remote right now, for a host
        that waits on it (a stream's close): None between operations."""
        return self._mirror.doing

    def _newest_local(self):
        v = ckpt_versions(self.local, self.name)
        return v[-1][1] if v else None

    def run(self, live=True, keep_checkpoints=False, stored=None):
        """One pass, as the core asks. live: the writer still has the
        file open (the pass mirrors it and stops); False: the writer
        ended it, and the pass also takes the steps of the end of the
        file: the local assembly, then the remote one
        (`keep_checkpoints`: leave the remote chunks and versions for
        readers that were told of them). Returns a status dict: state
        'idle' (nothing local yet), 'synced' (nothing new), 'shipped'
        (chunks and/or a version copied; 'object' names it, 'watermark'
        its rows), or 'local-finalized' (the finished file is there and
        no version); 'done': True when the file is finished in both
        copies; 'local_s', 'remote_s': the seconds the two assemblies
        took. `stored(object, rows)` is called at the moment the remote
        holds a new version, also in a pass that fails after it: what a
        ticket may say. Raises on any sink failure: the host retries
        next pass, which goes on from what was done."""
        if self.done:
            return {'state': 'idle', 'done': True}
        obj = self._newest_local()
        raw = self.local.get(obj) if obj is not None else None
        final = obj is None and self.local.exists(self.name)
        m = self._mirror
        try:
            m.local(obj, raw, final, live)
        except _core.CoreError as e:
            raise CheckpointError(str(e)) from None
        status = {'state': ('synced' if obj is not None else
                            'local-finalized' if final else 'idle'),
                  'done': False}
        if obj is not None and raw == self._last_shipped:
            status['object'] = self._last_object
        chunks = 0
        try:
            while True:
                action, name, size, flag = m.next()
                if action == _core.MIRROR_NOTHING:
                    return status
                if action == _core.MIRROR_DONE:
                    self.done = True
                    status['done'] = True
                    return status
                if action == _core.MIRROR_STAT:
                    exists = self.remote.exists(name)
                    m.remote_has(exists,
                                 self.remote.size(name) if exists else 0)
                    continue
                if action == _core.MIRROR_UPLOAD:
                    self.remote.put(name, self.local.get(name))
                    chunks += 1
                elif action == _core.MIRROR_UPLOAD_VERSION:
                    self.remote.put(name, raw)   # stored whole before any delete
                    self._last_shipped = raw
                    self._last_object = name
                elif action == _core.MIRROR_STORED:
                    status.update(state='shipped', chunks=chunks,
                                  watermark=size, object=name)
                    if stored is not None:
                        stored(name, size)
                elif action == _core.MIRROR_DELETE:
                    self.remote.delete(name)
                elif action == _core.MIRROR_ASSEMBLE_LOCAL:
                    t0 = time.monotonic()
                    finalize(self.name, self.local)
                    status['local_s'] = time.monotonic() - t0
                elif action == _core.MIRROR_ASSEMBLE_REMOTE:
                    t0 = time.monotonic()
                    self._assemble_remote(flag, keep_checkpoints)
                    status['remote_s'] = time.monotonic() - t0
                m.did()
        except _core.CoreError as e:
            m.failed()
            raise CheckpointError(f"{self.name}: {e}") from None
        except BaseException:
            m.failed()                  # the pass is over; the next goes on
            raise

    def _assemble_remote(self, from_shipped, keep):
        """Finalize the REMOTE copy from its own artifacts (server-side
        assembly; byte-identical to the local final file)."""
        if from_shipped:
            # The newest remote version is the one this shipper put
            # there: finalize from the bytes in hand, no read-back.
            ckpt, obj = self._mirror.shipped()
            return finalize(self.name, self.remote, ckpt_bytes=ckpt,
                            ckpt_object=obj, keep_checkpoints=keep)
        if ckpt_versions(self.remote, self.name):
            return finalize(self.name, self.remote, keep_checkpoints=keep)
        if self.remote.exists(self.name):
            return ('done', self.name)
        raise CheckpointError(
            f"shipper.close({self.name!r}): nothing shipped -- "
            f"poll() until synced before closing")

    def poll(self):
        """One mirror pass of a file its writer still has open. Returns
        the status dict of run(). Raises on any sink failure -- the host
        retries next poll."""
        return self.run(live=True)

    @property
    def synced(self):
        """True when the remote has the newest local version."""
        obj = self._newest_local()
        if obj is None:
            return not self.local.exists(self.name)  # finalized needs close()
        return self.local.get(obj) == self._last_shipped

    def close(self, keep_checkpoints=False):
        """Finalize the REMOTE copy from its own shipped artifacts
        (server-side assembly; byte-identical to the local final file).
        Call after the writer's close(); requires the final checkpoint
        to have shipped (poll until synced before closing the writer).
        `keep_checkpoints`: leave the remote chunks and versions for
        readers that were told of them; discard_checkpoints() later."""
        if self.done:
            return ('done', self.name)
        if self._newest_local() is not None:
            raise CheckpointError(
                f"shipper.close({self.name!r}): the local copy still has a "
                f"checkpoint -- close the writer first")
        self.run(live=False, keep_checkpoints=keep_checkpoints)
        return ('recovered', self.name)
