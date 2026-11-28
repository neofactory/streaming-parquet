"""Uploader: upload one live file's artifacts from a local sink to S3.

The two-copies model: the writer writes objects + checkpoint versions
to local disk at its own tight cadence (never blocked by the network);
the uploader copies them to S3 at whatever cadence the link allows.
After an outage, catch-up is automatic and cheap: an object never
changes, so the uploader sends each object of the NEWEST checkpoint
version that the remote lacks, then that version under its own name --
every intermediate version the outage swallowed, and every object only
such a version named, is skipped by construction. The uploader deletes a
remote version it uploaded once it has been superseded
SUPERSEDED_KEEP_S, and with it the objects no version it keeps names.

The RULES are the C core's (src/core/pqc.h, THE UPLOAD): what is sent,
the order, what is deleted, and the steps of the end of a file. This
class does what the core asks: it reads the local sink, and it puts,
deletes and assembles on the remote. The Go host does the same with
the same core.

Pollable, no threads: the host calls poll() from its own loop. A poll
that cannot reach S3 RAISES -- the host logs it loudly and keeps
polling; nothing is swallowed here, and the next poll goes on from what
was done. Ordering is preserved across the upload: every chunk a
checkpoint references is uploaded before that checkpoint.

    upload = Uploader('run.parquet', local=DirSink(d), remote=S3Sink(b))
    upload.poll()      # each host-loop tick; returns a status dict
    ...
    # shutdown protocol: write_table any remaining rows (so the last
    # checkpoint has no provisional tail), poll() until synced, then
    w.close()        # local file finalized by the writer
    upload.close()     # finalizes the REMOTE copy from its own
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


class Uploader:
    def __init__(self, name, local, remote,
                 superseded_keep_s=SUPERSEDED_KEEP_S):
        self.name = name
        self.local = local
        self.remote = remote
        try:
            self._upload = _core.Upload(name, superseded_keep_s)
        except _core.CoreError as e:
            raise CheckpointError(f"{name}: {e}") from None
        self._last_uploaded = None       # bytes of the last uploaded version
        self._last_object = None        # its object name
        self.done = False               # finished in both copies

    @property
    def doing(self):
        """What the uploader is doing on the remote right now, for a host
        that waits on it (a stream's close): None between operations."""
        return self._upload.doing

    def _newest_local(self):
        v = ckpt_versions(self.local, self.name)
        return v[-1][1] if v else None

    def run(self, live=True, keep_checkpoints=False, stored=None):
        """One pass, as the core asks. live: the writer still has the
        file open (the pass uploads it and stops); False: the writer
        ended it, and the pass also takes the steps of the end of the
        file: the local assembly, then the remote one
        (`keep_checkpoints`: leave the remote chunks and versions for
        readers that were told of them). Returns a status dict: state
        'idle' (nothing local yet), 'synced' (nothing new), 'uploaded'
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
        m = self._upload
        try:
            m.local(obj, raw, final, live)
        except _core.CoreError as e:
            raise CheckpointError(str(e)) from None
        status = {'state': ('synced' if obj is not None else
                            'local-finalized' if final else 'idle'),
                  'done': False}
        if obj is not None and raw == self._last_uploaded:
            status['object'] = self._last_object
        chunks = 0
        try:
            while True:
                action, name, size, flag = m.next()
                if action == _core.UPLOAD_NOTHING:
                    return status
                if action == _core.UPLOAD_DONE:
                    self.done = True
                    status['done'] = True
                    return status
                if action == _core.UPLOAD_STAT:
                    exists = self.remote.exists(name)
                    m.remote_has(exists,
                                 self.remote.size(name) if exists else 0)
                    continue
                if action == _core.UPLOAD_OBJECT:
                    self.remote.put(name, self.local.get(name))
                    chunks += 1
                elif action == _core.UPLOAD_VERSION:
                    self.remote.put(name, raw)   # stored whole before any delete
                    self._last_uploaded = raw
                    self._last_object = name
                elif action == _core.UPLOAD_STORED:
                    status.update(state='uploaded', chunks=chunks,
                                  watermark=size, object=name)
                    if stored is not None:
                        stored(name, size)
                elif action == _core.UPLOAD_DELETE:
                    self.remote.delete(name)
                elif action == _core.UPLOAD_ASSEMBLE_LOCAL:
                    t0 = time.monotonic()
                    finalize(self.name, self.local)
                    status['local_s'] = time.monotonic() - t0
                elif action == _core.UPLOAD_ASSEMBLE_REMOTE:
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

    def _assemble_remote(self, from_uploaded, keep):
        """Finalize the REMOTE copy from its own artifacts (server-side
        assembly; byte-identical to the local final file)."""
        if from_uploaded:
            # The newest remote version is the one this uploader put
            # there: finalize from the bytes in hand, no read-back.
            ckpt, obj = self._upload.uploaded()
            return finalize(self.name, self.remote, ckpt_bytes=ckpt,
                            ckpt_object=obj, keep_checkpoints=keep)
        if ckpt_versions(self.remote, self.name):
            return finalize(self.name, self.remote, keep_checkpoints=keep)
        if self.remote.exists(self.name):
            return ('done', self.name)
        raise CheckpointError(
            f"uploader.close({self.name!r}): nothing uploaded -- "
            f"poll() until synced before closing")

    def poll(self):
        """One upload pass of a file its writer still has open. Returns
        the status dict of run(). Raises on any sink failure -- the host
        retries next poll."""
        return self.run(live=True)

    @property
    def synced(self):
        """True when the remote has the newest local version."""
        obj = self._newest_local()
        if obj is None:
            return not self.local.exists(self.name)  # finalized needs close()
        return self.local.get(obj) == self._last_uploaded

    def close(self, keep_checkpoints=False):
        """Finalize the REMOTE copy from its own uploaded artifacts
        (server-side assembly; byte-identical to the local final file).
        Call after the writer's close(); requires the final checkpoint
        to have uploaded (poll until synced before closing the writer).
        `keep_checkpoints`: leave the remote chunks and versions for
        readers that were told of them; discard_checkpoints() later."""
        if self.done:
            return ('done', self.name)
        if self._newest_local() is not None:
            raise CheckpointError(
                f"uploader.close({self.name!r}): the local copy still has a "
                f"checkpoint -- close the writer first")
        self.run(live=False, keep_checkpoints=keep_checkpoints)
        return ('recovered', self.name)
