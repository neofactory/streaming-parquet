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
SUPERSEDED_KEEP_S, in the poll that ships a later one, and with it the
objects no version it keeps names.

Pollable, no threads: the host calls poll() from its own loop. A poll
that cannot reach S3 RAISES -- the host logs it loudly and keeps
polling; nothing is swallowed here. Ordering is preserved across the
mirror: every chunk a checkpoint references is shipped before that
checkpoint.

    ship = Shipper('run.parquet', local=DirSink(d), remote=S3Sink(b))
    ship.poll()      # each host-loop tick; returns a status dict
    ...
    # shutdown protocol: write_table any remaining rows (so the last
    # checkpoint has no provisional tail), poll() until synced, then
    w.close()        # local file finalized by the writer
    ship.close()     # finalizes the REMOTE copy from its own
                     # artifacts -- byte-identical to the local file

Loss window if the local disk dies: rows since the last successful
poll -- by design (see the streaming-parquet README).
"""

from . import (SUPERSEDED_KEEP_S, CheckpointError, CheckpointReader,
               _supersede, ckpt_versions, finalize)


class Shipper:
    def __init__(self, name, local, remote,
                 superseded_keep_s=SUPERSEDED_KEEP_S):
        self.name = name
        self.local = local
        self.remote = remote
        self._last_shipped = None       # bytes of the last shipped version
        self._last_object = None        # its object name
        # [remote version, superseded at, the objects it names]
        self._shipped = []
        self._there = set()             # objects known to be on the remote
        self._keep_s = superseded_keep_s
        # what the shipper is doing on the remote right now, for a host
        # that waits on it (a stream's close): None between operations
        self.doing = None

    def _newest_local(self):
        v = ckpt_versions(self.local, self.name)
        return v[-1][1] if v else None

    def poll(self):
        """One mirror pass. Returns a status dict:
        state 'idle' (nothing local yet), 'synced' (nothing new),
        'shipped' (chunks and/or a checkpoint version copied; 'object'
        names it), or 'local-finalized' (the local writer closed; call
        close()). Raises on any sink failure -- the host retries next
        poll."""
        obj = self._newest_local()
        if obj is None:
            if self.local.exists(self.name):
                return {'state': 'local-finalized'}
            return {'state': 'idle'}
        raw = self.local.get(obj)
        if raw == self._last_shipped:
            return {'state': 'synced', 'object': self._last_object}
        r = CheckpointReader(raw)       # validates magic/version/CRC
        shipped_chunks = 0
        for cname, length, crc in r.manifest:
            if cname in self._there:
                continue                # sent or seen: it never changes
            if (not self.remote.exists(cname)
                    or self.remote.size(cname) != length):
                self.doing = (f"uploading object {cname} "
                              f"({length / 1e6:.1f} MB)")
                try:
                    self.remote.put(cname, self.local.get(cname))
                finally:
                    self.doing = None
                shipped_chunks += 1
            self._there.add(cname)
        self.doing = f"uploading checkpoint {obj} ({len(raw) / 1e6:.1f} MB)"
        try:
            self.remote.put(obj, raw)   # stored whole before any delete
        finally:
            self.doing = None
        self._last_shipped = raw
        self._last_object = obj
        names = [m[0] for m in r.manifest]
        self._shipped = _supersede(self.remote, self._shipped, obj,
                                   self._keep_s, names)
        # what the remote may lack again: an object deleted with its
        # last version
        self._there &= set().union(*(v[2] for v in self._shipped))
        return {'state': 'shipped', 'chunks': shipped_chunks,
                'watermark': r.watermark, 'object': obj}

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
        self.doing = f"assembling {self.name} on the store"
        try:
            return self._close(keep_checkpoints)
        finally:
            self.doing = None

    def _close(self, keep):
        if self._last_shipped is not None:
            # The newest remote version is the one this shipper put
            # there: finalize from the bytes in hand, no read-back.
            return finalize(self.name, self.remote,
                            ckpt_bytes=self._last_shipped,
                            ckpt_object=self._last_object,
                            keep_checkpoints=keep)
        if ckpt_versions(self.remote, self.name):
            return finalize(self.name, self.remote, keep_checkpoints=keep)
        if self.remote.exists(self.name):
            return ('done', self.name)
        raise CheckpointError(
            f"shipper.close({self.name!r}): nothing shipped -- "
            f"poll() until synced before closing")
