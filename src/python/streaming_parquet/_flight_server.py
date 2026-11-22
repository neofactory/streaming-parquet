"""Embedded Arrow Flight server: readers pull the live rows.

A plain RPC, not a streaming push. Each DoGet call asks for the rows
from one seq on and gets one answer: the committed rows from that seq
to the newest, as one batch, with the ticket revision in the side
data. If there are no rows yet, the call waits up to WAIT_S for one to
arrive, then answers with an empty batch that carries the revision. The
reader appends what it got and asks again from the seq it reached. The
next request is the acknowledgment of the last answer.

Why not a stream of pushed batches, as this file once was. A push has
to choose how much may be in flight to a reader before the publisher
stops sending. Too little and the pipe idles for a round trip; too much
and a reader that stopped reading holds rows the publisher must keep,
and a batch stuck in a blocked send held a ring pin, which on
2026-11-07 stopped the writer and ended the publisher. Two writes in
flight alternate a full write with a one-row write for as long as the
path is slow. Every choice needs a timeout, a watchdog and a way to
abort one reader's blocked send, which the server side of Flight does
not offer. The pull has none of that to choose. One answer is in flight
per reader, it carries everything since the last one, so a reader that
keeps up gets a write per commit and a reader on a slow path gets a
write per round trip, sized by the round trip. The publisher keeps no
state per reader: no queue, no sender thread, no pin held past the
answer's own write. A reader that stops asking leaves nothing behind.
A reader behind by more than the ring gets the oldest row that is left
and the gap shows in the seq. Simplicity wins here, and it keeps the
ring's one rule: the writer never waits for a reader.

Wire: GetFlightInfo by path descriptor answers the schema and, in
TotalRecords, the committed row count, so a new reader starts at the
newest row. The DoGet ticket is "<name>@<from_seq>" or
"<name>@<from_seq>:col1,col2". The answer's app_metadata is 16 bytes
LE: the origin seq of the batch's first row (-1 for an empty batch),
then the ticket revision. The Go host (go/streaming/flightserver.go)
speaks the same wire.
"""

import threading
import time

import pyarrow as pa
import pyarrow.flight as flight

from ._diag import _diag

#: how long a DoGet waits for a row before answering empty (the empty
#: answer carries the ticket revision, so a reader learns of a ticket
#: change within this time even on a quiet stream)
WAIT_S = 1.0
#: the most rows one answer carries; a reader further behind asks again
MAX_ROWS = 65_536


def _side_data(first_seq, revision):
    import struct
    return struct.pack('<qq', first_seq, revision)


class _StreamInfo:
    """A registered stream: its schema, the Stream it reads from, the
    condition readers wait on, and the peers seen (for one log line
    each)."""

    def __init__(self, name, schema, stream_ref):
        self.name = name
        self.schema = schema
        self.stream_ref = stream_ref
        self.cond = threading.Condition()
        self.closed = False
        self.peers = set()

    def notify(self):
        with self.cond:
            self.cond.notify_all()

    def close(self):
        with self.cond:
            self.closed = True
            self.cond.notify_all()


class StreamFlightServer(flight.FlightServerBase):
    """Arrow Flight server over this process's live streams. One per
    process, started on the first create_stream() call."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._streams = {}  # name -> _StreamInfo
        self._lock = threading.Lock()
        self._shutting_down = False

    def register_stream(self, name, schema, stream_ref):
        with self._lock:
            self._streams[name] = _StreamInfo(name, schema, stream_ref)

    def unregister_stream(self, name):
        """Remove the stream and wake its waiting readers, who get an
        error and reconnect through the ticket: a closed publisher must
        look closed."""
        with self._lock:
            info = self._streams.pop(name, None)
        if info is not None:
            info.close()

    def notify(self, name):
        """Rows committed, or the ticket changed: wake the readers
        waiting on this stream. Called from commit; cheap."""
        with self._lock:
            info = self._streams.get(name)
        if info is not None:
            info.notify()

    def list_flights(self, context, criteria):
        with self._lock:
            stream_infos = list(self._streams.values())
        for info in stream_infos:
            descriptor = flight.FlightDescriptor.for_path(info.name)
            yield self._info(info, descriptor)

    def get_flight_info(self, context, descriptor):
        name = descriptor.path[0].decode("utf-8") if descriptor.path else ""
        with self._lock:
            info = self._streams.get(name)
        if info is None:
            raise flight.FlightUnavailableError(f"Unknown stream: {name}")
        return self._info(info, descriptor)

    def _info(self, info, descriptor):
        # TotalRecords: the committed row count, so a new reader's first
        # request starts at the newest row.
        committed = info.stream_ref._ring.newest_seq + 1
        endpoint = flight.FlightEndpoint(
            flight.Ticket(f"{info.name}@{committed}".encode("utf-8")), [])
        return flight.FlightInfo(info.schema, descriptor, [endpoint], committed, -1)

    def do_get(self, context, ticket):
        """One answer: the rows from the ticket's seq to the newest, or
        an empty batch after WAIT_S. The ticket: "<name>@<from_seq>" or
        "<name>@<from_seq>:col1,col2"."""
        raw = ticket.ticket.decode("utf-8")
        head, _, cols_str = raw.partition(":")
        name, at, from_str = head.partition("@")
        if not at or not from_str.isdigit():
            raise flight.FlightServerError(
                f"ticket {raw!r}: want '<name>@<from_seq>[:col1,col2]'")
        from_seq = int(from_str)
        requested = cols_str.split(",") if cols_str else None
        with self._lock:
            info = self._streams.get(name)
        if info is None:
            raise flight.FlightUnavailableError(f"Unknown stream: {name}")
        if requested is not None:
            indices = [info.schema.get_field_index(c) for c in requested]
            if -1 in indices:
                missing = requested[indices.index(-1)]
                raise flight.FlightServerError(f"stream {name}: no column {missing!r}")
            schema = pa.schema([info.schema.field(i) for i in indices])
        else:
            indices = None
            schema = info.schema
        from urllib.parse import unquote
        peer = unquote(context.peer()) if hasattr(context, 'peer') else "unknown"
        if peer not in info.peers:
            info.peers.add(peer)
            _diag.info(f"[{name}] READER {peer}: first request, from seq {from_seq}")

        stream = info.stream_ref
        deadline = time.monotonic() + WAIT_S
        with info.cond:
            revision = stream._ticket_revision
            while (stream._ring.newest_seq < from_seq and not info.closed
                   and not self._shutting_down
                   and stream._ticket_revision == revision):
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                info.cond.wait(left)
        if info.closed:
            raise flight.FlightUnavailableError(f"stream {name} closed")
        revision = stream._ticket_revision
        committed = stream._ring.newest_seq + 1
        if committed <= from_seq:
            empty = pa.RecordBatch.from_arrays(
                [pa.array([], type=f.type) for f in schema], schema=schema)
            return flight.GeneratorStream(schema, [(empty, _side_data(-1, revision))])
        # Pin the rows this answer carries, and no others; the pin rides
        # the batch and drops when Flight has written it. The ring may
        # have recycled rows below from_seq: the answer starts at the
        # oldest row left, and the reader sees the gap in the seq.
        rel = stream._data.pin(from_seq, min(committed, from_seq + MAX_ROWS))
        table = rel.detach()
        if indices is not None:
            table = table.select(indices)
        first = rel.lo

        def answer():
            running = first
            for batch in table.to_batches():
                if batch.num_rows:
                    yield batch, _side_data(running, revision)
                    running += batch.num_rows
        return flight.GeneratorStream(schema, answer())

    def shutdown(self):
        """Shut down the Flight server (called from module close)."""
        self._shutting_down = True
        with self._lock:
            infos = list(self._streams.values())
        for info in infos:
            info.notify()
        super().shutdown()


def start_flight_server():
    """Start a StreamFlightServer on an OS-assigned port. Returns (server, port)."""
    server = StreamFlightServer(location="grpc://0.0.0.0:0")
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    port = server.port
    return server, port
