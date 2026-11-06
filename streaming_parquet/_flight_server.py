"""Embedded Arrow Flight server for live stream fan-out."""

import queue
import threading
import time

import pyarrow as pa
import pyarrow.flight as flight

from ._diag import _diag


class _SubscriberQueue:
    """Per-subscriber queue with column filtering and overflow drop."""

    def __init__(self, column_indices=None, maxsize=100):
        self._queue = queue.Queue(maxsize=maxsize)
        self.column_indices = column_indices  # None means all columns
        self.closed = False

    def put(self, item):
        """Enqueue a (batch, first_seq, revision) item, dropping oldest on
        overflow (the drop is exactly why batches carry their seq:
        the receiver sees the gap and jumps, loudly)."""
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            try:
                self._queue.get_nowait()  # drop oldest
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                pass  # best-effort

    def get(self, timeout=1.0):
        """Block until a batch is available or timeout."""
        return self._queue.get(timeout=timeout)

    def close(self):
        self.closed = True


class _StreamInfo:
    """Metadata for a registered stream."""

    def __init__(self, name, schema, stream_ref):
        self.name = name
        self.schema = schema
        self.stream_ref = stream_ref
        self.subscribers = []  # list of _SubscriberQueue
        self._lock = threading.Lock()

    def add_subscriber(self, sub_queue):
        with self._lock:
            self.subscribers.append(sub_queue)

    def remove_subscriber(self, sub_queue):
        with self._lock:
            try:
                self.subscribers.remove(sub_queue)
            except ValueError:
                pass

    def push_batch(self, batch, first_seq=-1, revision=0):
        """Push a batch (whose first row is stream seq `first_seq`; -1
        for an empty batch) with the ticket revision to all subscriber
        queues, filtering columns per subscriber. Returns number of
        active subscribers pushed to."""
        with self._lock:
            subs = list(self.subscribers)
        count = 0
        for sub in subs:
            if sub.closed:
                continue
            if sub.column_indices is not None:
                filtered = batch.select(sub.column_indices)
            else:
                filtered = batch
            sub.put((filtered, first_seq, revision))
            count += 1
        return count


class StreamFlightServer(flight.FlightServerBase):
    """Arrow Flight server serving live streams.

    One per process, started on first create_stream() call.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._streams = {}  # name -> _StreamInfo
        self._lock = threading.Lock()
        self._shutting_down = False
        self._all_queues = []  # track all subscriber queues for shutdown

    def register_stream(self, name, schema, stream_ref):
        with self._lock:
            self._streams[name] = _StreamInfo(name, schema, stream_ref)

    def unregister_stream(self, name):
        """Remove the stream AND end its subscribers' do_get streams —
        a closed publisher must look closed to remote feeders (they
        reconnect and rediscover; leaving them attached to a dead
        registration hid same-process respawns from them)."""
        with self._lock:
            info = self._streams.pop(name, None)
        if info is not None:
            with info._lock:
                subs = list(info.subscribers)
            for sub in subs:
                sub.close()

    def has_subscribers(self, stream_name):
        """True while at least one Flight subscriber is attached to the
        stream. A publisher with none skips its pushes entirely."""
        with self._lock:
            info = self._streams.get(stream_name)
        if info is None:
            return False
        with info._lock:
            return any(not s.closed for s in info.subscribers)

    def push_batch(self, stream_name, batch, first_seq=-1, revision=0):
        """Push a RecordBatch (first row = stream seq `first_seq`) and
        the stream's ticket revision to all subscribers of a stream.
        Returns subscriber count."""
        with self._lock:
            info = self._streams.get(stream_name)
        if info is not None:
            return info.push_batch(batch, first_seq, revision)
        return 0

    def list_flights(self, context, criteria):
        with self._lock:
            stream_infos = list(self._streams.values())
        for info in stream_infos:
            descriptor = flight.FlightDescriptor.for_path(info.name)
            # Include schema in the FlightInfo
            endpoint = flight.FlightEndpoint(
                flight.Ticket(info.name.encode("utf-8")), []
            )
            yield flight.FlightInfo(
                info.schema, descriptor, [endpoint], -1, -1
            )

    def get_flight_info(self, context, descriptor):
        name = descriptor.path[0].decode("utf-8") if descriptor.path else ""
        with self._lock:
            info = self._streams.get(name)
        if info is None:
            raise flight.FlightUnavailableError(f"Unknown stream: {name}")
        endpoint = flight.FlightEndpoint(
            flight.Ticket(name.encode("utf-8")), []
        )
        return flight.FlightInfo(
            info.schema, descriptor, [endpoint], -1, -1
        )

    def do_get(self, context, ticket):
        """Long-lived streaming to a subscriber.

        The ticket bytes encode: stream_name or stream_name:col1,col2,...
        """
        raw = ticket.ticket.decode("utf-8")
        if ":" in raw:
            stream_name, cols_str = raw.split(":", 1)
            requested_columns = cols_str.split(",")
        else:
            stream_name = raw
            requested_columns = None

        with self._lock:
            info = self._streams.get(stream_name)
        if info is None:
            raise flight.FlightUnavailableError(f"Unknown stream: {stream_name}")

        # Determine column indices for filtering
        if requested_columns is not None:
            column_indices = [
                info.schema.get_field_index(c) for c in requested_columns
            ]
            sub_schema = pa.schema([info.schema.field(i) for i in column_indices])
        else:
            column_indices = None
            sub_schema = info.schema

        sub_queue = _SubscriberQueue(column_indices=column_indices)
        info.add_subscriber(sub_queue)
        with self._lock:
            self._all_queues.append(sub_queue)
            n_total = len(self._all_queues)

        from urllib.parse import unquote
        peer = unquote(context.peer()) if hasattr(context, 'peer') else "unknown"
        _diag.info(f"[{stream_name}] SUBSCRIBER CONNECTED: peer={peer}, total_subs={n_total}")

        def _generate():
            batches_sent = 0
            try:
                while not sub_queue.closed and not self._shutting_down:
                    if hasattr(context, 'is_cancelled') and context.is_cancelled():
                        break
                    try:
                        batch, first_seq, revision = sub_queue.get(timeout=1.0)
                        batches_sent += 1
                        if batches_sent == 1:
                            _diag.info(f"[{stream_name}] FIRST BATCH TO SUBSCRIBER: {batch.num_rows} rows, peer={peer}")
                        # 16 bytes LE: the origin seq of the first row
                        # (-1 for an empty batch), the attach-side
                        # alignment signal; then the ticket revision,
                        # which tells a live reader to re-read the ticket
                        import struct as _s
                        yield batch, _s.pack('<qq', first_seq, revision)
                    except queue.Empty:
                        continue
            finally:
                info.remove_subscriber(sub_queue)
                with self._lock:
                    try:
                        self._all_queues.remove(sub_queue)
                    except ValueError:
                        pass
                    n_remaining = len(self._all_queues)
                _diag.info(f"[{stream_name}] SUBSCRIBER DISCONNECTED: {batches_sent} batches sent, peer={peer}, remaining_subs={n_remaining}")

        return flight.GeneratorStream(sub_schema, _generate())

    def drain_subscribers(self, timeout=2.0):
        """Wait until all subscriber queues are empty or timeout expires.

        Call after streams have done their final drain but before shutdown().
        Returns immediately if there are no subscribers or no pending data.
        Gives up early if queue depth stops decreasing (stuck client).
        """
        deadline = time.time() + timeout
        prev_total = None
        stall_start = None
        while time.time() < deadline:
            with self._lock:
                queues = list(self._all_queues)
            if not queues:
                return
            total = sum(q._queue.qsize() for q in queues)
            if total == 0:
                return
            # If no progress for 0.5s, a client is stuck — stop waiting
            if prev_total is not None and total >= prev_total:
                if stall_start is None:
                    stall_start = time.time()
                elif time.time() - stall_start > 0.5:
                    _diag.warning(f"[flight] drain: {total} batch(es) stuck "
                                  f"in subscriber queues, proceeding with shutdown")
                    return
            else:
                stall_start = None
            prev_total = total
            time.sleep(0.05)

    def shutdown(self):
        """Shut down the Flight server (called from module close)."""
        self._shutting_down = True
        # Close all subscriber queues to unblock do_get generators
        with self._lock:
            for q in self._all_queues:
                q.close()
        super().shutdown()


def start_flight_server():
    """Start a StreamFlightServer on an OS-assigned port. Returns (server, port)."""
    server = StreamFlightServer(location="grpc://0.0.0.0:0")
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    # Get the actual port the OS assigned
    port = server.port
    return server, port
