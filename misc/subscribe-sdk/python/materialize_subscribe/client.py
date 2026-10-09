# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The pgwire transport: connect to Materialize and drive a subscription.

The transport is deliberately thin. It runs the ``DECLARE``/``FETCH`` loop and
hands every row to the tested :class:`Decoder`. All protocol judgement lives in
the decoder and the consistency engine, not here.

A background thread drains the cursor continuously into a *bounded* buffer,
rather than fetching only when the consumer asks. This is deliberate:
Materialize buffers a subscription's unread output in ``environmentd`` without
bound, so a consumer that stops fetching pushes an unbounded cost onto the
server. Draining continuously keeps that buffer on the client, where it is
bounded and fails loud (:class:`BufferOverflow`) if the consumer cannot keep up.
The client falls over, never the server.

``psycopg`` (v3) is imported lazily so the protocol core can be used and tested
without the driver installed.
"""

from __future__ import annotations

import queue as _queue
import threading
from typing import Any, Callable, List, Optional

from .batch import Batcher, ConsistentBatch
from .engine import ReleaseBuffer  # noqa: F401 - re-exported for cohort convenience
from .envelope import Decoder, Envelope, StreamMessage
from .errors import (
    BufferOverflow,
    CompactionHorizon,
    DependencyDropped,
    FatalError,
    SchemaMismatch,
    SubscribeError,
    TransientError,
)
from .statement import Subscribe
from .token import ResumeToken

# The cursor name for the subscription. One subscription owns its connection, so
# a fixed name is safe.
_CURSOR = "mz_subscribe_cursor"

# Default client-side buffer capacity, in decoded messages. A slow consumer that
# falls this far behind trips :class:`BufferOverflow`.
#
# TODO: make this configurable per subscription once we have real workloads to
# size it against.
DEFAULT_BUFFER_CAPACITY = 1 << 16

# How long a consumer blocks between checks that its producers are still alive,
# and how long ``close`` waits for a drain thread to wind down.
_POLL_SECONDS = 0.5
_JOIN_SECONDS = 2.0

#: A message wrapper: transforms a decoded message before it enters the buffer.
#: Single-view uses identity; a cohort tags each message with its member index.
Wrap = Callable[[StreamMessage], Any]


def _identity(message: StreamMessage) -> Any:
    return message


class _Buffer:
    """The bounded, thread-safe hand-off between drain threads and the consumer.

    One or more drain threads ``put`` decoded messages; the consumer ``recv``s
    them. When the buffer fills, ``put`` reports the overflow instead of blocking
    (blocking would backpressure the server). Termination is observed by every
    producer thread having exited: the consumer then surfaces the first recorded
    error, or a clean end.
    """

    def __init__(self, capacity: int) -> None:
        self._queue: _queue.Queue[Any] = _queue.Queue(maxsize=max(1, capacity))
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._error: Optional[SubscribeError] = None
        self._threads: List[threading.Thread] = []

    def set_threads(self, threads: List[threading.Thread]) -> None:
        self._threads = threads

    def should_stop(self) -> bool:
        return self._stop.is_set()

    def request_stop(self) -> None:
        """Signals every drain to stop, without waiting."""
        self._stop.set()

    def put(self, item: Any) -> bool:
        """Enqueues ``item``. Returns ``False`` if the buffer is full."""
        try:
            self._queue.put_nowait(item)
            return True
        except _queue.Full:
            return False

    def record_error(self, error: SubscribeError) -> None:
        """Records the first terminal error and signals every drain to stop."""
        with self._lock:
            if self._error is None:
                self._error = error
        self._stop.set()

    def recv(self) -> Any:
        """Returns the next buffered item.

        Raises the terminal :class:`SubscribeError` if a drain failed, or
        ``StopIteration`` on a clean end (every producer exited and the buffer
        drained).
        """
        while True:
            try:
                return self._queue.get(timeout=_POLL_SECONDS)
            except _queue.Empty:
                if not any(t.is_alive() for t in self._threads):
                    with self._lock:
                        error = self._error
                    if error is not None:
                        raise error
                    raise StopIteration

    def close(self) -> None:
        """Stops every drain and waits briefly for it to release its cursor."""
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=_JOIN_SECONDS)


class SubscribeClient:
    """A connection to Materialize for consuming subscriptions.

    One connection backs one subscription: starting a subscription hands the
    connection to a background drain thread. Transaction-mode connection poolers
    (such as PgBouncer) break the ``DECLARE``/``FETCH`` cursor and are not
    supported.

    Usable as a context manager, which closes the active subscription (and its
    connection) on exit.
    """

    def __init__(self, connection: Any, buffer_capacity: int) -> None:
        self._conn: Optional[Any] = connection
        self._buffer_capacity = buffer_capacity
        self._stream: Optional[Any] = None

    @classmethod
    def connect(
        cls, conninfo: str, buffer_capacity: int = DEFAULT_BUFFER_CAPACITY
    ) -> "SubscribeClient":
        """Connects to Materialize using a libpq-style connection string.

        ``buffer_capacity`` is the client-side buffer size in messages; a
        consumer that falls that far behind trips :class:`BufferOverflow`.
        """
        psycopg = _import_psycopg()
        try:
            connection = psycopg.connect(conninfo, autocommit=True)
        except psycopg.Error as exc:
            raise _classify(exc) from exc
        return cls(connection, buffer_capacity)

    def subscribe(self, subscribe: Subscribe) -> "BatchStream":
        """Starts a new subscription, taking the initial snapshot."""
        raw = self._open_raw(subscribe, subscribe.to_sql_initial())
        stream = BatchStream(
            raw,
            Batcher(subscribe.fingerprint(), subscribe.envelope, with_snapshot=True),
        )
        self._stream = stream
        return stream

    def resume(self, subscribe: Subscribe, token: ResumeToken) -> "BatchStream":
        """Resumes a subscription from ``token``, skipping the snapshot.

        Raises :class:`SchemaMismatch` if ``subscribe`` does not match the query
        the token was taken against.
        """
        _check_fingerprint(subscribe, token)
        raw = self._open_raw(subscribe, subscribe.to_sql_resume(token))
        stream = BatchStream(
            raw,
            Batcher(subscribe.fingerprint(), subscribe.envelope, with_snapshot=False),
        )
        self._stream = stream
        return stream

    def subscribe_raw(self, subscribe: Subscribe) -> "RawStream":
        """Starts a subscription and hands back the *raw* decoded stream: the
        timestamped changes and progress markers, before any batching.

        This is the composable substrate. Most callers want :meth:`subscribe`,
        which layers consistent batching on top. Reach for the raw stream to
        build a different consistency policy of your own.
        """
        raw = self._open_raw(subscribe, subscribe.to_sql_initial())
        self._stream = raw
        return raw

    def resume_raw(self, subscribe: Subscribe, token: ResumeToken) -> "RawStream":
        """Resumes a raw stream from ``token``. See :meth:`subscribe_raw`."""
        _check_fingerprint(subscribe, token)
        raw = self._open_raw(subscribe, subscribe.to_sql_resume(token))
        self._stream = raw
        return raw

    def close(self) -> None:
        """Closes the active subscription and its connection."""
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        elif self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "SubscribeClient":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def _open_raw(self, subscribe: Subscribe, subscribe_sql: str) -> "RawStream":
        if self._conn is None:
            raise SubscribeError(
                "this client already owns a subscription; use one client per "
                "subscription"
            )
        # Ownership of the connection transfers to the drain thread.
        conn = self._conn
        self._conn = None
        _declare_cursor(conn, subscribe_sql)
        buffer = _spawn_drain(
            conn,
            subscribe.envelope,
            subscribe.is_bounded(),
            _identity,
            self._buffer_capacity,
        )
        return RawStream(buffer)


class RawStream:
    """The raw decoded stream for one subscription: :data:`StreamMessage` values
    in arrival order, before any batching or consistency policy.

    This is layer one, the composable substrate the batcher and cohort build on.
    Iterate to pull messages. Usable as a context manager, which stops the drain
    on exit.
    """

    def __init__(self, buffer: _Buffer) -> None:
        self._buffer = buffer

    def __iter__(self) -> "RawStream":
        return self

    def __next__(self) -> StreamMessage:
        message: StreamMessage = self._buffer.recv()
        return message

    def close(self) -> None:
        self._buffer.close()

    def __enter__(self) -> "RawStream":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()


class BatchStream:
    """An iterator over the :class:`ConsistentBatch` values of one subscription.

    Iterate to pull batches. Each is complete: a consumer that applies it and
    persists its ``resume_token`` atomically achieves exactly-once state. A
    bounded (``UP TO``) subscription ends iteration when it drains; an unbounded
    one blocks until the next batch is ready. This is a raw stream plus the
    :class:`Batcher` consistency engine.
    """

    def __init__(self, raw: RawStream, batcher: Batcher) -> None:
        self._raw = raw
        self._batcher = batcher

    def __iter__(self) -> "BatchStream":
        return self

    def __next__(self) -> ConsistentBatch:
        for message in self._raw:
            batch = self._batcher.push(message)
            if batch is not None:
                return batch
        raise StopIteration

    def close(self) -> None:
        self._raw.close()

    def __enter__(self) -> "BatchStream":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()


def _check_fingerprint(subscribe: Subscribe, token: ResumeToken) -> None:
    """Rejects a resume whose query shape no longer matches the checkpoint."""
    fingerprint = subscribe.fingerprint()
    if fingerprint != token.fingerprint:
        raise SchemaMismatch(expected=token.fingerprint, actual=fingerprint)


def _declare_cursor(conn: Any, subscribe_sql: str) -> None:
    """Opens the subscription cursor inside a transaction on ``conn``.

    A cursor must live inside a transaction, which also holds the read so the
    frontier does not advance out from under a slow reader mid-batch. On failure
    this rolls back and closes the connection, since ownership was already taken
    from the caller.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN")
        cursor.execute(f"DECLARE {_CURSOR} CURSOR FOR {subscribe_sql}")
    except Exception as exc:  # noqa: BLE001 - re-raised as a typed error
        try:
            cursor.execute("ROLLBACK")
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
        raise _classify(exc) from exc


def _spawn_drain(
    conn: Any, envelope: Envelope, bounded: bool, wrap: Wrap, capacity: int
) -> _Buffer:
    """Starts one drain thread over ``conn`` feeding a fresh bounded buffer."""
    buffer = _Buffer(capacity)
    thread = threading.Thread(
        target=_run_drain,
        args=(conn, envelope, bounded, wrap, buffer),
        daemon=True,
    )
    buffer.set_threads([thread])
    thread.start()
    return buffer


def _run_drain(
    conn: Any, envelope: Envelope, bounded: bool, wrap: Wrap, buffer: _Buffer
) -> None:
    """The background drain: fetch, decode, and enqueue until the subscription
    ends, the consumer stops it, or the buffer overflows.

    Owns ``conn``: on exit it rolls back (releasing the cursor so the server
    stops buffering for a dead reader) and closes the connection.
    """
    fetch_sql = f"FETCH ALL {_CURSOR} WITH (timeout = '1s')"
    cursor = conn.cursor()
    decoder: Optional[Decoder] = None
    try:
        while not buffer.should_stop():
            try:
                cursor.execute(fetch_sql)
                rows: List[Any] = cursor.fetchall()
            except Exception as exc:  # noqa: BLE001 - re-raised as a typed error
                buffer.record_error(_classify(exc))
                return

            if rows and decoder is None:
                decoder = Decoder([col.name for col in cursor.description], envelope)
            for row in rows:
                assert decoder is not None
                try:
                    message = decoder.decode(row)
                except SubscribeError as exc:
                    buffer.record_error(exc)
                    return
                # Fail loud rather than block the drain: a full buffer means the
                # consumer fell behind, and blocking would backpressure the
                # server into unbounded buffering.
                if not buffer.put(wrap(message)):
                    buffer.record_error(BufferOverflow())
                    return

            # A fetch that returned no rows on a bounded subscription means it
            # has drained. An unbounded one just idled; loop and fetch again.
            if not rows and bounded:
                return
    finally:
        try:
            cursor.execute("ROLLBACK")
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass


def _import_psycopg() -> Any:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise SubscribeError(
            "the psycopg package is required to connect; install "
            "materialize-subscribe with the [client] extra"
        ) from exc
    return psycopg


def is_compaction_horizon(message: str) -> bool:
    """Whether a server error message denotes the compaction horizon.

    Matched by text because Materialize does not yet expose a dedicated SQLSTATE
    for it. Both the current and the historical (pre-#34712) wording are handled
    so the classification survives server upgrades. This is the *only* place the
    SDK matches an error message.
    """
    return (
        "could not find a valid timestamp" in message
        or "not valid for all inputs" in message
    )


def classify_server_message(message: str) -> SubscribeError:
    """Maps a server error message to a typed error.

    Pure, so it is unit-tested and kept in lockstep with the Rust SDK.
    """
    if is_compaction_horizon(message):
        return CompactionHorizon()
    if "dropped" in message and "dependenc" in message:
        return DependencyDropped(message)
    return FatalError(message)


def _classify(exc: Exception) -> SubscribeError:
    """Classifies a driver exception into the SDK's taxonomy.

    A server error carries a SQLSTATE; anything without one is a connection-level
    failure and therefore retryable. This discriminator mirrors the Rust SDK, so
    the two agree on what is retryable.
    """
    if isinstance(exc, SubscribeError):
        return exc

    diag = getattr(exc, "diag", None)
    message = str((diag and diag.message_primary) or exc)

    if getattr(exc, "sqlstate", None) is not None:
        return classify_server_message(message)

    # No SQLSTATE means a connection-level failure: retryable.
    return TransientError(message)
