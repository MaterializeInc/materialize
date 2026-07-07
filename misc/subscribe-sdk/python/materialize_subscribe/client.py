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
hands every row to the tested :class:`Decoder` and :class:`Batcher`. All
protocol judgement lives in those, not here.

``psycopg`` (v3) is imported lazily so the protocol core can be used and tested
without the driver installed.
"""

from __future__ import annotations

from typing import Any, Iterator, List, Optional

from .batch import Batcher, ConsistentBatch
from .envelope import Decoder
from .errors import (
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


class SubscribeClient:
    """A connection to Materialize for consuming subscriptions.

    One connection backs one subscription at a time. Transaction-mode connection
    poolers (such as PgBouncer) break the ``DECLARE``/``FETCH`` cursor and are
    not supported.

    Usable as a context manager, which closes the connection on exit.
    """

    def __init__(self, connection: Any) -> None:
        self._conn = connection

    @classmethod
    def connect(cls, conninfo: str) -> "SubscribeClient":
        """Connects to Materialize using a libpq-style connection string."""
        psycopg = _import_psycopg()
        try:
            connection = psycopg.connect(conninfo, autocommit=True)
        except psycopg.Error as exc:
            raise _classify(exc) from exc
        return cls(connection)

    def subscribe(self, subscribe: Subscribe) -> "BatchStream":
        """Starts a new subscription, taking the initial snapshot."""
        return self._open(subscribe, subscribe.to_sql_initial(), with_snapshot=True)

    def resume(self, subscribe: Subscribe, token: ResumeToken) -> "BatchStream":
        """Resumes a subscription from ``token``, skipping the snapshot.

        Raises :class:`SchemaMismatch` if ``subscribe`` does not match the query
        the token was taken against.
        """
        fingerprint = subscribe.fingerprint()
        if fingerprint != token.fingerprint:
            raise SchemaMismatch(expected=token.fingerprint, actual=fingerprint)
        return self._open(subscribe, subscribe.to_sql_resume(token), with_snapshot=False)

    def close(self) -> None:
        """Closes the underlying connection."""
        self._conn.close()

    def __enter__(self) -> "SubscribeClient":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def _open(
        self, subscribe: Subscribe, subscribe_sql: str, with_snapshot: bool
    ) -> "BatchStream":
        cursor = self._conn.cursor()
        try:
            # A cursor must live inside a transaction, which also holds the read
            # so the frontier does not advance out from under a slow reader.
            #
            # One subscription owns its connection for its lifetime. Roll back on
            # failure so the connection is not left in an open transaction.
            cursor.execute("BEGIN")
            cursor.execute(f"DECLARE {_CURSOR} CURSOR FOR {subscribe_sql}")
        except Exception as exc:  # noqa: BLE001 - re-raised as a typed error
            try:
                cursor.execute("ROLLBACK")
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
            raise _classify(exc) from exc
        return BatchStream(
            cursor=cursor,
            batcher=Batcher(subscribe.fingerprint(), with_snapshot),
            envelope=subscribe.envelope,
            bounded=subscribe.is_bounded(),
        )


class BatchStream:
    """An iterator over the :class:`ConsistentBatch` values of one subscription.

    Iterate to pull batches. Each is complete: a consumer that applies it and
    persists its ``resume_token`` atomically achieves exactly-once state. A
    bounded (``UP TO``) subscription ends iteration when it drains; an unbounded
    one blocks until the next batch is ready.
    """

    def __init__(self, cursor: Any, batcher: Batcher, envelope: Any, bounded: bool) -> None:
        self._cursor = cursor
        self._batcher = batcher
        self._envelope = envelope
        self._bounded = bounded
        self._fetch_sql = f"FETCH ALL {_CURSOR} WITH (timeout = '1s')"
        self._decoder: Optional[Decoder] = None

    def __iter__(self) -> "Iterator[ConsistentBatch]":
        return self

    def __next__(self) -> ConsistentBatch:
        while True:
            try:
                self._cursor.execute(self._fetch_sql)
                rows: List[Any] = self._cursor.fetchall()
            except Exception as exc:  # noqa: BLE001 - re-raised as a typed error
                raise _classify(exc) from exc

            # A bounded subscription's cursor eventually returns no rows; an
            # unbounded one keeps advancing, so an empty fetch there just means
            # "idle, poll again".
            if not rows and self._bounded:
                raise StopIteration

            if rows and self._decoder is None:
                columns = [col.name for col in self._cursor.description]
                self._decoder = Decoder(columns, self._envelope)

            for row in rows:
                assert self._decoder is not None
                batch = self._batcher.push(self._decoder.decode(row))
                if batch is not None:
                    return batch


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
