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

"""Multi-view consistency: subscribe to several views at once and observe them
at one shared, consistent moment.

Materialize gives every object a place on one logical timeline, so an
``mz_timestamp`` means the same instant across views. A cohort exploits that: it
runs one ``SUBSCRIBE`` per view (each on its own connection) and releases every
view only up to the *minimum* closed frontier across all of them. That minimum
is the latest instant final for every view at once, so the changes it hands back
form a genuine cross-view snapshot, never a mix of a newer view with a staler
one.

This is the same engine as the single-view :class:`Batcher`, generalized. A
single view is the cohort of one: its minimum frontier is its only frontier. The
cost of the guarantee is that a laggard member holds the joint moment back, and
the leading members' changes buffer in memory until it catches up.

Dynamic membership (adding, dropping, merging, or splitting members of a live
cohort) is out of scope here. A cohort's membership is fixed for its life.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from .client import (
    _JOIN_SECONDS,
    DEFAULT_BUFFER_CAPACITY,
    Wrap,
    _Buffer,
    _classify,
    _declare_cursor,
    _import_psycopg,
    _run_drain,
)
from .engine import ReleaseBuffer
from .envelope import Change, Data, Envelope
from .errors import CohortLagExceeded, ProtocolError, SchemaMismatch
from .statement import Subscribe
from .token import CohortToken

# Default cohort lag budget: the most changes the cohort buffers across all
# members while waiting for the slowest to advance the joint frontier. Exceeding
# it raises :class:`CohortLagExceeded` rather than growing memory without bound.
#
# TODO: make this configurable per cohort once there are workloads to size it.
DEFAULT_COHORT_LAG_BUDGET = 1 << 20


@dataclass(frozen=True)
class ViewChanges:
    """The changes one view contributes to a :class:`CohortMoment`."""

    name: str
    updates: List[Change]


@dataclass(frozen=True)
class CohortMoment:
    """A consistent moment across every view in a cohort.

    Every view's ``updates`` are closed at the same ``frontier``, so applying the
    whole moment moves a downstream store from one cross-view-consistent state to
    the next. Persist ``resume_token`` atomically with the effects for
    exactly-once state across the cohort.
    """

    views: List[ViewChanges]
    frontier: int
    resume_token: CohortToken
    is_snapshot: bool

    def is_empty(self) -> bool:
        """Whether no view carries any change. Empty moments still advance the
        frontier and carry a fresh token."""
        return all(not view.updates for view in self.views)


class _CohortMember:
    """One member's buffered state within a cohort."""

    def __init__(self, name: str, fingerprint: str, envelope: Envelope) -> None:
        self.name = name
        self.fingerprint = fingerprint
        self.buffer = ReleaseBuffer(envelope)
        self.first_frontier: Optional[int] = None


class CohortEngine:
    """The pure state machine behind a cohort: N per-member buffers released at
    their shared minimum frontier.

    Kept separate from the transport so it can be exhaustively unit-tested
    without a server.
    """

    def __init__(
        self,
        members: List[Tuple[str, str, Envelope]],
        with_snapshot: bool,
        lag_budget: int = DEFAULT_COHORT_LAG_BUDGET,
    ) -> None:
        self._members = [
            _CohortMember(name, fingerprint, envelope)
            for name, fingerprint, envelope in members
        ]
        self._with_snapshot = with_snapshot
        self._last_release: Optional[int] = None
        self._snapshot_complete = False
        self._lag_budget = lag_budget

    def push_data(self, idx: int, timestamp: int, change: Change) -> None:
        """Buffers a change from member ``idx``.

        Raises :class:`CohortLagExceeded` if the cohort is already holding
        ``lag_budget`` changes. This is the laggard backstop: a member that
        stalls pins the joint frontier, so its peers' changes would otherwise
        buffer without bound. The client fails loud instead.
        """
        buffered = sum(m.buffer.pending() for m in self._members)
        if buffered >= self._lag_budget:
            raise CohortLagExceeded(buffered=buffered, limit=self._lag_budget)
        self._members[idx].buffer.push_data(timestamp, change)

    def push_progress(self, idx: int, frontier: int) -> Optional[CohortMoment]:
        """Records progress from member ``idx``, and emits a joint moment if the
        minimum frontier across all members advanced."""
        member = self._members[idx]
        member.buffer.observe_progress(frontier)
        if member.first_frontier is None:
            member.first_frontier = frontier

        # The joint frontier is the minimum across members, defined only once
        # every member has reported at least one progress. Until then a silent
        # member could still emit at an earlier timestamp, so nothing is safe to
        # release.
        frontiers = [m.buffer.frontier for m in self._members]
        if any(f is None for f in frontiers):
            return None
        release = min(f for f in frontiers if f is not None)

        # Emit only when the joint frontier actually advances.
        if self._last_release is not None and release <= self._last_release:
            return None

        # Every member has reported, so every first frontier is set. The joint
        # snapshot is complete once the release frontier passes the last member's
        # AS OF.
        max_first = max(
            m.first_frontier for m in self._members if m.first_frontier is not None
        )

        views = [
            ViewChanges(name=m.name, updates=m.buffer.release_below(release))
            for m in self._members
        ]

        was_complete = self._snapshot_complete
        if not was_complete and release > max_first:
            self._snapshot_complete = True
        is_snapshot = (
            self._with_snapshot and not was_complete and self._snapshot_complete
        )

        self._last_release = release
        fingerprints = [m.fingerprint for m in self._members]

        return CohortMoment(
            views=views,
            frontier=release,
            resume_token=CohortToken(frontier=release, members=fingerprints),
            is_snapshot=is_snapshot,
        )


class Cohort:
    """A live cohort of subscriptions, yielding consistent cross-view moments.

    Each member runs its own ``SUBSCRIBE`` on its own connection, so a cohort of
    N views holds N connections. All members drain into one shared bounded
    buffer, so if the consumer falls behind, the cohort fails loud with
    :class:`BufferOverflow` rather than letting the server buffer.

    Iterate to pull :class:`CohortMoment` values. Usable as a context manager,
    which stops every member on exit.
    """

    def __init__(self, buffer: _Buffer, engine: CohortEngine) -> None:
        self._buffer = buffer
        self._engine = engine

    @classmethod
    def connect(cls, conninfo: str, members: List[Tuple[str, Subscribe]]) -> "Cohort":
        """Starts a fresh cohort over ``members``, each a ``(name, subscription)``
        pair. All members connect with ``conninfo`` and take their initial
        snapshot. Names identify a member's changes in each moment, and need not
        match the object name."""
        return cls._start(conninfo, members, None)

    @classmethod
    def resume(
        cls,
        conninfo: str,
        members: List[Tuple[str, Subscribe]],
        token: CohortToken,
    ) -> "Cohort":
        """Resumes a cohort from ``token``, reconstructing the exact joint cut.

        Raises :class:`SchemaMismatch` if the members no longer match the token
        (different shapes or order).
        """
        return cls._start(conninfo, members, token)

    @classmethod
    def _start(
        cls,
        conninfo: str,
        members: List[Tuple[str, Subscribe]],
        token: Optional[CohortToken],
    ) -> "Cohort":
        if not members:
            raise ProtocolError("a cohort needs at least one member")
        if token is not None:
            fingerprints = [sub.fingerprint() for _, sub in members]
            if fingerprints != token.members:
                raise SchemaMismatch(
                    expected=",".join(token.members),
                    actual=",".join(fingerprints),
                )

        psycopg = _import_psycopg()
        buffer = _Buffer(DEFAULT_BUFFER_CAPACITY)
        threads: List[threading.Thread] = []
        specs: List[Tuple[str, str, Envelope]] = []
        try:
            for idx, (name, sub) in enumerate(members):
                sql = (
                    sub.to_sql_initial()
                    if token is None
                    else sub.to_sql_resume_at(token.as_of())
                )
                conn = psycopg.connect(conninfo, autocommit=True)
                _declare_cursor(conn, sql)
                thread = threading.Thread(
                    target=_run_drain,
                    args=(conn, sub.envelope, sub.is_bounded(), _tagger(idx), buffer),
                    daemon=True,
                )
                thread.start()
                threads.append(thread)
                specs.append((name, sub.fingerprint(), sub.envelope))
        except Exception as exc:  # noqa: BLE001 - re-raised as a typed error
            buffer.request_stop()
            for thread in threads:
                thread.join(timeout=_JOIN_SECONDS)
            raise _classify(exc) from exc

        buffer.set_threads(threads)
        return cls(buffer, CohortEngine(specs, with_snapshot=token is None))

    def __iter__(self) -> "Cohort":
        return self

    def __next__(self) -> CohortMoment:
        while True:
            idx, message = self._buffer.recv()
            if isinstance(message, Data):
                self._engine.push_data(idx, message.timestamp, message.change)
            else:
                moment = self._engine.push_progress(idx, message.frontier)
                if moment is not None:
                    return moment

    def close(self) -> None:
        self._buffer.close()

    def __enter__(self) -> "Cohort":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()


def _tagger(index: int) -> Wrap:
    """A wrapper that tags every message with its member index."""
    return lambda message: (index, message)
