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

"""The consistency engine: buffer timestamped changes, then release everything
strictly below a frontier as one consolidated batch.

This one mechanism underlies both the single-view batcher and the multi-view
cohort. A single view is the one-member case: its release frontier is its own
progress frontier. A cohort of N views feeds N buffers and releases every buffer
below the *minimum* frontier across them, the latest timestamp closed for every
member at once.

Whatever the release frontier, the released set is *consolidated* before it
leaves the engine: within one batch the net effect per row is computed and rows
that cancel to nothing are dropped. A batch is therefore a clean net delta at its
frontier, not a replay of intra-window churn. Consolidation is scoped to a single
batch, never across batches, so a consumer that applies batches in order still
sees every intermediate settled state.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .envelope import (
    Change,
    Delete,
    Diff,
    Envelope,
    KeyViolation,
    Upsert,
    UpsertEnvelope,
)
from .errors import ProtocolError


class ReleaseBuffer:
    """A per-member buffer of timestamped changes awaiting release.

    Holds changes until a frontier closes them, enforcing the two ordering
    invariants the server guarantees: data never arrives below the frontier, and
    the frontier never regresses. A violation is a :class:`ProtocolError`.
    """

    def __init__(self, envelope: Envelope) -> None:
        self._envelope = envelope
        self._buffered: List[Tuple[int, Change]] = []
        self._frontier: Optional[int] = None

    @property
    def frontier(self) -> Optional[int]:
        """The highest frontier observed so far."""
        return self._frontier

    def pending(self) -> int:
        """The number of changes currently buffered, before consolidation. Used
        by the cohort to bound how much a laggard can make its peers buffer."""
        return len(self._buffered)

    def push_data(self, timestamp: int, change: Change) -> None:
        """Buffers a change. Raises if its timestamp is below the observed
        frontier, which the server promises never happens."""
        if self._frontier is not None and timestamp < self._frontier:
            raise ProtocolError(
                f"change at timestamp {timestamp} arrived after the frontier "
                f"advanced to {self._frontier}"
            )
        self._buffered.append((timestamp, change))

    def observe_progress(self, frontier: int) -> None:
        """Records a progress frontier. Raises if it regresses."""
        if self._frontier is not None and frontier < self._frontier:
            raise ProtocolError(
                f"frontier went backwards from {self._frontier} to {frontier}"
            )
        self._frontier = frontier

    def release_below(self, release_frontier: int) -> List[Change]:
        """Drains and consolidates every buffered change with a timestamp
        strictly below ``release_frontier``. Changes at or above it stay
        buffered."""
        closed: List[Change] = []
        still_open: List[Tuple[int, Change]] = []
        for timestamp, change in self._buffered:
            if timestamp < release_frontier:
                closed.append(change)
            else:
                still_open.append((timestamp, change))
        self._buffered = still_open
        return consolidate(self._envelope, closed)


def consolidate(envelope: Envelope, changes: List[Change]) -> List[Change]:
    """Collapses a batch of changes to its net effect, preserving
    first-appearance order.

    Diff changes sum their multiplicities per row; upsert changes keep only the
    last operation per key.
    """
    if isinstance(envelope, UpsertEnvelope):
        return _consolidate_upsert(changes)
    return _consolidate_diff(changes)


def _consolidate_diff(changes: List[Change]) -> List[Change]:
    """Sums signed multiplicities per row, dropping rows whose net diff is
    zero."""
    order: List[Any] = []
    totals: Dict[Any, List[Any]] = {}
    for change in changes:
        # A diff-envelope stream only ever carries `Diff`.
        if isinstance(change, Diff):
            key = _freeze(change.row)
            entry = totals.get(key)
            if entry is None:
                totals[key] = [change.row, change.diff]
                order.append(key)
            else:
                entry[1] += change.diff
    return [
        Diff(row=totals[key][0], diff=totals[key][1])
        for key in order
        if totals[key][1] != 0
    ]


def _consolidate_upsert(changes: List[Change]) -> List[Change]:
    """Keeps the last operation observed for each key, in first-seen key order.
    A window that upserts then deletes a key nets to the delete, and vice
    versa."""
    order: List[Any] = []
    last: Dict[Any, Change] = {}
    for change in changes:
        # An upsert-envelope stream never carries a bare diff.
        if isinstance(change, (Upsert, Delete, KeyViolation)):
            key = _freeze(change.key)
            if key not in last:
                order.append(key)
            last[key] = change
    return [last[key] for key in order]


def _freeze(value: Any) -> Any:
    """Turns a row (or nested value) into a hashable key for consolidation.

    Lists and dicts become tuples; scalars pass through. Rows only compare equal
    when their frozen forms do, which is exactly per-cell value equality.
    """
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze(v)) for k, v in value.items()))
    return value
