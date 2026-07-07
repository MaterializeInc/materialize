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

"""The progress-driven batcher: the state machine that turns a stream of
changes and progress markers into *consistent, closed* batches.

This is the part every hand-rolled subscribe consumer gets wrong. The rule it
enforces:

* Buffer changes as they arrive.
* When a progress marker advances the frontier to ``F``, every change with a
  timestamp strictly below ``F`` is now final. Emit exactly those as one batch,
  tagged with frontier ``F`` and a resume token for ``F``.
* Never emit a change before its timestamp is closed, and never emit the same
  timestamp twice.

A consumer that applies each emitted batch and persists its token atomically
gets exactly-once *state*: after any crash, resuming from the last token neither
drops nor duplicates data.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from .envelope import Change, Data, Progress, StreamMessage
from .errors import ProtocolError
from .token import ResumeToken


@dataclass(frozen=True)
class ConsistentBatch:
    """A batch of changes for a contiguous, now-closed range of timestamps.

    Every change in ``updates`` has a timestamp strictly below ``frontier``, and
    no future batch will contain a timestamp below ``frontier``. ``resume_token``
    checkpoints exactly this position.
    """

    updates: List[Change]
    frontier: int
    resume_token: ResumeToken
    is_snapshot: bool

    def is_empty(self) -> bool:
        """Whether the batch carries no changes.

        Empty batches still advance the frontier and carry a fresh token, which
        lets a consumer keep its checkpoint moving during idle periods so it
        does not age out of the source's retained history.
        """
        return not self.updates


class Batcher:
    """Accumulates stream messages and emits :class:`ConsistentBatch` values as
    timestamps close.

    Feed every decoded message to :meth:`push`. It returns a batch when a
    progress marker closes one or more timestamps, and ``None`` otherwise.
    """

    def __init__(self, fingerprint: str, with_snapshot: bool) -> None:
        self._fingerprint = fingerprint
        self._with_snapshot = with_snapshot
        self._buffered: List[Tuple[int, Change]] = []
        self._last_frontier: Optional[int] = None
        self._first_frontier: Optional[int] = None
        self._snapshot_emitted = False

    def push(self, message: StreamMessage) -> Optional[ConsistentBatch]:
        """Feeds one decoded message. Returns a batch when the frontier
        advances."""
        if isinstance(message, Data):
            if self._last_frontier is not None and message.timestamp < self._last_frontier:
                raise ProtocolError(
                    f"change at timestamp {message.timestamp} arrived after the "
                    f"frontier advanced to {self._last_frontier}"
                )
            self._buffered.append((message.timestamp, message.change))
            return None

        if isinstance(message, Progress):
            return self._advance(message.frontier)

        raise ProtocolError(f"unexpected stream message: {message!r}")

    def _advance(self, frontier: int) -> Optional[ConsistentBatch]:
        if self._last_frontier is not None:
            if frontier < self._last_frontier:
                raise ProtocolError(
                    f"frontier went backwards from {self._last_frontier} to {frontier}"
                )
            if frontier == self._last_frontier:
                # No advance: nothing new is closed.
                return None

        if self._first_frontier is None:
            self._first_frontier = frontier

        # Everything strictly below the new frontier is now final. Retain the
        # rest (timestamps >= frontier, which may include changes at exactly this
        # frontier that are not yet closed).
        closed: List[Change] = []
        still_open: List[Tuple[int, Change]] = []
        for timestamp, change in self._buffered:
            if timestamp < frontier:
                closed.append(change)
            else:
                still_open.append((timestamp, change))
        self._buffered = still_open

        # The snapshot is the data closed once we advance past the first frontier
        # (the effective AS OF). Mark the first such batch.
        #
        # NOTE: this relies on the server emitting a leading Progress(as_of)
        # before any snapshot rows (it does, unconditionally, when PROGRESS is
        # set). That progress sets first_frontier; the snapshot rows sit at
        # as_of and are closed by the next, higher progress. A transport that
        # delivered snapshot data before any progress would mistag it.
        is_snapshot = (
            self._with_snapshot
            and not self._snapshot_emitted
            and frontier != self._first_frontier
        )
        if is_snapshot:
            self._snapshot_emitted = True

        self._last_frontier = frontier

        return ConsistentBatch(
            updates=closed,
            frontier=frontier,
            resume_token=ResumeToken(frontier=frontier, fingerprint=self._fingerprint),
            is_snapshot=is_snapshot,
        )
