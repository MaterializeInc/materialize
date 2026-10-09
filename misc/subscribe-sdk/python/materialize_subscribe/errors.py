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

"""The error taxonomy consumers match on to decide how to recover."""

from __future__ import annotations


class SubscribeError(Exception):
    """Base class for every error raised by the SDK.

    The ``is_retryable`` property captures the common "reconnect and resume"
    versus "give up" split so callers rarely need to catch every subclass.
    """

    #: Whether reconnecting and resuming from the last token is a sensible
    #: automatic response. Overridden to ``True`` only by :class:`TransientError`.
    is_retryable: bool = False


class CompactionHorizon(SubscribeError):
    """The resume point is older than the object's retained history.

    The gap cannot be filled. Recover by re-snapshotting or by accepting a gap,
    per the caller's policy, or widen ``RETAIN HISTORY`` on the subscribed
    object to make a larger resume window available.
    """

    def __init__(self) -> None:
        super().__init__(
            "resume point is older than the subscribed object's retained "
            "history (compaction horizon); re-snapshot or widen RETAIN HISTORY"
        )


class DependencyDropped(SubscribeError):
    """A dependency of the subscribed object (or its cluster) was dropped."""


class SchemaMismatch(SubscribeError):
    """A resumed query does not match the one the checkpoint was taken against.

    Resuming would silently mix incompatible results, so it is refused.
    """

    def __init__(self, expected: str, actual: str) -> None:
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"resumed query does not match the checkpoint "
            f"(expected {expected}, got {actual})"
        )


class TransientError(SubscribeError):
    """A transient connection or protocol interruption; retryable."""

    is_retryable = True


class BufferOverflow(SubscribeError):
    """The consumer fell too far behind and the client-side buffer filled.

    The client drains the server continuously so the server never buffers
    unboundedly, which means a slow consumer must fail here rather than push
    that cost onto Materialize. Recover by consuming faster, widening the
    buffer, or resuming from the last token once caught up.
    """

    def __init__(self) -> None:
        super().__init__(
            "client-side buffer overflowed: the consumer fell behind the "
            "subscription. Consume faster or widen the buffer, then resume "
            "from the last token"
        )


class CohortLagExceeded(SubscribeError):
    """A cohort member lagged so far behind that the changes buffered for its
    peers, waiting on it, exceeded the lag budget.

    Consistency requires holding a leading member's changes until the slowest
    catches up, so a stalled member would otherwise grow memory without bound.
    Recover by investigating the slow view, widening the budget, or resuming
    from the last token once it recovers.
    """

    def __init__(self, buffered: int, limit: int) -> None:
        self.buffered = buffered
        self.limit = limit
        super().__init__(
            f"cohort lag budget exceeded: {buffered} changes buffered waiting "
            f"on a lagging member (budget {limit}). Investigate the slow view "
            f"or widen the budget"
        )


class InvalidToken(SubscribeError):
    """A resume token could not be decoded."""


class ProtocolError(SubscribeError):
    """The server sent a stream that violates the subscribe protocol.

    Indicates a bug, not a recoverable condition.
    """


class FatalError(SubscribeError):
    """A non-retryable server error.

    An auth failure, a malformed query, or a dataflow error that poisons the
    stream (Materialize repeats such an error until the data or view changes,
    so retrying will not help).
    """
