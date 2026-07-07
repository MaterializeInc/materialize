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

import pytest

from materialize_subscribe import Batcher, Data, Diff, Progress, ProtocolError


def insert(id_: str) -> Diff:
    return Diff(row=[id_], diff=1)


def data(timestamp: int, change: Diff) -> Data:
    return Data(timestamp=timestamp, change=change)


def test_buffers_until_the_timestamp_closes() -> None:
    b = Batcher("fp", with_snapshot=False)
    assert b.push(data(5, insert("a"))) is None
    assert b.push(data(5, insert("b"))) is None
    batch = b.push(Progress(frontier=6))
    assert batch is not None
    assert batch.updates == [insert("a"), insert("b")]
    assert batch.frontier == 6
    assert batch.resume_token.frontier == 6


def test_closes_a_timestamp_with_mixed_retractions_and_multiplicities() -> None:
    # A single timestamp can hold inserts, retractions, and multiplicities
    # beyond one; all of them close together and in arrival order.
    b = Batcher("fp", with_snapshot=False)
    insert3 = Diff(row=["a"], diff=3)
    retract = Diff(row=["b"], diff=-1)
    b.push(data(5, insert3))
    b.push(data(5, retract))
    b.push(data(5, insert("c")))
    batch = b.push(Progress(frontier=6))
    assert batch is not None
    assert batch.updates == [insert3, retract, insert("c")]


def test_data_at_the_frontier_stays_open() -> None:
    b = Batcher("fp", with_snapshot=False)
    b.push(data(5, insert("a")))
    b.push(data(6, insert("b")))
    batch = b.push(Progress(frontier=6))
    assert batch is not None and batch.updates == [insert("a")]
    batch = b.push(Progress(frontier=7))
    assert batch is not None and batch.updates == [insert("b")]


def test_empty_batches_still_advance_the_token() -> None:
    b = Batcher("fp", with_snapshot=False)
    batch = b.push(Progress(frontier=10))
    assert batch is not None
    assert batch.is_empty()
    assert batch.frontier == 10
    assert batch.resume_token.frontier == 10


def test_duplicate_frontier_emits_nothing() -> None:
    b = Batcher("fp", with_snapshot=False)
    assert b.push(Progress(frontier=10)) is not None
    assert b.push(Progress(frontier=10)) is None


def test_snapshot_batch_is_the_first_past_the_initial_frontier() -> None:
    b = Batcher("fp", with_snapshot=True)
    leading = b.push(Progress(frontier=100))
    assert leading is not None and not leading.is_snapshot and leading.is_empty()
    b.push(data(100, insert("snap1")))
    b.push(data(100, insert("snap2")))
    snap = b.push(Progress(frontier=101))
    assert snap is not None and snap.is_snapshot
    assert snap.updates == [insert("snap1"), insert("snap2")]
    b.push(data(101, insert("live")))
    live = b.push(Progress(frontier=102))
    assert live is not None and not live.is_snapshot


def test_resume_never_marks_a_snapshot() -> None:
    b = Batcher("fp", with_snapshot=False)
    b.push(Progress(frontier=100))
    b.push(data(100, insert("x")))
    batch = b.push(Progress(frontier=101))
    assert batch is not None and not batch.is_snapshot


def test_late_data_below_the_frontier_is_a_protocol_error() -> None:
    b = Batcher("fp", with_snapshot=False)
    b.push(Progress(frontier=10))
    with pytest.raises(ProtocolError):
        b.push(data(5, insert("late")))


def test_regressing_frontier_is_a_protocol_error() -> None:
    b = Batcher("fp", with_snapshot=False)
    b.push(Progress(frontier=10))
    with pytest.raises(ProtocolError):
        b.push(Progress(frontier=9))


def test_token_carries_the_fingerprint() -> None:
    b = Batcher("my-fingerprint", with_snapshot=False)
    batch = b.push(Progress(frontier=1))
    assert batch is not None
    assert batch.resume_token.fingerprint == "my-fingerprint"
