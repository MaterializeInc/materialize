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

from materialize_subscribe import Delete, Diff, DiffEnvelope, ProtocolError, Upsert
from materialize_subscribe.engine import ReleaseBuffer, consolidate


def diff(id_: str, d: int) -> Diff:
    return Diff(row=[id_], diff=d)


def upsert(id_: str, value: str) -> Upsert:
    return Upsert(key=[id_], value=[value])


def test_diff_consolidation_sums_multiplicities_per_row() -> None:
    out = consolidate(DiffEnvelope(), [diff("a", 3), diff("b", -1), diff("a", 2)])
    assert out == [diff("a", 5), diff("b", -1)]


def test_diff_consolidation_drops_net_zero_rows() -> None:
    out = consolidate(DiffEnvelope(), [diff("a", 1), diff("b", 1), diff("a", -1)])
    assert out == [diff("b", 1)]


def test_diff_consolidation_preserves_first_seen_order() -> None:
    out = consolidate(DiffEnvelope(), [diff("c", 1), diff("a", 1), diff("b", 1)])
    assert out == [diff("c", 1), diff("a", 1), diff("b", 1)]


def test_upsert_consolidation_keeps_last_op_per_key() -> None:
    from materialize_subscribe import UpsertEnvelope

    env = UpsertEnvelope(key=["id"])
    out = consolidate(env, [upsert("1", "v1"), upsert("2", "x"), upsert("1", "v2")])
    assert out == [upsert("1", "v2"), upsert("2", "x")]


def test_upsert_consolidation_collapses_upsert_then_delete() -> None:
    from materialize_subscribe import UpsertEnvelope

    env = UpsertEnvelope(key=["id"])
    out = consolidate(env, [upsert("1", "v"), Delete(key=["1"])])
    assert out == [Delete(key=["1"])]


def test_release_holds_data_at_or_above_the_frontier() -> None:
    buf = ReleaseBuffer(DiffEnvelope())
    buf.push_data(5, diff("a", 1))
    buf.push_data(6, diff("b", 1))
    assert buf.release_below(6) == [diff("a", 1)]
    assert buf.release_below(7) == [diff("b", 1)]


def test_release_consolidates_across_the_window() -> None:
    buf = ReleaseBuffer(DiffEnvelope())
    buf.push_data(5, diff("a", 1))
    buf.push_data(6, diff("a", -1))
    buf.observe_progress(7)
    assert buf.release_below(7) == []


def test_data_below_the_frontier_is_rejected() -> None:
    buf = ReleaseBuffer(DiffEnvelope())
    buf.observe_progress(10)
    with pytest.raises(ProtocolError):
        buf.push_data(5, diff("late", 1))


def test_regressing_frontier_is_rejected() -> None:
    buf = ReleaseBuffer(DiffEnvelope())
    buf.observe_progress(10)
    with pytest.raises(ProtocolError):
        buf.observe_progress(9)
