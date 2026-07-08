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

from typing import List

import pytest

from materialize_subscribe import Diff, DiffEnvelope, ProtocolError
from materialize_subscribe.cohort import CohortEngine


def diff(id_: str, d: int) -> Diff:
    return Diff(row=[id_], diff=d)


def engine(names: List[str]) -> CohortEngine:
    specs = [(name, f"fp-{name}", DiffEnvelope()) for name in names]
    return CohortEngine(specs, with_snapshot=True)


def test_no_release_until_every_member_reports() -> None:
    e = engine(["a", "b"])
    assert e.push_progress(0, 10) is None
    moment = e.push_progress(1, 5)
    assert moment is not None and moment.frontier == 5


def test_releases_at_the_minimum_frontier() -> None:
    e = engine(["a", "b"])
    e.push_data(0, 3, diff("a3", 1))
    e.push_data(0, 7, diff("a7", 1))
    e.push_data(1, 4, diff("b4", 1))
    e.push_progress(0, 10)
    moment = e.push_progress(1, 6)
    assert moment is not None and moment.frontier == 6
    assert moment.views[0].updates == [diff("a3", 1)]
    assert moment.views[1].updates == [diff("b4", 1)]


def test_laggard_holds_the_joint_moment() -> None:
    e = engine(["fast", "slow"])
    e.push_data(0, 5, diff("x", 1))
    assert e.push_progress(0, 100) is None
    moment = e.push_progress(1, 6)
    assert moment is not None and moment.frontier == 6
    assert moment.views[0].updates == [diff("x", 1)]
    assert moment.views[1].updates == []


def test_moment_carries_a_token_at_the_joint_frontier() -> None:
    e = engine(["a", "b"])
    e.push_progress(0, 8)
    moment = e.push_progress(1, 5)
    assert moment is not None
    assert moment.resume_token.frontier == 5
    assert moment.resume_token.members == ["fp-a", "fp-b"]


def test_per_view_changes_are_consolidated() -> None:
    e = engine(["a", "b"])
    e.push_data(0, 3, diff("x", 1))
    e.push_data(0, 4, diff("x", -1))
    e.push_progress(0, 10)
    moment = e.push_progress(1, 6)
    assert moment is not None
    assert moment.views[0].updates == []


def test_snapshot_completes_once_the_last_member_passes_its_as_of() -> None:
    e = engine(["a", "b"])
    assert e.push_progress(0, 5) is None
    leading = e.push_progress(1, 8)
    assert leading is not None and leading.frontier == 5 and not leading.is_snapshot
    e.push_progress(0, 9)
    done = e.push_progress(1, 9)
    assert done is not None and done.frontier == 9 and done.is_snapshot
    e.push_progress(0, 11)
    live = e.push_progress(1, 11)
    assert live is not None and not live.is_snapshot


def test_single_member_cohort_matches_a_batcher() -> None:
    e = engine(["only"])
    leading = e.push_progress(0, 100)
    assert leading is not None and leading.frontier == 100 and not leading.is_snapshot
    e.push_data(0, 100, diff("snap", 1))
    snap = e.push_progress(0, 101)
    assert snap is not None and snap.is_snapshot
    assert snap.views[0].updates == [diff("snap", 1)]


def test_protocol_errors_surface_from_members() -> None:
    e = engine(["a", "b"])
    e.push_progress(0, 10)
    with pytest.raises(ProtocolError):
        e.push_data(0, 5, diff("late", 1))
