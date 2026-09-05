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

from materialize_subscribe import (
    CompactionHorizon,
    DependencyDropped,
    FatalError,
)
from materialize_subscribe.client import classify_server_message, is_compaction_horizon


def test_classifies_compaction_horizon_wordings() -> None:
    assert is_compaction_horizon("could not find a valid timestamp for the query")
    assert is_compaction_horizon("Timestamp (123) is not valid for all inputs")
    assert not is_compaction_horizon("some unrelated error")


def test_classifies_server_messages() -> None:
    # Mirrors the Rust SDK's classify_server_messages test so the two taxonomies
    # stay in lockstep.
    assert isinstance(
        classify_server_message("could not find a valid timestamp for the query"),
        CompactionHorizon,
    )
    assert isinstance(
        classify_server_message("dependency winning_bids was dropped"),
        DependencyDropped,
    )
    assert isinstance(
        classify_server_message('column "x" does not exist'),
        FatalError,
    )
