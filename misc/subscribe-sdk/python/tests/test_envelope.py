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

from materialize_subscribe import (
    Data,
    Decoder,
    Delete,
    Diff,
    DiffEnvelope,
    KeyViolation,
    Progress,
    ProtocolError,
    Upsert,
    UpsertEnvelope,
)


def test_diff_decoder_reads_progress() -> None:
    decoder = Decoder(
        ["mz_timestamp", "mz_progressed", "mz_diff", "id", "name"], DiffEnvelope()
    )
    # Text encoding (mz_progressed = "t").
    assert decoder.decode(["100", "t", None, None, None]) == Progress(frontier=100)
    # Native psycopg values (mz_progressed = True) decode the same way.
    assert decoder.decode([100, True, None, None, None]) == Progress(frontier=100)


def test_diff_decoder_preserves_multiplicity_and_sign() -> None:
    decoder = Decoder(["mz_timestamp", "mz_progressed", "mz_diff", "id"], DiffEnvelope())
    assert decoder.decode(["7", "f", "3", "42"]) == Data(
        timestamp=7, change=Diff(row=["42"], diff=3)
    )
    assert decoder.decode(["7", "f", "-2", "42"]) == Data(
        timestamp=7, change=Diff(row=["42"], diff=-2)
    )


def test_upsert_decoder_handles_all_states() -> None:
    columns = ["id", "mz_state", "mz_timestamp", "name", "email", "mz_progressed"]
    decoder = Decoder(columns, UpsertEnvelope(key=["id"]))

    assert decoder.decode(["1", "upsert", "50", "alice", "a@x", "f"]) == Data(
        timestamp=50, change=Upsert(key=["1"], value=["alice", "a@x"])
    )
    assert decoder.decode(["1", "delete", "51", None, None, "f"]) == Data(
        timestamp=51, change=Delete(key=["1"])
    )
    assert decoder.decode(["1", "key_violation", "52", None, None, "f"]) == Data(
        timestamp=52, change=KeyViolation(key=["1"])
    )


def test_upsert_decoder_reads_progress() -> None:
    columns = ["id", "mz_state", "mz_timestamp", "name", "mz_progressed"]
    decoder = Decoder(columns, UpsertEnvelope(key=["id"]))
    assert decoder.decode([None, None, "200", None, "t"]) == Progress(frontier=200)


def test_multi_column_key_is_projected_in_order() -> None:
    columns = ["region", "id", "mz_state", "mz_timestamp", "total", "mz_progressed"]
    decoder = Decoder(columns, UpsertEnvelope(key=["region", "id"]))
    assert decoder.decode(["us", "9", "upsert", "5", "100", "f"]) == Data(
        timestamp=5, change=Upsert(key=["us", "9"], value=["100"])
    )


def test_missing_metadata_column_is_rejected() -> None:
    with pytest.raises(ProtocolError):
        Decoder(["mz_timestamp", "mz_diff", "id"], DiffEnvelope())


def test_unknown_key_column_is_rejected() -> None:
    columns = ["id", "mz_state", "mz_timestamp", "mz_progressed"]
    with pytest.raises(ProtocolError):
        Decoder(columns, UpsertEnvelope(key=["nonexistent"]))


def test_unexpected_state_is_rejected() -> None:
    columns = ["id", "mz_state", "mz_timestamp", "mz_progressed"]
    decoder = Decoder(columns, UpsertEnvelope(key=["id"]))
    with pytest.raises(ProtocolError):
        decoder.decode(["1", "bogus", "5", "f"])
