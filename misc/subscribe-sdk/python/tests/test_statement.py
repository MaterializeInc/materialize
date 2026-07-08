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

from materialize_subscribe import ResumeToken, Subscribe


def test_initial_takes_snapshot_and_omits_as_of() -> None:
    assert (
        Subscribe.object("orders").to_sql_initial()
        == "SUBSCRIBE orders WITH (PROGRESS, SNAPSHOT = true)"
    )


def test_resume_disables_snapshot_and_sets_as_of_to_frontier_minus_one() -> None:
    sub = Subscribe.object("orders")
    token = ResumeToken(frontier=100, fingerprint=sub.fingerprint())
    assert (
        sub.to_sql_resume(token)
        == "SUBSCRIBE orders WITH (PROGRESS, SNAPSHOT = false) AS OF 99"
    )


def test_query_relation_is_parenthesized() -> None:
    assert (
        Subscribe.query("SELECT id FROM orders WHERE paid").to_sql_initial()
        == "SUBSCRIBE (SELECT id FROM orders WHERE paid) WITH (PROGRESS, SNAPSHOT = true)"
    )


def test_upsert_envelope_precedes_with_clause() -> None:
    assert (
        Subscribe.object("orders").envelope_upsert(["id"]).to_sql_initial()
        == 'SUBSCRIBE orders ENVELOPE UPSERT (KEY ("id")) WITH (PROGRESS, SNAPSHOT = true)'
    )


def test_multi_column_key_and_up_to() -> None:
    sql = (
        Subscribe.object("sales")
        .envelope_upsert(["region", "id"])
        .up_to(500)
        .to_sql_initial()
    )
    assert sql == (
        'SUBSCRIBE sales ENVELOPE UPSERT (KEY ("region", "id")) '
        "WITH (PROGRESS, SNAPSHOT = true) UP TO 500"
    )


def test_key_identifiers_are_quote_escaped() -> None:
    sql = Subscribe.object("t").envelope_upsert(['weird"name']).to_sql_initial()
    assert 'KEY ("weird""name")' in sql


def test_fingerprint_is_stable_and_distinguishes_shape() -> None:
    a = Subscribe.object("orders")
    assert a.fingerprint() == Subscribe.object("orders").fingerprint()
    assert a.fingerprint() != Subscribe.object("other").fingerprint()
    assert (
        a.fingerprint()
        != Subscribe.object("orders").envelope_upsert(["id"]).fingerprint()
    )
    assert (
        Subscribe.object("orders").envelope_upsert(["id"]).fingerprint()
        != Subscribe.object("orders").envelope_upsert(["id", "region"]).fingerprint()
    )
    # UP TO does not change the shape, so it does not change the fingerprint.
    assert a.fingerprint() == Subscribe.object("orders").up_to(9).fingerprint()


def test_fingerprint_matches_cross_language_vectors() -> None:
    # These exact values are also asserted by the Rust SDK, so both compute the
    # same fingerprint (and therefore interchangeable tokens) for the same
    # subscription shape.
    assert Subscribe.object("orders").fingerprint() == "f44d7c35ad6f569c"
    assert (
        Subscribe.object("orders").envelope_upsert(["id"]).fingerprint()
        == "a9392428ac0ed8c9"
    )
