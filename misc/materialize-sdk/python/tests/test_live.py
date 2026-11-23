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

"""Live subscriptions against a running Materialize at MATERIALIZE_URL."""

import os
import uuid

import psycopg
import pytest
from materialize_sdk import LiveSubscription, ServerError

URL = os.environ.get("MATERIALIZE_URL")
pytestmark = pytest.mark.skipif(URL is None, reason="needs MATERIALIZE_URL")


@pytest.fixture
def table():
    name = f"py_live_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(URL, autocommit=True) as admin:
        admin.execute(f"CREATE TABLE {name} (id int)")
        admin.execute(f"INSERT INTO {name} VALUES (1), (2)")
        yield name, admin
        admin.execute(f"DROP TABLE {name}")


def ids(event):
    return sorted(update["row"][0] for update in event["updates"])


def test_a_table_streams_its_snapshot_then_its_changes(table):
    name, admin = table
    with LiveSubscription(URL, f"SELECT * FROM {name}") as live:
        snapshot = next(live)
        assert snapshot["event"] == "batch" and snapshot["snapshot"]
        assert ids(snapshot) == ["1", "2"]
        assert all(isinstance(u["timestamp"], int) for u in snapshot["updates"])
        assert live.as_of is not None

        admin.execute(f"INSERT INTO {name} VALUES (3)")
        changed = next(event for event in live if event["updates"])
        assert ids(changed) == ["3"]
        assert isinstance(changed["lower"], int)


def test_a_missing_table_fails_with_its_classification():
    with pytest.raises(ServerError) as failed:
        LiveSubscription(URL, "SELECT * FROM no_such_table_here")
    # Materialize reports an unknown name as an internal error, which before
    # the first progress row may come from the object changing.
    assert failed.value.sqlstate == "XX000"
    assert "unknown catalog item" in str(failed.value)
    assert failed.value.classification == {
        "action": "check_object",
        "otherwise": "fatal",
    }


def test_a_query_cannot_smuggle_in_a_second_statement():
    # Each statement parses, so only the protocol refuses the text.
    with pytest.raises(ServerError) as failed:
        LiveSubscription(URL, "SELECT 1); SELECT 2; SUBSCRIBE (SELECT 3")
    assert "multiple commands" in str(failed.value)


def test_a_refused_connection_fails_with_its_classification():
    with pytest.raises(ServerError) as failed:
        LiveSubscription("postgres://materialize@127.0.0.1:1/materialize", "SELECT 1")
    assert failed.value.classification == {"action": "kind", "kind": "transient"}
