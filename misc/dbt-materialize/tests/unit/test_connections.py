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

import psycopg2
import pytest
from dbt.adapters.contracts.connection import Connection
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.materialize.connections import (
    DEFAULT_SESSION_PARAMETERS,
    NOT_MATERIALIZE_ERROR,
    MaterializeConnectionManager,
    MaterializeCredentials,
    _build_options_string,
)


def parse_options(options_string):
    """Turn a libpq options string back into a dictionary, undoing the escaping
    of spaces."""
    parsed = {}
    for part in options_string.replace("\\ ", "\x00").split(" "):
        key, _, value = part.partition("=")
        parsed[key.lstrip("-")] = value.replace("\x00", " ")
    return parsed


def test_defaults_are_applied():
    options = parse_options(_build_options_string(None, None))

    assert options == DEFAULT_SESSION_PARAMETERS


def test_user_options_win_over_defaults():
    options = parse_options(
        _build_options_string({"welcome_message": "on", "cluster": "my_cluster"}, None)
    )

    assert options["welcome_message"] == "on"
    assert options["cluster"] == "my_cluster"
    # Defaults the user did not override are still there.
    assert options["auto_route_catalog_queries"] == "on"


def test_search_path_is_appended():
    options = parse_options(_build_options_string(None, "my_schema"))

    assert options["search_path"] == "my_schema"


def test_values_with_spaces_are_escaped():
    options_string = _build_options_string({"application_name": "my app"}, None)

    assert "--application_name=my\\ app" in options_string
    assert parse_options(options_string)["application_name"] == "my app"


def test_credentials_type():
    credentials = MaterializeCredentials(
        host="localhost",
        port=6875,
        database="materialize",
        schema="public",
        user="materialize",
        password="",
    )

    assert credentials.type == "materialize"
    assert "cluster" in credentials._connection_keys()
    assert "options" in credentials._connection_keys()


def test_non_string_values_are_stringified():
    options = parse_options(
        _build_options_string({"welcome_message": True, "statement_timeout": 5}, None)
    )

    assert options["welcome_message"] == "True"
    assert options["statement_timeout"] == "5"


def open_with_connect_error(monkeypatch, message):
    calls = []

    def failing_connect(**kwargs):
        calls.append(kwargs)
        raise psycopg2.OperationalError(message)

    monkeypatch.setattr(psycopg2, "connect", failing_connect)
    monkeypatch.setattr(
        "dbt.adapters.base.connections.sleep", lambda _: None, raising=False
    )
    credentials = MaterializeCredentials(
        host="localhost",
        port=5432,
        database="postgres",
        schema="public",
        user="postgres",
        password="",
        retries=3,
    )
    connection = Connection(type="materialize", name="test", credentials=credentials)
    with pytest.raises(FailedToConnectError) as exc_info:
        MaterializeConnectionManager.open(connection)
    return str(exc_info.value), calls


def test_postgres_rejecting_session_parameters_fails_fast(monkeypatch):
    message, calls = open_with_connect_error(
        monkeypatch,
        'FATAL:  unrecognized configuration parameter "auto_route_catalog_queries"',
    )

    assert NOT_MATERIALIZE_ERROR in message
    assert "auto_route_catalog_queries" in message
    assert len(calls) == 1, "a rejected session parameter must not be retried"


def test_other_connection_errors_are_retried_unchanged(monkeypatch):
    message, calls = open_with_connect_error(
        monkeypatch, "could not connect to server: Connection refused"
    )

    assert NOT_MATERIALIZE_ERROR not in message
    assert len(calls) == 4
