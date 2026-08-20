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

from dbt.adapters.materialize.connections import (
    DEFAULT_SESSION_PARAMETERS,
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


def test_backslashes_are_escaped():
    options_string = _build_options_string({"application_name": "my\\app"}, None)

    assert "--application_name=my\\\\app" in options_string


def test_backslash_before_space_is_escaped_in_order():
    # Backslashes must be escaped before spaces: the input backslash becomes
    # two, and the space gains its own new one.
    options_string = _build_options_string({"application_name": "my\\ app"}, None)

    assert "--application_name=my\\\\\\ app" in options_string


def test_non_string_values_are_stringified():
    options = parse_options(
        _build_options_string({"welcome_message": True, "statement_timeout": 5}, None)
    )

    assert options["welcome_message"] == "True"
    assert options["statement_timeout"] == "5"
