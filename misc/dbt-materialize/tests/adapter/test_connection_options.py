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
from dbt.adapters.materialize.__version__ import version as __version__


class TestConnectionOptionsOverride:
    """Verify that `options` in the dbt profile reaches the server."""

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "materialize",
            "threads": 1,
            "host": "{{ env_var('DBT_HOST', 'localhost') }}",
            "user": "materialize",
            "pass": "password",
            "database": "materialize",
            "port": "{{ env_var('DBT_PORT', 6875) }}",
            "options": {
                "welcome_message": "on",
                "auto_route_catalog_queries": "off",
            },
        }

    def test_options_override(self, project):
        # Override these session variables opposite to their default values
        result = project.run_sql(
            "SELECT current_setting('welcome_message')", fetch="one"
        )
        assert result[0] == "on"

        result = project.run_sql(
            "SELECT current_setting('auto_route_catalog_queries')", fetch="one"
        )
        assert result[0] == "off"


class TestConnectionOptionsDefaults:
    """Verify that defaults still apply when no `options` block is set."""

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "materialize",
            "threads": 1,
            "host": "{{ env_var('DBT_HOST', 'localhost') }}",
            "user": "materialize",
            "pass": "password",
            "database": "materialize",
            "port": "{{ env_var('DBT_PORT', 6875) }}",
        }

    @pytest.mark.parametrize(
        "setting,expected",
        [
            ("welcome_message", "off"),
            ("auto_route_catalog_queries", "on"),
            ("current_object_missing_warnings", "off"),
            ("application_name", f"dbt-materialize v{__version__}"),
        ],
    )
    def test_defaults(self, project, setting, expected):
        result = project.run_sql(f"SELECT current_setting('{setting}')", fetch="one")
        assert result[0] == expected


class TestConnectionOptionsOverrideEscapeSpaces:
    """Verify spaces in options are properly escaped and reflected in Materialize"""

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "materialize",
            "threads": 1,
            "host": "{{ env_var('DBT_HOST', 'localhost') }}",
            "user": "materialize",
            "pass": "password",
            "database": "materialize",
            "options": {
                "application_name": "application  with spaces",
            },
            "port": "{{ env_var('DBT_PORT', 6875) }}",
        }

    @pytest.mark.parametrize(
        "setting,expected",
        [
            ("application_name", "application  with spaces"),
        ],
    )
    def test_defaults(self, project, setting, expected):
        result = project.run_sql(f"SELECT current_setting('{setting}')", fetch="one")
        print(result)
        assert result[0] == expected
