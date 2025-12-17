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
from dbt.tests.util import run_dbt

# Source materialization in dedicated schema (for non-strict tests)
load_gen_source = """
{{ config(
    materialized='source',
    database='materialize',
    schema='sources'
    )
}}
FROM LOAD GENERATOR AUCTION
FOR ALL TABLES;
"""

# Source materialization in default schema (for strict mode conflict tests)
source_in_default_schema = """
{{ config(
    materialized='source',
    database='materialize'
) }}
FROM LOAD GENERATOR COUNTER;
"""

# Second source in default schema (for coexistence tests)
source_in_default_schema_2 = """
{{ config(
    materialized='source',
    database='materialize'
) }}
FROM LOAD GENERATOR AUCTION
FOR ALL TABLES;
"""

# Source with index (for strict_mode index blocking tests)
source_with_index = """
{{ config(
    materialized='source',
    database='materialize',
    indexes=[{'columns': ['counter']}]
) }}
FROM LOAD GENERATOR COUNTER;
"""

# Source table that creates its own source via pre_hook (for strict_mode tests)
# In strict_mode, sources must be created outside of dbt, so we use pre_hook SQL
source_table_with_prehook = """
{{ config(
    materialized='source_table',
    database='materialize',
    pre_hook="CREATE SOURCE IF NOT EXISTS {{ target.schema }}_auction_source FROM LOAD GENERATOR AUCTION"
) }}
FROM SOURCE {{ target.schema }}_auction_source
(REFERENCE "bids")
"""

source_table_with_prehook_2 = """
{{ config(
    materialized='source_table',
    database='materialize'
) }}
FROM SOURCE {{ target.schema }}_auction_source
(REFERENCE "auctions")
"""

# Fixtures for source_table models (reference source from different schema) - for non-strict tests
source_table_model = """
{{ config(
    materialized='source_table',
    database='materialize'
) }}
FROM SOURCE {{ ref('test_load_gen_source') }}
(REFERENCE "bids")
"""

source_table_model_2 = """
{{ config(
    materialized='source_table',
    database='materialize'
) }}
FROM SOURCE {{ ref('test_load_gen_source') }}
(REFERENCE "auctions")
"""

# Fixtures for other materialization types
view_model = """
{{ config(materialized='view') }}

SELECT 1 AS id
"""

materialized_view_model = """
{{ config(materialized='materialized_view') }}

SELECT 1 AS id
"""

# Sink fixtures - sinks need a source materialized_view to read from
sink_source_mv = """
{{ config(
    materialized='materialized_view',
    schema='sink_sources'
) }}

SELECT 1 AS id
"""

sink_model = """
{{ config(
    materialized='sink',
    pre_hook="CREATE CONNECTION IF NOT EXISTS kafka_connection TO KAFKA (BROKER '{{ env_var('KAFKA_ADDR', 'localhost:9092') }}', SECURITY PROTOCOL PLAINTEXT)"
) }}
FROM {{ ref('sink_source_mv') }}
INTO KAFKA CONNECTION kafka_connection (TOPIC 'test-sink-1')
FORMAT JSON
ENVELOPE DEBEZIUM
"""

sink_model_2 = """
{{ config(
    materialized='sink'
) }}
FROM {{ ref('sink_source_mv') }}
INTO KAFKA CONNECTION kafka_connection (TOPIC 'test-sink-2')
FORMAT JSON
ENVELOPE DEBEZIUM
"""


class TestStrictModeDisabled:
    """Test that mixing source_tables with other types works when strict_mode is disabled (default)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "strict_mode_disabled"}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_load_gen_source.sql": load_gen_source,
            "test_source_table.sql": source_table_model,
            "test_view.sql": view_model,
        }

    def test_mixed_types_allowed(self, project):
        """Without strict_mode, source_tables and views can coexist in the same schema."""
        results = run_dbt(["run"])
        assert len(results) == 3


class TestStrictModeSourceTableBlocked:
    """Test that source_table is blocked when schema contains views (strict_mode enabled)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_source_table_blocked",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_view.sql": view_model,
            "test_source_table.sql": source_table_with_prehook,
        }

    def test_source_table_blocked_in_schema_with_view(self, project):
        """With strict_mode, mixing source_table and view in same schema fails at compile time."""
        run_dbt(["run"], expect_pass=False)


class TestStrictModeViewBlocked:
    """Test that view is blocked when schema contains source_tables (strict_mode enabled)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_view_blocked",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_source_table.sql": source_table_with_prehook,
            "test_view.sql": view_model,
        }

    def test_view_blocked_in_schema_with_source_table(self, project):
        """With strict_mode, mixing view and source_table in same schema fails at compile time."""
        run_dbt(["run"], expect_pass=False)


class TestStrictModeSourceTablesCanCoexist:
    """Test that multiple source_tables can coexist in the same schema (strict_mode enabled)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_source_tables_coexist",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_source_table.sql": source_table_with_prehook,
            "test_source_table_2.sql": source_table_with_prehook_2,
        }

    def test_multiple_source_tables_allowed(self, project):
        """With strict_mode, multiple source_tables can coexist in the same schema."""
        # Run all models - source is created via pre_hook, both source_tables in default schema
        results = run_dbt(["run"])
        # Should have 2 results: 2 source_tables (source created via pre_hook doesn't count)
        assert len(results) == 2


class TestStrictModeSinkBlocked:
    """Test that sink is blocked when schema contains views (strict_mode enabled)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_sink_blocked",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sink_source_mv.sql": sink_source_mv,
            "test_view.sql": view_model,
            "test_sink.sql": sink_model,
        }

    def test_sink_blocked_in_schema_with_view(self, project):
        """With strict_mode, mixing sink and view in same schema fails at compile time."""
        run_dbt(["run"], expect_pass=False)


class TestStrictModeViewBlockedBySink:
    """Test that view is blocked when schema contains sinks (strict_mode enabled)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_view_blocked_by_sink",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sink_source_mv.sql": sink_source_mv,
            "test_sink.sql": sink_model,
            "test_view.sql": view_model,
        }

    def test_view_blocked_in_schema_with_sink(self, project):
        """With strict_mode, mixing view and sink in same schema fails at compile time."""
        run_dbt(["run"], expect_pass=False)


class TestStrictModeSinksCanCoexist:
    """Test that multiple sinks can coexist in the same schema (strict_mode enabled)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_sinks_coexist",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sink_source_mv.sql": sink_source_mv,
            "test_sink.sql": sink_model,
            "test_sink_2.sql": sink_model_2,
        }

    def test_multiple_sinks_allowed(self, project):
        """With strict_mode, multiple sinks can coexist in the same schema."""
        # Run all models - source mv will be in separate schema, both sinks in default schema
        results = run_dbt(["run"])
        # Should have 3 results: 1 materialized_view + 2 sinks
        assert len(results) == 3


class TestStrictModeSourceBlocked:
    """Test that source is blocked when schema contains views (strict_mode enabled)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_source_blocked",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_view.sql": view_model,
            "test_source.sql": source_in_default_schema,
        }

    def test_source_blocked_in_schema_with_view(self, project):
        """With strict_mode, mixing source and view in same schema fails at compile time."""
        run_dbt(["run"], expect_pass=False)


class TestStrictModeViewBlockedBySource:
    """Test that view is blocked when schema contains sources (strict_mode enabled)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_view_blocked_by_source",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_source.sql": source_in_default_schema,
            "test_view.sql": view_model,
        }

    def test_view_blocked_in_schema_with_source(self, project):
        """With strict_mode, mixing view and source in same schema fails at compile time."""
        run_dbt(["run"], expect_pass=False)


class TestStrictModeSourcesCanCoexist:
    """Test that multiple sources can coexist in the same schema (strict_mode enabled)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_sources_coexist",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_source.sql": source_in_default_schema,
            "test_source_2.sql": source_in_default_schema_2,
        }

    def test_multiple_sources_allowed(self, project):
        """With strict_mode, multiple sources can coexist in the same schema."""
        results = run_dbt(["run"])
        # Should have 2 results: 2 sources
        assert len(results) == 2


class TestStrictModeSourceIndexBlocked:
    """Test that source index creation is blocked in strict_mode."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_source_index_blocked",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_source_with_index.sql": source_with_index,
        }

    def test_source_index_blocked_in_strict_mode(self, project):
        """With strict_mode, creating indexes on sources fails at compile time."""
        run_dbt(["run"], expect_pass=False)
