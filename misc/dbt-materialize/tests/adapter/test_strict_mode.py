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
    database='materialize',
    cluster='quickstart'
) }}
FROM LOAD GENERATOR COUNTER;
"""

# Second source in default schema with different cluster (for coexistence tests)
# In strict_mode, each source must have its own dedicated cluster
source_in_default_schema_2 = """
{{ config(
    materialized='source',
    database='materialize',
    cluster='source_cluster_2',
    pre_hook=[
        "DROP CLUSTER IF EXISTS source_cluster_2 CASCADE",
        "CREATE CLUSTER source_cluster_2 SIZE = 'bootstrap'"
    ]
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

# Cluster isolation fixtures - sources with specific clusters
source_on_cluster_a = """
{{ config(
    materialized='source',
    database='materialize',
    schema='sources',
    cluster='cluster_a'
) }}
FROM LOAD GENERATOR COUNTER;
"""

source_on_cluster_a_2 = """
{{ config(
    materialized='source',
    database='materialize',
    schema='sources',
    cluster='cluster_a'
) }}
FROM LOAD GENERATOR AUCTION
FOR ALL TABLES;
"""

mv_on_cluster_a = """
{{ config(
    materialized='materialized_view',
    cluster='cluster_a'
) }}
SELECT 1 AS id;
"""

# Sinks on specific clusters - for cluster isolation tests
# Both sinks use the default cluster (quickstart) to test that sinks can share clusters
sink_on_cluster_b = """
{{ config(
    materialized='sink',
    schema='sinks',
    pre_hook="CREATE CONNECTION IF NOT EXISTS kafka_connection TO KAFKA (BROKER '{{ env_var('KAFKA_ADDR', 'localhost:9092') }}', SECURITY PROTOCOL PLAINTEXT)"
) }}
FROM {{ ref('sink_source_mv') }}
INTO KAFKA CONNECTION kafka_connection (TOPIC 'test-sink-cluster-1')
FORMAT JSON
ENVELOPE DEBEZIUM
"""

sink_on_cluster_b_2 = """
{{ config(
    materialized='sink',
    schema='sinks'
) }}
FROM {{ ref('sink_source_mv') }}
INTO KAFKA CONNECTION kafka_connection (TOPIC 'test-sink-cluster-2')
FORMAT JSON
ENVELOPE DEBEZIUM
"""

# MV on default cluster - used for sink/MV conflict tests
mv_on_cluster_b = """
{{ config(
    materialized='materialized_view'
) }}
SELECT 1 AS id;
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
# MV uses its own cluster so it doesn't conflict with sinks in strict_mode
sink_source_mv = """
{{ config(
    materialized='materialized_view',
    schema='sink_sources',
    cluster='mv_cluster',
    pre_hook=[
        "DROP CLUSTER IF EXISTS mv_cluster CASCADE",
        "CREATE CLUSTER mv_cluster SIZE = 'bootstrap'"
    ]
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


# =============================================================================
# Cluster Isolation Tests
# =============================================================================


class TestStrictModeSourceDedicatedCluster:
    """Test that sources must have dedicated clusters in strict_mode."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_source_dedicated_cluster",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_source_1.sql": source_on_cluster_a,
            "test_source_2.sql": source_on_cluster_a_2,
        }

    def test_sources_cannot_share_cluster(self, project):
        """With strict_mode, two sources cannot share the same cluster."""
        run_dbt(["run"], expect_pass=False)


class TestStrictModeSourceCannotShareWithMV:
    """Test that sources cannot share cluster with MVs in strict_mode."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_source_mv_cluster_conflict",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_source.sql": source_on_cluster_a,
            "test_mv.sql": mv_on_cluster_a,
        }

    def test_source_and_mv_cannot_share_cluster(self, project):
        """With strict_mode, source and MV cannot share the same cluster."""
        run_dbt(["run"], expect_pass=False)


class TestStrictModeSinkCannotShareWithMV:
    """Test that sinks cannot share cluster with MVs in strict_mode."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_sink_mv_cluster_conflict",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sink_source_mv.sql": sink_source_mv,
            "test_sink.sql": sink_on_cluster_b,
            "test_mv.sql": mv_on_cluster_b,
        }

    def test_sink_and_mv_cannot_share_cluster(self, project):
        """With strict_mode, sink and MV cannot share the same cluster."""
        run_dbt(["run"], expect_pass=False)


class TestStrictModeSinksCanShareCluster:
    """Test that multiple sinks can share the same cluster in strict_mode."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "strict_mode_sinks_share_cluster",
            "vars": {"strict_mode": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sink_source_mv.sql": sink_source_mv,
            "test_sink_1.sql": sink_on_cluster_b,
            "test_sink_2.sql": sink_on_cluster_b_2,
        }

    def test_multiple_sinks_can_share_cluster(self, project):
        """With strict_mode, multiple sinks can share the same cluster."""
        results = run_dbt(["run"])
        # Should have 3 results: 1 MV (source for sinks) + 2 sinks
        assert len(results) == 3
