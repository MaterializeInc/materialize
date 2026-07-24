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


class TestClusterOps:

    @pytest.fixture(autouse=True)
    def cleanup(self, project):
        project.run_sql("DROP CLUSTER IF EXISTS test_cluster CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS test_cluster_dbt_deploy CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS test_manual_schedule CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS test_on_refresh_schedule CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS test_autoscaling CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS test_autoscaling_dbt_deploy CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS test_autoscaling_unmanaged CASCADE")

    def test_create_and_drop_cluster(self, project):
        # Test creating a cluster
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_cluster", "size": "scale=1,workers=1", "replication_factor": 1, "ignore_existing_objects": true, "force_deploy_suffix": true}',
            ]
        )

        # Verify cluster creation
        result = project.run_sql(
            "SELECT name FROM mz_clusters WHERE name = 'test_cluster_dbt_deploy'",
            fetch="one",
        )
        assert result is not None, "Cluster was not created successfully"

        # Verify cluster properties
        properties = get_cluster_properties(project, "test_cluster_dbt_deploy")
        assert properties[1] == "scale=1,workers=1"
        assert properties[2] == "1"
        assert properties[5] == "manual"
        assert properties[6] is None

        # Test dropping the cluster
        run_dbt(
            [
                "run-operation",
                "drop_cluster",
                "--args",
                '{"cluster_name": "test_cluster_dbt_deploy"}',
            ]
        )

        # Verify cluster dropping
        result = project.run_sql(
            "SELECT name FROM mz_clusters WHERE name = 'test_cluster_dbt_deploy'",
            fetch="one",
        )
        assert result is None, "Cluster was not dropped successfully"

    def test_create_existing_cluster(self, project):
        # Create the cluster for the first time
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_cluster", "size": "scale=1,workers=1", "replication_factor": 1, "ignore_existing_objects": true, "force_deploy_suffix": true}',
            ]
        )

        # Try creating the same cluster again
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_cluster", "size": "scale=1,workers=1", "replication_factor": 1, "ignore_existing_objects": true, "force_deploy_suffix": true}',
            ]
        )

        # Verify the cluster exists
        result = project.run_sql(
            "SELECT name FROM mz_clusters WHERE name = 'test_cluster_dbt_deploy'",
            fetch="one",
        )
        assert (
            result is not None
        ), "Cluster should exist after attempting to create it again"

        # Verify cluster properties
        properties = get_cluster_properties(project, "test_cluster_dbt_deploy")
        assert properties[1] == "scale=1,workers=1"
        assert properties[2] == "1"
        assert properties[5] == "manual"
        assert properties[6] is None

        # Cleanup
        run_dbt(
            [
                "run-operation",
                "drop_cluster",
                "--args",
                '{"cluster_name": "test_cluster_dbt_deploy"}',
            ]
        )

    def test_drop_nonexistent_cluster(self, project):
        # Attempt to drop a nonexistent cluster
        run_dbt(
            [
                "run-operation",
                "drop_cluster",
                "--args",
                '{"cluster_name": "nonexistent_cluster"}',
            ]
        )

        # Verify no cluster exists
        result = project.run_sql(
            "SELECT name FROM mz_clusters WHERE name = 'nonexistent_cluster'",
            fetch="one",
        )
        assert result is None, "Nonexistent cluster should not exist"

    def test_create_cluster_with_objects(self, project):
        # Create a cluster and add an object to it
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_cluster", "size": "scale=1,workers=1", "replication_factor": 1, "ignore_existing_objects": true, "force_deploy_suffix": true}',
            ]
        )
        project.run_sql("CREATE MATERIALIZED VIEW test_view AS SELECT 1")

        # Attempt to create the same cluster without ignoring existing objects
        with pytest.raises(Exception):
            run_dbt(
                [
                    "run-operation",
                    "create_cluster",
                    "--args",
                    '{"cluster_name": "test_cluster", "size": "scale=1,workers=1", "replication_factor": 1, "ignore_existing_objects": false, "force_deploy_suffix": true}',
                ],
                expect_pass=False,
            )

        # Cleanup
        project.run_sql("DROP MATERIALIZED VIEW IF EXISTS test_view")
        run_dbt(
            [
                "run-operation",
                "drop_cluster",
                "--args",
                '{"cluster_name": "test_cluster_dbt_deploy"}',
            ]
        )

    def test_create_cluster_with_manual_schedule(self, project):
        # Test creating a cluster with manual schedule type
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_manual_schedule", "size": "scale=1,workers=1", "replication_factor": 2, "schedule_type": "manual", "ignore_existing_objects": true, "force_deploy_suffix": true}',
            ]
        )

        # Verify cluster creation
        result = project.run_sql(
            "SELECT name FROM mz_clusters WHERE name = 'test_manual_schedule_dbt_deploy'",
            fetch="one",
        )
        assert (
            result is not None
        ), "Cluster with manual schedule was not created successfully"

        # Verify cluster properties
        properties = get_cluster_properties(project, "test_manual_schedule_dbt_deploy")
        assert properties[1] == "scale=1,workers=1"
        assert properties[2] == "2"
        assert properties[5] == "manual"
        assert properties[6] is None

        # Cleanup
        run_dbt(
            [
                "run-operation",
                "drop_cluster",
                "--args",
                '{"cluster_name": "test_manual_schedule_dbt_deploy"}',
            ]
        )

    def test_create_cluster_with_on_refresh_schedule(self, project):
        # Test creating a cluster with on-refresh schedule type
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_on_refresh_schedule", "size": "scale=1,workers=1", "schedule_type": "on-refresh", "refresh_hydration_time_estimate": "10m", "ignore_existing_objects": true, "force_deploy_suffix": true}',
            ]
        )

        # Verify cluster creation
        result = project.run_sql(
            "SELECT name FROM mz_clusters WHERE name = 'test_on_refresh_schedule_dbt_deploy'",
            fetch="one",
        )
        assert (
            result is not None
        ), "Cluster with on-refresh schedule was not created successfully"

        # Verify cluster properties
        properties = get_cluster_properties(
            project, "test_on_refresh_schedule_dbt_deploy"
        )
        assert properties[1] == "scale=1,workers=1"
        assert properties[5] == "on-refresh"
        assert str(properties[6]) == "0:10:00"

        # Cleanup
        run_dbt(
            [
                "run-operation",
                "drop_cluster",
                "--args",
                '{"cluster_name": "test_on_refresh_schedule_dbt_deploy"}',
            ]
        )

    def test_create_cluster_without_force_deploy_suffix(self, project):
        # Test creating a cluster without force deploy suffix
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_cluster", "size": "scale=1,workers=1", "replication_factor": 1, "ignore_existing_objects": true, "force_deploy_suffix": false}',
            ]
        )

        # Verify cluster creation
        result = project.run_sql(
            "SELECT name FROM mz_clusters WHERE name = 'test_cluster'",
            fetch="one",
        )
        assert (
            result is not None
        ), "Cluster without force deploy suffix was not created successfully"

        # Verify cluster properties
        properties = get_cluster_properties(project, "test_cluster")
        assert properties[1] == "scale=1,workers=1"
        assert properties[2] == "1"
        assert properties[5] == "manual"
        assert properties[6] is None

        # Cleanup
        run_dbt(
            [
                "run-operation",
                "drop_cluster",
                "--args",
                '{"cluster_name": "test_cluster"}',
            ]
        )

    def test_create_cluster_with_auto_scaling_strategy(self, project):
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_autoscaling", "size": "scale=1,workers=1", "auto_scaling_strategy": {"on_hydration": {"hydration_size": "scale=1,workers=2", "linger_duration": "15s"}}, "force_deploy_suffix": false}',
            ]
        )

        strategy = get_cluster_auto_scaling_strategy(project, "test_autoscaling")
        assert strategy is not None, "Autoscaling strategy was not configured"
        assert strategy[0] == "scale=1,workers=2"
        assert strategy[1] == 15

    def test_create_cluster_with_auto_scaling_strategy_no_linger(self, project):
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_autoscaling", "size": "scale=1,workers=1", "auto_scaling_strategy": {"on_hydration": {"hydration_size": "scale=1,workers=2"}}, "force_deploy_suffix": false}',
            ]
        )

        strategy = get_cluster_auto_scaling_strategy(project, "test_autoscaling")
        assert strategy is not None, "Autoscaling strategy was not configured"
        assert strategy[0] == "scale=1,workers=2"

    def test_create_cluster_auto_scaling_strategy_validation(self, project):
        # Hydration size equal to the cluster size is a no-op burst
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_autoscaling", "size": "scale=1,workers=1", "auto_scaling_strategy": {"on_hydration": {"hydration_size": "scale=1,workers=1"}}}',
            ],
            expect_pass=False,
        )

        # Missing hydration_size
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_autoscaling", "size": "scale=1,workers=1", "auto_scaling_strategy": {"on_hydration": {"linger_duration": "15s"}}}',
            ],
            expect_pass=False,
        )

        # Unknown sub-policy
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_autoscaling", "size": "scale=1,workers=1", "auto_scaling_strategy": {"on_demand": {"hydration_size": "scale=1,workers=2"}}}',
            ],
            expect_pass=False,
        )

        # Cannot be combined with a non-manual schedule
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_autoscaling", "size": "scale=1,workers=1", "schedule_type": "on-refresh", "auto_scaling_strategy": {"on_hydration": {"hydration_size": "scale=1,workers=2"}}}',
            ],
            expect_pass=False,
        )

    def test_alter_cluster_auto_scaling_strategy(self, project):
        project.run_sql("CREATE CLUSTER test_autoscaling SIZE = 'scale=1,workers=1'")

        # Set a strategy on an existing cluster
        run_dbt(
            [
                "run-operation",
                "alter_cluster_auto_scaling_strategy",
                "--args",
                '{"cluster_name": "test_autoscaling", "auto_scaling_strategy": {"on_hydration": {"hydration_size": "scale=1,workers=2", "linger_duration": "15s"}}}',
            ]
        )

        strategy = get_cluster_auto_scaling_strategy(project, "test_autoscaling")
        assert strategy is not None, "Autoscaling strategy was not configured"
        assert strategy[0] == "scale=1,workers=2"
        assert strategy[1] == 15

        # Remove the strategy via RESET
        run_dbt(
            [
                "run-operation",
                "alter_cluster_auto_scaling_strategy",
                "--args",
                '{"cluster_name": "test_autoscaling"}',
            ]
        )

        strategy = get_cluster_auto_scaling_strategy(project, "test_autoscaling")
        assert strategy is None, "Autoscaling strategy was not removed"

        # Disable via an empty strategy (AUTO SCALING STRATEGY = ())
        run_dbt(
            [
                "run-operation",
                "alter_cluster_auto_scaling_strategy",
                "--args",
                '{"cluster_name": "test_autoscaling", "auto_scaling_strategy": {"on_hydration": {"hydration_size": "scale=1,workers=2"}}}',
            ]
        )
        run_dbt(
            [
                "run-operation",
                "alter_cluster_auto_scaling_strategy",
                "--args",
                '{"cluster_name": "test_autoscaling", "auto_scaling_strategy": {}}',
            ]
        )

        strategy = get_cluster_auto_scaling_strategy(project, "test_autoscaling")
        assert strategy is None, "Autoscaling strategy was not disabled"

    def test_alter_cluster_auto_scaling_strategy_errors(self, project):
        # Nonexistent cluster
        run_dbt(
            [
                "run-operation",
                "alter_cluster_auto_scaling_strategy",
                "--args",
                '{"cluster_name": "test_autoscaling", "auto_scaling_strategy": {"on_hydration": {"hydration_size": "scale=1,workers=2"}}}',
            ],
            expect_pass=False,
        )

        # Unmanaged cluster
        project.run_sql(
            "CREATE CLUSTER test_autoscaling_unmanaged REPLICAS (r1 (SIZE 'scale=1,workers=1'))"
        )
        run_dbt(
            [
                "run-operation",
                "alter_cluster_auto_scaling_strategy",
                "--args",
                '{"cluster_name": "test_autoscaling_unmanaged", "auto_scaling_strategy": {"on_hydration": {"hydration_size": "scale=1,workers=2"}}}',
            ],
            expect_pass=False,
        )

    def test_alter_cluster_auto_scaling_strategy_validation(self, project):
        # Hydration size equal to the cluster's current size, validated against
        # the size fetched from the catalog
        project.run_sql("CREATE CLUSTER test_autoscaling SIZE = 'scale=1,workers=1'")
        run_dbt(
            [
                "run-operation",
                "alter_cluster_auto_scaling_strategy",
                "--args",
                '{"cluster_name": "test_autoscaling", "auto_scaling_strategy": {"on_hydration": {"hydration_size": "scale=1,workers=1"}}}',
            ],
            expect_pass=False,
        )

        # Cluster with a non-manual schedule, validated against the schedule
        # fetched from the catalog
        project.run_sql(
            "CREATE CLUSTER test_on_refresh_schedule (SIZE = 'scale=1,workers=1', SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'))"
        )
        run_dbt(
            [
                "run-operation",
                "alter_cluster_auto_scaling_strategy",
                "--args",
                '{"cluster_name": "test_on_refresh_schedule", "auto_scaling_strategy": {"on_hydration": {"hydration_size": "scale=1,workers=2"}}}',
            ],
            expect_pass=False,
        )

    def test_create_cluster_without_size_and_name(self, project):
        # Test creating a cluster without providing a size parameter
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_cluster", "replication_factor": 1, "ignore_existing_objects": true, "force_deploy_suffix": true}',
            ],
            expect_pass=False,
        )

        # Test creating a cluster without providing a name parameter
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"size": "scale=1,workers=1", "replication_factor": 1, "ignore_existing_objects": true, "force_deploy_suffix": true}',
            ],
            expect_pass=False,
        )

        # Test invalid cluster size
        run_dbt(
            [
                "run-operation",
                "create_cluster",
                "--args",
                '{"cluster_name": "test_cluster", "size": "invalid_size", "replication_factor": 1, "ignore_existing_objects": true, "force_deploy_suffix": true}',
            ],
            expect_pass=False,
        )


def get_cluster_auto_scaling_strategy(project, cluster_name):
    """Return (hydration_size, linger_secs) for the cluster's configured
    autoscaling strategy, or None when no strategy is configured."""
    query = f"""
    SELECT
        s.strategy->'on_hydration'->>'hydration_size' AS hydration_size,
        (s.strategy->'on_hydration'->'linger_duration'->>'secs')::bigint AS linger_secs
    FROM mz_internal.mz_cluster_auto_scaling_strategies s
    JOIN mz_clusters c ON c.id = s.cluster_id
    WHERE c.name = '{cluster_name}'
    """
    result = project.run_sql(query, fetch="one")
    # A row with a null hydration size can linger while a burst from a
    # just-removed policy drains.
    if result is None or result[0] is None:
        return None
    return result


def get_cluster_properties(project, cluster_name):
    query = f"""
    SELECT
        c.managed,
        c.size,
        c.replication_factor,
        c.id AS cluster_id,
        c.name AS cluster_name,
        cs.type AS schedule_type,
        cs.refresh_hydration_time_estimate
    FROM mz_clusters c
    LEFT JOIN mz_internal.mz_cluster_schedules cs ON cs.cluster_id = c.id
    WHERE c.name = '{cluster_name}'
    """
    return project.run_sql(query, fetch="one")
