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
