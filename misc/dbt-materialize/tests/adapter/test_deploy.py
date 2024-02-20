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

project_deployment_configuration = {
    "vars": {
        "deployment": {
            "default": {
                "clusters": [
                    {
                        "prod": "blue",
                        "prod_deploy": "green",
                    }
                ],
                "schemas": [
                    {
                        "prod": "blue",
                        "prod_deploy": "green",
                    }
                ],
            }
        },
    }
}


class TestTargetDeploy:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return project_deployment_configuration

    @pytest.fixture(autouse=True)
    def cleanup(self, project):
        project.run_sql("DROP CLUSTER IF EXISTS blue CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS green CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS blue CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS green CASCADE")

    def test_dbt_deploy(self, project):
        project.run_sql("CREATE CLUSTER blue SIZE = '1'")
        project.run_sql("CREATE CLUSTER green SIZE = '1'")
        project.run_sql("CREATE SCHEMA blue")
        project.run_sql("CREATE SCHEMA green")

        before_clusters = dict(
            project.run_sql(
                "SELECT name, id FROM mz_clusters WHERE name IN ('blue', 'green')",
                fetch="all",
            )
        )
        before_schemas = dict(
            project.run_sql(
                "SELECT name, id FROM mz_schemas WHERE name IN ('blue', 'green')",
                fetch="all",
            )
        )

        run_dbt(["run-operation", "deploy_promote"])

        after_clusters = dict(
            project.run_sql(
                "SELECT name, id FROM mz_clusters WHERE name IN ('blue', 'green')",
                fetch="all",
            )
        )
        after_schemas = dict(
            project.run_sql(
                "SELECT name, id FROM mz_schemas WHERE name IN ('blue', 'green')",
                fetch="all",
            )
        )

        assert before_clusters["blue"] == after_clusters["green"]
        assert before_clusters["green"] == after_clusters["blue"]
        assert before_schemas["blue"] == after_schemas["green"]
        assert before_schemas["blue"] == after_schemas["green"]

    def test_dbt_deploy_with_force(self, project):
        project.run_sql("CREATE CLUSTER blue SIZE = '1'")
        project.run_sql("CREATE CLUSTER green SIZE = '1'")
        project.run_sql("CREATE SCHEMA blue")
        project.run_sql("CREATE SCHEMA green")

        before_clusters = dict(
            project.run_sql(
                "SELECT name, id FROM mz_clusters WHERE name IN ('blue', 'green')",
                fetch="all",
            )
        )
        before_schemas = dict(
            project.run_sql(
                "SELECT name, id FROM mz_schemas WHERE name IN ('blue', 'green')",
                fetch="all",
            )
        )

        run_dbt(["run-operation", "deploy_promote", "--args", "{wait: true}"])

        after_clusters = dict(
            project.run_sql(
                "SELECT name, id FROM mz_clusters WHERE name IN ('blue', 'green')",
                fetch="all",
            )
        )
        after_schemas = dict(
            project.run_sql(
                "SELECT name, id FROM mz_schemas WHERE name IN ('blue', 'green')",
                fetch="all",
            )
        )

        assert before_clusters["blue"] == after_clusters["green"]
        assert before_clusters["green"] == after_clusters["blue"]
        assert before_schemas["blue"] == after_schemas["green"]
        assert before_schemas["blue"] == after_schemas["green"]

    def test_dbt_deploy_missing_deployment_cluster(self, project):
        project.run_sql("CREATE CLUSTER blue SIZE = '1'")
        project.run_sql("CREATE SCHEMA blue")
        project.run_sql("CREATE SCHEMA green")

        run_dbt(["run-operation", "deploy_promote"], expect_pass=False)

    def test_dbt_deploy_missing_deployment_schema(self, project):
        project.run_sql("CREATE CLUSTER blue SIZE = '1'")
        project.run_sql("CREATE CLUSTER green SIZE = '1'")
        project.run_sql("CREATE SCHEMA blue")

        run_dbt(["run-operation", "deploy_promote"], expect_pass=False)

    def test_fails_on_unmanaged_cluster(self, project):
        project.run_sql("CREATE CLUSTER blue REPLICAS ()")
        project.run_sql("CREATE SCHEMA blue")

        run_dbt(["run-operation", "deploy_init"], expect_pass=False)

    def test_dbt_create_and_destroy_deployment_environment(self, project):
        project.run_sql("CREATE CLUSTER blue SIZE = '1'")
        project.run_sql("CREATE SCHEMA blue")

        run_dbt(["run-operation", "deploy_init"])

        (size, replication_factor) = project.run_sql(
            "SELECT size, replication_factor FROM mz_clusters WHERE name = 'green'",
            fetch="one",
        )

        assert size == "1"
        assert replication_factor == "1"

        result = project.run_sql(
            "SELECT count(*) = 1 FROM mz_schemas WHERE name = 'green'", fetch="one"
        )
        assert bool(result[0])

        run_dbt(["run-operation", "deploy_cleanup"])

        result = project.run_sql(
            "SELECT count(*) = 0 FROM mz_clusters WHERE name = 'green'", fetch="one"
        )
        assert bool(result[0])

        result = project.run_sql(
            "SELECT count(*) = 0 FROM mz_schemas WHERE name = 'green'", fetch="one"
        )
        assert bool(result[0])

    def test_cluster_contains_objects(self, project):
        project.run_sql("CREATE CLUSTER blue SIZE = '1'")
        project.run_sql("CREATE SCHEMA blue")
        project.run_sql("CREATE SCHEMA green")
        project.run_sql("CREATE CLUSTER green SIZE = '1'")

        project.run_sql("CREATE MATERIALIZED VIEW mv IN CLUSTER green AS SELECT 1")

        run_dbt(["run-operation", "deploy_init"], expect_pass=False)
        run_dbt(
            [
                "run-operation",
                "deploy_init",
                "--args",
                "{ignore_existing_objects: True}",
            ]
        )

    def test_schema_contains_objects(self, project):
        project.run_sql("CREATE CLUSTER blue SIZE = '1'")
        project.run_sql("CREATE SCHEMA blue")
        project.run_sql("CREATE SCHEMA green")
        project.run_sql("CREATE CLUSTER green SIZE = '1'")

        project.run_sql("CREATE VIEW green.view AS SELECT 1")

        run_dbt(["run-operation", "deploy_init"], expect_pass=False)
        run_dbt(
            [
                "run-operation",
                "deploy_init",
                "--args",
                "{ignore_existing_objects: True}",
            ]
        )
