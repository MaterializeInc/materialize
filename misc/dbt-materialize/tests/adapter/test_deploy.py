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
from fixtures import (
    test_materialized_view,
    test_materialized_view_index,
    test_sink,
    test_source,
    test_view_index,
)


class TestCIFixture:
    @pytest.fixture(autouse=True)
    def cleanup(self, project):
        project.run_sql("COMMENT ON CLUSTER quickstart IS NULL")
        project.run_sql("COMMENT ON SCHEMA public IS NULL")

    def test_ci_tags_match(self, project):
        run_dbt(
            [
                "run-operation",
                "set_cluster_ci_tag",
                "--args",
                "{cluster: quickstart, ci_tag: test}",
            ]
        )
        run_dbt(
            [
                "run-operation",
                "set_schema_ci_tag",
                "--args",
                "{schema: public, ci_tag: test}",
            ]
        )

        run_dbt(
            [
                "run-operation",
                "check_cluster_ci_tag",
                "--args",
                "{cluster: quickstart, ci_tag: test}",
            ]
        )
        run_dbt(
            [
                "run-operation",
                "check_schema_ci_tag",
                "--args",
                "{schema: public, ci_tag: test}",
            ]
        )

    def test_ci_tags_mismatch(self, project):
        run_dbt(
            [
                "run-operation",
                "set_cluster_ci_tag",
                "--args",
                "{cluster: quickstart, ci_tag: test}",
            ]
        )
        run_dbt(
            [
                "run-operation",
                "set_schema_ci_tag",
                "--args",
                "{schema: public, ci_tag: test}",
            ]
        )

        run_dbt(
            [
                "run-operation",
                "check_cluster_ci_tag",
                "--args",
                "{cluster: quickstart, ci_tag: different}",
            ],
            expect_pass=False,
        )
        run_dbt(
            [
                "run-operation",
                "check_schema_ci_tag",
                "--args",
                "{schema: public, ci_tag: different}",
            ],
            expect_pass=False,
        )


class TestRunWithDeploy:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "deployment": {
                    "default": {"clusters": ["quickstart"], "schemas": ["public"]}
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_materialized_view_index.sql": test_materialized_view_index,
            "test_view_index.sql": test_view_index,
        }

    @pytest.fixture(autouse=True)
    def cleanup(self, project):
        project.run_sql("DROP CLUSTER IF EXISTS quickstart_dbt_deploy CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS public_dbt_deploy CASCADE")

    def test_deployment_run(self, project):
        # the test runner overrides schemas
        # so we can only validate the cluster
        # configuration is overriden.

        run_dbt(["run-operation", "deploy_init"])
        run_dbt(["run", "--vars", "deploy: True"])

        mat_views = project.run_sql(
            """
            SELECT count(*)
            FROM mz_materialized_views
            JOIN mz_clusters ON mz_materialized_views.cluster_id = mz_clusters.id
            WHERE mz_clusters.name = 'quickstart_dbt_deploy'
               AND mz_materialized_views.id LIKE 'u%'""",
            fetch="one",
        )

        assert int(mat_views[0]) == 1

        indexes = project.run_sql(
            """
            SELECT count(*)
            FROM mz_indexes
            JOIN mz_clusters ON mz_indexes.cluster_id = mz_clusters.id
            WHERE mz_clusters.name = 'quickstart_dbt_deploy'
               AND mz_indexes.id LIKE 'u%'""",
            fetch="one",
        )

        assert int(indexes[0]) == 2

        run_dbt(["run-operation", "deploy_cleanup"])


class TestSourceFail:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "deployment": {
                    "default": {"clusters": ["quickstart"], "schemas": ["public"]}
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_source.sql": test_source,
        }

    @pytest.fixture(autouse=True)
    def cleanup(self, project):
        project.run_sql("DROP CLUSTER IF EXISTS quickstart_dbt_deploy CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS public_dbt_deploy CASCADE")

    def test_source_fails(self, project):
        run_dbt(["run-operation", "deploy_init"])
        run_dbt(["run", "--vars", "deploy: True"], expect_pass=False)
        run_dbt(["run-operation", "deploy_cleanup"])


class TestSinkFail:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "deployment": {
                    "default": {"clusters": ["quickstart"], "schemas": ["public"]}
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_materialized_view.sql": test_materialized_view,
            "test_sink.sql": test_sink,
        }

    @pytest.fixture(autouse=True)
    def cleanup(self, project):
        project.run_sql("DROP CLUSTER IF EXISTS quickstart_dbt_deploy CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS public_dbt_deploy CASCADE")

    def test_source_fails(self, project):
        run_dbt(["run-operation", "deploy_init"])
        run_dbt(["run", "--vars", "deploy: True"], expect_pass=False)
        run_dbt(["run-operation", "deploy_cleanup"])


class TestTargetDeploy:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "deployment": {
                    "default": {
                        "clusters": ["prod"],
                        "schemas": ["prod"],
                    }
                },
            }
        }

    @pytest.fixture(autouse=True)
    def cleanup(self, project):
        project.run_sql("DROP CLUSTER IF EXISTS prod CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS prod_dbt_deploy CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS prod CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS prod_dbt_deploy CASCADE")

    def test_dbt_deploy(self, project):
        project.run_sql("CREATE CLUSTER prod SIZE = '1'")
        project.run_sql("CREATE CLUSTER prod_dbt_deploy SIZE = '1'")
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA prod_dbt_deploy")

        before_clusters = dict(
            project.run_sql(
                "SELECT name, id FROM mz_clusters WHERE name IN ('prod', 'prod_dbt_deploy')",
                fetch="all",
            )
        )
        before_schemas = dict(
            project.run_sql(
                "SELECT name, id FROM mz_schemas WHERE name IN ('prod', 'prod_dbt_deploy')",
                fetch="all",
            )
        )

        run_dbt(["run-operation", "deploy_promote"])

        after_clusters = dict(
            project.run_sql(
                "SELECT name, id FROM mz_clusters WHERE name IN ('prod', 'prod_dbt_deploy')",
                fetch="all",
            )
        )
        after_schemas = dict(
            project.run_sql(
                "SELECT name, id FROM mz_schemas WHERE name IN ('prod', 'prod_dbt_deploy')",
                fetch="all",
            )
        )

        assert before_clusters["prod"] == after_clusters["prod_dbt_deploy"]
        assert before_clusters["prod_dbt_deploy"] == after_clusters["prod"]
        assert before_schemas["prod"] == after_schemas["prod_dbt_deploy"]
        assert before_schemas["prod"] == after_schemas["prod_dbt_deploy"]

    def test_dbt_deploy_with_force(self, project):
        project.run_sql("CREATE CLUSTER prod SIZE = '1'")
        project.run_sql("CREATE CLUSTER prod_dbt_deploy SIZE = '1'")
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA prod_dbt_deploy")

        before_clusters = dict(
            project.run_sql(
                "SELECT name, id FROM mz_clusters WHERE name IN ('prod', 'prod_dbt_deploy')",
                fetch="all",
            )
        )
        before_schemas = dict(
            project.run_sql(
                "SELECT name, id FROM mz_schemas WHERE name IN ('prod', 'prod_dbt_deploy')",
                fetch="all",
            )
        )

        run_dbt(["run-operation", "deploy_promote", "--args", "{wait: true}"])

        after_clusters = dict(
            project.run_sql(
                "SELECT name, id FROM mz_clusters WHERE name IN ('prod', 'prod_dbt_deploy')",
                fetch="all",
            )
        )
        after_schemas = dict(
            project.run_sql(
                "SELECT name, id FROM mz_schemas WHERE name IN ('prod', 'prod_dbt_deploy')",
                fetch="all",
            )
        )

        assert before_clusters["prod"] == after_clusters["prod_dbt_deploy"]
        assert before_clusters["prod_dbt_deploy"] == after_clusters["prod"]
        assert before_schemas["prod"] == after_schemas["prod_dbt_deploy"]
        assert before_schemas["prod"] == after_schemas["prod_dbt_deploy"]

    def test_dbt_deploy_missing_deployment_cluster(self, project):
        project.run_sql("CREATE CLUSTER prod SIZE = '1'")
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA prod_dbt_deploy")

        run_dbt(["run-operation", "deploy_promote"], expect_pass=False)

    def test_dbt_deploy_missing_deployment_schema(self, project):
        project.run_sql("CREATE CLUSTER prod SIZE = '1'")
        project.run_sql("CREATE CLUSTER prod_dbt_deploy SIZE = '1'")
        project.run_sql("CREATE SCHEMA prod")

        run_dbt(["run-operation", "deploy_promote"], expect_pass=False)

    def test_fails_on_unmanaged_cluster(self, project):
        project.run_sql("CREATE CLUSTER prod REPLICAS ()")
        project.run_sql("CREATE SCHEMA prod")

        run_dbt(["run-operation", "deploy_init"], expect_pass=False)

    def test_dbt_deploy_init_and_cleanup(self, project):
        project.run_sql("CREATE CLUSTER prod SIZE = '1'")
        project.run_sql("CREATE SCHEMA prod")

        run_dbt(["run-operation", "deploy_init"])

        (size, replication_factor) = project.run_sql(
            "SELECT size, replication_factor FROM mz_clusters WHERE name = 'prod_dbt_deploy'",
            fetch="one",
        )

        assert size == "1"
        assert replication_factor == "1"

        result = project.run_sql(
            "SELECT count(*) = 1 FROM mz_schemas WHERE name = 'prod_dbt_deploy'",
            fetch="one",
        )
        assert bool(result[0])

        run_dbt(["run-operation", "deploy_cleanup"])

        result = project.run_sql(
            "SELECT count(*) = 0 FROM mz_clusters WHERE name = 'prod_dbt_deploy'",
            fetch="one",
        )
        assert bool(result[0])

        result = project.run_sql(
            "SELECT count(*) = 0 FROM mz_schemas WHERE name = 'prod_dbt_deploy'",
            fetch="one",
        )
        assert bool(result[0])

    def test_cluster_contains_objects(self, project):
        project.run_sql("CREATE CLUSTER prod SIZE = '1'")
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA prod_dbt_deploy")
        project.run_sql("CREATE CLUSTER prod_dbt_deploy SIZE = '1'")

        project.run_sql(
            "CREATE MATERIALIZED VIEW mv IN CLUSTER prod_dbt_deploy AS SELECT 1"
        )

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
        project.run_sql("CREATE CLUSTER prod SIZE = '1'")
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA prod_dbt_deploy")
        project.run_sql("CREATE CLUSTER prod_dbt_deploy SIZE = '1'")

        project.run_sql("CREATE VIEW prod_dbt_deploy.view AS SELECT 1")

        run_dbt(["run-operation", "deploy_init"], expect_pass=False)
        run_dbt(
            [
                "run-operation",
                "deploy_init",
                "--args",
                "{ignore_existing_objects: True}",
            ]
        )
