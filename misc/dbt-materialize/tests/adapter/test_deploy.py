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


class TestApplyGrantsAndPrivileges:
    @pytest.fixture(autouse=True)
    def cleanup(self, project):
        project.run_sql("DROP ROLE my_role")
        project.run_sql("DROP SCHEMA IF EXISTS blue_schema CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS green_schema CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS blue_cluster  CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS green_cluster CASCADE")

    def apply_schema_default_privileges(self, project):
        project.run_sql("CREATE ROLE my_role")
        project.run_sql("CREATE SCHEMA blue_schema")
        project.run_sql("CREATE SCHEMA green_schema")
        project.run_sql(
            "ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA blue_schema GRANT SELECT ON TABLES TO my_role"
        )

        run_dbt(
            [
                "run-operation",
                "internal_copy_schema_grants",
                "--args",
                "{from: blue_schema, to: green_schema}",
            ]
        )

        result = project.run_sql(
            """SELECT count(*) = 1
             FROM mz_internal.mz_show_default_privileges
             WHERE database = current_database()
                AND schema = 'green_schema'
                AND grantee = 'my_role'
                AND object_type = 'table'
                AND privilege_type = 'SELECT'""",
            fetch="one",
        )

        assert bool(result[0])

    def apply_schema_grants(self, project):
        project.run_sql("CREATE ROLE my_role")
        project.run_sql("CREATE SCHEMA blue_schema")
        project.run_sql("CREATE SCHEMA green_schema")

        project.run_sql("GRANT CREATE ON SCHEMA green_schema TO my_role")
        project.run_sql("GRANT USAGE ON SCHEMA blue_schema TO my_role")
        run_dbt(
            [
                "run-operation",
                "internal_copy_schema_grants",
                "--args",
                "{from: blue_schema, to: green_schema}",
            ]
        )

        result = project.run_sql(
            """
            SELECT count(*) = 0
            FROM mz_internal.mz_show_schema_privileges
            WHERE grantee = 'my_role'
                AND name = 'green_schema'
                AND privilege_type = 'CREATE'""",
            fetch="one",
        )

        assert bool(result[0])

        result = project.run_sql(
            """
            SELECT count(*) = 1
            FROM mz_internal.mz_show_schema_privileges
            WHERE grantee = 'my_role'
                AND name = 'green_schema'
                AND privilege_type = 'USAGE'""",
            fetch="one",
        )

        assert bool(result[0])

    def apply_cluster_grants(self, project):
        project.run_sql("CREATE ROLE my_role")
        project.run_sql("CREATE CLUSTER blue_cluster SIZE = '1'")
        project.run_sql("CREATE CLUSTER green_cluster SIZE = '1'")

        project.run_sql("GRANT CREATE ON CLUSTER green_cluster TO my_role")
        project.run_sql("GRANT USAGE ON CLUSTER blue_cluster TO my_role")
        run_dbt(
            [
                "run-operation",
                "internal_copy_cluster_grants",
                "--args",
                "{from: blue_cluster, to: green_cluster}",
            ]
        )

        result = project.run_sql(
            """
            SELECT count(*) = 0
            FROM mz_internal.mz_show_cluster_privileges
            WHERE grantee = 'my_role'
                AND name = 'green_cluster'
                AND privilege_type = 'CREATE'""",
            fetch="one",
        )

        assert bool(result[0])

        result = project.run_sql(
            """
            SELECT count(*) = 1
            FROM mz_internal.mz_show_cluster_privileges
            WHERE grantee = 'my_role'
                AND name = 'green_cluster'
                AND privilege_type = 'USAGE'""",
            fetch="one",
        )

        assert bool(result[0])


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


class TestPermissionValidation:
    """Tests for database permissions. We run against the internal
    macros because the materialize user is a superuser and we'd short
    circuit these checks if executing deploy_validate_permissions"""

    def test_createcluster_permissions(self, project):
        run_dbt(["run-operation", "internal_ensure_createcluster_permission"])

    def test_database_create_permissions(self, project):
        run_dbt(["run-operation", "internal_ensure_database_permission"])

    def test_schema_owner_permissions(self, project):
        project.run_sql("CREATE SCHEMA my_schema")
        run_dbt(
            [
                "run-operation",
                "internal_ensure_schema_ownership",
                "--args",
                "{schemas: ['my_schema']}",
            ]
        )

    def test_cluster_owner_permissions(self, project):
        run_dbt(
            [
                "run-operation",
                "internal_ensure_cluster_ownership",
                "--args",
                "{clusters: ['quickstart']}",
            ]
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
        # configuration is overridden.

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
