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

import time

import pytest
from dbt.tests.util import run_dbt
from fixtures import (
    test_materialized_view,
    test_materialized_view_deploy,
    test_materialized_view_index,
    test_sink,
    test_sink_deploy,
    test_source,
    test_view_index,
)


class TestApplyGrantsAndPrivileges:
    @pytest.fixture(autouse=True)
    def cleanup(self, project):
        role_exists = project.run_sql(
            "SELECT count(*) = 1 FROM mz_roles WHERE name = 'my_role'",
            fetch="one",
        )[0]
        if role_exists:
            project.run_sql("DROP OWNED BY my_role")
            project.run_sql("DROP ROLE IF EXISTS my_role")
        project.run_sql("DROP SCHEMA IF EXISTS blue_schema CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS green_schema CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS blue_cluster CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS green_cluster CASCADE")

    def test_apply_schema_default_privileges(self, project):
        project.run_sql("CREATE ROLE my_role")
        project.run_sql("GRANT my_role TO materialize")
        project.run_sql("CREATE SCHEMA blue_schema")
        project.run_sql("CREATE SCHEMA green_schema")
        project.run_sql(
            "ALTER DEFAULT PRIVILEGES FOR ROLE my_role IN SCHEMA blue_schema GRANT SELECT ON TABLES TO my_role"
        )

        run_dbt(
            [
                "run-operation",
                "internal_copy_schema_default_privs",
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

        assert result[0]

    def test_apply_schema_grants(self, project):
        project.run_sql("CREATE ROLE my_role")
        project.run_sql("GRANT my_role TO materialize")
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

        assert result[0]

        result = project.run_sql(
            """
            SELECT count(*) = 1
            FROM mz_internal.mz_show_schema_privileges
            WHERE grantee = 'my_role'
                AND name = 'green_schema'
                AND privilege_type = 'USAGE'""",
            fetch="one",
        )

        assert result[0]

    def test_apply_cluster_grants(self, project):
        project.run_sql("CREATE ROLE my_role")
        project.run_sql("GRANT my_role TO materialize")
        project.run_sql("CREATE CLUSTER blue_cluster SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE CLUSTER green_cluster SIZE = 'scale=1,workers=1'")

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

        assert result[0]

        result = project.run_sql(
            """
            SELECT count(*) = 1
            FROM mz_internal.mz_show_cluster_privileges
            WHERE grantee = 'my_role'
                AND name = 'green_cluster'
                AND privilege_type = 'USAGE'""",
            fetch="one",
        )

        assert result[0]


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
    def dbt_profile_target(self):
        return {
            "type": "materialize",
            "threads": 1,
            "host": "{{ env_var('DBT_HOST', 'localhost') }}",
            "user": "materialize",
            "pass": "password",
            "database": "materialize",
            "port": "{{ env_var('DBT_PORT', 6875) }}",
            "cluster": "quickstart",
        }

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

        sources = project.run_sql(
            """
            SELECT count(*)
            FROM mz_sources
            JOIN mz_clusters ON mz_sources.cluster_id = mz_clusters.id
            WHERE mz_clusters.name = 'quickstart_dbt_deploy'
               AND mz_sources.id LIKE 'u%'""",
            fetch="one",
        )

        assert int(sources[0]) == 1

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
        run_dbt(["run"])
        run_dbt(["run-operation", "deploy_init"], expect_pass=False)


class TestTargetDeploy:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "deployment": {
                    "default": {
                        "clusters": ["prod"],
                        "schemas": ["prod", "staging"],
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
        project.run_sql("DROP SCHEMA IF EXISTS staging CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS staging_dbt_deploy CASCADE")

    def test_dbt_deploy(self, project):
        project.run_sql("CREATE CLUSTER prod SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE CLUSTER prod_dbt_deploy SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA prod_dbt_deploy")
        project.run_sql("CREATE SCHEMA staging")
        project.run_sql("CREATE SCHEMA staging_dbt_deploy")

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
        before_staging_schemas = dict(
            project.run_sql(
                "SELECT name, id FROM mz_schemas WHERE name IN ('staging', 'staging_dbt_deploy')",
                fetch="all",
            )
        )

        run_dbt(["run-operation", "deploy_promote", "--args", "{dry_run: true}"])

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
        after_staging_schemas = dict(
            project.run_sql(
                "SELECT name, id FROM mz_schemas WHERE name IN ('staging', 'staging_dbt_deploy')",
                fetch="all",
            )
        )

        assert before_clusters["prod"] != after_clusters["prod_dbt_deploy"]
        assert before_clusters["prod_dbt_deploy"] != after_clusters["prod"]
        assert before_schemas["prod"] != after_schemas["prod_dbt_deploy"]
        assert before_schemas["prod_dbt_deploy"] != after_schemas["prod"]
        assert (
            before_staging_schemas["staging"]
            != after_staging_schemas["staging_dbt_deploy"]
        )
        assert (
            before_staging_schemas["staging_dbt_deploy"]
            != after_staging_schemas["staging"]
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
        after_staging_schemas = dict(
            project.run_sql(
                "SELECT name, id FROM mz_schemas WHERE name IN ('staging', 'staging_dbt_deploy')",
                fetch="all",
            )
        )

        assert before_clusters["prod"] == after_clusters["prod_dbt_deploy"]
        assert before_clusters["prod_dbt_deploy"] == after_clusters["prod"]
        assert before_schemas["prod"] == after_schemas["prod_dbt_deploy"]
        assert before_schemas["prod_dbt_deploy"] == after_schemas["prod"]
        assert (
            before_staging_schemas["staging"]
            == after_staging_schemas["staging_dbt_deploy"]
        )
        assert (
            before_staging_schemas["staging_dbt_deploy"]
            == after_staging_schemas["staging"]
        )

        # Verify that both schemas are tagged correctly
        for schema_name in ["prod", "staging"]:
            tagged_schema_comment = project.run_sql(
                f"""
                SELECT c.comment
                FROM mz_internal.mz_comments c
                JOIN mz_schemas s USING (id)
                WHERE s.name = '{schema_name}';
                """,
                fetch="one",
            )

            assert (
                tagged_schema_comment is not None
            ), f"No comment found for schema {schema_name}"
            assert (
                "Deployment by" in tagged_schema_comment[0]
            ), f"Missing deployment info in {schema_name} comment"
            assert (
                "on" in tagged_schema_comment[0]
            ), f"Missing timestamp in {schema_name} comment"

    def test_dbt_deploy_with_force(self, project):
        project.run_sql("CREATE CLUSTER prod SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE CLUSTER prod_dbt_deploy SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA prod_dbt_deploy")
        project.run_sql("CREATE SCHEMA staging")
        project.run_sql("CREATE SCHEMA staging_dbt_deploy")

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
        project.run_sql("CREATE CLUSTER prod SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA prod_dbt_deploy")
        project.run_sql("CREATE SCHEMA staging")
        project.run_sql("CREATE SCHEMA staging_dbt_deploy")

        run_dbt(["run-operation", "deploy_promote"], expect_pass=False)

    def test_dbt_deploy_missing_deployment_schema(self, project):
        project.run_sql("CREATE CLUSTER prod SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE CLUSTER prod_dbt_deploy SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE SCHEMA prod")

        run_dbt(["run-operation", "deploy_promote"], expect_pass=False)

    def test_fails_on_unmanaged_cluster(self, project):
        project.run_sql("CREATE CLUSTER prod REPLICAS ()")
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA staging")
        project.run_sql("CREATE SCHEMA staging_dbt_deploy")

        run_dbt(["run-operation", "deploy_init"], expect_pass=False)

    def test_dbt_deploy_init_with_refresh_hydration_time(self, project):
        project.run_sql(
            "CREATE CLUSTER prod (SIZE = 'scale=1,workers=1', SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'))"
        )
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA staging")

        run_dbt(["run-operation", "deploy_init"])

        # Get the rehydration time from the cluster
        cluster_type = project.run_sql(
            """
            SELECT cs.type
            FROM mz_internal.mz_cluster_schedules cs
            JOIN mz_clusters c ON cs.cluster_id = c.id
            WHERE c.name = 'prod_dbt_deploy'
            """,
            fetch="one",
        )

        assert cluster_type == ("on-refresh",)

    def test_dbt_deploy_init_and_cleanup(self, project):
        project.run_sql("CREATE CLUSTER prod SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA staging")

        run_dbt(["run-operation", "deploy_init"])

        (size, replication_factor) = project.run_sql(
            "SELECT size, replication_factor FROM mz_clusters WHERE name = 'prod_dbt_deploy'",
            fetch="one",
        )

        assert size == "scale=1,workers=1"
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
        project.run_sql("CREATE CLUSTER prod SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA prod_dbt_deploy")
        project.run_sql("CREATE CLUSTER prod_dbt_deploy SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE SCHEMA staging")
        project.run_sql("CREATE SCHEMA staging_dbt_deploy")

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
        project.run_sql("CREATE CLUSTER prod SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE SCHEMA prod")
        project.run_sql("CREATE SCHEMA prod_dbt_deploy")
        project.run_sql("CREATE CLUSTER prod_dbt_deploy SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE SCHEMA staging")
        project.run_sql("CREATE SCHEMA staging_dbt_deploy")

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


class TestLagTolerance:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "deployment": {
                    "default": {"clusters": ["quickstart"], "schemas": ["public"]}
                }
            }
        }

    @pytest.fixture(autouse=True)
    def cleanup(self, project):
        project.run_sql("DROP CLUSTER IF EXISTS quickstart_dbt_deploy CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS public_dbt_deploy CASCADE")

    def test_deploy_await_custom_lag_threshold(self, project):
        run_dbt(["run-operation", "deploy_init"])
        result = run_dbt(
            [
                "run-operation",
                "deploy_await",
                "--args",
                "{poll_interval: 5, lag_threshold: '5s'}",
            ]
        )
        assert len(result) > 0 and result[0].status == "success"

    def test_deploy_promote_custom_lag_threshold(self, project):
        run_dbt(["run-operation", "deploy_init"])
        result = run_dbt(
            [
                "run-operation",
                "deploy_promote",
                "--args",
                "{wait: true, poll_interval: 5, lag_threshold: '5s'}",
            ]
        )
        assert len(result) > 0 and result[0].status == "success"

    def test_default_lag_threshold(self, project):
        run_dbt(["run-operation", "deploy_init"])
        result = run_dbt(["run-operation", "deploy_await"])
        assert len(result) > 0 and result[0].status == "success"

    def test_large_lag_threshold(self, project):
        run_dbt(["run-operation", "deploy_init"])
        result = run_dbt(
            [
                "run-operation",
                "deploy_await",
                "--args",
                "{poll_interval: 5, lag_threshold: '1h'}",
            ]
        )
        assert len(result) > 0 and result[0].status == "success"


class TestEndToEndDeployment:
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
            "cluster": "quickstart",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_materialized_view_deploy.sql": test_materialized_view_deploy,
            "test_sink_deploy.sql": test_sink_deploy,
        }

    @pytest.fixture(autouse=True)
    def cleanup(self, project):
        project.run_sql("DROP ROLE IF EXISTS test_role")
        project.run_sql("DROP CLUSTER IF EXISTS quickstart_dbt_deploy CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS public_dbt_deploy CASCADE")
        project.run_sql("DROP CLUSTER IF EXISTS sinks_cluster CASCADE")
        project.run_sql("DROP SCHEMA IF EXISTS sinks_schema CASCADE")
        project.run_sql("DROP TABLE IF EXISTS source_table")
        project.run_sql("DROP SOURCE IF EXISTS sink_validation_source")

    def test_full_deploy_process(self, project):
        # Create test role and grant it to materialize
        project.run_sql("CREATE ROLE test_role")
        project.run_sql("GRANT test_role TO materialize")

        # Prepare the source table, the sink cluster and schema
        project.run_sql("CREATE TABLE source_table (val INTEGER)")
        project.run_sql("CREATE CLUSTER sinks_cluster SIZE = 'scale=1,workers=1'")
        project.run_sql("CREATE SCHEMA sinks_schema")

        project.run_sql("INSERT INTO source_table VALUES (1)")
        run_dbt(["run"])

        created_schema = project.created_schemas[0]

        # Set up initial grants on the production schema and cluster
        project.run_sql(f"GRANT USAGE ON SCHEMA {created_schema} TO test_role")
        project.run_sql(f"GRANT CREATE ON SCHEMA {created_schema} TO test_role")
        project.run_sql(
            f"ALTER DEFAULT PRIVILEGES FOR ROLE test_role IN SCHEMA {created_schema} GRANT SELECT ON TABLES TO test_role"
        )
        project.run_sql("GRANT USAGE ON CLUSTER quickstart TO test_role")
        project.run_sql("GRANT CREATE ON CLUSTER quickstart TO test_role")

        # Store initial grants for later comparison
        initial_schema_grants = project.run_sql(
            f"""
            WITH schema_privilege AS (
                SELECT mz_internal.mz_aclexplode(s.privileges).*
                FROM mz_schemas s
                JOIN mz_databases d ON s.database_id = d.id
                WHERE d.name = current_database()
                    AND s.name = '{created_schema}'
            )
            SELECT privilege_type, grantee.name as grantee
            FROM schema_privilege
            JOIN mz_roles grantee ON grantee = grantee.id
            WHERE grantee.name = 'test_role'
            ORDER BY privilege_type, grantee
        """,
            fetch="all",
        )

        initial_cluster_grants = project.run_sql(
            """
            WITH cluster_privilege AS (
                SELECT mz_internal.mz_aclexplode(privileges).*
                FROM mz_clusters
                WHERE name = 'quickstart'
            )
            SELECT privilege_type, grantee.name as grantee
            FROM cluster_privilege
            JOIN mz_roles grantee ON grantee = grantee.id
            WHERE grantee.name = 'test_role'
            ORDER BY privilege_type, grantee
        """,
            fetch="all",
        )

        initial_default_privs = project.run_sql(
            f"""
            SELECT privilege_type, grantee, object_type
            FROM mz_internal.mz_show_default_privileges
            WHERE database = current_database()
                AND schema = '{created_schema}'
                AND grantee = 'test_role'
            ORDER BY privilege_type, grantee, object_type
        """,
            fetch="all",
        )

        project_config = f"{{deployment: {{default: {{clusters: ['quickstart'], schemas: ['{created_schema}']}}}}}}"
        project_config_deploy = f"{{deployment: {{default: {{clusters: ['quickstart'], schemas: ['{created_schema}']}}}}, deploy: True}}"

        # Validate the initial sink result
        project.run_sql(
            "CREATE SOURCE sink_validation_source FROM KAFKA CONNECTION kafka_connection (TOPIC 'testdrive-test-sink-1') FORMAT JSON"
        )
        run_with_retry(
            project, "SELECT count(*) FROM sink_validation_source", expected_count=1
        )

        # Insert another row and validate the sink
        project.run_sql("INSERT INTO source_table VALUES (2)")
        run_with_retry(
            project, "SELECT count(*) FROM sink_validation_source", expected_count=2
        )

        # Initialize the deployment environment
        run_dbt(["run-operation", "deploy_init", "--vars", project_config])

        # Verify grants were copied to deployment schema
        deploy_schema_grants = project.run_sql(
            f"""
            WITH schema_privilege AS (
                SELECT mz_internal.mz_aclexplode(s.privileges).*
                FROM mz_schemas s
                JOIN mz_databases d ON s.database_id = d.id
                WHERE d.name = current_database()
                    AND s.name = '{created_schema}_dbt_deploy'
            )
            SELECT privilege_type, grantee.name as grantee
            FROM schema_privilege
            JOIN mz_roles grantee ON grantee = grantee.id
            WHERE grantee.name = 'test_role'
            ORDER BY privilege_type, grantee
        """,
            fetch="all",
        )

        # Verify grants were copied to deployment cluster
        deploy_cluster_grants = project.run_sql(
            """
            WITH cluster_privilege AS (
                SELECT mz_internal.mz_aclexplode(privileges).*
                FROM mz_clusters
                WHERE name = 'quickstart_dbt_deploy'
            )
            SELECT privilege_type, grantee.name as grantee
            FROM cluster_privilege
            JOIN mz_roles grantee ON grantee = grantee.id
            WHERE grantee.name = 'test_role'
            ORDER BY privilege_type, grantee
        """,
            fetch="all",
        )

        deploy_default_privs = project.run_sql(
            f"""
            SELECT privilege_type, grantee, object_type
            FROM mz_internal.mz_show_default_privileges
            WHERE database = current_database()
                AND schema = '{created_schema}_dbt_deploy'
                AND grantee = 'test_role'
            ORDER BY privilege_type, grantee, object_type
        """,
            fetch="all",
        )

        # Assert grants match between production and deployment environments
        assert (
            initial_schema_grants == deploy_schema_grants
        ), "Schema grants do not match between production and deployment"
        assert (
            initial_cluster_grants == deploy_cluster_grants
        ), "Cluster grants do not match between production and deployment"
        assert (
            initial_default_privs == deploy_default_privs
        ), "Default privileges do not match between production and deployment"

        # Run the deploy with the deploy flag set to True and exclude the sink creation
        run_dbt(
            [
                "run",
                "--vars",
                project_config_deploy,
                "--exclude",
                "config.materialized:sink",
            ]
        )

        # Ensure the validation source has not changed
        run_with_retry(
            project, "SELECT count(*) FROM sink_validation_source", expected_count=2
        )

        # Insert a new row and validate the new sink result after the deploy
        project.run_sql("INSERT INTO source_table VALUES (3)")
        run_with_retry(
            project, "SELECT count(*) FROM sink_validation_source", expected_count=3
        )

        # Get the IDs of the materialized views
        before_view_id = project.run_sql(
            f"SELECT id FROM mz_materialized_views WHERE name = 'test_materialized_view_deploy' AND schema_id = (SELECT id FROM mz_schemas WHERE name = '{created_schema}')",
            fetch="one",
        )[0]
        after_view_id = project.run_sql(
            f"SELECT id FROM mz_materialized_views WHERE name = 'test_materialized_view_deploy' AND schema_id = (SELECT id FROM mz_schemas WHERE name = '{created_schema}_dbt_deploy')",
            fetch="one",
        )[0]

        before_sink = project.run_sql(
            "SELECT name, id, create_sql FROM mz_sinks WHERE name = 'test_sink_deploy'",
            fetch="one",
        )
        assert (
            before_view_id in before_sink[2]
        ), "Before deployment, the sink should point to the original view ID"

        run_dbt(
            [
                "run-operation",
                "deploy_promote",
                "--args",
                "{dry_run: true}",
                "--vars",
                project_config,
            ]
        )

        after_sink = project.run_sql(
            "SELECT name, id, create_sql FROM mz_sinks WHERE name = 'test_sink_deploy'",
            fetch="one",
        )

        assert (
            before_view_id in after_sink[2]
        ), "The sink should point to the same view ID after a dry_run"

        assert before_sink[0] == after_sink[0], "Sink name should be the same"

        # Promote and ensure the sink points to the new objects
        run_dbt(["run-operation", "deploy_promote", "--vars", project_config])

        after_sink = project.run_sql(
            "SELECT name, id, create_sql FROM mz_sinks WHERE name = 'test_sink_deploy'",
            fetch="one",
        )

        after_view_id = project.run_sql(
            f"SELECT id FROM mz_materialized_views WHERE name = 'test_materialized_view_deploy' AND schema_id = (SELECT id FROM mz_schemas WHERE name = '{created_schema}')",
            fetch="one",
        )[0]

        assert (
            after_view_id in after_sink[2]
        ), "After deployment, the sink should point to the new view ID"

        assert before_sink[0] == after_sink[0], "Sink name should be the same"
        assert (
            before_view_id != after_view_id
        ), "Sink's view ID should be different after deployment"

        run_dbt(["run-operation", "deploy_cleanup", "--vars", project_config])


def run_with_retry(project, sql, expected_count, retries=5, delay=3, fetch="one"):
    for i in range(retries):
        result = project.run_sql(sql, fetch=fetch)
        if result[0] == expected_count:
            return result
        time.sleep(delay)
    raise AssertionError(
        f"Expected count to be {expected_count}, but got {result[0]} after {retries} retries"
    )
