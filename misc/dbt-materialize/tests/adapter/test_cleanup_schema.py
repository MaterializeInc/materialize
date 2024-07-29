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


class TestCleanupSchema:
    @pytest.fixture(autouse=True)
    def cleanup(self, project):
        project.run_sql(f"DROP SCHEMA IF EXISTS {project.test_schema} CASCADE")
        project.run_sql("DROP ROLE IF EXISTS test_role")

    def test_cleanup_schema(self, project):
        # Setup
        project.run_sql(f"CREATE SCHEMA {project.test_schema}")
        project.run_sql(f"CREATE TABLE {project.test_schema}.test_table (id INT)")
        project.run_sql("CREATE ROLE test_role")
        project.run_sql(f"GRANT USAGE ON SCHEMA {project.test_schema} TO test_role")

        # Run the macro
        run_dbt(["run-operation", "cleanup_schema"])

        # Verify the schema was dropped and recreated
        schema_exists = project.run_sql(
            f"SELECT count(*) FROM mz_schemas WHERE name = '{project.test_schema}'",
            fetch="one",
        )[0]
        assert schema_exists == 1, "Schema should exist after cleanup"

        # Verify the table was dropped
        table_exists = project.run_sql(
            f"SELECT count(*) FROM mz_tables WHERE name = 'test_table' AND schema_id = (SELECT id FROM mz_schemas WHERE name = '{project.test_schema}')",
            fetch="one",
        )[0]
        assert table_exists == 0, "Table should not exist after cleanup"

        can_use_schema = project.run_sql(
            f"SELECT has_schema_privilege('test_role', '{project.test_schema}', 'USAGE')",
            fetch="one",
        )[0]
        assert not can_use_schema, "Grant should not exist after cleanup"

    def test_cleanup_schema_dry_run(self, project):
        # Setup
        project.run_sql(f"CREATE SCHEMA {project.test_schema}")
        project.run_sql(f"CREATE TABLE {project.test_schema}.test_table (id INT)")

        # Run the macro in dry run mode
        run_dbt(["run-operation", "cleanup_schema", "--args", "{dry_run: True}"])

        # Verify the schema and table still exist
        schema_exists = project.run_sql(
            f"SELECT count(*) FROM mz_schemas WHERE name = '{project.test_schema}'",
            fetch="one",
        )[0]
        assert schema_exists == 1, "Schema should still exist after dry run"

        table_exists = project.run_sql(
            f"SELECT count(*) FROM mz_tables WHERE name = 'test_table' AND schema_id = (SELECT id FROM mz_schemas WHERE name = '{project.test_schema}')",
            fetch="one",
        )[0]
        assert table_exists == 1, "Table should still exist after dry run"
