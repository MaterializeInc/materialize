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

import os

import pytest
from dbt.cli.main import dbtRunner

freshness_via_metadata_schema_yml = """version: 2
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: "{{ env_var('DBT_GET_LAST_RELATION_TEST_SCHEMA') }}"
    tables:
      - name: test_table
"""


class TestGetLastRelationModified:
    @pytest.fixture(scope="class", autouse=True)
    def set_env_vars(self, project):
        os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"] = project.test_schema
        yield
        del os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"]

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": freshness_via_metadata_schema_yml}

    @pytest.fixture(scope="class")
    def custom_schema(self, project, set_env_vars):
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database,
                schema=os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"],
            )
            project.adapter.drop_schema(relation)
            project.adapter.create_schema(relation)

        yield relation.schema

        with project.adapter.connection_named("__test"):
            project.adapter.drop_schema(relation)

    def test_get_last_relation_modified(self, project, set_env_vars, custom_schema):
        project.run_sql(
            f"create table {custom_schema}.test_table (id integer, name varchar(100) not null);"
        )

        warning_or_error = False

        def probe(e):
            nonlocal warning_or_error
            if e.info.level in ["warning", "error"]:
                warning_or_error = True

        runner = dbtRunner(callbacks=[probe])
        runner.invoke(["source", "freshness"])

        # The 'source freshness' command should succeed without warnings or errors.
        assert not warning_or_error
