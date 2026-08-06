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
from dbt.tests.util import run_dbt, run_dbt_and_capture

view_model = """
{{ config(materialized='view') }}

SELECT 1 AS id
"""

view_model_invalid_index = """
{{ config(
    materialized='view',
    indexes=[{'columns': 'id'}]
) }}

SELECT 1 AS id
"""

mv_model = """
{{ config(materialized='materialized_view') }}

SELECT 1 AS id
"""

rename_view_macro = """
{% macro rename_view(from_name, to_name, relation_type='view') %}
  {% set from_relation = api.Relation.create(
      database=target.database, schema=target.schema, identifier=from_name, type=relation_type) %}
  {% set to_relation = api.Relation.create(
      database=target.database, schema=target.schema, identifier=to_name, type=relation_type) %}
  {% do adapter.rename_relation(from_relation, to_relation) %}
{% endmacro %}

{% macro drop_unsupported_relation(name) %}
  {% set relation = api.Relation.create(
      database=target.database, schema=target.schema, identifier=name, type='source_table') %}
  {% do adapter.drop_relation(relation) %}
{% endmacro %}
"""


class TestInvalidIndexConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_view.sql": view_model_invalid_index}

    def test_invalid_index_config_is_reported(self, project):
        _, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "Could not parse index config" in output


class TestRelationMacros:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_view.sql": view_model, "my_mv.sql": mv_model}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"relation_macros.sql": rename_view_macro}

    def test_rename_view(self, project):
        run_dbt(["run"])

        run_dbt(
            [
                "run-operation",
                "rename_view",
                "--args",
                "{from_name: my_view, to_name: my_renamed_view}",
            ]
        )

        result = project.run_sql(
            f"""
            SELECT count(*)
            FROM mz_views v
            JOIN mz_schemas s ON v.schema_id = s.id
            WHERE v.name = 'my_renamed_view' AND s.name = '{project.test_schema}'
            """,
            fetch="one",
        )

        assert result[0] == 1

    def test_rename_materialized_view(self, project):
        run_dbt(["run"])

        run_dbt(
            [
                "run-operation",
                "rename_view",
                "--args",
                "{from_name: my_mv, to_name: my_renamed_mv, relation_type: materialized_view}",
            ]
        )

        result = project.run_sql(
            f"""
            SELECT count(*)
            FROM mz_materialized_views mv
            JOIN mz_schemas s ON mv.schema_id = s.id
            WHERE mv.name = 'my_renamed_mv' AND s.name = '{project.test_schema}'
            """,
            fetch="one",
        )

        assert result[0] == 1

    def test_drop_relation_of_unsupported_type(self, project):
        _, output = run_dbt_and_capture(
            [
                "run-operation",
                "drop_unsupported_relation",
                "--args",
                "{name: my_view}",
            ],
            expect_pass=False,
        )

        assert "Don't know how to drop relation" in output
