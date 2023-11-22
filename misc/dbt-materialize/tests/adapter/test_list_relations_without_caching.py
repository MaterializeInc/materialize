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

import json

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

NUM_VIEWS = 100

TABLE_BASE_SQL = """
{{ config(materialized='table') }}

select 1 as id
""".lstrip()

VIEW_X_SQL = """
select id from {{ ref('my_model_base') }}
""".lstrip()

MACROS__VALIDATE__MATERIALIZE__LIST_RELATIONS_WITHOUT_CACHING = """
{% macro validate_list_relations_without_caching(schema_relation) %}
    {% set relation_list_result = materialize__list_relations_without_caching(schema_relation) %}
    {% set n_relations = relation_list_result | length %}
    {{ log("n_relations: " ~ n_relations) }}
{% endmacro %}
"""


def parse_json_logs(json_log_output):
    parsed_logs = []
    for line in json_log_output.split("\n"):
        try:
            log = json.loads(line)
        except ValueError:
            continue

        parsed_logs.append(log)

    return parsed_logs


def find_result_in_parsed_logs(parsed_logs, result_name):
    return next(
        (
            item["data"]["msg"]
            for item in parsed_logs
            if result_name in item["data"].get("msg", "msg")
        ),
        False,
    )


def find_exc_info_in_parsed_logs(parsed_logs, exc_info_name):
    return next(
        (
            item["data"]["exc_info"]
            for item in parsed_logs
            if exc_info_name in item["data"].get("exc_info", "exc_info")
        ),
        False,
    )


class TestListRelationsWithoutCachingSingle:
    @pytest.fixture(scope="class")
    def models(self):
        my_models = {"my_model_base.sql": TABLE_BASE_SQL}
        for view in range(0, NUM_VIEWS):
            my_models.update({f"my_model_{view}.sql": VIEW_X_SQL})

        return my_models

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_list_relations_without_caching.sql": MACROS__VALIDATE__MATERIALIZE__LIST_RELATIONS_WITHOUT_CACHING,
        }

    def test__materialize__list_relations_without_caching_termination(self, project):
        run_dbt(["run", "-s", "my_model_base"])

        database = project.database
        schemas = project.created_schemas

        for schema in schemas:
            kwargs = {"schema_relation": {"database": database, "schema": schema}}
            _, log_output = run_dbt_and_capture(
                [
                    "--debug",
                    "--log-format=json",
                    "run-operation",
                    "validate_list_relations_without_caching",
                    "--args",
                    str(kwargs),
                ]
            )

            parsed_logs = parse_json_logs(log_output)
            n_relations = find_result_in_parsed_logs(parsed_logs, "n_relations")

            assert n_relations == "n_relations: 1"
