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
from dbt.tests.adapter.unit_testing.test_case_insensitivity import (
    BaseUnitTestCaseInsensivity,
)
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.util import run_dbt, run_dbt_and_capture

my_model = """
{{ config(materialized='view') }}

SELECT 1 AS id
"""

my_model_unit_test_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given: []
    expect:
      rows:
        - {id: 1}
"""


class TestMaterializeUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["TIMESTAMPTZ '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["'1'::numeric", "1"],
            [
                """'{"bar": "baz", "balance": 7.77, "active": false}'::json""",
                """'{"bar": "baz", "balance": 7.77, "active": false}'""",
            ],
            ["MZ_TIMESTAMP '2023-05-21 12:34:56'", "2023-05-21 12:34:56"],
            # TODO: array types
            # ["LIST[1, 2, 3]", """'[1, 2, 3]'"""],
            # ["LIST['a', 'b', 'c']", """'["a", "b", "c"]'"""],
            # ["ARRAY[1,2,3]", """'{1, 2, 3}'"""],
            # ["ARRAY[1.0,2.0,3.0]", """'{1.0, 2.0, 3.0}'"""],
            # ["ARRAY[1::numeric,2::numeric,3::numeric]", """'{1.0, 2.0, 3.0}'"""],
            # ["ARRAY['a','b','c']", """'{"a", "b", "c"}'"""],
            # ["ARRAY[true,true,false]", """'{true, true, false}'"""],
            # ["ARRAY[DATE '2020-01-02']", """'{"2020-01-02"}'"""],
            # ["ARRAY[TIMESTAMP '2013-11-03 00:00:00-0']", """'{"2013-11-03 00:00:00-0"}'"""],
            # ["ARRAY[TIMESTAMPTZ '2013-11-03 00:00:00-0']", """'{"2013-11-03 00:00:00-0"}'"""],
        ]


class TestMaterializeUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestMaterializeUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass


class TestMaterializeUnitTestCluster:
    """The unit test materialization must run its query against the configured
    cluster, like the data test materialization does. Unit test queries only
    reference fixtures, so Materialize answers them without a cluster, which is
    why this checks the statement dbt issues rather than the query result."""

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
            "my_model.sql": my_model,
            "my_model_unit_test.yml": my_model_unit_test_yml,
        }

    def test_unit_test_sets_configured_cluster(self, project):
        run_dbt(["run"])

        _, output = run_dbt_and_capture(["--debug", "test"])
        assert "set cluster = quickstart" in output
