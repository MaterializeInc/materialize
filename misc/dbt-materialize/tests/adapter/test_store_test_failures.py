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
from dbt.contracts.results import TestStatus
from dbt.tests.adapter.store_test_failures_tests.basic import (
    StoreTestFailuresAsBase,
    StoreTestFailuresAsExceptions,
    StoreTestFailuresAsGeneric,
    StoreTestFailuresAsInteractions,
    StoreTestFailuresAsProjectLevelEphemeral,
    StoreTestFailuresAsProjectLevelOff,
    StoreTestFailuresAsProjectLevelView,
    TestResult,
)
from dbt.tests.adapter.store_test_failures_tests.fixtures import (
    models__file_model_but_with_a_no_good_very_long_name,
    models__fine_model,
)
from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import (
    StoreTestFailuresBase,
)
from dbt.tests.util import run_dbt

TEST__MATERIALIZED_VIEW_TRUE = """
{{ config(store_failures_as="materialized_view", store_failures=True) }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__MATERIALIZED_VIEW_FALSE = """
{{ config(store_failures_as="materialized_view", store_failures=False) }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__MATERIALIZED_VIEW_UNSET = """
{{ config(store_failures_as="materialized_view") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


class TestStoreTestFailures(StoreTestFailuresBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fine_model.sql": models__fine_model,
            "fine_model_but_with_a_no_good_very_long_name.sql": models__file_model_but_with_a_no_good_very_long_name,
        }


class TestMaterializeStoreTestFailures(TestStoreTestFailures):
    pass


class TestStoreTestFailuresAsInteractions(StoreTestFailuresAsInteractions):
    pass


class TestStoreTestFailuresAsProjectLevelOff(StoreTestFailuresAsProjectLevelOff):
    pass


class TestStoreTestFailuresAsProjectLevelView(StoreTestFailuresAsProjectLevelView):
    pass


class TestStoreTestFailuresAsGeneric(StoreTestFailuresAsGeneric):
    pass


class TestStoreTestFailuresAsProjectLevelEphemeral(
    StoreTestFailuresAsProjectLevelEphemeral
):
    pass


class TestStoreTestFailuresAsExceptions(StoreTestFailuresAsExceptions):
    def test_tests_run_unsuccessfully_and_raise_appropriate_exception(self, project):
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 1
        result = results[0]
        assert "Compilation Error" in result.message
        assert "'error' is not a valid value" in result.message
        assert (
            "Accepted values are: ['ephemeral', 'table', 'view', 'materialized_view']"
            in result.message
        )


class TestStoreTestFailuresAsProjectLevelMaterializeView(StoreTestFailuresAsBase):
    """
    These scenarios test that `store_failures_as` at the project level takes precedence over `store_failures`
    at the model level.

    Test Scenarios:

    - If `store_failures_as = "materialized_view"` in the project and `store_failures = False` in the model,
    then store the failures in a materialized view.
    - If `store_failures_as = "materialized_view"` in the project and `store_failures = True` in the model,
    then store the failures in a materialized view.
    - If `store_failures_as = "materialized_view"` in the project and `store_failures` is not set,
    then store the failures in a materialized view.
    """

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "results_true.sql": TEST__MATERIALIZED_VIEW_TRUE,
            "results_false.sql": TEST__MATERIALIZED_VIEW_FALSE,
            "results_unset.sql": TEST__MATERIALIZED_VIEW_UNSET,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"tests": {"store_failures_as": "materialized_view"}}

    def test_tests_run_successfully_and_are_stored_as_expected(self, project):
        expected_results = {
            TestResult("results_true", TestStatus.Fail, "materialized_view"),
            TestResult("results_false", TestStatus.Fail, "materialized_view"),
            TestResult("results_unset", TestStatus.Fail, "materialized_view"),
        }
        self.run_and_assert(project, expected_results)
