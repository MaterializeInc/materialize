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
from dbt.tests.util import check_result_nodes_by_name, run_dbt
from fixtures import not_null, test_materialized_view, unique


class TestDataTest:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "data_tests"}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_materialized_view.sql": test_materialized_view,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "unique.sql": unique,
            "not_null.sql": not_null,
        }

    def test_store_failures(self, project):
        # run models
        results = run_dbt(["run"])
        # run result length
        assert len(results) == 1

        results = run_dbt(["test"], expect_pass=False)  # expect failing test
        assert len(results) == 2

        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["fail", "pass"]
