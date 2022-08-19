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
from dbt.tests.util import check_relations_equal, run_dbt
from fixtures import (  # test_sink, todo: re-enable once #14195 lands
    actual_indexes,
    expected_indexes,
    test_index,
    test_materialized_view,
    test_materialized_view_index,
    test_source,
    test_source_index,
    test_view_index,
)


class TestCustomMaterializations:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "custom_materializations"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_indexes.csv": expected_indexes,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_materialized_view.sql": test_materialized_view,
            "test_materialized_view_index.sql": test_materialized_view_index,
            "test_view_index.sql": test_view_index,
            "test_source.sql": test_source,
            "test_index.sql": test_index,
            "test_source_index.sql": test_source_index,
            # "test_sink.sql": test_sink, todo: re-enable once #14195 lands
            "actual_indexes.sql": actual_indexes,
        }

    def test_custom_materializations(self, project):
        # seed seeds
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1
        # run models
        results = run_dbt(["run"])
        # run result length
        # assert len(results) == 8 todo: re-enable once #14195 lands
        assert len(results) == 7

        # relations_equal
        check_relations_equal(
            project.adapter, ["test_materialized_view", "test_view_index"]
        )

        check_relations_equal(project.adapter, ["actual_indexes", "expected_indexes"])

        # TODO(morsapaes): add test that ensures that the source/sink emit the
        # correct data once sinks land.
        # From benesch: Ideally we'd just ingest the sink back into Materialize
        # with the source and then use `relations_equal` with `test_materialized_view`.
