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
from fixtures import (
    actual_indexes,
    expected_indexes_binary,
    expected_indexes_cloud,
    test_index,
    test_materialized_view,
    test_materialized_view_index,
    test_sink,
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
            "expected_indexes_cloud.csv": expected_indexes_cloud,
            "expected_indexes_binary.csv": expected_indexes_binary,
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
            "test_sink.sql": test_sink,
            "actual_indexes.sql": actual_indexes,
        }

    def test_custom_materializations(self, project):
        # seed seeds
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 2
        # run models
        results = run_dbt(["run"])
        # run result length
        assert len(results) == 8
        # relations_equal
        check_relations_equal(
            project.adapter, ["test_materialized_view", "test_view_index"]
        )

        mz_version = project.run_sql(f"select mz_version()", fetch="one")[0]
        if project.adapter.is_materialize_cloud(mz_version):
            check_relations_equal(
                project.adapter, ["actual_indexes", "expected_indexes_cloud"]
            )
        else:
            check_relations_equal(
                project.adapter, ["actual_indexes", "expected_indexes_binary"]
            )

        # TODO(morsapaes): add test that ensures that the source/sink emit the
        # correct data once sinks land.
        # From benesch: Ideally we'd just ingest the sink back into Materialize
        # with the source and then use `relations_equal` with `test_materialized_view`.
