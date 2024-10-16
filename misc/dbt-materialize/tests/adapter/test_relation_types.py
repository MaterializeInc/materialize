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
from dbt.contracts.results import CatalogArtifact
from dbt.tests.util import run_dbt
from fixtures import (
    test_materialized_view,
    test_seed,
    # test_sink,
    test_source,
    # test_subsources,
    test_table_index,
    test_view_index,
)


class TestCatalogRelationTypes:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"test_seed.csv": test_seed}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "test_materialized_view.sql": test_materialized_view,
            # "test_sink.sql": test_sink,
            "test_source.sql": test_source,
            # "test_subsource.sql": test_subsources,
            "test_table.sql": test_table_index,
            "test_view.sql": test_view_index,
        }

    @pytest.fixture(scope="class", autouse=True)
    def docs(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield run_dbt(["docs", "generate"])

    @pytest.mark.parametrize(
        "node_name,relation_type",
        [
            ("seed.test.test_seed", "table"),
            ("model.test.test_materialized_view", "materialized_view"),
            # ("model.test.test_sink", "sink"),
            ("model.test.test_source", "source"),
            # ("model.test.test_subsource", "subsource"),
            # NOTE(dehume): Tables are materialized as materialized views
            # https://github.com/MaterializeInc/database-issues/issues/1623
            ("model.test.test_table", "materialized_view"),
            ("model.test.test_view", "view"),
        ],
    )
    def test_relation_types_populate_correctly(
        self, docs: CatalogArtifact, node_name: str, relation_type: str
    ):
        assert node_name in docs.nodes
        node = docs.nodes[node_name]
        assert node.metadata.type == relation_type
