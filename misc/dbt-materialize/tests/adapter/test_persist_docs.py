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
import os

import pytest
from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)
from dbt.tests.util import run_dbt
from fixtures import (
    cross_database_reference_schema_yml,
    cross_database_reference_sql,
)


class TestPersistDocsMaterialize(BasePersistDocs):
    pass


class TestPersistDocsColumnMissingMaterialize(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumnMaterialize(
    BasePersistDocsCommentOnQuotedColumn
):
    pass


class TestCrossDatabaseDocs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cross_db_reference.sql": cross_database_reference_sql,
            "schema.yml": cross_database_reference_schema_yml,
        }

    def test_cross_db_docs_generate(self, project):
        results = run_dbt(["run"])

        # Run dbt docs generate to generate documentation
        results = run_dbt(["docs", "generate"])
        assert results is not None

        # Validate the generated documentation
        with open(os.path.join(project.project_root, "target/catalog.json")) as f:
            catalog = json.load(f)

        assert "source.test.test_database_1.table1" in catalog["sources"]
        assert "source.test.test_database_2.table2" in catalog["sources"]

        cross_db_reference = catalog["nodes"]["model.test.cross_db_reference"]
        assert "id" in cross_db_reference["columns"]
