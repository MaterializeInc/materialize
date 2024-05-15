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
from dbt.tests.adapter.dbt_clone.fixtures import (
    custom_can_clone_tables_false_macros_sql,
    get_schema_name_sql,
    infinite_macros_sql,
    macros_sql,
)
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseClone
from dbt.tests.util import run_dbt


class BaseCloneOverride(BaseClone):
    @pytest.fixture(scope="class")
    def snapshots(self):
        pass

    def run_and_save_state(self, project_root, with_snapshot=False):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["run"])
        assert len(results) == 2
        results = run_dbt(["test"])
        assert len(results) == 2

        # copy files
        self.copy_state(project_root)


class BaseCloneNotPossible(BaseCloneOverride):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
            "my_can_clone_tables.sql": custom_can_clone_tables_false_macros_sql,
            "infinite_macros.sql": infinite_macros_sql,
            "get_schema_name.sql": get_schema_name_sql,
        }

    def test_can_clone_false(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root, with_snapshot=True)

        clone_args = [
            "clone",
            "--defer",
            "--state",
            "state",
            "--target",
            "otherschema",
        ]

        results = run_dbt(clone_args)
        assert len(results) == 3

        schema_relations = project.adapter.list_relations(
            database=project.database, schema=other_schema
        )
        assert all(r.type == "view" for r in schema_relations)

        # objects already exist, so this is a no-op
        results = run_dbt(clone_args)
        assert len(results) == 3
        assert all("no-op" in r.message.lower() for r in results)

        # recreate all objects
        results = run_dbt([*clone_args, "--full-refresh"])
        assert len(results) == 3

        # select only models this time
        results = run_dbt([*clone_args, "--resource-type", "model"])
        assert len(results) == 2
        assert all("no-op" in r.message.lower() for r in results)


class TestMaterializeCloneNotPossible(BaseCloneNotPossible):
    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_seeds"
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass
