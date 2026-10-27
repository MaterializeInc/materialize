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
from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import (
    BaseMergeExcludeColumns,
)
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
from dbt.tests.util import run_dbt_and_capture


@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestMergeExcludeColumns(BaseMergeExcludeColumns):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    pass


incremental_model = """
{{ config(materialized='incremental') }}

SELECT 1 AS id
"""


class TestIncrementalNotSupported:
    """The incremental materialization must explain itself instead of failing
    on the way to raising the error."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_incremental_model.sql": incremental_model}

    def test_incremental_model_explains_why_it_is_unsupported(self, project):
        _, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "does not support incremental models" in output
