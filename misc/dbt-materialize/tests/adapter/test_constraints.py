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
from dbt.tests.adapter.constraints.fixtures import (
    model_contract_header_schema_yml,
)
from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintQuotedColumn,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsColumnsEqual,
    BaseIncrementalConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalContractSqlHeader,
    BaseModelConstraintsRuntimeEnforcement,
    BaseTableConstraintsColumnsEqual,
    BaseTableContractSqlHeader,
    BaseViewConstraintsColumnsEqual,
)

# Materialize does not support time zones or materializing session variables, so
# override the original fixture.
override_model_contract_sql_header_sql = """
{{
  config(
    materialized = "table"
  )
}}

{% call set_sql_header(config) %}
set session time zone 'UTC';
{%- endcall %}
select 'UTC' as column_name
"""


class TestTableConstraintsColumnsEqualMaterialize(BaseTableConstraintsColumnsEqual):
    pass


class TestViewConstraintsColumnsEqualMaterialize(BaseViewConstraintsColumnsEqual):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support constraints")
class TestConstraintQuotedColumnMaterialize(BaseConstraintQuotedColumn):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestIncrementalContractSqlHeaderMaterialize(BaseIncrementalContractSqlHeader):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestIncrementalConstraintsColumnsEqualMaterialize(
    BaseIncrementalConstraintsColumnsEqual
):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestIncrementalConstraintsRollbackMaterialize(BaseIncrementalConstraintsRollback):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestIncrementalConstraintsRuntimeDdlEnforcementMaterialize(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support constraints")
class TestConstraintsRuntimeDdlEnforcementMaterialize(
    BaseConstraintsRuntimeDdlEnforcement
):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support constraints")
class TestModelConstraintsRuntimeEnforcementMaterialize(
    BaseModelConstraintsRuntimeEnforcement
):
    pass


class TestTableContractSqlHeaderMaterialize(BaseTableContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": override_model_contract_sql_header_sql,
            "constraints_schema.yml": model_contract_header_schema_yml,
        }
