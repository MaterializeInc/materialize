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
from dbt.tests.util import run_dbt, run_sql_with_adapter
from fixtures import (
    contract_invalid_cluster_schema_yml,
    contract_pseudo_types_yml,
    nullability_assertions_schema_yml,
    test_materialized_view,
    test_pseudo_types,
    test_view,
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


class TestNullabilityAssertions:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "nullability_assertions_schema.yml": nullability_assertions_schema_yml,
            "test_nullability_assertions_ddl.sql": test_materialized_view,
        }

    def test_ddl_enforcement(self, project):

        nullability_assertions_model = ".".join(
            [
                project.adapter.config.credentials.database,
                project.adapter.config.credentials.schema,
                "test_nullability_assertions_ddl",
            ]
        )

        run_dbt(["run"])

        # confirm that the correct constraint DDL is generated
        nullability_assertions_ddl = run_sql_with_adapter(
            project.adapter,
            f"SHOW CREATE MATERIALIZED VIEW {nullability_assertions_model}",
            fetch="one",
        )
        assert (
            'ASSERT NOT NULL = "a", ASSERT NOT NULL = "b"'
            in nullability_assertions_ddl[1]
        )


class TestTableContractSqlHeaderMaterialize(BaseTableContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": override_model_contract_sql_header_sql,
            "constraints_schema.yml": model_contract_header_schema_yml,
        }


class TestContractInvalidCluster:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "contract_invalid_cluster.yml": contract_invalid_cluster_schema_yml,
            "contract_invalid_cluster.sql": test_view,
        }

    # In the absence of the pre-installed `quickstart` cluster, Materialize should
    # not error if data contracts are enforced.
    # See database-issues#7091: https://github.com/MaterializeInc/database-issues/issues/7091
    def test_materialize_drop_quickstart(self, project):
        project.run_sql("DROP CLUSTER quickstart CASCADE")

        run_dbt(["run", "--models", "contract_invalid_cluster"], expect_pass=True)

        project.run_sql("CREATE CLUSTER quickstart SIZE = '1'")


class TestContractPseudoTypes:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "contract_pseudo_types.yml": contract_pseudo_types_yml,
            "contract_pseudo_types.sql": test_pseudo_types,
        }

    # Pseudo-types in Materialize cannot be cast using the cast() function, so we
    # special-handle their NULL casting for contract validation.
    # See database-issues#5211: https://github.com/MaterializeInc/database-issues/issues/5211
    def test_pseudo_types(self, project):
        run_dbt(["run", "--models", "contract_pseudo_types"], expect_pass=True)
