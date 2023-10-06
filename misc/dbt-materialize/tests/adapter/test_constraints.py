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

from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseTableContractSqlHeader,
    BaseIncrementalContractSqlHeader,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseModelConstraintsRuntimeEnforcement,
    BaseConstraintQuotedColumn,
)

class MaterializeTableConstraintsColumnsEqual(BaseTableConstraintsColumnsEqual):
    pass

class MaterializeViewConstraintsColumnsEqual(BaseViewConstraintsColumnsEqual):
    pass

class MaterializeViewConstraintsColumnsEqual(BaseViewConstraintsColumnsEqual):
    pass

class MaterializeModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    pass

class MaterializeConstraintQuotedColumn(BaseConstraintQuotedColumn):
    pass

@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestIncrementalContractSqlHeader(BaseIncrementalContractSqlHeader):
    pass

@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestIncrementalConstraintsColumnsEqual(BaseIncrementalConstraintsColumnsEqual):
    pass

@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestIncrementalConstraintsRuntimeDdlEnforcement(BaseIncrementalConstraintsRuntimeDdlEnforcement):
    pass

@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestIncrementalConstraintsRuntimeDdlEnforcement(BaseIncrementalConstraintsRuntimeDdlEnforcement):
    pass