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
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseModelConstraintsRuntimeEnforcement
)


class TestBaseTableConstraintsColumnsEqual(BaseTableConstraintsColumnsEqual):
    pass


class TestBaseViewConstraintsColumnsEqual(BaseViewConstraintsColumnsEqual):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestBaseIncrementalConstraintsColumnsEqual(BaseIncrementalConstraintsColumnsEqual):
    pass

class TestBaseConstraintsRuntimeDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
    pass

class TestBaseConstraintsRollback(BaseConstraintsRollback):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestBaseIncrementalConstraintsRuntimeDdlEnforcement(BaseIncrementalConstraintsRuntimeDdlEnforcement):
    pass

@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestBaseIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
    pass

class TestBaseModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    pass