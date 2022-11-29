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
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_cast_bool_to_text import (
    models__test_cast_bool_to_text_yml,
)
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_listagg import BaseListagg

models__test_cast_bool_to_text_sql = """
with data_bool as (
    select 0=1 as input, 'false' as expected
    UNION ALL
    select 1=1 as input, 'true' as expected
),
data_null as (
    select null as input, null as expected
)
select
    {{ cast_bool_to_text("input") }} as actual,
    expected
from data_bool
UNION ALL
select
    {{ cast_bool_to_text("input") }} as actual,
    expected
from data_null
"""


# The `cast_bool_to_text` macro works as expected, but we must alter the test case
# because set operation type conversions do not work properly.
# See https://github.com/MaterializeInc/materialize/issues/3331
class TestCastBoolToText(BaseCastBoolToText):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_cast_bool_to_text.yml": models__test_cast_bool_to_text_yml,
            "test_cast_bool_to_text.sql": self.interpolate_macro_namespace(
                models__test_cast_bool_to_text_sql, "cast_bool_to_text"
            ),
        }

    pass


@pytest.mark.skip(reason="Materialize supports the list_agg() function")
class TestListagg(BaseListagg):
    pass
