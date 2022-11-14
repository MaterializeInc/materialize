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
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampAware
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesQuote,
)
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral

models__test_cast_bool_to_text_sql = """
with data_bool as (
    select 0=1 as input, 'false' as expected union all
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


class TestAnyValue(BaseAnyValue):
    pass


@pytest.mark.skip(
    reason="Materialize does not yet support bool_or() see https://github.com/MaterializeInc/materialize/issues/3154"
)
class TestBoolOr(BaseBoolOr):
    pass


# The cast_bool_to_text macro works as expected, but we must alter the test case
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


class TestConcat(BaseConcat):
    pass


class TestDateAdd(BaseDateAdd):
    pass


class TestDateDiff(BaseDateDiff):
    pass


class TestDateTrunc(BaseDateTrunc):
    pass


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesQuote):
    pass


class TestExcept(BaseExcept):
    pass


class TestHash(BaseHash):
    pass


class TestIntersect(BaseIntersect):
    pass


class TestLastDay(BaseLastDay):
    pass


class TestLength(BaseLength):
    pass


## The base listagg method in dbt takes a delimiter, and returns text.
## This behavior differs in Materialize. https://materialize.com/docs/sql/functions/list_agg
@pytest.mark.skip(
    reason="Materialize supports list_agg() as a built-in function: https://materialize.com/docs/sql/functions/list_agg"
)
class TestListagg(BaseListagg):
    pass


class TestPosition(BasePosition):
    pass


class TestReplace(BaseReplace):
    pass


class TestRight(BaseRight):
    pass


class TestSafeCast(BaseSafeCast):
    pass


class TestSplitPart(BaseSplitPart):
    pass


class TestStringLiteral(BaseStringLiteral):
    pass


@pytest.mark.skip(reason="Materialize does not yet support array_append().")
class TestArrayAppend(BaseArrayAppend):
    pass


@pytest.mark.skip(reason="Materialize does not yet support array_concat().")
class TestArrayConcat(BaseArrayConcat):
    pass


@pytest.mark.skip(reason="Materialize does not yet support array_concat().")
class TestArrayConcat(BaseArrayConstruct):
    pass


class TestCurrentTimestamp(BaseCurrentTimestampAware):
    pass
