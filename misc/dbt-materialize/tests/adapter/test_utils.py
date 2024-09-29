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
from dbt.tests.adapter.utils.fixture_cast_bool_to_text import (
    models__test_cast_bool_to_text_yml,
)
from dbt.tests.adapter.utils.fixture_date_spine import (
    models__test_date_spine_yml,
)
from dbt.tests.adapter.utils.fixture_get_intervals_between import (
    models__test_get_intervals_between_yml,
)
from dbt.tests.adapter.utils.test_cast import BaseCast
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampAware
from dbt.tests.adapter.utils.test_date_spine import BaseDateSpine
from dbt.tests.adapter.utils.test_generate_series import BaseGenerateSeries
from dbt.tests.adapter.utils.test_get_intervals_between import BaseGetIntervalsBetween
from dbt.tests.adapter.utils.test_get_powers_of_two import BaseGetPowersOfTwo
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps

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

# The upstream test_date_spine has PostgreSQL-specific logic that doesn't
# get picked up because it conditions on `target == "postgres"`. So we just
# hardcode that PostgreSQL-specific logic here.
models__test_date_spine_sql = """
with generated_dates as (
    {{ date_spine("day", "'2023-09-01'::date", "'2023-09-10'::date") }}
), expected_dates as (
    select '2023-09-01'::date as expected
    union all
    select '2023-09-02'::date as expected
    union all
    select '2023-09-03'::date as expected
    union all
    select '2023-09-04'::date as expected
    union all
    select '2023-09-05'::date as expected
    union all
    select '2023-09-06'::date as expected
    union all
    select '2023-09-07'::date as expected
    union all
    select '2023-09-08'::date as expected
    union all
    select '2023-09-09'::date as expected
), joined as (
    select
        generated_dates.date_day,
        expected_dates.expected
    from generated_dates
    left join expected_dates on generated_dates.date_day = expected_dates.expected
)

SELECT * from joined
"""

# The upstream get_intervals_between has PostgreSQL-specific logic that doesn't
# get picked up because it conditions on `target == "postgres"`. So we just
# hardcode that PostgreSQL-specific logic here.
models__test_get_intervals_between_sql = """
SELECT
    {{ get_intervals_between("'2023-09-01'::date", "'2023-09-12'::date", "day") }} as intervals,
  11 as expected
"""


class TestCast(BaseCast):
    pass


# The `cast_bool_to_text` macro works as expected, but we must alter the test case
# because set operation type conversions do not work properly.
# See https://github.com/MaterializeInc/database-issues/issues/1065
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


# Materialize implements `timestamp`, which is an equivalent abbreviation for
# `timestamp without timezone` (as required by the SQL standard)
# See https://www.postgresql.org/docs/current/datatype-datetime.html
class TestCurrentTimestamps(BaseCurrentTimestamps):
    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "current_timestamp": "timestamp with time zone",
            "current_timestamp_in_utc_backcompat": "timestamp without time zone",
            "current_timestamp_backcompat": "timestamp without time zone",
        }

    pass


class TestCurrentTimestamp(BaseCurrentTimestampAware):
    pass


class TestDateSpine(BaseDateSpine):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_spine.yml": models__test_date_spine_yml,
            "test_date_spine.sql": self.interpolate_macro_namespace(
                models__test_date_spine_sql, "date_spine"
            ),
        }


class TestGenerateSeries(BaseGenerateSeries):
    pass


class TestGetIntervalsBeteween(BaseGetIntervalsBetween):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_get_intervals_between.yml": models__test_get_intervals_between_yml,
            "test_get_intervals_between.sql": self.interpolate_macro_namespace(
                models__test_get_intervals_between_sql, "get_intervals_between"
            ),
        }


class TestGetPowersOfTwo(BaseGetPowersOfTwo):
    pass
