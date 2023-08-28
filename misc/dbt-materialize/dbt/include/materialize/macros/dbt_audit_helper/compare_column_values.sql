-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License in the LICENSE file at the
-- root of this repository, or online at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

{% macro materialize__compare_column_values(a_query, b_query, primary_key, column_to_compare) -%}
with a_query as (
    {{ a_query }}
),

b_query as (
    {{ b_query }}
),

joined as (
    select
        coalesce(a_query.{{ primary_key }}, b_query.{{ primary_key }}) as {{ primary_key }},
        a_query.{{ column_to_compare }} as a_query_value,
        b_query.{{ column_to_compare }} as b_query_value,
        case
            when a_query.{{ column_to_compare }} = b_query.{{ column_to_compare }} then '✅: perfect match'
            when a_query.{{ column_to_compare }} is null and b_query.{{ column_to_compare }} is null then '✅: both are null'
            when a_query.{{ primary_key }} is null then '🤷: ‍missing from a'
            when b_query.{{ primary_key }} is null then '🤷: missing from b'
            when a_query.{{ column_to_compare }} is null then '🤷: value is null in a only'
            when b_query.{{ column_to_compare }} is null then '🤷: value is null in b only'
            when a_query.{{ column_to_compare }} != b_query.{{ column_to_compare }} then '🙅: ‍values do not match'
            else 'unknown' -- this should never happen
        end as match_status,
        case
            when a_query.{{ column_to_compare }} = b_query.{{ column_to_compare }} then 0
            when a_query.{{ column_to_compare }} is null and b_query.{{ column_to_compare }} is null then 1
            when a_query.{{ primary_key }} is null then 2
            when b_query.{{ primary_key }} is null then 3
            when a_query.{{ column_to_compare }} is null then 4
            when b_query.{{ column_to_compare }} is null then 5
            when a_query.{{ column_to_compare }} != b_query.{{ column_to_compare }} then 6
            else 7 -- this should never happen
        end as match_order

    from a_query

    full outer join b_query on a_query.{{ primary_key }} = b_query.{{ primary_key }}
),

aggregated as (
    select
        match_status,
        match_order,
        count(*) as count_records
    from joined

    group by match_status, match_order
)

select
    match_status,
    count_records,
    -- TODO(morsapaes): Materialize doesn't support window functions yet,
    -- so adding a ugly hack. Once we do, revert to the original:
    -- round(100.0 * count_records / sum(count_records) over (), 2) as percent_of_total
    round(100.0 * count_records / (select sum(count_records) from aggregated), 2) as percent_of_total

from aggregated

order by match_order

{% endmacro %}
