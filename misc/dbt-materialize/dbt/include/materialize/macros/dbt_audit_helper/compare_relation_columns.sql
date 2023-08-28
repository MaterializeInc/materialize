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

{% macro materialize__compare_relation_columns(a_relation, b_relation) %}

with a_cols as (
    {{ audit_helper.get_columns_in_relation_sql(a_relation) }}
),

b_cols as (
    {{ audit_helper.get_columns_in_relation_sql(b_relation) }}
)

select
    column_name,
    a_cols.ordinal_position as a_ordinal_position,
    b_cols.ordinal_position as b_ordinal_position,
    a_cols.data_type as a_data_type,
    b_cols.data_type as b_data_type,
    coalesce(a_cols.ordinal_position = b_cols.ordinal_position, false) as has_ordinal_position_match,
    coalesce(a_cols.data_type = b_cols.data_type, false) as has_data_type_match
from a_cols
full outer join b_cols using (column_name)
order by coalesce(a_cols.ordinal_position, b_cols.ordinal_position)

{% endmacro %}


{% macro materialize__get_columns_in_relation_sql(relation) %}
{#-
From: https://github.com/dbt-labs/dbt/blob/23484b18b71010f701b5312f920f04529ceaa6b2/plugins/postgres/dbt/include/postgres/macros/adapters.sql#L32
Edited to include ordinal_position
-#}
  select
      ordinal_position,
      column_name,
      data_type,
      character_maximum_length,
      numeric_precision,
      numeric_scale

  from {{ relation.information_schema('columns') }}
  where table_name = '{{ relation.identifier }}'
    {% if relation.schema %}
    and table_schema = '{{ relation.schema }}'
    {% endif %}
  order by ordinal_position
{% endmacro %}
