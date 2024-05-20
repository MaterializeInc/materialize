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

{% macro materialize__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    {%- if select_sql_header is not none -%}
    {{ select_sql_header }}
    {%- endif -%}
    --#dbt_sbq_parse_header#--
    select * from (
        {{ select_sql }}
    ) as __dbt_sbq
{% endmacro %}

{% macro materialize__get_empty_schema_sql(columns) %}
    {%- set col_err = [] -%}
    {%- set col_naked_numeric = [] -%}
    select
    {% for i in columns %}
      {%- set col = columns[i] -%}
      {%- if col['data_type'] is not defined -%}
        {%- do col_err.append(col['name']) -%}
      {#-- If this column's type is just 'numeric' then it is missing precision/scale, raise a warning --#}
      {%- elif col['data_type'].strip().lower() in ('numeric', 'decimal', 'number') -%}
        {%- do col_naked_numeric.append(col['name']) -%}
      {%- endif -%}
      {% set col_name = adapter.quote(col['name']) if col.get('quote') else col['name'] %}
      -- NOTE(morsapaes): in a future release of dbt, the default macro will use
      -- the global cast macro, as modified here, and we can remove this custom
      -- override. See: https://github.com/dbt-labs/dbt-adapters/pull/165
      {{ cast("null", col['data_type']) }} as {{ col_name }}{{ ", " if not loop.last }}
    {%- endfor -%}
    {%- if (col_err | length) > 0 -%}
      {{ exceptions.column_type_missing(column_names=col_err) }}
    {%- elif (col_naked_numeric | length) > 0 -%}
      {{ exceptions.warn("Detected columns with numeric type and unspecified precision/scale, this can lead to unintended rounding: " ~ col_naked_numeric ~ "`") }}
    {%- endif -%}
{% endmacro %}
