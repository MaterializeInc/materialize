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

{%- materialization unit, adapter='materialize' -%}

  {% set relations = [] %}

  {% set expected_rows = config.get('expected_rows') %}
  {% set expected_sql = config.get('expected_sql') %}
  {% set tested_expected_column_names = expected_rows[0].keys() if (expected_rows | length) > 0 else get_columns_in_query(sql) %}

  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set temp_relation = make_temp_relation(target_relation) -%}

  {%- call statement(auto_begin=True) -%}
    {{ materialize__create_view_as(temp_relation, get_empty_subquery_sql(sql)) }}
  {%- endcall -%}

  {%- set columns_in_relation = adapter.get_columns_in_relation(temp_relation) -%}
  {%- set column_name_to_data_types = {} -%}
  {%- for column in columns_in_relation -%}
    {%- do column_name_to_data_types.update({column.name|lower: column.data_type}) -%}
  {%- endfor -%}

  {% if not expected_sql %}
    {% set expected_sql = get_expected_sql(expected_rows, column_name_to_data_types) %}
  {% endif %}

  {% set unit_test_sql = get_unit_test_sql(sql, expected_sql, tested_expected_column_names) %}

  {% call statement('main', fetch_result=True) -%}
    {{ unit_test_sql }}
  {%- endcall %}

  {% do adapter.drop_relation(temp_relation) %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
