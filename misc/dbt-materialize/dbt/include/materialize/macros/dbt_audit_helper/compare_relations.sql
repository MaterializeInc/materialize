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

{% macro materialize__compare_relations(a_relation, b_relation, exclude_columns=[], primary_key=None, summarize=true) %}

{%- set a_columns = adapter.get_columns_in_relation(a_relation) -%}

{% set check_columns=audit_helper.pop_columns(a_columns, exclude_columns) %}

{% set check_cols_csv = check_columns | map(attribute='quoted') | join(', ') %}

{% set a_query %}
select
    {{ check_cols_csv }}

from {{ a_relation }}
{% endset %}

{% set b_query %}
select
    {{ check_cols_csv }}

from {{ b_relation }}
{% endset %}

{{ dbt_materialize_utils.materialize__compare_queries(a_query, b_query, primary_key) }}

{% endmacro %}
