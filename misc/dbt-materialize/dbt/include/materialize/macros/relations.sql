-- Copyright 2020 Josh Wills. All rights reserved.
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

{% macro materialize_get_columns(relation) -%}
  {% call statement('get_columns', fetch_result=True, auto_begin=False) %}
    show columns from {{ relation }}
  {% endcall %}
  {{ return(load_result('get_columns').table) }}
{% endmacro %}

{% macro materialize_get_full_views(schema) -%}
  {% call statement('get_full_views', fetch_result=True, auto_begin=False) %}
    show full views from {{ schema }}
  {% endcall %}
  {{ return(load_result('get_full_views').table) }}
{% endmacro %}

{% macro materialize_get_sources(schema) -%}
  {% call statement('get_sources', fetch_result=True, auto_begin=False) %}
    show sources from {{ schema }}
  {% endcall %}
  {{ return(load_result('get_sources').table) }}
{% endmacro %}

{% macro materialize_show_view(relation) -%}
  {% call statement('show_view', fetch_result=True, auto_begin=False) %}
    show create view {{ relation }}
  {% endcall %}
  {{ return(load_result('show_view').table) }}
{% endmacro %}

{% macro materialize_get_schemas() -%}
  {% call statement('get_schemas', fetch_result=True, auto_begin=False) %}
    show extended schemas
  {% endcall %}
  {{ return(load_result('get_schemas').table) }}
{% endmacro %}
