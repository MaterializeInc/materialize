-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

{% macro materialize_get_columns(relation) -%}
  {% call statement('get_columns', fetch_result=True, auto_begin=True) %}
    show columns from {{ relation }}
  {% endcall %}
  {{ return(load_result('get_columns').table) }}
{% endmacro %}

{% macro materialize_get_full_views(schema) -%}
  {% call statement('get_full_views', fetch_result=True, auto_begin=True) %}
    show full views from {{ schema }}
  {% endcall %}
  {{ return(load_result('get_full_views').table) }}
{% endmacro %}

{% macro materialize_get_sources(schema) -%}
  {% call statement('get_sources', fetch_result=True, auto_begin=True) %}
    show sources from {{ schema }}
  {% endcall %}
  {{ return(load_result('get_sources').table) }}
{% endmacro %}

{% macro materialize_show_view(relation) -%}
  {% call statement('show_view', fetch_result=True, auto_begin=True) %}
    show create view {{ relation }}
  {% endcall %}
  {{ return(load_result('show_view').table) }}
{% endmacro %}

{% macro materialize_get_schemas() -%}
  {% call statement('get_schemas', fetch_result=True, auto_begin=True) %}
    show extended schemas
  {% endcall %}
  {{ return(load_result('get_schemas').table) }}
{% endmacro %}
