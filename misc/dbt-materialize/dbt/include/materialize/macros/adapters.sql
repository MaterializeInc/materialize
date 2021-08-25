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

{% macro materialize__create_view_as(relation, sql) -%}
  create view {{ relation }}
  as (
    {{ sql }}
  );
{%- endmacro %}

{% macro materialize__create_materialized_view_as(relation, sql) -%}
  create materialized view {{ relation }}
  as (
    {{ sql }}
  );
{%- endmacro %}

{% macro materialize__create_arbitrary_object(sql) -%}
    {{ sql }}
{%- endmacro %}

{% macro materialize__create_schema(relation) -%}
  {% if relation.database -%}
    {{ adapter.verify_database(relation.database) }}
  {%- endif -%}
  {%- call statement('create_schema', auto_begin=False) -%}
    create schema if not exists {{ relation.without_identifier().include(database=False) }}
  {%- endcall -%}
{% endmacro %}

{% macro materialize__drop_schema(relation) -%}
  {% if relation.database -%}
    {{ adapter.verify_database(relation.database) }}
  {%- endif -%}
  {%- call statement('drop_schema', auto_begin=False) -%}
    drop schema if exists {{ relation.without_identifier().include(database=False) }} cascade
  {%- endcall -%}
{% endmacro %}

{% macro materialize__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation', auto_begin=False) -%}
    alter view {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}

{% macro materialize__drop_source(source_name) -%}
  {% call statement('drop_source', auto_begin=False) -%}
    drop source if exists {{ source_name }} cascade
  {%- endcall %}
{% endmacro %}

{% macro materialize__drop_index(index_name) -%}
  {% call statement('drop_index', auto_begin=False) -%}
    drop index if exists {{ index_name }} cascade
  {%- endcall %}
{% endmacro %}

{% macro materialize__drop_sink(sink_name) -%}
  {% call statement('drop_sink', auto_begin=False) -%}
    drop sink if exists {{ sink_name }}
  {%- endcall %}
{% endmacro %}

{% macro materialize__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop view if exists {{ relation }} cascade
  {%- endcall %}
{% endmacro %}

{% macro materialize__list_schemas(database) %}
  {% if database -%}
    {{ adapter.verify_database(database) }}
  {%- endif -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    show schemas from {{ database }}
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro materialize__current_timestamp() -%}
  now()
{%- endmacro %}

{% macro materialize__snapshot_get_time() -%}
  {{ current_timestamp() }}::timestamp without time zone
{%- endmacro %}
