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

-- Most of these macros are direct copies of their PostgreSQL counterparts.
-- See: https://github.com/dbt-labs/dbt-core/blob/13b18654f/plugins/postgres/dbt/include/postgres/macros/adapters.sql

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

{% macro materialize__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter view {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}

{% macro materialize__drop_source(source_name) -%}
  {% call statement('drop_source') -%}
    drop source if exists {{ source_name }} cascade
  {%- endcall %}
{% endmacro %}

{% macro materialize__drop_index(index_name) -%}
  {% call statement('drop_index') -%}
    drop index if exists {{ index_name }}
  {%- endcall %}
{% endmacro %}

{% macro materialize__drop_sink(sink_name) -%}
  {% call statement('drop_sink') -%}
    drop sink if exists {{ sink_name }}
  {%- endcall %}
{% endmacro %}

{% macro materialize__drop_relation(relation) -%}
  {% call statement('drop_relation') -%}
    drop view if exists {{ relation }} cascade
  {%- endcall %}
{% endmacro %}

{% macro materialize__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set comma_separated_columns = ", ".join(index_config.columns) -%}
  {%- set index_name = index_config.render(relation) -%}
    create index if not exists
      "{{ index_name }}"
      on {{ relation }} {% if index_config.type -%}
        using {{ index_config.type }}
  {%- endif %}
  ({{ comma_separated_columns }});
{%- endmacro %}
