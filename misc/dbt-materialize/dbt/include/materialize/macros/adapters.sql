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
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
    {%- endif %}
  as (
    {{ sql }}
  );
{%- endmacro %}

{% macro materialize__create_materialized_view_as(relation, sql) -%}
  {%- set cluster = config.get('cluster', target.cluster) -%}

  create materialized view {{ relation }}
  {% if cluster %}
    in cluster {{ cluster }}
  {% endif %}

  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {% set nullability_assertions = [] %}
    {% set user_provided_columns = model['columns'] %}
    {% for i in user_provided_columns %}
      {% set col = user_provided_columns[i] %}
      {% for c in col['constraints'] %}
        {% set constraint_type = c['type'] %}
        {% if constraint_type == 'not_null' %}
          {% do nullability_assertions.append(col['name']) %}
        {% endif %}
      {% endfor %}
    {% endfor %}

    {{ get_assert_columns_equivalent(sql) }}
    -- Explicitly throw a warning rather than silently ignore configured
    -- constraints for tables and materialized views.
    -- See /relations/columns_spec_ddl.sql for details.
    {% if not nullability_assertions %}
      {{ get_table_columns_and_constraints(nullability_assertions) }}
    {% endif %}
  {%- endif %}

  {% if nullability_assertions %}
    with (
      {% for col in nullability_assertions %}
        assert not null {{ col }} {{ "," if not loop.last }}
      {% endfor %}
    )
  {% endif %}

  as (
    {{ sql }}
  );
{%- endmacro %}

{% macro materialize__create_arbitrary_object(sql) -%}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{exceptions.warn("Model contracts cannot be enforced for custom materializations (see dbt-core #7213)")}}
    {%- endif %}
    {{ sql }}
{%- endmacro %}

{% macro materialize__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    {% if relation.type == 'view' %}
      alter view {{ from_relation }} rename to {{ target_name }}
    {% else %}
      alter materialized view {{ from_relation }} rename to {{ target_name }}
    {% endif %}
  {%- endcall %}
{% endmacro %}

{% macro materialize__drop_relation(relation) -%}
  {% call statement('drop_relation') -%}
    {% if relation.type == 'view' %}
      drop view if exists {{ relation }} cascade
    {% elif relation.type == 'materializedview' %}
      drop materialized view if exists {{ relation }} cascade
    {% elif relation.type == 'sink' %}
      drop sink if exists {{ relation }}
    {% elif relation.type == 'source' %}
      drop source if exists {{ relation }} cascade
    {% elif relation.type == 'index' %}
      drop index if exists {{ relation }}
    {% endif %}
  {%- endcall %}
{% endmacro %}

{% macro materialize__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    delete from {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro materialize__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set cluster = index_config.cluster or config.get('cluster', target.cluster) -%}
    create
    {% if index_config.default -%}
      default
    {%- endif %}
    index
    {% if index_config.name -%}
      "{{ index_config.name }}"
    {%- endif %}
    {% if cluster -%}
      in cluster {{ cluster }}
    {%- endif %}
    on {{ relation }}
    {% if index_config.columns -%}
      ({{ ", ".join(index_config.columns) }})
    {%- endif %};
{%- endmacro %}


-- In the dbt-adapter we extend the Relation class to include sinks and indexes
{% macro materialize__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
        d.name as database,
        s.name as schema,
        o.name,
        case when o.type = 'materialized-view' then 'materializedview'
             else o.type
        end as type
    from mz_objects o
    left join mz_sources so on o.id = so.id
    join mz_schemas s on o.schema_id = s.id and s.name = '{{ schema_relation.schema }}'
    join mz_databases d on s.database_id = d.id and d.name = '{{ schema_relation.database }}'
    where o.type in ('table', 'source', 'view', 'materialized-view', 'index', 'sink')
      --Exclude subsources and progress subsources, which aren't relevant in this
      --context and can bork the adapter (see #20483)
      and coalesce(so.type, '') not in ('subsource', 'progress')
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}
