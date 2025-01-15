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
  {%- set cluster = adapter.generate_final_cluster_name(config.get('cluster', target.cluster)) -%}

  create materialized view {{ relation }}
  {% if cluster %}
    in cluster {{ cluster }}
  {% endif %}

  {# Scheduled refreshes #}
  {%- set refresh_interval = config.get('refresh_interval') -%}
  {%- if refresh_interval -%}
    with (
      {{ materialize__get_refresh_interval_sql(relation, refresh_interval) }}
    )
  {%- endif %}

  {# Contracts and constraints #}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}

    {% set ns = namespace(c_constraints=False, m_constraints=False) %}
    {# Column-level constraints #}
    {% set raw_columns = model['columns'] %}
    {% for c_id, c_details in raw_columns.items() if c_details['constraints'] != [] %}
      {% set ns.c_constraints = True %}
    {%- endfor %}

    {# Model-level constraints #}

    -- NOTE(morsapaes): not_null constraints are not originally supported in
    -- dbt-core at model-level, since model-level constraints are intended for
    -- multi-columns constraints. Any model-level constraint is ignored in
    -- dbt-materialize, albeit silently.
    {% if model['constraints'] != [] %}
      {% set ns.m_constraints = True %}
    {%- endif %}

    {% if ns.c_constraints %}
      with
        {{ get_table_columns_and_constraints() }}
    {%- endif %}
  {%- endif %}

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

{% macro materialize__create_source(relation, sql) -%}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{exceptions.warn("Model contracts cannot be enforced for custom materializations (see dbt-core #7213)")}}
  {%- endif %}

  {%- set cluster = adapter.generate_final_cluster_name(config.get('cluster', target.cluster)) -%}

  create source {{ relation }}
  {% if cluster %}
    in cluster {{ cluster }}
  {% endif %}

  {{ sql }}
  ;
{%- endmacro %}

{% macro materialize__create_source_table(relation, sql) %}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{exceptions.warn("Model contracts cannot be enforced for custom materializations (see dbt-core #7213)")}}
  {%- endif %}

  create table {{ relation }}
  {{ sql }}
{% endmacro %}

{% macro materialize__create_sink(relation, sql) -%}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{exceptions.warn("Model contracts cannot be enforced for custom materializations (see dbt-core #7213)")}}
  {%- endif %}

  {%- set cluster = adapter.generate_final_cluster_name(config.get('cluster', target.cluster)) -%}

  create sink {{ relation }}
  {% if cluster %}
    in cluster {{ cluster }}
  {% endif %}

  {{ sql }}
  ;
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
    {% elif relation.is_materialized_view %}
      drop materialized view if exists {{ relation }} cascade
    {% elif relation.type == 'sink' %}
      drop sink if exists {{ relation }}
    {% elif relation.type == 'source' %}
      drop source if exists {{ relation }} cascade
    {% elif relation.type == 'index' %}
      drop index if exists {{ relation }}
    -- Tables are not supported as a materialization type in dbt-materialize,
    -- but seeds and source tables are materialized as tables.
    {% elif relation.type == 'table' %}
      drop table if exists {{ relation }} cascade
    {% endif %}
  {%- endcall %}
{% endmacro %}

{% macro set_cluster(cluster) %}
  set cluster = {{ cluster }};
{% endmacro %}

{% macro materialize__truncate_relation(relation) -%}
  -- Materialize does not support the TRUNCATE command, so we work around that
  -- by using an unqualified DELETE.
  {% call statement('truncate_relation') -%}
    delete from {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro truncate_relation_sql(relation, cluster) -%}
  -- DELETE requires a scan of the relation, so it needs a valid cluster to run
  -- against. This is expected to fail if no cluster is specified for the
  -- target in the materialization configuration, `profiles.yml`, _and_ the
  -- default cluster for the user is invalid(or intentionally set to
  -- mz_catalog_server, which cannot query user data).
  {% if cluster -%}
      {% do run_query(set_cluster(cluster)) -%}
  {%- endif %}
  {{ truncate_relation(relation) }}
{% endmacro %}

{% macro materialize__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set cluster = index_config.cluster or config.get('cluster', target.cluster) -%}
  {%- set cluster = adapter.generate_final_cluster_name(cluster) -%}

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

{% macro materialize__persist_docs(relation, model, for_relation, for_columns) -%}
  {% if for_relation and config.persist_relation_docs() and model.description %}
    {% do run_query(alter_relation_comment(relation, model.description)) %}
  {% endif %}

  {% if for_columns and config.persist_column_docs() and model.columns %}
    {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
    {% set column_dict = model.columns %}
    -- Materialize does not support running multiple COMMENT ON commands in a
    -- transaction, so we work around that by forcing a transaction per comment
    -- instead
    -- See: https://github.com/MaterializeInc/database-issues/issues/6759
    {% for column_name in column_dict if (column_name in existing_columns) %}
      {% set comment = column_dict[column_name]['description'] %}
      {% set quote = column_dict[column_name]['quote'] %}
      {% do run_query(materialize__alter_column_comment_single(relation, column_name, quote, comment)) %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro materialize__apply_grants(relation, grant_config, should_revoke) -%}
  {{ exceptions.raise_compiler_error(
        """
        dbt-materialize does not implement the grants configuration.

        If this feature is important to you, please reach out!
        """
    )}}
{% endmacro %}

{% macro materialize__get_refresh_interval_sql(relation, refresh_interval_dict) -%}
  {%- set refresh_interval = adapter.parse_refresh_interval(refresh_interval_dict) -%}
    {% if refresh_interval.at -%}
      refresh at '{{ refresh_interval.at }}'
    {%- endif %}
    {% if refresh_interval.at_creation -%}
      refresh at creation
    {%- endif %}
    {% if refresh_interval.every -%}
      {% if refresh_interval.at or refresh_interval.at_creation -%}
        ,
      {%- endif %}
      refresh every '{{ refresh_interval.every }}'
      {% if refresh_interval.aligned_to -%}
        aligned to '{{ refresh_interval.aligned_to }}'
      {%- endif %}
    {%- endif %}
    {% if refresh_interval.on_commit -%}
      refresh on commit
    {%- endif %}
{%- endmacro %}

{% macro materialize__alter_column_comment_single(relation, column_name, quote, comment) %}
  {% set escaped_comment = postgres_escape_comment(comment) %}
  comment on column {{ relation }}.{{ adapter.quote(column_name) if quote else column_name }} is {{ escaped_comment }};
{% endmacro %}

-- In the dbt-adapter we extend the Relation class to include sinks and indexes
{% macro materialize__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
        d.name as database,
        s.name as schema,
        o.name,
        case when o.type = 'materialized-view' then 'materialized_view'
             else o.type
        end as type
    from mz_objects o
    left join mz_sources so on o.id = so.id
    join mz_schemas s on o.schema_id = s.id and s.name = '{{ schema_relation.schema }}'
    join mz_databases d on s.database_id = d.id and d.name = '{{ schema_relation.database }}'
    where o.type in ('table', 'source', 'view', 'materialized-view', 'index', 'sink')
      -- Exclude subsources and progress subsources, which aren't relevant in this
      -- context and can bork the adapter (see database-issues#6162)
      and coalesce(so.type, '') not in ('subsource', 'progress')
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}
