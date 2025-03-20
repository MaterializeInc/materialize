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

{%- materialization test, adapter='materialize' -%}

  {% set relations = [] %}

  -- For an overview of the precedence logic behind store_failures and
  -- store_failures_at, see dbt-core #8653.
  {% if should_store_failures() %}

    {% set identifier = model['alias'] %}
    {% set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}

    {% set store_failures_as = config.get('store_failures_as') %}
    {% if store_failures_as == none %}{% set store_failures_as = 'table' %}{% endif %}
    {% if store_failures_as not in ['table', 'view', 'materialized_view'] %}
        {{ exceptions.raise_compiler_error(
            "'" ~ store_failures_as ~ "' is not a valid value for `store_failures_as`. "
            "Accepted values are: ['ephemeral', 'table', 'view', 'materialized_view']"
        ) }}
    {% endif %}

    {% set target_relation = api.Relation.create(
        identifier=identifier, schema=schema, database=database, type=store_failures_as) -%} %}

    {% if old_relation %}
        {% do adapter.drop_relation(old_relation) %}
    {% endif %}

    {% if store_failures_as == 'view' %}
        {% call statement(auto_begin=True) %}
            {{ materialize__create_view_as(target_relation, sql) }}
        {% endcall %}
    {% else %}
        {% call statement(auto_begin=True) %}
            {{ materialize__create_materialized_view_as(target_relation, sql) }}
        {% endcall %}
    {% endif %}

    {% do relations.append(target_relation) %}

    {% set main_sql %}
        select *
        from {{ target_relation }}
    {% endset %}

    {{ adapter.commit() }}

  {% else %}

      {% set main_sql = sql %}

  {% endif %}

  {% set limit = config.get('limit') %}
  {% set fail_calc = config.get('fail_calc') %}
  {% set warn_if = config.get('warn_if') %}
  {% set error_if = config.get('error_if') %}
  -- Tests compile to ad-hoc queries, which need a cluster to run against. If no
  -- cluster is configured for data tests, use the target cluster from
  -- `profiles.yml`. If none exists, fall back to the default cluster
  -- configured for the connected user.
  -- See: misc/dbt-materialize/dbt/adapters/materialize/connections.py
  {%- set cluster = adapter.generate_final_cluster_name(config.get('cluster', target.cluster)) %}

  {% call statement('main', fetch_result=True) -%}

    {{ get_test_sql(main_sql, fail_calc, warn_if, error_if, limit, cluster)}}

  {%- endcall %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
