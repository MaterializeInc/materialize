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

{% macro deploy_promote(wait=False, poll_interval=15, dry_run=False) %}
{#
  Performs atomic deployment of current dbt targets to production,
  based on the deployment configuration specified in the dbt_project.yml file.
  This macro ensures all deployment targets, including schemas and clusters,
  are fully hydrated and deployed together as a single atomic operation.
  If any part of the deployment fails, the entire deployment is rolled back
  to maintain consistency and prevent partial updates.

  ## Arguments
  - `wait` (boolean, optional): Waits for the deployment to be fully hydrated.
      Defaults to false. It is recommended you call `deploy_await` manually and
      run additional validation checks before promoting a deployment to production.
  - `poll_interval` (integer): The interval, in seconds, between each readiness check.
  - `dry_run` (boolean, optional): When True, prints out the commands that would
    be executed as part of the deployment workflow (without executing them).

  ## Returns
  None: This macro performs deployment actions but does not return a value.
#}

{% set current_target_name = target.name %}
{% set deployment = var('deployment') %}
{% set target_config = deployment[current_target_name] %}

-- Check if the target-specific configuration exists
{% if not target_config %}
    {{ exceptions.raise_compiler_error("No deployment configuration found for target " ~ current_target_name) }}
{% endif %}

{{ log("Creating deployment environment for target " ~ current_target_name, info=True) }}

{% set clusters = target_config.get('clusters', []) %}
{% set schemas = target_config.get('schemas', []) %}

-- Check that all production schemas
-- and clusters already exist
{% for schema in schemas %}
    {% set deploy_schema = schema ~ "_dbt_deploy" %}
    {% if not schema_exists(schema) %}
        {{ exceptions.raise_compiler_error("Production schema " ~ schema ~ " does not exist") }}
    {% endif %}
    {% if not schema_exists(deploy_schema) %}
        {{ exceptions.raise_compiler_error("Deployment schema " ~ deploy_schema ~ " does not exist") }}
    {% endif %}
{% endfor %}

{% for cluster in clusters %}
    {% set deploy_cluster = adapter.generate_final_cluster_name(cluster, force_deploy_suffix=True) %}
    {% if not cluster_exists(cluster) %}
        {{ exceptions.raise_compiler_error("Production cluster " ~ cluster ~ " does not exist") }}
    {% endif %}
    {% if not cluster_exists(deploy_cluster) %}
        {{ exceptions.raise_compiler_error("Deployment cluster " ~ deploy_cluster ~ " does not exist") }}
    {% endif %}
{% endfor %}

{% if wait %}
    {{ deploy_await(poll_interval) }}
{% endif %}

{% if not dry_run %}
    {% call statement('swap', fetch_result=True, auto_begin=False) -%}
    BEGIN;

    {% for schema in schemas %}
        {% set deploy_schema = schema ~ "_dbt_deploy" %}
        {{ log("Swapping schemas " ~ schema ~ " and " ~ deploy_schema, info=True) }}
        ALTER SCHEMA {{ adapter.quote(schema) }} SWAP WITH {{ adapter.quote(deploy_schema) }};
    {% endfor %}

    {% for cluster in clusters %}
        {% set deploy_cluster = adapter.generate_final_cluster_name(cluster, force_deploy_suffix=True) %}
        {{ log("Swapping clusters " ~ adapter.generate_final_cluster_name(cluster) ~ " and " ~ deploy_cluster, info=True) }}
        ALTER CLUSTER {{ adapter.quote(cluster) }} SWAP WITH {{ adapter.quote(deploy_cluster) }};
    {% endfor %}

    COMMIT;
    {%- endcall %}
{% else %}
    {{ log("Starting dry run...", info=True) }}
    {% for schema in schemas %}
        {% set deploy_schema = schema ~ "_dbt_deploy" %}
        {{ log("DRY RUN: Swapping schemas " ~ schema ~ " and " ~ deploy_schema, info=True) }}
        {{ log("DRY RUN: ALTER SCHEMA " ~ adapter.quote(schema) ~ " SWAP WITH " ~ adapter.quote(deploy_schema), info=True) }}
    {% endfor %}

    {% for cluster in clusters %}
        {% set deploy_cluster = adapter.generate_final_cluster_name(cluster, force_deploy_suffix=True) %}
        {{ log("DRY RUN: Swapping clusters " ~ adapter.generate_final_cluster_name(cluster) ~ " and " ~ deploy_cluster, info=True) }}
        {{ log("DRY RUN: ALTER CLUSTER " ~ adapter.quote(cluster) ~ " SWAP WITH " ~ adapter.quote(deploy_cluster), info=True) }}
    {% endfor %}
    {{ log("Dry run completed. The statements above were **not** executed against Materialize.", info=True) }}
{% endif %}

{% set sinks_to_alter = discover_sinks(schemas) %}
{{ process_sinks(sinks_to_alter, dry_run) }}

{% endmacro %}

{% macro cluster_contains_sinks(cluster) %}
    {% set query %}
        SELECT count(*) > 0
        FROM mz_sinks
        JOIN mz_clusters ON mz_sinks.cluster_id = mz_clusters.id
        WHERE mz_clusters.name = {{ dbt.string_literal(cluster) }}
    {% endset %}

    {% set count = run_query(query) %}
    {% if execute %}
        {{ return(count.rows[0][0]) }}
    {% endif %}
{% endmacro %}

{% macro schema_contains_sinks(schema) %}
    {% set query %}
        SELECT count(*) > 0
        FROM mz_sinks
        JOIN mz_schemas ON mz_sinks.schema_id = mz_schemas.id
        JOIN mz_databases ON mz_schemas.database_id = mz_databases.id
        WHERE mz_schemas.name = {{ dbt.string_literal(schema) }}
            AND mz_databases.name = current_database()
    {% endset %}

    {% set count = run_query(query) %}
    {% if execute %}
        {{ return(count.rows[0][0]) }}
    {% endif %}
{% endmacro %}

{% macro get_current_database() %}
    {% set current_database_query = "SELECT current_database()" %}
    {% set results = run_query(current_database_query) %}
    {% if execute %}
        {% set current_database = results.rows[0][0] %}
        {{ return(current_database) }}
    {% else %}
        {{ return(None) }}
    {% endif %}
{% endmacro %}

{% macro discover_sinks(schemas) %}
    {% set sinks_to_alter = [] %}
    {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") | selectattr("config.materialized", "equalto", "sink") %}
        {% set upstream_node = graph.nodes[node.depends_on.nodes[0]] %}
        {% set upstream_schema = upstream_node.schema %}

        {% if upstream_schema in schemas %}
            {% set sink_database = node.database %}
            {% set sink_schema = node.schema %}
            {% set sink_name = node.name %}
            {% set new_upstream_relation = adapter.quote(upstream_node.database) ~ '.' ~ adapter.quote(upstream_schema) ~ '.' ~ adapter.quote(upstream_node.name) %}
            {% set sink = {
                "database": sink_database,
                "schema": sink_schema,
                "name": sink_name,
                "new_upstream_relation": new_upstream_relation
            } %}
            {% do sinks_to_alter.append(sink) %}
        {% endif %}
    {% endfor %}
    {{ return(sinks_to_alter) }}
{% endmacro %}

{% macro process_sinks(sinks, dry_run) %}
    {% if sinks|length > 0 %}
        {% for sink in sinks %}
            {% if sink['database'] and sink['schema'] and sink['name'] and sink['new_upstream_relation'] %}
                {% if not dry_run %}
                    {% call statement('alter_sink_' ~ loop.index, fetch_result=True, auto_begin=False) %}
                        {{ log("Running ALTER SINK " ~ adapter.quote(sink['database']) ~ "." ~ adapter.quote(sink['schema']) ~ "." ~ adapter.quote(sink['name']) ~ " SET FROM " ~ sink['new_upstream_relation'], info=True) }}
                        ALTER SINK {{ adapter.quote(sink['database']) }}.{{ adapter.quote(sink['schema']) }}.{{ adapter.quote(sink['name']) }} SET FROM {{ sink['new_upstream_relation'] }};
                    {% endcall %}
                {% else %}
                    {{ log("DRY RUN: ALTER SINK " ~ adapter.quote(sink['database']) ~ "." ~ adapter.quote(sink['schema']) ~ "." ~ adapter.quote(sink['name']) ~ " SET FROM " ~ sink['new_upstream_relation'], info=True) }}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% else %}
        {{ log("No sinks to process.", info=True) }}
    {% endif %}
{% endmacro %}
