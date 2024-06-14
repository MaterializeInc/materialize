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
  - `dry_run` (boolean, optional): When True, prints the actions that would be taken without executing them.

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
    {% if schema_contains_sinks(deploy_schema) %}
        {{ exceptions.raise_compiler_error("""
        Deployment schema " ~ deploy_schema ~ " contains sinks. This is not currently
        supported by the deploy_promote macro.

        If this feature is important to you, please reach out!
        """) }}
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
    {% if cluster_contains_sinks(deploy_cluster) %}
        {{ exceptions.raise_compiler_error("""
        Deployment cluster " ~ deploy_cluster ~ " contains sinks. This is not currently
        supported by the deploy_promote macro.

        If this feature is important to you, please reach out!
        """) }}
    {% endif %}
{% endfor %}

{% if wait %}
    {{ deploy_await(poll_interval) }}
{% endif %}

-- Check sinks and prepare for ALTER SINK commands
{% for schema in schemas %}
    {% set sinks_and_upstream_relations = get_sinks_and_upstream_relations(schema) %}
    {% if sinks_and_upstream_relations is not none and sinks_and_upstream_relations.rows %}
        {% for sink in sinks_and_upstream_relations.rows %}
            {% set sink_name = adapter.quote(sink[2]) %}
            {% set sink_schema_name = adapter.quote(sink[3]) %}
            {% set sink_database_name = adapter.quote(sink[4]) %}
            {% set upstream_relation_name = adapter.quote(sink[5]) %}
            {% set upstream_relation_schema = adapter.quote(sink[6]) %}
            {% set deploy_schema = adapter.quote(sink[6] ~ "_dbt_deploy") %}
            {% set new_upstream_relation = get_current_database() ~ '.' ~ deploy_schema ~ '.' ~ upstream_relation_name %}
            {% if not dry_run %}
                {{ log("Altering sink " ~ sink_database_name ~ "." ~ sink_schema_name ~ "." ~ sink_name ~ " to use new upstream relation " ~ new_upstream_relation, info=True) }}
                ALTER SINK {{ sink_database_name ~ "." ~ sink_schema_name ~ "." ~ sink_name }} SET FROM {{ new_upstream_relation }};
            {% else %}
                {{ log("DRY RUN: ALTER SINK " ~ sink_database_name ~ "." ~ sink_schema_name ~ "." ~ sink_name ~ " SET FROM " ~ new_upstream_relation, info=True) }}
            {% endif %}
        {% endfor %}
    {% else %}
        {{ log("No sinks found in schema " ~ schema ~ ", skipping ALTER SINK operations for this schema", info=True) }}
    {% endif %}
{% endfor %}

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
    {{ log("DRY RUN: No actual schema or cluster swaps performed.", info=True) }}
{% endif %}
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

{% macro get_sinks_and_upstream_relations(schema) %}
    {% set query %}
    SELECT
        mz_object_dependencies.object_id,
        mz_object_dependencies.referenced_object_id,
        mz_objects.name AS sink_name,
        mz_schemas.name AS sink_schema_name,
        mz_databases.name AS sink_database_name,
        upstream_relations.name AS upstream_relation_name,
        upstream_relation_schemas.name AS upstream_relation_schema,
        upstream_relation_databases.name AS upstream_relation_database,
        upstream_relations.type AS upstream_relation_type
    FROM mz_internal.mz_object_dependencies
    JOIN mz_objects ON mz_object_dependencies.object_id = mz_objects.id
    JOIN mz_schemas ON mz_objects.schema_id = mz_schemas.id
    JOIN mz_databases ON mz_schemas.database_id = mz_databases.id
    JOIN mz_objects AS upstream_relations ON mz_object_dependencies.referenced_object_id = upstream_relations.id
    JOIN mz_schemas AS upstream_relation_schemas ON upstream_relations.schema_id = upstream_relation_schemas.id
    JOIN mz_databases AS upstream_relation_databases ON upstream_relation_schemas.database_id = upstream_relation_databases.id
    WHERE mz_objects.type = 'sink'
        AND upstream_relations.type IN ('table', 'materialized-view', 'source')
        AND upstream_relation_databases.name = current_database()
        AND upstream_relation_schemas.name = {{ dbt.string_literal(schema) }};
    {% endset %}
    {% set results = run_query(query) %}
    {{ log("Found " ~ results.rows|length ~ " sinks in schema: " ~ schema, info=True) }}
    {{ return(results) }}
{% endmacro %}
