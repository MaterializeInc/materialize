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

{% macro deploy_sink_validation() %}
-- Deploy Sink Validation
-- This macro queries all sinks and their dependent objects in the current database and specified schema.
-- Sinks in other schemas and databases are also included if they reference objects in the specified schema.
-- It prints debug information including the list of sinks to be altered, their current dependent objects,
-- and the new dependent objects from the _dbt_deploy schema.

{% set current_target_name = target.name %}
{% set deployment = var('deployment') %}
{% set target_config = deployment[current_target_name] %}

-- Check if the target-specific configuration exists
{% if not target_config %}
    {{ exceptions.raise_compiler_error("No deployment configuration found for target " ~ current_target_name) }}
{% endif %}

{% set clusters = target_config.get('clusters', []) %}
{% set schemas = target_config.get('schemas', []) %}

-- Get the current database name
{% set current_database_query = "SELECT current_database()" %}
{% set results = run_query(current_database_query) %}
{% if execute %}
    {% set current_database = adapter.quote(results.rows[0][0]) %}
{% endif %}

-- Loop through each schema and print sink details
{% for schema in schemas %}
    {% set sinks_and_upstream_relations_query %}
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

    {% set sinks_and_upstream_relations = run_query(sinks_and_upstream_relations_query) %}
    {% if sinks_and_upstream_relations is not none and sinks_and_upstream_relations.rows %}
        -- Print debug information
        {{ log("Sinks and their upstream relations in schema: " ~ schema, info=True) }}
        {% for sink in sinks_and_upstream_relations.rows %}
            {% set sink_name = adapter.quote(sink[2]) %}
            {% set sink_schema_name = adapter.quote(sink[3]) %}
            {% set sink_database_name = adapter.quote(sink[4]) %}
            {% set upstream_relation_name = adapter.quote(sink[5]) %}
            {% set upstream_relation_schema = adapter.quote(sink[6]) %}
            {% set deploy_schema = adapter.quote(sink[6] ~ "_dbt_deploy") %}
            {% set new_upstream_relation = current_database ~ '.' ~ deploy_schema ~ '.' ~ upstream_relation_name %}
            {{ log("  Sink: " ~ sink_name ~ " (Schema: " ~ schema ~ ")", info=True) }}
            {{ log("    Current upstream relation: " ~ current_database ~ '.' ~ upstream_relation_schema ~ '.' ~ upstream_relation_name, info=True) }}
            {{ log("    New upstream relation: " ~ new_upstream_relation, info=True) }}
            {{ log("    The sink will be altered to use the new upstream relation:", info=True) }}
            {{ log("    ALTER SINK " ~ sink_database_name ~ "." ~ sink_schema_name ~ "." ~ sink_name ~ " SET FROM " ~ new_upstream_relation, info=True) }}
        {% endfor %}
    {% else %}
        {{ log("No sinks found in schema: " ~ schema, info=True) }}
    {% endif %}
{% endfor %}
{% endmacro %}
