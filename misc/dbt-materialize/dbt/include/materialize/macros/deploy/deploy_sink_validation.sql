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

-- Loop through each schema and print sink details
{% for schema in schemas %}
    {% set sinks_and_sources_query %}
    SELECT
        mz_object_dependencies.object_id,
        mz_object_dependencies.referenced_object_id,
        mz_objects.name AS sink_name,
        mz_schemas.name AS sink_schema_name,
        mz_databases.name AS database_name,
        source_objects.name AS source_name,
        source_schemas.name AS source_schema_name,
        source_objects.type AS source_type
    FROM mz_internal.mz_object_dependencies
    JOIN mz_objects ON mz_object_dependencies.object_id = mz_objects.id
    JOIN mz_schemas ON mz_objects.schema_id = mz_schemas.id
    JOIN mz_databases ON mz_schemas.database_id = mz_databases.id
    JOIN mz_objects AS source_objects ON mz_object_dependencies.referenced_object_id = source_objects.id
    JOIN mz_schemas AS source_schemas ON source_objects.schema_id = source_schemas.id
    WHERE mz_objects.type = 'sink'
      AND source_objects.type IN ('table', 'materialized-view', 'source')
      AND mz_databases.name = current_database()
      AND mz_schemas.name = {{ dbt.string_literal(schema) }};
    {% endset %}

    {% set sinks_and_sources = run_query(sinks_and_sources_query) %}
    {% if execute %}
        -- Print debug information
        {{ log("Sinks and their sources in schema: " ~ schema, info=True) }}
        {% for sink in sinks_and_sources.rows %}
            {% set sink_name = adapter.quote(sink[2]) %}
            {% set sink_schema_name = adapter.quote(sink[3]) %}
            {% set source_name = adapter.quote(sink[5]) %}
            {% set source_schema = sink[6] %}
            {% set deploy_schema = adapter.quote(source_schema ~ "_dbt_deploy") %}
            {% set new_source = deploy_schema ~ '.' ~ source_name %}
            {{ log("  Sink: " ~ sink_name ~ " (Schema: " ~ schema ~ ")", info=True) }}
            {{ log("    Current Source: " ~ source_schema ~ '.' ~ source_name, info=True) }}
            {{ log("    New Source: " ~ new_source, info=True) }}
            {{ log("    The sink will be altered to use the new source:", info=True) }}
            {{ log("    ALTER SINK " ~ sink_schema_name ~ "." ~ sink_name ~ " SET FROM " ~ new_source, info=True) }}
        {% endfor %}
    {% endif %}
{% endfor %}
{% endmacro %}
