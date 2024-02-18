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

{% macro create_deployment_environment(ignore_existing_objects=False) %}

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
    {% if not schema_exists(schema.prod) %}
        {{ exceptions.raise_compiler_error("Production schema " ~ schema.prod ~ " does not exist") }}
    {% endif %}
{% endfor %}

{% for cluster in clusters %}
    {% if not cluster_exists(cluster.prod) %}
        {{ exceptions.raise_compiler_error("Production cluster " ~ cluster.prod ~ " does not exist") }}
    {% endif %}
{% endfor %}

{% for schema in schemas %}
    {% if schema_exists(schema.prod_deploy) %}
        {{ log("Deployment schema " ~ schema.prod_deploy ~ " already exists", info=True)}}
        {% set schema_empty %}
            SELECT count(*)
            FROM mz_objects
            JOIN mz_schemas ON mz_objects.schema_id = mz_schemas.id
            JOIN mz_databases ON mz_databases.id = mz_schemas.database_id
            WHERE mz_schemas.name = lower(trim('{{ schema.prod_deploy }}'))
                AND mz_objects.id LIKE 'u%'
                AND mz_databases.name = current_database()
        {% endset %}

        {% set schema_object_count = run_query(schema_empty) %}
        {% if execute %}
            {% if schema_object_count and schema_object_count.columns[0] and schema_object_count.rows[0][0] > 0 %}
                {% if ignore_existing_objects %}
                    {{ log("[Warning] Deployment schema " ~ schema.prod_deploy ~ " is not empty", info=True) }}
                    {{ log("[Warning] Confirm the objects it contains are expected before deployment", info=True) }}
                {% else %}
                    {{ exceptions.raise_compiler_error("""
                        Deployment schema """ ~ schema.prod_deploy ~ """ already exists and is not empty.
                        This is potentially dangerous as you may end up deploying objects to production you
                        do not intend.

                        If you are certain the objects in this schema are supposed to exist, you can ignore this
                        error by setting ignore_existing_objects to True.

                        dbt run-operation create_deployment_environment --args '{ignore_existing_objects: True}'
                    """) }}
                {% endif %}
            {% endif %}
        {% endif %}

    {% else %}
        {{ log("Creating deployment schema " ~ schema.prod_deploy, info=True)}}
        {% set create_schema %}
        CREATE SCHEMA {{ schema.prod_deploy }};
        {% endset %}
        {{ run_query(create_schema) }}
    {% endif %}
{% endfor %}

{% for cluster in clusters %}
    {% if cluster_exists(cluster.prod_deploy) %}
        {{ log("Deployment cluster " ~ cluster.prod_deploy ~ " already exists", info=True) }}
        {% set cluster_empty %}
            WITH dataflows AS (
                SELECT mz_indexes.id
                FROM mz_indexes
                JOIN mz_clusters ON mz_indexes.cluster_id = mz_clusters.id
                WHERE mz_clusters.name = lower(trim('{{ cluster.prod_deploy }}'))

                UNION ALL

                SELECT mz_materialized_views.id
                FROM mz_materialized_views
                JOIN mz_clusters ON mz_materialized_views.cluster_id = mz_clusters.id
                WHERE mz_clusters.name = lower(trim('{{ cluster.prod_deploy }}'))

                UNION ALL

                SELECT mz_sources.id
                FROM mz_sources
                JOIN mz_clusters ON mz_clusters.id = mz_sources.cluster_id
                WHERE mz_clusters.name = lower(trim('{{ cluster.prod_deploy }}'))

                UNION ALL

                SELECT mz_sinks.id
                FROM mz_sinks
                JOIN mz_clusters ON mz_clusters.id = mz_sinks.cluster_id
                WHERE mz_clusters.name = lower(trim('{{ cluster.prod_deploy }}'))
            )

            SELECT count(*)
            FROM dataflows
            WHERE id LIKE 'u%'
        {% endset %}


        {% set cluster_object_count = run_query(cluster_empty) %}
        {% if execute %}
            {% if cluster_object_count and cluster_object_count.columns[0] and cluster_object_count.rows[0][0] > 0 %}
                {% if ignore_existing_objects %}
                    {{ log("[Warning] Deployment cluster " ~ cluster.prod_deploy ~ " is not empty", info=True) }}
                    {{ log("[Warning] Confirm the objects it contains are expected before deployment", info=True) }}
                {% else %}
                    {{ exceptions.raise_compiler_error("""
                        Deployment cluster """ ~ cluster.prod_deploy ~ """ already exists and is not empty.
                        This is potentially dangerous as you may end up deploying objects to production you
                        do not intend.

                        If you are certain the objects in this cluster are supposed to exist, you can ignore this
                        error by setting ignore_existing_objects to True.

                        dbt run-operation create_deployment_environment --args '{ignore_existing_objects: True}'
                    """) }}
                {% endif %}
            {% endif %}
        {% endif %}

    {% else %}
        {{ log("Creating deployment cluster " ~ cluster.prod_deploy ~ " like cluster " ~ cluster.prod, info=True)}}
        {% set cluster_configuration %}
            SELECT managed, size, replication_factor
            FROM mz_clusters
            WHERE name = lower(trim('{{ cluster.prod }}'))
        {% endset %}

        {% set cluster_config_results = run_query(cluster_configuration) %}

        {% if execute %}
            {% set results = cluster_config_results.rows[0] %}

            {% set managed = results[0] %}
            {% set size = results[1] %}
            {% set replication_factor = results[2] %}

            {% if not managed %}
                {{ exceptions.raise_compiler_error("Production cluster " ~ cluster.prod ~ " is not managed") }}
            {% endif %}

            {% set create_cluster %}
                CREATE CLUSTER {{ cluster.prod_deploy }} (
                    SIZE = '{{ size }}',
                    REPLICATION FACTOR = {{ replication_factor }}
                );
            {% endset %}
            {{ run_query(create_cluster) }}
        {% endif %}
    {% endif %}
{% endfor %}
{% endmacro %}
