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

{#
This macro creates a cluster with the specified properties.

  ## Arguments
  - cluster_name (str): The name of the cluster. This parameter is required.
  - size (str): The size of the cluster. This parameter is required.
  - replication_factor (int, optional): The replication factor for the cluster. Only applicable when schedule_type is 'manual'.
  - schedule_type (str, optional): The type of schedule for the cluster. Accepts 'manual' or 'on-refresh'.
  - refresh_hydration_time_estimate (str, optional): The estimated hydration time for the cluster. Only applicable when schedule_type is 'on-refresh'.
  - ignore_existing_objects (bool, optional): Whether to ignore existing objects in the cluster. Defaults to false.
  - force_deploy_suffix (bool, optional): Whether to forcefully add a deploy suffix to the cluster name. Defaults to false.

  Incompatibilities:
  - replication_factor is only applicable when schedule_type is 'manual'.
  - refresh_hydration_time_estimate is only applicable when schedule_type is 'on-refresh'.
#}
{% macro create_cluster(
    cluster_name,
    size,
    replication_factor=none,
    schedule_type=none,
    refresh_hydration_time_estimate=none,
    ignore_existing_objects=false,
    force_deploy_suffix=false
) %}

    {# Input validation #}
    {% if not cluster_name %}
        {{ exceptions.raise_compiler_error("cluster_name must be provided") }}
    {% endif %}

    {% if not size %}
        {{ exceptions.raise_compiler_error("size must be provided") }}
    {% endif %}

    {% set deploy_cluster = adapter.generate_final_cluster_name(cluster_name, force_deploy_suffix) %}

    {% if cluster_exists(deploy_cluster) %}
        {{ log("Deployment cluster " ~ deploy_cluster ~ " already exists", info=True) }}
        {% set cluster_empty %}
            WITH dataflows AS (
                SELECT mz_indexes.id
                FROM mz_indexes
                JOIN mz_clusters ON mz_indexes.cluster_id = mz_clusters.id
                WHERE mz_clusters.name = {{ dbt.string_literal(deploy_cluster) }}

                UNION ALL

                SELECT mz_materialized_views.id
                FROM mz_materialized_views
                JOIN mz_clusters ON mz_materialized_views.cluster_id = mz_clusters.id
                WHERE mz_clusters.name = {{ dbt.string_literal(deploy_cluster) }}

                UNION ALL

                SELECT mz_sources.id
                FROM mz_sources
                JOIN mz_clusters ON mz_clusters.id = mz_sources.cluster_id
                WHERE mz_clusters.name = {{ dbt.string_literal(deploy_cluster) }}

                UNION ALL

                SELECT mz_sinks.id
                FROM mz_sinks
                JOIN mz_clusters ON mz_clusters.id = mz_sinks.cluster_id
                WHERE mz_clusters.name = {{ dbt.string_literal(deploy_cluster) }}
            )

            SELECT count(*)
            FROM dataflows
            WHERE id LIKE 'u%'
        {% endset %}

        {% set cluster_object_count = run_query(cluster_empty) %}
        {% if execute %}
            {% if cluster_object_count and cluster_object_count.columns[0] and cluster_object_count.rows[0][0] > 0 %}
                {% if check_cluster_ci_tag(deploy_cluster) %}
                    {{ log("Cluster " ~ deploy_cluster ~ " was already created for this pull request", info=True) }}
                {% elif ignore_existing_objects %}
                    {{ log("[Warning] Deployment cluster " ~ deploy_cluster ~ " is not empty", info=True) }}
                    {{ log("[Warning] Confirm the objects it contains are expected before deployment", info=True) }}
                {% else %}
                    {{ exceptions.raise_compiler_error("
                        Deployment cluster " ~ deploy_cluster ~ " already exists and is not empty.
                        This is potentially dangerous as you may end up deploying objects to production you
                        do not intend.

                        If you are certain the objects in this cluster are supposed to exist, you can ignore this
                        error by setting ignore_existing_objects to True.

                        dbt run-operation create_cluster --args '{ignore_existing_objects: True}'
                    ") }}
                {% endif %}
            {% endif %}
        {% endif %}
    {% else %}
        {{ log("Creating deployment cluster " ~ deploy_cluster, info=True)}}
        {% set create_cluster_ddl %}
            CREATE CLUSTER {{ deploy_cluster }} (
                SIZE = {{ dbt.string_literal(size) }}
                {% if replication_factor is not none and ( schedule_type == 'manual' or schedule_type is none ) %}
                    , REPLICATION FACTOR = {{ replication_factor }}
                {% elif schedule_type == 'on-refresh' %}
                    {% if refresh_hydration_time_estimate is not none %}
                        , SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = {{ dbt.string_literal(refresh_hydration_time_estimate) }})
                    {% else %}
                        , SCHEDULE = ON REFRESH
                    {% endif %}
                {% endif %}
            )
        {% endset %}
        {{ run_query(create_cluster_ddl) }}
        {{ set_cluster_ci_tag(deploy_cluster) }}
    {% endif %}

    {{ return(deploy_cluster) }}
{% endmacro %}
