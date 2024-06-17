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
-- This macro creates a new cluster for a given name, size, and replication factor.
-- Parameters:
--   cluster_name (str): The name of the cluster to create.
--   size (str): The size of the cluster, default is '50cc' (optional).
--   replication_factor (int): The replication factor of the cluster, default is 1 (optional).
--   ignore_existing_objects (bool): Whether to ignore existing objects in the cluster, default is False (optional).
--   force_deploy_suffix (bool): Whether to add _dbt_deploy suffix to the cluster name, default is False (optional).
#}
{% macro create_cluster(cluster_name, size='50cc', replication_factor=1, ignore_existing_objects=False, force_deploy_suffix=False) %}

{%- if cluster_name -%}

  {% set deploy_cluster = adapter.generate_final_cluster_name(cluster_name, force_deploy_suffix=force_deploy_suffix) %}

  {% if cluster_exists(deploy_cluster) %}

    {{ log("Cluster " ~ deploy_cluster ~ " already exists", info=True) }}

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
          {{ exceptions.raise_compiler_error("""
            Deployment cluster """ ~ deploy_cluster ~ """ already exists and is not empty.
            This is potentially dangerous as you may end up deploying objects to production you
            do not intend.

            If you are certain the objects in this cluster are supposed to exist, you can ignore this
            error by setting ignore_existing_objects to True.

            dbt run-operation create_cluster --args '{cluster_name: <cluster_name>, ignore_existing_objects: true}'
          """) }}
        {% endif %}
      {% endif %}
    {% endif %}

  {% else %}

    {{ log("Creating cluster " ~ deploy_cluster ~ " with size " ~ size ~ " and replication factor " ~ replication_factor, info=True) }}

    {% set create_cluster_query %}
      CREATE CLUSTER {{ deploy_cluster }} (
          SIZE = {{ dbt.string_literal(size) }},
          REPLICATION FACTOR = {{ replication_factor }}
      );
    {% endset %}

    {{ run_query(create_cluster_query) }}
    {{ set_cluster_ci_tag(deploy_cluster) }}
    {{ log("Cluster " ~ deploy_cluster ~ " created successfully.", info=True) }}

  {% endif %}

{%- else -%}

  {{ exceptions.raise_compiler_error("Invalid arguments. Missing cluster name!") }}

{%- endif %}

{% endmacro %}
