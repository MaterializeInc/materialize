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
This macro creates an unmanaged cluster by cloning the replicas of an
existing unmanaged origin cluster. SIZE and AVAILABILITY ZONE are copied
per replica. Internal replicas are skipped. Replicas without a SIZE
(orchestrated replicas using COMPUTECTL/STORAGECTL ADDRESSES) cannot be
cloned and produce a compiler error.

  ## Arguments
  - cluster_name (str): The logical cluster name. The deploy suffix is applied
    via `force_deploy_suffix`.
  - origin_cluster (str): The name of the existing unmanaged cluster whose
    replicas should be cloned.
  - ignore_existing_objects (bool, optional): Whether to ignore existing
    objects in the deploy cluster. Defaults to false.
  - force_deploy_suffix (bool, optional): Whether to forcefully add a deploy
    suffix to the cluster name. Defaults to false.
#}
{% macro create_unmanaged_cluster(
    cluster_name,
    origin_cluster,
    ignore_existing_objects=false,
    force_deploy_suffix=false
) %}

    {% if not cluster_name %}
        {{ exceptions.raise_compiler_error("cluster_name must be provided") }}
    {% endif %}

    {% if not origin_cluster %}
        {{ exceptions.raise_compiler_error("origin_cluster must be provided") }}
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

                        dbt run-operation create_unmanaged_cluster --args '{ignore_existing_objects: True}'
                    ") }}
                {% endif %}
            {% endif %}
        {% endif %}
    {% else %}
        {{ log("Creating deployment cluster " ~ deploy_cluster ~ " from unmanaged origin " ~ origin_cluster, info=True) }}

        {% set replica_query %}
            SELECT cr.name, cr.size, cr.availability_zone
            FROM mz_cluster_replicas cr
            JOIN mz_clusters c ON cr.cluster_id = c.id
            LEFT JOIN mz_internal.mz_internal_cluster_replicas icr ON icr.id = cr.id
            WHERE c.name = {{ dbt.string_literal(origin_cluster) }}
              AND icr.id IS NULL
            ORDER BY cr.name
        {% endset %}

        {% set replica_results = run_query(replica_query) %}

        {% if execute %}
            {% for row in replica_results.rows %}
                {% if row[1] is none %}
                    {{ exceptions.raise_compiler_error(
                        "Cannot clone replica '" ~ row[0] ~ "' of unmanaged cluster '"
                        ~ origin_cluster ~ "': it has no SIZE. Orchestrated replicas "
                        ~ "(using COMPUTECTL/STORAGECTL ADDRESSES) cannot be cloned by "
                        ~ "blue/green deployments. Recreate the cluster as managed or "
                        ~ "replace this replica with a sized one."
                    ) }}
                {% endif %}
            {% endfor %}

            {% set replica_clauses = [] %}
            {% for row in replica_results.rows %}
                {% set parts = ["SIZE " ~ dbt.string_literal(row[1])] %}
                {% if row[2] is not none %}
                    {% do parts.append("AVAILABILITY ZONE " ~ dbt.string_literal(row[2])) %}
                {% endif %}
                {% do replica_clauses.append(adapter.quote(row[0]) ~ " (" ~ parts | join(", ") ~ ")") %}
            {% endfor %}

            {% set create_cluster_ddl %}
                CREATE CLUSTER {{ adapter.quote(deploy_cluster) }} REPLICAS (
                    {{ replica_clauses | join(",\n                    ") }}
                )
            {% endset %}
            {{ run_query(create_cluster_ddl) }}
            {{ set_cluster_ci_tag(deploy_cluster) }}
        {% endif %}
    {% endif %}

    {{ return(deploy_cluster) }}
{% endmacro %}
