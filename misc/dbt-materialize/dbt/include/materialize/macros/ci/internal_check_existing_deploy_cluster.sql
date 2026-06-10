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
Shared pre-existence check for the cluster-creation macros invoked by
`deploy_init`. When the deployment cluster already exists, this macro
counts user objects in it and either:

  - logs and continues, if the cluster was created earlier in the same
    pull request (matching CI tag) or `ignore_existing_objects` is true,
  - raises a compiler error otherwise.

Keeping this in one place ensures the managed (`create_cluster`) and
unmanaged (`create_unmanaged_cluster`) paths stay in lockstep.

  ## Arguments
  - deploy_cluster (str): The fully-qualified deployment cluster name.
  - ignore_existing_objects (bool): Whether to tolerate a non-empty
    pre-existing deployment cluster.
#}
{% macro internal_check_existing_deploy_cluster(deploy_cluster, ignore_existing_objects) %}
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
                    error by re-running deploy_init with ignore_existing_objects: True:

                    dbt run-operation deploy_init --args '{ignore_existing_objects: True}'
                ") }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}
