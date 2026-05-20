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
        {{ internal_check_existing_deploy_cluster(deploy_cluster, ignore_existing_objects) }}
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
            {% set unsized_replicas = [] %}
            {% for row in replica_results.rows %}
                {% if row[1] is none %}
                    {% do unsized_replicas.append(row[0]) %}
                {% endif %}
            {% endfor %}
            {% if unsized_replicas %}
                {{ exceptions.raise_compiler_error(
                    "Cannot clone the following replicas of unmanaged cluster '"
                    ~ origin_cluster ~ "' because they have no SIZE: "
                    ~ unsized_replicas | join(", ") ~ ". Orchestrated replicas "
                    ~ "(using COMPUTECTL/STORAGECTL ADDRESSES) cannot be cloned by "
                    ~ "blue/green deployments. Recreate the cluster as managed or "
                    ~ "replace these replicas with sized ones."
                ) }}
            {% endif %}

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
