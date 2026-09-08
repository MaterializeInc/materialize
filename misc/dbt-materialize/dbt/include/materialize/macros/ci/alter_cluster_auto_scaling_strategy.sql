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
This macro sets or removes the autoscaling strategy of an existing managed
cluster.

  ## Arguments
  - cluster_name (str): The name of the cluster. This parameter is required.
  - auto_scaling_strategy (dict, optional): The autoscaling strategy to set,
    e.g. {'on_hydration': {'hydration_size': '800cc', 'linger_duration': '15s'}}.
    When omitted, the strategy is removed via RESET (AUTO SCALING STRATEGY).
    An empty mapping ({}) disables autoscaling via the equivalent
    AUTO SCALING STRATEGY = () form.

  ## Usage
  dbt run-operation alter_cluster_auto_scaling_strategy --args '{cluster_name: quickstart, auto_scaling_strategy: {on_hydration: {hydration_size: "800cc", linger_duration: "15s"}}}'
  dbt run-operation alter_cluster_auto_scaling_strategy --args '{cluster_name: quickstart}'

  Incompatibilities:
  - Only managed clusters support an autoscaling strategy.
  - The strategy cannot be combined with a schedule other than 'manual', and
    its hydration_size must differ from the cluster's size.
#}
{% macro alter_cluster_auto_scaling_strategy(cluster_name, auto_scaling_strategy=none) %}

    {% if not cluster_name %}
        {{ exceptions.raise_compiler_error("cluster_name must be provided") }}
    {% endif %}

    {% if not cluster_exists(cluster_name) %}
        {{ exceptions.raise_compiler_error("Cluster " ~ cluster_name ~ " does not exist") }}
    {% endif %}

    {% set cluster_configuration %}
        SELECT
            c.managed,
            c.size,
            cs.type AS schedule_type
        FROM mz_clusters c
        LEFT JOIN mz_internal.mz_cluster_schedules cs ON cs.cluster_id = c.id
        WHERE c.name = {{ dbt.string_literal(cluster_name) }}
    {% endset %}

    {% set cluster_config_results = run_query(cluster_configuration) %}

    {% if execute %}
        {% set results = cluster_config_results.rows[0] %}
        {% set managed = results[0] %}
        {% set size = results[1] %}
        {% set schedule_type = results[2] %}

        {% if not managed %}
            {{ exceptions.raise_compiler_error(
                "Cluster " ~ cluster_name ~ " is unmanaged. AUTO SCALING STRATEGY "
                ~ "is only supported on managed clusters."
            ) }}
        {% endif %}

        {% if auto_scaling_strategy is none %}
            {{ log("Removing autoscaling strategy from cluster " ~ cluster_name, info=True) }}
            {% set alter_cluster_ddl %}
                ALTER CLUSTER {{ adapter.quote(cluster_name) }} RESET (AUTO SCALING STRATEGY)
            {% endset %}
        {% else %}
            {% do internal_validate_auto_scaling_strategy(auto_scaling_strategy, size, schedule_type) %}
            {{ log("Setting autoscaling strategy on cluster " ~ cluster_name, info=True) }}
            {% set alter_cluster_ddl %}
                ALTER CLUSTER {{ adapter.quote(cluster_name) }} SET ({{ internal_render_auto_scaling_strategy(auto_scaling_strategy) }})
            {% endset %}
        {% endif %}

        {{ run_query(alter_cluster_ddl) }}
    {% endif %}
{% endmacro %}
