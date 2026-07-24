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
  - auto_scaling_strategy (dict, optional): An autoscaling strategy for the cluster,
    e.g. {'on_hydration': {'hydration_size': '800cc', 'linger_duration': '15s'}}.
    The cluster temporarily bursts to the hydration size while it has un-hydrated
    objects. Only applicable when schedule_type is 'manual'.

  Incompatibilities:
  - replication_factor is only applicable when schedule_type is 'manual'.
  - refresh_hydration_time_estimate is only applicable when schedule_type is 'on-refresh'.
  - auto_scaling_strategy is only applicable when schedule_type is 'manual', and
    its hydration_size must differ from size.
#}
{# NOTE: new optional arguments must be appended at the end of the signature,
   since callers may invoke this macro with positional arguments. #}
{% macro create_cluster(
    cluster_name,
    size,
    replication_factor=none,
    schedule_type=none,
    refresh_hydration_time_estimate=none,
    ignore_existing_objects=false,
    force_deploy_suffix=false,
    auto_scaling_strategy=none
) %}

    {# Input validation #}
    {% if not cluster_name %}
        {{ exceptions.raise_compiler_error("cluster_name must be provided") }}
    {% endif %}

    {% if not size %}
        {{ exceptions.raise_compiler_error("size must be provided") }}
    {% endif %}

    {% if auto_scaling_strategy is not none %}
        {% do internal_validate_auto_scaling_strategy(auto_scaling_strategy, size, schedule_type) %}
    {% endif %}

    {% set deploy_cluster = adapter.generate_final_cluster_name(cluster_name, force_deploy_suffix) %}

    {% if cluster_exists(deploy_cluster) %}
        {{ internal_check_existing_deploy_cluster(deploy_cluster, ignore_existing_objects) }}
    {% else %}
        {{ log("Creating deployment cluster " ~ deploy_cluster, info=True)}}
        {% set create_cluster_ddl %}
            CREATE CLUSTER {{ adapter.quote(deploy_cluster) }} (
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
                {% if auto_scaling_strategy is not none %}
                    , {{ internal_render_auto_scaling_strategy(auto_scaling_strategy) }}
                {% endif %}
            )
        {% endset %}
        {{ run_query(create_cluster_ddl) }}
        {{ set_cluster_ci_tag(deploy_cluster) }}
    {% endif %}

    {{ return(deploy_cluster) }}
{% endmacro %}
