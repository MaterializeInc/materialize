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

{% macro deploy_await(poll_interval=15, lag_threshold='1s') %}
{#
  Waits for all objects within the deployment clusters to be fully hydrated,
  polling the cluster's readiness status at a specified interval.

  ## Arguments
  - `poll_interval` (integer): The interval, in seconds, between each readiness
    check.
  - `lag_threshold` (string): The maximum lag threshold, which determines when
    all objects in the environment are considered hydrated and it''s safe to
    perform the cutover step.

  ## Returns None: This macro does not return a value but will halt execution
     until the specified cluster's objects are fully hydrated.
#}

{% set current_target_name = target.name %}
{% set deployment = var('deployment') %}
{% set target_config = deployment[current_target_name] %}

-- Check if the target-specific configuration exists
{% if not target_config %}
    {{ exceptions.raise_compiler_error("No deployment configuration found for target " ~ current_target_name) }}
{% endif %}

{% set clusters = target_config.get('clusters', []) %}

{% for cluster in clusters %}
    {% set deploy_cluster = adapter.generate_final_cluster_name(cluster, force_deploy_suffix=True) %}
    {{ await_cluster_ready(deploy_cluster, poll_interval, lag_threshold) }}
{% endfor %}
{% endmacro %}
