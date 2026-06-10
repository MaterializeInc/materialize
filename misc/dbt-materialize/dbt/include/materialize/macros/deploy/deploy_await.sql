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
  polling the clusters' readiness status at a specified interval.

  Unlike previous versions that polled each cluster independently (issuing
  one query per cluster per poll iteration), this macro consolidates all
  cluster checks into a single query to reduce catalog server load.

  ## Arguments
  - `poll_interval` (integer): The interval, in seconds, between each readiness
    check.
  - `lag_threshold` (string): The maximum lag threshold, which determines when
    all objects in the environment are considered hydrated and it's safe to
    perform the cutover step.

  ## Returns None: This macro does not return a value but will halt execution
     until all deployment clusters' objects are fully hydrated.
#}

{% set current_target_name = target.name %}
{% set deployment = var('deployment') %}
{% set target_config = deployment[current_target_name] %}

-- Check if the target-specific configuration exists
{% if not target_config %}
    {{ exceptions.raise_compiler_error("No deployment configuration found for target " ~ current_target_name) }}
{% endif %}

{% set clusters = target_config.get('clusters', []) %}

{# Collect all deployment cluster names upfront #}
{% set deploy_clusters = [] %}
{% for cluster in clusters %}
    {% do deploy_clusters.append(adapter.generate_final_cluster_name(cluster, force_deploy_suffix=True)) %}
{% endfor %}

{% if deploy_clusters | length == 0 %}
    {{ log("No deployment clusters configured, skipping await.", info=True) }}
    {{ return(none) }}
{% endif %}

{{ log("Awaiting readiness for " ~ deploy_clusters | length ~ " cluster(s): " ~ deploy_clusters | join(", "), info=True) }}

{# Main polling loop — single query for all clusters #}
{% for i in range(1, 100000) %}
    {% set statuses = are_clusters_ready(deploy_clusters, lag_threshold) %}

    {# Check if all clusters are ready #}
    {% set all_ready = [] %}
    {% for cluster in deploy_clusters %}
        {% if cluster in statuses and statuses[cluster].ready %}
            {% do all_ready.append(cluster) %}
        {% endif %}
    {% endfor %}

    {% if all_ready | length == deploy_clusters | length %}
        {{ log("All " ~ deploy_clusters | length ~ " cluster(s) are ready.", info=True) }}
        {{ return(none) }}
    {% endif %}

    -- Be a good citizen and don't overwhelm mz_catalog_server
    {{ adapter.sleep(poll_interval) }}
{% endfor %}
{{ exceptions.raise_compiler_error("Clusters failed to become ready within a reasonable amount of time: " ~ deploy_clusters | join(", ")) }}
{% endmacro %}
