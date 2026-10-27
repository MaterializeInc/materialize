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
  is_cluster_ready macro
  ======================

  Checks if a cluster is ready for use by evaluating:
  - Replica health (detects OOM-killed replicas)
  - Hydration status (are all objects hydrated?)
  - Lag (is the cluster caught up within the allowed threshold?)

  Returns a dictionary with:
  - ready: bool - True if status is 'ready'
  - status: str - 'ready', 'hydrating', 'lagging', or 'failing'
  - failure_reason: str - 'no_replicas', 'all_replicas_problematic', or None
  - hydrated_count: int - Number of hydrated objects
  - total_count: int - Total objects to hydrate
  - max_lag_secs: int - Maximum lag in seconds
  - total_replicas: int - Total replicas in cluster
  - problematic_replicas: int - Replicas with 3+ OOM kills in 24h
#}

{% macro is_cluster_ready(cluster=target.cluster|default(none), lag_threshold='10s') %}

{% if cluster is none %}
    {{ exceptions.raise_compiler_error("No cluster specified and no default cluster found in target profile.") }}
{% endif %}

{% set statuses = are_clusters_ready([cluster], lag_threshold) %}

{% if execute %}
    {#- are_clusters_ready reports clusters it did not find as failing, so there
        is always an entry for the requested cluster. -#}
    {{ return(statuses[cluster]) }}
{% endif %}

{% endmacro %}
