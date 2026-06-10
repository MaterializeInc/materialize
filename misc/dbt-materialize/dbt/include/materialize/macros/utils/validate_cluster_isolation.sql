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
  Cluster Isolation Validation Macros
  ====================================

  These macros enforce cluster isolation rules when strict_mode is enabled.
  They use the dbt manifest/graph for compile-time validation, meaning:
  - No database connection required
  - Validation happens during `dbt compile` or `dbt run`
  - Conflicts are caught before any SQL is executed

  Cluster Isolation Rules (strict_mode only):
  -------------------------------------------
  1. Sources require dedicated clusters:
     - Each source must be on its own cluster
     - Sources cannot share clusters with any other object type
     - This ensures source ingestion workloads are isolated

  2. Sinks can share clusters with other sinks only:
     - Multiple sinks can be on the same cluster
     - Sinks cannot share clusters with views, MVs, or sources
     - This allows grouping related sink workloads together

  3. Views, MVs, and tables can share clusters freely:
     - These can share clusters with each other
     - They cannot be on clusters that have sources or sinks

  Why these rules?
  ----------------
  - Sources perform ingestion which can have unpredictable resource usage
  - Sinks perform exports which may have different SLAs than query workloads
  - Separating workload types allows for better resource isolation and management

  Enable strict_mode in dbt_project.yml:
    vars:
      strict_mode: true
#}


{% macro validate_source_cluster_isolation(target_cluster) %}
{#
  Called from: source materialization
  Purpose: Ensure each source has a dedicated cluster

  Logic:
  1. Skip validation if strict_mode is not enabled
  2. Scan all nodes in the dbt graph (manifest)
  3. Find nodes on the same cluster
  4. If any other objects share this cluster, raise an error
#}

  {# Step 1: Only run validation when strict_mode is enabled #}
  {% if var('strict_mode', False) %}

    {# Step 2: Initialize list to collect conflicting objects #}
    {% set conflicts = [] %}

    {# Step 3: Iterate through all nodes in the dbt manifest #}
    {% for node in graph.nodes.values() %}

      {# Step 4: Get this node's cluster (explicit or default) #}
      {% set node_cluster = node.config.cluster or target.cluster %}

      {# Step 5: Check if this node is on the same cluster #}
      {% if node_cluster == target_cluster %}

        {# Step 6: Record any object that isn't the current source #}
        {% set materialized = node.config.materialized %}
        {% if materialized and materialized != 'source' %}
          {% do conflicts.append({'name': node.name, 'type': materialized}) %}
        {% elif materialized == 'source' and node.name != model.name %}
          {# Another source on the same cluster #}
          {% do conflicts.append({'name': node.name, 'type': 'source'}) %}
        {% endif %}

      {% endif %}
    {% endfor %}

    {# Step 7: If any conflicts were found, raise a compiler error #}
    {% if conflicts | length > 0 %}
      {% set conflict_names = conflicts | map(attribute='name') | join(", ") %}
      {{ exceptions.raise_compiler_error(
        "Source '" ~ model.name ~ "' in cluster '" ~ target_cluster ~ "' " ~
        "cannot share cluster with other objects in strict_mode. " ~
        "Each source must have a dedicated cluster. " ~
        "Conflicts: " ~ conflict_names
      ) }}
    {% endif %}

  {% endif %}
{% endmacro %}


{% macro validate_sink_cluster_isolation(target_cluster) %}
{#
  Called from: sink materialization
  Purpose: Ensure sinks only share clusters with other sinks

  Logic:
  1. Skip validation if strict_mode is not enabled
  2. Scan all nodes in the dbt graph (manifest)
  3. Find nodes on the same cluster
  4. If any non-sink objects share this cluster, raise an error
#}

  {# Step 1: Only run validation when strict_mode is enabled #}
  {% if var('strict_mode', False) %}

    {# Step 2: Initialize list to collect conflicting objects #}
    {% set conflicts = [] %}

    {# Step 3: Iterate through all nodes in the dbt manifest #}
    {% for node in graph.nodes.values() %}

      {# Step 4: Get this node's cluster (explicit or default) #}
      {% set node_cluster = node.config.cluster or target.cluster %}

      {# Step 5: Check if this node is on the same cluster #}
      {% if node_cluster == target_cluster %}

        {% set materialized = node.config.materialized %}

        {# Step 6: Record any non-sink object #}
        {% if materialized and materialized != 'sink' %}
          {% do conflicts.append({'name': node.name, 'type': materialized}) %}
        {% endif %}

      {% endif %}
    {% endfor %}

    {# Step 7: If any conflicts were found, raise a compiler error #}
    {% if conflicts | length > 0 %}
      {% set conflict_names = conflicts | map(attribute='name') | join(", ") %}
      {{ exceptions.raise_compiler_error(
        "Sink '" ~ model.name ~ "' in cluster '" ~ target_cluster ~ "' " ~
        "cannot share cluster with non-sink objects in strict_mode. " ~
        "Sinks can only share clusters with other sinks. " ~
        "Conflicts: " ~ conflict_names
      ) }}
    {% endif %}

  {% endif %}
{% endmacro %}


{% macro validate_non_source_sink_cluster_isolation(target_cluster) %}
{#
  Called from: view, materialized_view, table materializations
  Purpose: Prevent these types from being on clusters with sources or sinks

  Logic:
  1. Skip validation if strict_mode is not enabled
  2. Scan all nodes in the dbt graph (manifest)
  3. Find nodes on the same cluster
  4. If any sources or sinks share this cluster, raise an error
#}

  {# Step 1: Only run validation when strict_mode is enabled #}
  {% if var('strict_mode', False) %}

    {# Step 2: Initialize flags to track isolated types #}
    {% set sources = [] %}
    {% set sinks = [] %}

    {# Step 3: Iterate through all nodes in the dbt manifest #}
    {% for node in graph.nodes.values() %}

      {# Step 4: Get this node's cluster (explicit or default) #}
      {% set node_cluster = node.config.cluster or target.cluster %}

      {# Step 5: Check if this node is on the same cluster #}
      {% if node_cluster == target_cluster %}

        {# Step 6: Check if this node is a source or sink #}
        {% if node.config.materialized == 'source' %}
          {% do sources.append(node.name) %}
        {% elif node.config.materialized == 'sink' %}
          {% do sinks.append(node.name) %}
        {% endif %}

      {% endif %}
    {% endfor %}

    {# Step 7: If cluster contains sources, block creation #}
    {% if sources | length > 0 %}
      {{ exceptions.raise_compiler_error(
        "Cannot create object in cluster '" ~ target_cluster ~ "'. " ~
        "Cluster contains source objects which require dedicated clusters in strict_mode. " ~
        "Sources: " ~ sources | join(", ")
      ) }}
    {% endif %}

    {# Step 8: If cluster contains sinks, block creation #}
    {% if sinks | length > 0 %}
      {{ exceptions.raise_compiler_error(
        "Cannot create object in cluster '" ~ target_cluster ~ "'. " ~
        "Cluster contains sink objects which can only share with other sinks in strict_mode. " ~
        "Sinks: " ~ sinks | join(", ")
      ) }}
    {% endif %}

  {% endif %}
{% endmacro %}
