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

{% macro deploy_get_objects(dry_run=False) %}
    {% set clusters = {} %}
    {% set schemas = {} %}
    {% set sink_clusters = {} %}
    {% set sink_schemas = {} %}
    {% set mixed_clusters = {} %}
    {% set mixed_schemas = {} %}

    {# Get clusters and schemas to exclude from project variables #}
    {% set deployment_vars = var('deployment', {}).get(target.name, {}) %}
    {% set exclude_clusters = deployment_vars.get('exclude_clusters', []) %}
    {% set exclude_schemas = deployment_vars.get('exclude_schemas', []) %}

    {# Handle None values for exclude_clusters and exclude_schemas #}
    {% set exclude_clusters = exclude_clusters if exclude_clusters is not none else [] %}
    {% set exclude_schemas = exclude_schemas if exclude_schemas is not none else [] %}

    {# Flatten exclude_clusters and exclude_schemas #}
    {% set flattened_exclude_clusters = [] %}
    {% set flattened_exclude_schemas = [] %}
    {% for item in exclude_clusters %}
        {% if item is string %}
            {% do flattened_exclude_clusters.append(item) %}
        {% elif item is iterable and item is not string %}
            {% do flattened_exclude_clusters.extend(item) %}
        {% endif %}
    {% endfor %}
    {% for item in exclude_schemas %}
        {% if item is string %}
            {% do flattened_exclude_schemas.append(item) %}
        {% elif item is iterable and item is not string %}
            {% do flattened_exclude_schemas.extend(item) %}
        {% endif %}
    {% endfor %}

    {% if dry_run %}
        {{ log("DRY RUN: Starting deploy_get_objects macro", info=True) }}
        {{ log("DRY RUN: Excluded clusters from deployment: " ~ flattened_exclude_clusters, info=True) }}
        {{ log("DRY RUN: Excluded schemas from deployment: " ~ flattened_exclude_schemas, info=True) }}
    {% endif %}

    {# Add default cluster from profiles.yml if not excluded #}
    {% set default_cluster = target.cluster %}
    {% set default_schema = target.schema %}

    {# Analyze dbt model graph #}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type in ['model', 'seed', 'test'] %}
            {% set node_cluster = node.config.get('cluster', default_cluster) %}
            {% set node_schema = node.schema %}
            {% set is_sink = node.config.materialized == 'sink' %}

            {% if is_sink %}
                {% do sink_clusters.update({node_cluster: true}) %}
                {% do sink_schemas.update({node_schema: true}) %}
                {% if node_cluster in clusters %}
                    {% do mixed_clusters.update({node_cluster: true}) %}
                {% endif %}
                {% if node_schema in schemas %}
                    {% do mixed_schemas.update({node_schema: true}) %}
                {% endif %}
            {% else %}
                {% if node_cluster and node_cluster not in flattened_exclude_clusters %}
                    {% do clusters.update({node_cluster: true}) %}
                    {% if node_cluster in sink_clusters %}
                        {% do mixed_clusters.update({node_cluster: true}) %}
                    {% endif %}
                {% endif %}
                {% if node_schema and node_schema not in flattened_exclude_schemas %}
                    {% do schemas.update({node_schema: true}) %}
                    {% if node_schema in sink_schemas %}
                        {% do mixed_schemas.update({node_schema: true}) %}
                    {% endif %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# Check for mixed clusters and schemas #}
    {% if mixed_clusters %}
        {% set error_message %}
Error: The following clusters contain both sinks and other models:
{{ mixed_clusters.keys() | join(", ") }}
Sinks must be in dedicated clusters separate from other models.
        {% endset %}
        {{ exceptions.raise_compiler_error(error_message) }}
    {% endif %}

    {% if mixed_schemas %}
        {% set error_message %}
Error: The following schemas contain both sinks and other models:
{{ mixed_schemas.keys() | join(", ") }}
Sinks must be in dedicated schemas separate from other models.
        {% endset %}
        {{ exceptions.raise_compiler_error(error_message) }}
    {% endif %}

    {# Remove sink clusters and schemas from deployment lists #}
    {% for cluster in sink_clusters %}
        {% do clusters.pop(cluster, None) %}
    {% endfor %}
    {% for schema in sink_schemas %}
        {% do schemas.pop(schema, None) %}
    {% endfor %}

    {% set final_clusters = clusters.keys() | list %}
    {% set final_schemas = schemas.keys() | list %}

    {% if dry_run %}
        {{ log("DRY RUN: Default cluster from profiles.yml: " ~ default_cluster, info=True) }}
        {{ log("DRY RUN: Default schema from profiles.yml: " ~ default_schema, info=True) }}
        {{ log("DRY RUN: Clusters containing sinks (excluded from deployment): " ~ sink_clusters.keys() | list, info=True) }}
        {{ log("DRY RUN: Schemas containing sinks (excluded from deployment): " ~ sink_schemas.keys() | list, info=True) }}
        {{ log("DRY RUN: Final cluster list for deployment: " ~ final_clusters, info=True) }}
        {{ log("DRY RUN: Final schema list for deployment: " ~ final_schemas, info=True) }}
    {% endif %}

    {% do return({'clusters': final_clusters, 'schemas': final_schemas}) %}
{% endmacro %}
