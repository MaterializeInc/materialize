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
    {% set excluded_clusters_sinks = {} %}
    {% set excluded_schemas_sinks = {} %}
    {% set checked_clusters = {} %}
    {% set checked_schemas = {} %}

    {# Get clusters and schemas to exclude from project variables #}
    {% set deployment_vars = var('deployment', {}).get(target.name, {}) %}
    {% set exclude_clusters = deployment_vars.get('exclude_clusters', []) %}
    {% set exclude_schemas = deployment_vars.get('exclude_schemas', []) %}

    {# Handle None values for exclude_clusters and exclude_schemas #}
    {% set exclude_clusters = exclude_clusters if exclude_clusters is not none else [] %}
    {% set exclude_schemas = exclude_schemas if exclude_schemas is not none else [] %}

    {# Flatten exclude_clusters and exclude_schemas #}
    {% set flattened_exclude_clusters = [] %}
    {% for item in exclude_clusters %}
        {% if item is string %}
            {% do flattened_exclude_clusters.append(item) %}
        {% elif item is iterable and item is not string %}
            {% do flattened_exclude_clusters.extend(item) %}
        {% endif %}
    {% endfor %}

    {% set flattened_exclude_schemas = [] %}
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

    {# Add cluster and schema from the current target #}
    {% if target.cluster and target.cluster not in flattened_exclude_clusters and target.cluster not in checked_clusters %}
        {% do checked_clusters.update({target.cluster: true}) %}
        {% if not cluster_contains_sinks(target.cluster) %}
            {% do clusters.update({target.cluster: true}) %}
        {% else %}
            {% do excluded_clusters_sinks.update({target.cluster: true}) %}
        {% endif %}
    {% endif %}

    {% if target.schema and target.schema not in flattened_exclude_schemas and target.schema not in checked_schemas %}
        {% do checked_schemas.update({target.schema: true}) %}
        {% if not schema_contains_sinks(target.schema) %}
            {% do schemas.update({target.schema: true}) %}
        {% else %}
            {% do excluded_schemas_sinks.update({target.schema: true}) %}
        {% endif %}
    {% endif %}

    {# Add clusters and schemas from models, seeds, and tests #}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type in ['model', 'seed', 'test'] %}
            {% set node_cluster = node.config.get('cluster', target.cluster) %}
            {% if node_cluster and node_cluster not in flattened_exclude_clusters and node_cluster not in checked_clusters %}
                {% do checked_clusters.update({node_cluster: true}) %}
                {% if not cluster_contains_sinks(node_cluster) %}
                    {% do clusters.update({node_cluster: true}) %}
                {% else %}
                    {% do excluded_clusters_sinks.update({node_cluster: true}) %}
                {% endif %}
            {% endif %}

            {% if node.schema and node.schema not in flattened_exclude_schemas and node.schema not in checked_schemas %}
                {% do checked_schemas.update({node.schema: true}) %}
                {% if not schema_contains_sinks(node.schema) %}
                    {% do schemas.update({node.schema: true}) %}
                {% else %}
                    {% do excluded_schemas_sinks.update({node.schema: true}) %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% set cluster_list = clusters.keys() | list %}
    {% set schema_list = schemas.keys() | list %}

    {% if dry_run %}
        {{ log("DRY RUN: Excluded clusters containing sinks: " ~ excluded_clusters_sinks.keys() | list, info=True) }}
        {{ log("DRY RUN: Excluded schemas containing sinks: " ~ excluded_schemas_sinks.keys() | list, info=True) }}
        {{ log("DRY RUN: Final cluster list: " ~ cluster_list, info=True) }}
        {{ log("DRY RUN: Final schema list: " ~ schema_list, info=True) }}
    {% endif %}

    {% do return({'clusters': cluster_list, 'schemas': schema_list}) %}
{% endmacro %}
