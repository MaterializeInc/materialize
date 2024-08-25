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

{% macro deploy_get_objects(debug=False) %}
    {% set clusters = {} %}
    {% set schemas = {} %}

    {# Get ignored clusters and schemas from project variables #}
    {% set deployment_vars = var('deployment', {}).get(target.name, {}) %}
    {% set ignored_clusters = deployment_vars.get('ignored_clusters', []) %}
    {% set ignored_schemas = deployment_vars.get('ignored_schemas', []) %}

    {% if debug %}
        {{ log("Debug: Starting deploy_get_objects macro", info=True) }}
        {{ log("Debug: Ignored clusters: " ~ ignored_clusters, info=True) }}
        {{ log("Debug: Ignored schemas: " ~ ignored_schemas, info=True) }}
    {% endif %}

    {# Add cluster and schema from the current target #}
    {% if target.cluster and target.cluster not in ignored_clusters %}
        {% do clusters.update({target.cluster: true}) %}
    {% endif %}
    {% if target.schema and target.schema not in ignored_schemas %}
        {% do schemas.update({target.schema: true}) %}
    {% endif %}

    {# Add clusters and schemas from the models #}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {% if node.config.cluster and node.config.cluster not in ignored_clusters %}
                {% do clusters.update({node.config.cluster: true}) %}
            {% endif %}
            {% if node.schema and node.schema not in ignored_schemas %}
                {% do schemas.update({node.schema: true}) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% set cluster_list = clusters.keys() | list %}
    {% set schema_list = schemas.keys() | list %}

    {% if debug %}
        {{ log("Debug: Final cluster list: " ~ cluster_list, info=True) }}
        {{ log("Debug: Final schema list: " ~ schema_list, info=True) }}
    {% endif %}

    {% do return({'clusters': cluster_list, 'schemas': schema_list}) %}
{% endmacro %}
