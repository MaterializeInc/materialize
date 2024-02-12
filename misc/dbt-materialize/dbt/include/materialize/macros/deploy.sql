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

{% macro wait_until_ready(cluster) %}
{% for i in range(1, 100000) %}
    {% if is_cluster_ready(cluster) %}
        {{ return(true) }}
    {% endif %}
    -- Hydration takes time. Be a good
    -- citizen and don't overwhelm mz_introspection
    {{ adapter.sleep(5) }}
{% endfor %}
{{ exceptions.CompilationError("Cluster " ~ cluster ~ " failed to hydrate within a reasonable amount of time") }}
{% endmacro %}

{% macro deploy(force=false) %}

{% set current_target_name = target.name %}
{% set deployment = var('deployment') %}
{% set target_config = deployment[current_target_name] %}

-- Check if the target-specific configuration exists
{% if not target_config %}
    {{ log("No deployment configuration found for target " ~ current_target_name, info=True) }}
{% endif %}

{{ log("Creating deployment environment for target " ~ current_target_name, info=True) }}

{% set clusters = target_config.get('clusters', []) %}
{% set schemas = target_config.get('schemas', []) %}

        -- Check that all production schemas
        -- and clusters already exist
{% for schema in schemas %}
    {% if not schema_exist(schema.prod) %}
        {{ exceptions.CompilationError("Production schema " ~ schema.prod ~ " does not exist") }}
    {% endif %}
    {% if not schema_exist(schema.prod_deploy) %}
        {{ exceptions.CompilationError("Deployment schema " ~ schema.prod_deploy ~ " does not exist") }}
    {% endif %}
{% endfor %}

{% for cluster in clusters %}
    {% if not cluster_exists(cluster.prod) %}
        {{ exceptions.CompilationError("Production cluster " ~ cluster.prod ~ " does not exist") }}
    {% endif %}
    {% if not cluster_exists(cluster.prod_deploy) %}
        {{ exceptions.CompilationError("Deployment cluster " ~ cluster.prod_deploy ~ " does not exist") }}
    {% endif %}
{% endfor %}

{% if not force %}
    {% for cluster in clusters %}
        {{ wait_until_ready(cluster.prod_deploy) }}
    {% endfor %}
{% endif %}

{% call statement('swap', fetch_result=True, auto_begin=False) -%}
BEGIN;

{% for schema in schemas %}
    {{ log("swapping schemas " ~ schema.prod ~ " and " ~ schema.prod_deploy, info=True) }}
    ALTER SCHEMA {{ schema.prod }} SWAP WITH {{ schema.prod_deploy }};
{% endfor %}

{% for cluster in clusters %}
    {{ log("swapping clusters " ~ cluster.prod ~ " and " ~ cluster.prod_deploy, info=True) }}
    ALTER CLUSTER {{ cluster.prod }} SWAP WITH {{ cluster.prod_deploy }};
{% endfor %}

COMMIT;
{%- endcall %}
{% endmacro %}
