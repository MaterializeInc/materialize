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

{% macro deploy_cleanup() %}

{% set current_target_name = target.name %}
{% set deployment = var('deployment') %}
{% set target_config = deployment[current_target_name] %}

-- Check if the target-specific configuration exists
{% if not target_config %}
    {{ exceptions.raise_compiler_error("No deployment configuration found for target " ~ current_target_name) }}
{% endif %}

{{ log("Dropping deployment environment for target " ~ current_target_name, info=True) }}

{% set clusters = target_config.get('clusters', []) %}
{% set schemas = target_config.get('schemas', []) %}

{% for schema in schemas %}
    {% set deploy_schema = schema ~ "_dbt_deploy" %}
    {{ log("Dropping schema " ~ deploy_schema ~ " for target " ~ current_target_name, info=True) }}
    {% set drop_schema %}
    DROP SCHEMA IF EXISTS {{ adapter.quote(deploy_schema) }} CASCADE;
    {% endset %}
    {{ run_query(drop_schema) }}
{% endfor %}

{% for cluster in clusters %}
    {% set deploy_cluster = adapter.generate_final_cluster_name(cluster, force_deploy_suffix=True) %}
    {{ log("Dropping cluster " ~ deploy_cluster ~ " for target " ~ current_target_name, info=True) }}
    {% set drop_cluster %}
    DROP CLUSTER IF EXISTS {{ adapter.quote(deploy_cluster) }} CASCADE;
    {% endset %}
    {{ run_query(drop_cluster) }}
{% endfor %}

{% endmacro %}
