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

{% macro drop_deployment_environment() %}

{% set current_target_name = target.name %}
{% set deployment = var('deployment') %}
{% set target_config = deployment[current_target_name] %}

-- Check if the target-specific configuration exists
{% if target_config %}
    {{ exceptions.CompilationError("No deployment configuration found for target " ~ current_target_name) }}
{% endif %}

{{ log("Dropping deployment environment for target " ~ current_target_name, info=True) }}

{% set clusters = target_config.get('clusters', []) %}
{% set schemas = target_config.get('schemas', []) %}

{% for schema in schemas %}
    {{ log("Dropping schema " ~ schema.prod_deploy ~ " for target " ~ current_target_name, info=True) }}
    DROP SCHEMA IF EXISTS {{ schema.prod_deploy }} CASCADE;
{% endfor %}

{% for cluster in clusters %}
    {{ log("Dropping cluster " ~ cluster.prod_deploy ~ " for target " ~ current_target_name, info=True) }}
    DROP CLUSTER IF EXISTS {{ cluster.prod_deploy }} CASCADE;
{% endfor %}

{% endmacro %}
