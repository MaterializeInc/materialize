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

{% macro wait_until_ready(cluster, poll_interval) %}
{#
  Waits for all objects within a specified cluster to be fully hydrated,
  polling the cluster's readiness status at a specified interval.

  ## Arguments
  - `cluster_name` (string): The name of the cluster to check for readiness.
  - `poll_interval` (integer): The interval, in seconds, between each readiness check.

  ## Returns
  None: This macro does not return a value but will halt execution until the specified
  cluster's objects are fully hydrated.
#}
{% for i in range(1, 100000) %}
    {% if is_cluster_ready(cluster) %}
        {{ return(true) }}
    {% endif %}
    -- Hydration takes time. Be a good
    -- citizen and don't overwhelm mz_introspection
    {{ adapter.sleep(poll_interval) }}
{% endfor %}
{{ exceptions.raise_compiler_error("Cluster " ~ cluster ~ " failed to hydrate within a reasonable amount of time") }}
{% endmacro %}

{% macro deploy(force=false, poll_interval=15) %}
{#
  Performs atomic deployment of current dbt targets to production,
  based on the deployment configuration specified in the dbt_project.yml file.
  This macro ensures all deployment targets, including schemas and clusters,
  are fully hydrated and deployed together as a single atomic operation.
  If any part of the deployment fails, the entire deployment is rolled back
  to maintain consistency and prevent partial updates.

  ## Arguments
  - `force` (boolean, optional): Skips the hydration checks for deployment targets if set to true. Defaults to false. It is not recommended to override this argument to skip readiness checks, as it may lead to deploying targets that are not fully prepared for production use.
  - `poll_interval` (integer): The interval, in seconds, between each readiness check.

  ## Returns
  None: This macro performs deployment actions but does not return a value.
#}

{% set current_target_name = target.name %}
{% set deployment = var('deployment') %}
{% set target_config = deployment[current_target_name] %}

-- Check if the target-specific configuration exists
{% if not target_config %}
    {{ exceptions.raise_compiler_error("No deployment configuration found for target " ~ current_target_name) }}
{% endif %}

{{ log("Creating deployment environment for target " ~ current_target_name, info=True) }}

{% set clusters = target_config.get('clusters', []) %}
{% set schemas = target_config.get('schemas', []) %}

-- Check that all production schemas
-- and clusters already exist
{% for schema in schemas %}
    {% if not schema_exists(schema.prod) %}
        {{ exceptions.raise_compiler_error("Production schema " ~ schema.prod ~ " does not exist") }}
    {% endif %}
    {% if not schema_exists(schema.prod_deploy) %}
        {{ exceptions.raise_compiler_error("Deployment schema " ~ schema.prod_deploy ~ " does not exist") }}
    {% endif %}
{% endfor %}

{% for cluster in clusters %}
    {% if not cluster_exists(cluster.prod) %}
        {{ exceptions.raise_compiler_error("Production cluster " ~ cluster.prod ~ " does not exist") }}
    {% endif %}
    {% if not cluster_exists(cluster.prod_deploy) %}
        {{ exceptions.raise_compiler_error("Deployment cluster " ~ cluster.prod_deploy ~ " does not exist") }}
    {% endif %}
{% endfor %}

{% if not force %}
    {% for cluster in clusters %}
        {{ wait_until_ready(cluster.prod_deploy, poll_interval) }}
    {% endfor %}
{% endif %}

{% call statement('swap', fetch_result=True, auto_begin=False) -%}
BEGIN;

{% for schema in schemas %}
    {{ log("Swapping schemas " ~ schema.prod ~ " and " ~ schema.prod_deploy, info=True) }}
    ALTER SCHEMA {{ schema.prod }} SWAP WITH {{ schema.prod_deploy }};
{% endfor %}

{% for cluster in clusters %}
    {{ log("Swapping clusters " ~ cluster.prod ~ " and " ~ cluster.prod_deploy, info=True) }}
    ALTER CLUSTER {{ cluster.prod }} SWAP WITH {{ cluster.prod_deploy }};
{% endfor %}

COMMIT;
{%- endcall %}
{% endmacro %}
