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
-- This macro drops a cluster used in CI.
-- Parameters:
--   cluster_name (str): The name of the cluster to drop.
#}
{% macro drop_cluster(cluster_name) %}

{%- if cluster_name -%}

  {{ log("Dropping cluster " ~ cluster_name ~ "...", info=True) }}

  {% call statement('drop_cluster') -%}
      DROP CLUSTER IF EXISTS {{ cluster_name }} CASCADE
  {%- endcall -%}

  {{ log("Cluster " ~ cluster_name ~ " dropped.", info=True) }}

{%- else -%}

  {{ exceptions.raise_compiler_error("Invalid arguments. Missing cluster name!") }}

{%- endif -%}

{% endmacro %}
