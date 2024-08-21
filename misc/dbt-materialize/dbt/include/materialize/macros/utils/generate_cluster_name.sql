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

{% macro generate_deploy_cluster_name(custom_cluster_name) -%}
        {{ custom_cluster_name }}_dbt_deploy
{%- endmacro %}

{% macro generate_cluster_name(custom_cluster_name) -%}
    {% if var('deploy', false) and not custom_cluster_name -%}
        {{ exceptions.raise_compiler_error("When the 'deploy' variable is set, you must specify a valid target cluster in 'profiles.yml', or via the 'cluster' configuration") }}
    {% elif custom_cluster_name -%}
        {{ custom_cluster_name }}
    {%- endif %}
{%- endmacro %}
