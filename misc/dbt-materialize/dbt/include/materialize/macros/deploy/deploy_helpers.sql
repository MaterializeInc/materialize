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
Returns the deployment configuration for the current target, raising a compiler
error when the `deployment` variable has no entry for it. Every deploy operation
starts from this configuration.
#}
{% macro internal_get_deployment_config() %}
    {% set target_config = var('deployment')[target.name] %}

    {% if not target_config %}
        {{ exceptions.raise_compiler_error("No deployment configuration found for target " ~ target.name) }}
    {% endif %}

    {{ return(target_config) }}
{% endmacro %}
