-- Copyright 2020 Josh Wills. All rights reserved.
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

-- Fork of generate_schema_name to add deployment suffix
-- See https://docs.getdbt.com/docs/build/custom-schemas
{% macro materialize__generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- set deploy_suffix = "_dbt_deploy" if var('deploy', False) else "" -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}{{ deploy_suffix }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}{{ deploy_suffix }}

    {%- endif -%}

{%- endmacro %}
