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

{% macro materialize__cast(expression, data_type) -%}
    {#-- Handle types that don't support cast(NULL as type) --#}
    {%- if expression.strip().lower() == "null" and data_type.strip().lower() == "map" -%}
      NULL::map[text => text]
    {%- elif expression.strip().lower() == "null" and data_type.strip().lower() == "list" -%}
       NULL::text list
    {%- elif expression.strip().lower() == "null" and data_type.strip().lower() == "record" -%}
      (SELECT row() WHERE false)
    {%- else -%}
      cast({{ expression }} as {{ data_type }})
    {%- endif -%}
{%- endmacro %}
