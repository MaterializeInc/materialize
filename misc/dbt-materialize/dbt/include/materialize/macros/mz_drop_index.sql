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

{% macro mz_drop_index(name, default=False, if_exists=True, cascade=False) -%}
  {# todo@jldlaughlin: figure out docs and hooks! #}
  {% set dropstmt %}
    DROP INDEX
    {% if if_exists %}
        IF EXISTS
    {% endif %}
    {% if default %}
        {{ "{}_default_idx".format(name.rstrip()) }}
    {% else %}
        {{ name }}
    {% endif %}
    {% if cascade %}
        CASCADE
    {% endif %}
  {% endset %}

  {% do run_query(dropstmt) %}

{% endmacro %}
