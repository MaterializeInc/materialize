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

{% macro mz_create_index(obj_name, idx_name=None, default=False, col_refs=None, with_options=None) -%}
  {# todo@jldlaughlin: figure out docs and hooks! #}
  {% set createstmt %}
    CREATE
    {% if default %}
      DEFAULT INDEX
    {% else %}
      INDEX
        {% if idx_name %}
            {{ idx_name }}
        {% endif %}
    {% endif %}
    ON {{ obj_name }}
    {% if col_refs %}
      (
        {% for col in col_refs %}
          {{ col }}{{ ", " if not loop.last else "" }}
        {% endfor %}
      )
    {% endif %}
    {% if with_options %}
      WITH
      (
        {% for option in with_options %}
          {{ option }}{{ ", " if not loop.last else "" }}
        {% endfor %}
      )
    {% endif %}
  {% endset %}

  {% do run_query(createstmt) %}

{% endmacro %}
