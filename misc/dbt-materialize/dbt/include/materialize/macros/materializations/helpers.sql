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

{% macro run_hooks(hooks, inside_transaction=True) %}
  {% for hook in hooks | selectattr('transaction', 'equalto', inside_transaction)  %}
    {% set rendered = render(hook.get('sql')) | trim %}
    {% if (rendered | length) > 0 %}
      {% call statement(auto_begin=inside_transaction) %}
        {{ rendered }}
      {% endcall %}
    {% endif %}
  {% endfor %}
{% endmacro %}
