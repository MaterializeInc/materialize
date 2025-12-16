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

{% materialization source, adapter='materialize' %}
  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(identifier=identifier,
                                              schema=schema,
                                              database=database) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='source') -%}

  {% if old_relation %}
    {% if var('strict_mode', False) %}
      {# In strict_mode, skip recreation if relation exists #}
      {{ log("Relation " ~ old_relation ~ " already exists, skipping creation.", info=True) }}
      {% call statement('main') -%}
        SELECT 1
      {%- endcall %}
      {{ return({'relations': [target_relation]}) }}
    {% else %}
      {{ adapter.drop_relation(old_relation) }}
    {% endif %}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main') -%}
    {{ materialize__create_source(target_relation, sql) }}
  {%- endcall %}

  {{ create_indexes(target_relation) }}
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
