-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

{% materialization view, adapter='materialize' %}
  {%- set identifier = model['alias'] -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='view') -%}

  {{ adapter.drop_relation(target_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% call statement('main', auto_begin=False) -%}
    {{ create_view_as(target_relation, sql) }}
  {%- endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
