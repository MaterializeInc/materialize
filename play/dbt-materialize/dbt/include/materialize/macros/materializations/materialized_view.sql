{% materialization materializedview, adapter='materialize' %}
  {%- set identifier = model['alias'] -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='materializedview') -%}

  {{ adapter.drop_relation(target_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% call statement('main', auto_begin=False) -%}
    {{ materialize__create_materialized_view_as(target_relation, sql) }}
  {%- endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
