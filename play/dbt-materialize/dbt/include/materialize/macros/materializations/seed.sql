{% macro materialize__load_csv_rows(model, agate_table) -%}
  {% set cols_sql %}
    {% for column in agate_table.column_names %}
      column{{loop.index}} as {{column}}
      {%- if not loop.last%},{%- endif %}
    {% endfor %}
  {% endset %}

  {% set bindings = [] %}
  {% for chunk in agate_table.rows | batch(1000) %}
        {% for row in chunk %}
            {% do bindings.extend(row) %}
        {% endfor %}
  {% endfor %}

  {% set sql %}
    -- {{ bindings | length }}
    create materialized view {{ this.render() }} AS (
      select {{ cols_sql }} from (VALUES
      {% for chunk in agate_table.rows | batch(1)  -%} 
        ({%- for column in agate_table.column_names -%}
            %s
            {%- if not loop.last%},{%- endif %}
        {%- endfor -%})
        {%- if not loop.last%},{%- endif %}
      {%- endfor %}         
      ) AS tbl
    )
  {% endset %}

  {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True, auto_begin=False) %}
  {{ return(sql) }}

{%- endmacro %}

{% materialization seed, adapter='materialize' %}

  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', status='OK', agate_table=agate_table) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% if old_relation %}
     {{ adapter.drop_relation(old_relation) }}
  {% endif %}

  -- build model
  {% set status = 'CREATE' %}
  {% set sql = load_csv_rows(model, agate_table) %}

  {% call noop_statement('main', status) %}
    -- dbt seed --
    {{ sql }}
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% set target_relation = this.incorporate(type='materializedview') %}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
