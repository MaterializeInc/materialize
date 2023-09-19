{% macro cluster_test_sql(main_sql, fail_calc, warn_if, error_if, limit, cluster) -%}
    {% if cluster %}
        {% call statement(auto_begin=True) %}
            set cluster = {{ cluster }}
        {% endcall %}
    {% endif %}

  {{ adapter.dispatch('get_test_sql', 'dbt')(main_sql, fail_calc, warn_if, error_if, limit) }}
{%- endmacro %}