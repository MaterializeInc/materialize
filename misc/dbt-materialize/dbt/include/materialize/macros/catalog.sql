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

{% macro materialize__get_catalog(information_schema, schemas) -%}

    {% set query %}
        with tables as (
            {{ materialize__get_catalog_tables_sql(information_schema) }}
            {{ materialize__get_catalog_schemas_where_clause_sql(schemas) }}
        ),
        columns as (
            {{ materialize__get_catalog_columns_sql(information_schema) }}
            {{ materialize__get_catalog_schemas_where_clause_sql(schemas) }}
        )
        {{ materialize__get_catalog_results_sql() }}
    {%- endset -%}

    {{ return(run_query(query)) }}

{%- endmacro %}

{% macro materialize__get_catalog_relations(information_schema, relations) -%}

    {% set query %}
        with tables as (
            {{ materialize__get_catalog_tables_sql(information_schema) }}
            {{ materialize__get_catalog_relations_where_clause_sql(relations) }}
        ),
        columns as (
            {{ materialize__get_catalog_columns_sql(information_schema) }}
            {{ materialize__get_catalog_relations_where_clause_sql(relations) }}
        )
        {{ materialize__get_catalog_results_sql() }}
    {%- endset -%}

    {{ return(run_query(query)) }}

{%- endmacro %}

{% macro materialize__get_catalog_tables_sql(information_schema) -%}
  {% set database = information_schema.database %}
    select
        d.name as table_database,
        s.name as table_schema,
        o.name as table_name,
        case
            --This macro is used for the dbt documentation. We use
            --the source type in mz_sources here instead of that
            --in mz_objects to correctly report subsources.
            when o.type = 'source' then so.type
            else o.type end as table_type,
        obj_desc.comment as table_comment,
        r.name as table_owner
    from mz_objects o
    join mz_schemas s on o.schema_id = s.id
    join mz_databases d on s.database_id = d.id and d.name = '{{ database }}'
    join mz_roles r on o.owner_id = r.id
    left join mz_sources so on o.id = so.id
    left outer join mz_internal.mz_comments obj_desc on (o.id = obj_desc.id and obj_desc.object_sub_id is null)
{%- endmacro %}

{% macro materialize__get_catalog_columns_sql(information_schema) -%}
  {% set database = information_schema.database %}
    select
        d.name as table_database,
        s.name as table_schema,
        o.name as table_name,
        c.name as column_name,
        c.position as column_index,
        c.type as column_type,
        col_desc.comment as column_comment
    from mz_objects o
    join mz_schemas s on o.schema_id = s.id
    join mz_databases d on s.database_id = d.id and d.name = '{{ database }}'
    join mz_columns c on c.id = o.id
    left outer join mz_internal.mz_comments col_desc on (o.id = col_desc.id and col_desc.object_sub_id = c.position)
{%- endmacro %}

{% macro materialize__get_catalog_results_sql() -%}
    select *
    from tables
    join columns using ("table_database", "table_schema", "table_name")
    order by "column_index"
{%- endmacro %}

{% macro materialize__get_catalog_schemas_where_clause_sql(schemas) -%}
    where ({%- for schema in schemas -%}
        upper(s.name) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
    {%- endfor -%})
{%- endmacro %}

{% macro materialize__get_catalog_relations_where_clause_sql(relations) -%}
    where (
        {%- for relation in relations -%}
            {% if relation.schema and relation.identifier %}
                (
                    upper(s.name) = upper('{{ relation.schema }}')
                    and upper(o.name) = upper('{{ relation.identifier }}')
                )
            {% elif relation.schema %}
                (
                    upper(s.name) = upper('{{ relation.schema }}')
                )
            {% else %}
                {% do exceptions.raise_compiler_error(
                    '`get_catalog_relations` requires a list of relations, each with a schema'
                ) %}
            {% endif %}

            {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}
