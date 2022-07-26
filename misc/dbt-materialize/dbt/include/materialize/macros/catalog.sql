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

  {%- call statement('catalog', fetch_result=True) -%}
    {#
      If the user has multiple databases set and the first one is wrong, this will fail.
      But we won't fail in the case where there are multiple quoting-difference-only dbs, which is better.
    #}
    {% set database = information_schema.database %}
    {{ adapter.verify_database(database) }}

    select
        '{{ database }}' as table_database,
        sch.nspname as table_schema,
        tbl.relname as table_name,
        case tbl.relkind
            when 'v' then 'VIEW'
            else 'BASE TABLE'
        end as table_type,
        tbl_desc.description as table_comment,
        col.attname as column_name,
        col.attnum as column_index,
        col.type as column_type,
        col_desc.description as column_comment,
        pg_get_userbyid(tbl.relowner) as table_owner

    from pg_catalog.pg_namespace sch
    join pg_catalog.pg_class tbl on tbl.relnamespace = sch.oid
    join (select mz_columns.name as attname, position as attnum, mz_relations.oid as attrelid, FALSE as attisdropped, mz_columns.type as type
          from mz_columns join mz_relations on mz_columns.id = mz_relations.id)
          as col on col.attrelid = tbl.oid
    left outer join pg_catalog.pg_description tbl_desc on (tbl_desc.objoid = tbl.oid and tbl_desc.objsubid = 0)
    left outer join pg_catalog.pg_description col_desc on (col_desc.objoid = tbl.oid and col_desc.objsubid = col.attnum)

    where (
        {%- for schema in schemas -%}
          upper(sch.nspname) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
      )
      and tbl.relkind in ('r', 'v', 'f', 'p', 'm') -- o[r]dinary table, [v]iew, [f]oreign table, [p]artitioned table, [m]aterialized view. Other values are [i]ndex, [S]equence, [c]omposite type, [t]OAST table
      and col.attnum > 0 -- negative numbers are used for system columns such as oid
      and not col.attisdropped -- column as not been dropped

    order by
        sch.nspname,
        tbl.relname,
        col.attnum

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}

{% macro postgres__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.database }}' as database,
      tablename as name,
      schemaname as schema,
      'table' as type
    from pg_tables
    where schemaname ilike '{{ schema_relation.schema }}'
    union all
    select
      '{{ schema_relation.database }}' as database,
      viewname as name,
      schemaname as schema,
      'view' as type
    from pg_views
    where schemaname ilike '{{ schema_relation.schema }}'
    union all
    select
      '{{ schema_relation.database }}' as database,
      matviewname as name,
      schemaname as schema,
      'materializedview' as type
    from pg_matviews
    where schemaname ilike '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}
