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

  {% set database = information_schema.database %}
  {{ adapter.verify_database(database) }}

  {%- call statement('catalog', fetch_result=True) -%}
        select
            d.name as table_database,
            s.name as table_schema,
            o.name as table_name,
            case when o.type = 'materialized-view' then 'materialized_view'
                 --This macro is used for the dbt documentation. We use
                 --the source type in mz_sources here instead of that
                 --in mz_objects to correctly report subsources.
                 when o.type = 'source' and so.type = 'subsource' then so.type
                 when o.type = 'source' and so.type = 'progress' then so.type
                 else o.type end as table_type,
            obj_desc.comment as table_comment,
            c.name as column_name,
            c.position as column_index,
            c.type as column_type,
            col_desc.comment as column_comment,
            r.name as table_owner
        from mz_objects o
        join mz_schemas s on o.schema_id = s.id
        join mz_databases d on s.database_id = d.id and d.name = '{{ database }}'
        join mz_columns c on c.id = o.id
        join mz_roles r on o.owner_id = r.id
        left join mz_sources so on o.id = so.id
        left outer join mz_internal.mz_comments obj_desc on (o.id = obj_desc.id and obj_desc.object_sub_id is null)
        left outer join mz_internal.mz_comments col_desc on (o.id = col_desc.id and col_desc.object_sub_id = c.position)
        where s.name in (
            {%- for schema in schemas -%}
                '{{ schema }}' {%- if not loop.last %}, {% endif -%}
            {%- endfor -%}
        )
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
