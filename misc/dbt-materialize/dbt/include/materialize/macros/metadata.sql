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

-- Most of these macros are direct copies of their PostgreSQL counterparts.
-- See: https://github.com/dbt-labs/dbt-core/blob/13b18654f/plugins/postgres/dbt/include/postgres/macros/adapters.sql

{% macro materialize__get_relation_last_modified(information_schema, relations) -%}

  {% set database = information_schema.database %}
  {%- call statement('last_modified', fetch_result=True) -%}
        select
            s.name as schema,
            o.name as identifier,
            u.last_modified as last_modified,
            {{ current_timestamp() }} as snapshotted_at
        from mz_objects o
        join mz_schemas s on o.schema_id = s.id
        join mz_databases d on s.database_id = d.id and d.name = '{{ database }}'
        join (
            select
                details->>'id' as id,
                max(occurred_at) as last_modified
            from mz_audit_events
            group by 1
        ) u on u.id = o.id
        where (
        {%- for relation in relations -%}
            (upper(s.name) = upper('{{ relation.schema }}') and
            upper(o.name) = upper('{{ relation.identifier }}')){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
        )
  {%- endcall -%}

  {{ return(load_result('last_modified')) }}

{% endmacro %}
