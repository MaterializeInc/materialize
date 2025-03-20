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

-- Due to a PostgreSQL compatibility issue in processing aliases in the GROUP BY
-- clause, we must override this macro.
-- See: https://github.com/dbt-labs/dbt-postgres/blob/main/dbt/include/postgres/macros/relations.sql

{% macro materialize__get_relations() -%}

  {%- call statement('relations', fetch_result=True) -%}
    with relation as (
        select
            pg_rewrite.ev_class as class,
            pg_rewrite.oid as id
        from pg_rewrite
    ),
    class as (
        select
            oid as id,
            relname as name,
            relnamespace as schema,
            relkind as kind
        from pg_class
    ),
    dependency as (
        select distinct
            objid as id,
            refobjid as ref
        from pg_depend
    ),
    schema as (
        select
            oid as id,
            nspname as name
        from pg_namespace
        where nspname != 'information_schema' and nspname not like 'pg\_%'
    ),
    referenced as (
        select
            relation.id AS id,
            referenced_class.name,
            referenced_class.schema,
            referenced_class.kind
        from relation
        join class as referenced_class on relation.class=referenced_class.id
        where referenced_class.kind in ('r', 'v', 'm')
    ),
    relationships as (
        select
            referenced.name as referenced_name,
            referenced.schema as referenced_schema_id,
            dependent_class.name as dependent_name,
            dependent_class.schema as dependent_schema_id,
            referenced.kind as kind
        from referenced
        join dependency on referenced.id=dependency.id
        join class as dependent_class on dependency.ref=dependent_class.id
        where
            (referenced.name != dependent_class.name or
             referenced.schema != dependent_class.schema)
    )

    select
        referenced_schema.name as referenced_schema,
        relationships.referenced_name as referenced_name,
        dependent_schema.name as dependent_schema,
        relationships.dependent_name as dependent_name
    from relationships
    join schema as dependent_schema on relationships.dependent_schema_id=dependent_schema.id
    join schema as referenced_schema on relationships.referenced_schema_id=referenced_schema.id
    group by referenced_schema.name, referenced_name, dependent_schema.name, dependent_name
    order by referenced_schema, referenced_name, dependent_schema, dependent_name;
  {%- endcall -%}

  {{ return(load_result('relations').table) }}
{% endmacro %}

{% macro materialize_get_relations() %}
  {{ return(materialize__get_relations()) }}
{% endmacro %}
