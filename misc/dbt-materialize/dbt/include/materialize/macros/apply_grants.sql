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

{%- macro quote_grantees(grantees) -%}
    {% set quoted_grantees = [] %}
        {%- for grantee in grantees -%}
        {%- do quoted_grantees.append(adapter.quote(grantee)) -%}
    {%- endfor -%}
    {{ return(quoted_grantees) }}
{%- endmacro -%}

{%- macro materialize__get_grant_sql(relation, privilege, grantees) -%}
    {% set quoted_grantees = quote_grantees(grantees) %}
    grant {{ privilege }} on {{ relation }} to {{ quoted_grantees | join(', ') }}
{%- endmacro -%}

{%- macro materialize__get_revoke_sql(relation, privilege, grantees) -%}
    {% set quoted_grantees = quote_grantees(grantees) %}
    revoke {{ privilege }} on {{ relation }} from {{ quoted_grantees | join(', ') }}
{%- endmacro -%}

{%- macro materialize__get_show_grant_sql(relation) -%}
  select grantee, privilege_type
  from {{ relation.information_schema('role_table_grants') }}
      where grantor = current_role
        and grantee != current_role
        and table_schema = '{{ relation.schema }}'
        and table_name = '{{ relation.identifier }}'
{%- endmacro -%}
