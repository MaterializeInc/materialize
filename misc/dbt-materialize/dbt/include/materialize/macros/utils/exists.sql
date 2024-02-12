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

{% macro schema_exist(schema) %}
{% set query %}
    SELECT *
    FROM mz_schemas
    JOIN mz_databases ON mz_schemas.database_id = mz_databases.id
    WHERE mz_databases.name = current_database()
    AND mz_schemas.name = lower('{{ schema }}')
{%- endset -%}

{% set results = run_query(query) %}

{% if results|length > 0 %}
    {{ return(true) }}
{% else %}
    {{ return(false) }}
{% endif %}
{% endmacro %}

{% macro cluster_exists(cluster) %}
{% set query %}
    SELECT * FROM mz_clusters
    WHERE name = lower('{{ cluster }}')
{%- endset -%}

{% set results = run_query(query) %}

{% if results|length > 0 %}
    {{ return(true) }}
{% else %}
    {{ return(false) }}
{% endif %}
{% endmacro %}
