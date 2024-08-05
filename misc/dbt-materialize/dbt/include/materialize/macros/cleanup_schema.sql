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

-- This macro resets the target schema by dropping and recreating it.
{% macro cleanup_schema(dry_run=False) %}
  {% set target_schema = target.schema %}
  {% set target_database = target.database %}

  {% if execute %}
    {% set drop_schema_sql %}
      DROP SCHEMA IF EXISTS {{ target_database }}.{{ target_schema }} CASCADE;
    {% endset %}

    {% set create_schema_sql %}
      CREATE SCHEMA {{ target_database }}.{{ target_schema }};
    {% endset %}

    {% if dry_run %}
      {{ log("Dry run. Would execute:", info=True) }}
      {{ log(drop_schema_sql, info=True) }}
      {{ log(create_schema_sql, info=True) }}
    {% else %}
      {% do log("Executing cleanup SQL...", info=True) %}
      {% do adapter.commit() %}
      {% do adapter.execute(drop_schema_sql) %}
      {% do adapter.commit() %}
      {% do adapter.execute(create_schema_sql) %}
      {% do adapter.commit() %}
      {% do log("Cleanup complete. Schema " ~ target_database ~ "." ~ target_schema ~ " has been reset.", info=True) %}
    {% endif %}
  {% endif %}
{% endmacro %}
