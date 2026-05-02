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

{#
  Schema Isolation Validation Macros
  ==================================

  These macros enforce schema isolation rules when strict_mode is enabled.
  They use the dbt manifest/graph for compile-time validation, meaning:
  - No database connection required
  - Validation happens during `dbt compile` or `dbt run`
  - Conflicts are caught before any SQL is executed

  Rules enforced:
  1. source_tables can only share a schema with other source_tables or seeds
  2. sinks can only share a schema with other sinks
  3. Other types (view, materialized_view, source, table) cannot be in schemas
     that contain source_tables or sinks

  Enable strict_mode in dbt_project.yml:
    vars:
      strict_mode: true
#}


{% macro validate_source_table_schema_isolation(target_schema, target_database) %}
{#
  Called from: source_table materialization
  Purpose: Ensure source_tables are isolated from incompatible types

  Logic:
  1. Skip validation if strict_mode is not enabled
  2. Scan all nodes in the dbt graph (manifest)
  3. Find nodes in the same schema/database
  4. Collect any materialization types that aren't allowed (not source_table or seed)
  5. If forbidden types found, raise an error
#}

  {# Step 1: Only run validation when strict_mode is enabled #}
  {% if var('strict_mode', False) %}

    {# Step 2: Initialize list to collect forbidden materialization types #}
    {% set forbidden_types = [] %}

    {# source_tables can coexist with other source_tables and seeds (both create tables) #}
    {% set allowed_types = ['source_table', 'seed'] %}

    {# Step 3: Iterate through all nodes in the dbt manifest #}
    {% for node in graph.nodes.values() %}

      {# Step 4: Check if this node is in the same schema and database #}
      {% if node.schema == target_schema and node.database == target_database %}

        {% set materialized = node.config.materialized %}

        {# Step 5: If the node's materialization type is not allowed, record it #}
        {% if materialized and materialized not in allowed_types %}
          {% do forbidden_types.append(materialized) %}
        {% endif %}

      {% endif %}
    {% endfor %}

    {# Step 6: Remove duplicates from the list #}
    {% set forbidden_types = forbidden_types | unique | list %}

    {# Step 7: If any forbidden types were found, raise a compiler error #}
    {% if forbidden_types | length > 0 %}
      {{ exceptions.raise_compiler_error(
        "Cannot create source_table in schema '" ~ target_schema ~ "'. " ~
        "Schema contains other materialization types: " ~ forbidden_types | join(", ") ~ ". " ~
        "source_table materializations must be in a dedicated schema."
      ) }}
    {% endif %}

  {% endif %}
{% endmacro %}


{% macro validate_sink_schema_isolation(target_schema, target_database) %}
{#
  Called from: sink materialization
  Purpose: Ensure sinks are isolated from all other types

  Logic:
  1. Skip validation if strict_mode is not enabled
  2. Scan all nodes in the dbt graph (manifest)
  3. Find nodes in the same schema/database
  4. Collect any materialization types that aren't 'sink'
  5. If forbidden types found, raise an error
#}

  {# Step 1: Only run validation when strict_mode is enabled #}
  {% if var('strict_mode', False) %}

    {# Step 2: Initialize list to collect forbidden materialization types #}
    {% set forbidden_types = [] %}

    {# Step 3: Iterate through all nodes in the dbt manifest #}
    {% for node in graph.nodes.values() %}

      {# Step 4: Check if this node is in the same schema and database #}
      {% if node.schema == target_schema and node.database == target_database %}

        {% set materialized = node.config.materialized %}

        {# Step 5: If the node is not a sink, record its type #}
        {% if materialized and materialized != 'sink' %}
          {% do forbidden_types.append(materialized) %}
        {% endif %}

      {% endif %}
    {% endfor %}

    {# Step 6: Remove duplicates from the list #}
    {% set forbidden_types = forbidden_types | unique | list %}

    {# Step 7: If any forbidden types were found, raise a compiler error #}
    {% if forbidden_types | length > 0 %}
      {{ exceptions.raise_compiler_error(
        "Cannot create sink in schema '" ~ target_schema ~ "'. " ~
        "Schema contains other materialization types: " ~ forbidden_types | join(", ") ~ ". " ~
        "sink materializations must be in a dedicated schema."
      ) }}
    {% endif %}

  {% endif %}
{% endmacro %}


{% macro validate_source_schema_isolation(target_schema, target_database) %}
{#
  Called from: source materialization
  Purpose: Ensure sources are isolated from all other types

  Logic:
  1. Skip validation if strict_mode is not enabled
  2. Scan all nodes in the dbt graph (manifest)
  3. Find nodes in the same schema/database
  4. Collect any materialization types that aren't 'source'
  5. If forbidden types found, raise an error
#}

  {# Step 1: Only run validation when strict_mode is enabled #}
  {% if var('strict_mode', False) %}

    {# Step 2: Initialize list to collect forbidden materialization types #}
    {% set forbidden_types = [] %}

    {# Step 3: Iterate through all nodes in the dbt manifest #}
    {% for node in graph.nodes.values() %}

      {# Step 4: Check if this node is in the same schema and database #}
      {% if node.schema == target_schema and node.database == target_database %}

        {% set materialized = node.config.materialized %}

        {# Step 5: If the node is not a source, record its type #}
        {% if materialized and materialized != 'source' %}
          {% do forbidden_types.append(materialized) %}
        {% endif %}

      {% endif %}
    {% endfor %}

    {# Step 6: Remove duplicates from the list #}
    {% set forbidden_types = forbidden_types | unique | list %}

    {# Step 7: If any forbidden types were found, raise a compiler error #}
    {% if forbidden_types | length > 0 %}
      {{ exceptions.raise_compiler_error(
        "Cannot create source in schema '" ~ target_schema ~ "'. " ~
        "Schema contains other materialization types: " ~ forbidden_types | join(", ") ~ ". " ~
        "source materializations must be in a dedicated schema."
      ) }}
    {% endif %}

  {% endif %}
{% endmacro %}


{% macro validate_non_source_table_schema_isolation(target_schema, target_database) %}
{#
  Called from: view, materialized_view, table materializations
  Purpose: Prevent these types from being created in schemas reserved for
           sources, source_tables, or sinks

  Logic:
  1. Skip validation if strict_mode is not enabled
  2. Scan all nodes in the dbt graph (manifest)
  3. Find nodes in the same schema/database
  4. Check if any are sources, sinks, or source_tables
  5. If found, raise an error (schema is reserved)
#}

  {# Step 1: Only run validation when strict_mode is enabled #}
  {% if var('strict_mode', False) %}

    {# Step 2: Initialize flags to track if we find isolated types #}
    {% set has_source = false %}
    {% set has_sink = false %}
    {% set has_source_table = false %}

    {# Step 3: Iterate through all nodes in the dbt manifest #}
    {% for node in graph.nodes.values() %}

      {# Step 4: Check if this node is in the same schema and database #}
      {% if node.schema == target_schema and node.database == target_database %}

        {# Step 5: Check if this node is a source, sink, or source_table #}
        {% if node.config.materialized == 'source' %}
          {% set has_source = true %}
        {% elif node.config.materialized == 'sink' %}
          {% set has_sink = true %}
        {% elif node.config.materialized == 'source_table' %}
          {% set has_source_table = true %}
        {% endif %}

      {% endif %}
    {% endfor %}

    {# Step 6: If schema contains sources, block creation of this object #}
    {% if has_source %}
      {{ exceptions.raise_compiler_error(
        "Cannot create object in schema '" ~ target_schema ~ "'. " ~
        "Schema contains source objects. " ~
        "source materializations must be in a dedicated schema."
      ) }}
    {% endif %}

    {# Step 7: If schema contains sinks, block creation of this object #}
    {% if has_sink %}
      {{ exceptions.raise_compiler_error(
        "Cannot create object in schema '" ~ target_schema ~ "'. " ~
        "Schema contains sink objects. " ~
        "sink materializations must be in a dedicated schema."
      ) }}
    {% endif %}

    {# Step 8: If schema contains source_tables, block creation of this object #}
    {% if has_source_table %}
      {{ exceptions.raise_compiler_error(
        "Cannot create object in schema '" ~ target_schema ~ "'. " ~
        "Schema contains source_table objects. " ~
        "source_table materializations must be in a dedicated schema."
      ) }}
    {% endif %}

  {% endif %}
{% endmacro %}
