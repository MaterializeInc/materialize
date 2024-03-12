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

{% macro set_cluster_ci_tag(cluster, ci_tag=env_var('CI_TAG', '')) %}
    {% if ci_tag != '' %}
        {% set ci_comment %}
            COMMENT ON CLUSTER {{ adapter.quote(cluster) }} IS {{ dbt.string_literal(ci_tag) }};
        {% endset %}
        {{ run_query(ci_comment) }}
    {% endif %}
{% endmacro %}

{% macro set_schema_ci_tag(schema, ci_tag=env_var('CI_TAG', '')) %}
    {% if ci_tag != '' %}
        {% set ci_comment %}
            COMMENT ON SCHEMA {{ adapter.quote(schema) }} IS {{ dbt.string_literal(ci_tag) }};
        {% endset %}
        {{ run_query(ci_comment) }}
    {% endif %}
{% endmacro %}

{% macro check_cluster_ci_tag(cluster, ci_tag=env_var('CI_TAG', '')) %}
    {% if ci_tag == '' %}
        {{ return(false) }}
    {% endif %}

    {% set query %}
        SELECT comment = {{ dbt.string_literal(ci_tag) }}, comment
        FROM mz_internal.mz_comments
        JOIN mz_clusters USING (id)
        WHERE mz_clusters.name = {{ dbt.string_literal(cluster) }}
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute %}
        {% if results|length > 0 %}
            {% if results.rows[0][0] is false %}
                {{ exceptions.raise_compiler_error("""
                    Deployment cluster """ ~ cluster ~ """ already exists and is tagged
                    for CI deployment """ ~ results.rows[0][1]
                )}}
            {% else %}
                {{ return(true) }}
            {% endif %}
        {% endif %}

        {{ return(false) }}
    {% endif %}
{% endmacro %}

{% macro check_schema_ci_tag(schema, ci_tag=env_var('CI_TAG', '')) %}
    {% if ci_tag == '' %}
        {{ return(false) }}
    {% endif %}

    {% set query %}
        SELECT comment = {{ dbt.string_literal(ci_tag) }}, comment
        FROM mz_internal.mz_comments c
        JOIN mz_schemas s ON c.id = s.id
        JOIN mz_databases d ON s.database_id = d.id
        WHERE s.name = {{ dbt.string_literal(schema) }}
            AND d.name = current_database()
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute %}
        {% if results|length > 0 %}
            {% if results.rows[0][0] is false %}
                {{ exceptions.raise_compiler_error("""
                    Deployment schema """ ~ schema ~ """ already exists and is tagged
                    for CI deployment """ ~ results.rows[0][1]
                )}}
            {% else %}
                {{ return(true) }}
            {% endif %}
        {% endif %}

        {{ return(false) }}
    {% endif %}
{% endmacro %}
