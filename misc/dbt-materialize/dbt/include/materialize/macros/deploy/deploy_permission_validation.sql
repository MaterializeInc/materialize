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

-- The deployment user must be a superuser OR
--
-- Have CREATE and USAGE rights on the database
-- Have CREATECLUSTER rights
-- Have OWNERSHIP rights on the production clusters and schemas
{% macro deploy_validate_permissions(clusters=[], schemas=[]) %}

{% set is_super_user %}
    SELECT mz_is_superuser();
{% endset %}

{% set super_user = run_query(is_super_user) %}
{% if execute %}
    {% if not super_user.rows[0][0] %}
        {{ internal_ensure_database_permission() }}
        {{ internal_ensure_createcluster_permission() }}
        {{ internal_ensure_schema_ownership(schemas) }}
        {{ internal_ensure_cluster_ownership(clusters) }}
    {% endif %}
{% endif %}

{% endmacro %}

{% macro internal_ensure_database_permission() %}
{% set database_permissions %}
WITH create_permission AS (
    SELECT count(*) = 0 AS missing_create
    FROM mz_internal.mz_show_my_database_privileges
    WHERE privilege_type = 'CREATE' AND name = current_database()
),

usage_permission AS (
    SELECT count(*) = 0 AS missing_usage
    FROM mz_internal.mz_show_my_database_privileges
    WHERE privilege_type = 'USAGE' AND name = current_database()
)

SELECT missing_create, missing_usage, quote_ident(current_role), current_database()
FROM create_permission, usage_permission;
{% endset %}

{% set database_check = run_query(database_permissions) %}
{% if execute %}
    {% if database_check.rows[0][0] or database_check[0][1] %}
        {{ exceptions.raise_compiler_error("""
            Missing necessary permissions to execute a deployment. The current role """ ~
            database_check.rows[0][2] ~ """ needs CREATE and USAGE privileges on database """ ~
            database_check.rows[0][3] ~ """.

            Hint: `GRANT USAGE, CREATE ON DATABASE """ ~ database_check.rows[0][2] ~ """ TO """ ~ database_check.rows[0][3] ~ """;`
        """) }}
    {% endif %}
{% endif %}
{% endmacro %}

{% macro internal_ensure_createcluster_permission() %}
{% set has_create_cluster = run_query("SELECT has_system_privilege('CREATECLUSTER') IS FALSE, current_user()") %}
{% if execute %}
    {% if has_create_cluster.rows[0][0] %}
        {{ exceptions.raise_compiler_error("""
            Missing necessary permissions to execute a deployment. The current role
            """ ~ has_create_cluster.rows[0][1] ~ """ needs CREATECLUSTER privlidges.

            Hint: `GRANT CREATECLUSTER ON SYSTEM TO """ ~ has_create_cluster.rows[0][1] ~ """;`
        """) }}
    {% endif %}
{% endif %}
{% endmacro %}

{% macro internal_ensure_cluster_ownership(clusters=[]) %}
{% set has_cluster_ownership %}
WITH clusters_under_deployment AS (
    SELECT name, owner_id FROM mz_clusters
    WHERE name IN (
    {% if clusters|length > 0 %}
        {% for cluster in clusters %}
            {{ dbt.string_literal(cluster) }}{% if not loop.last %},{% endif %}
        {% endfor %}
    {% else %}
        NULL
    {% endif %}
    )
),

cluster_with_ownership AS (
    SELECT c.name FROM clusters_under_deployment c
    INNER JOIN mz_roles ON c.owner_id = mz_roles.id
    WHERE mz_roles.name = current_role
),

missing_ownership AS (
    SELECT name
    FROM cluster_with_ownership

    EXCEPT

    SELECT name
    FROM clusters_under_deployment
)

SELECT name, quote_ident(current_role)
FROM missing_ownership
{% endset %}

{% set cluster_ownership = run_query(has_cluster_ownership) %}
{% if execute %}
    {% if cluster_ownership|length > 0 %}
        {{ exceptions.raise_compiler_error("""
            Missing necessary permissions to execute a deployment. The current role """
            ~ cluster_ownership.rows[0][1] ~ """ needs to be an owner of cluster """ ~ cluster_ownership.rows[0][0] ~ """.

            Hint: `ALTER CLUSTER """ ~ cluster_ownership.rows[0][0] ~ """ OWNER TO """ ~ cluster_ownership.rows[0][1] ~ """;`
        """)}}
    {% endif %}
{% endif %}
{% endmacro %}

{% macro internal_ensure_schema_ownership(schemas=[]) %}
{% set has_schema_ownership %}
WITH schemas_under_deployment AS (
    SELECT mz_schemas.*
    FROM mz_schemas
    JOIN mz_databases ON mz_schemas.database_id = mz_databases.id
    WHERE mz_databases.name = current_database() AND mz_schemas.name IN (
    {% if schemas|length > 0 %}
        {% for schema in schemas %}
            {{ dbt.string_literal(schema) }}{% if not loop.last %},{% endif %}
        {% endfor %}
    {% else %}
        NULL
    {% endif %}
    )
),

schemas_with_ownership AS (
    SELECT s.name FROM schemas_under_deployment s
    INNER JOIN mz_roles ON s.owner_id = mz_roles.id
    WHERE mz_roles.name = current_role
),

missing_ownership AS (
    SELECT name
    FROM schemas_with_ownership

    EXCEPT

    SELECT name
    FROM schemas_under_deployment
)

SELECT name, quote_ident(current_role)
FROM missing_ownership
{% endset %}

{% set schema_ownership = run_query(has_schema_ownership) %}
{% if execute %}
    {% if schema_ownership|length > 0 %}
        {{ exceptions.raise_compiler_error("""
            Missing necessary permissions to execute a deployment. The current role """
            ~ schema_ownership.rows[0][1] ~ """ needs to be an owner of cluster """ ~
            schema_ownership.rows[0][0] ~ """.

            Hint: `ALTER SCHEMA """ ~ schema_ownership.rows[0][0] ~ """ OWNER TO """ ~ schema_ownership.rows[0][1] ~ """;`
        """)}}
    {% endif %}
{% endif %}
{% endmacro %}
