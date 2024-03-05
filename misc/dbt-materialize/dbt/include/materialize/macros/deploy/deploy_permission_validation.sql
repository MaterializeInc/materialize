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

{% macro deploy_permission_validation(clusters=[]) %}

{% set is_not_super_user %}
    SELECT mz_is_superuser() IS FALSE;
{% endset %}

{% set not_super_user = run_query(is_not_super_user) %}
{% if execute %}
    {% if not_super_user.rows[0][0] %}
        {{ internal_ensure_database_create_permission() }}
        {{ internal_ensure_createcluster_permission() }}
        {{ internal_ensure_cluster_ownership(clusters) }}
    {% endif %}
{% endif %}

{% endmacro %}

{% macro internal_ensure_database_create_permission() %}
{% set database_can_create %}
WITH permission AS (
    SELECT count(*) = 0 AS missing_permission
    FROM mz_internal.mz_show_my_database_privileges
    WHERE privilege_type = 'CREATE' AND name = current_database()
)

SELECT missing_permission, current_user(), current_database()
FROM permission;
{% endset %}

{% set database_check = run_query(database_can_create) %}
{% if execute %}
    {% if database_check.rows[0][0] %}
        {{ exceptions.raise_compiler_error("""
            Missing necessary permissions to execute a deployment.

            The current user """ ~ database_check.rows[0][1] ~ """ needs
            CREATE privlidges on database """ ~ database_check.rows[0][2]
        ) }}
    {% endif %}
{% endif %}
{% endmacro %}

{% macro internal_ensure_createcluster_permission() %}
{% set has_create_cluster = run_query("SELECT has_system_privilege('CREATECLUSTER') IS FALSE, current_user()") %}
{% if execute %}
    {% if has_create_cluster.rows[0][0] %}
        {{ exceptions.raise_compiler_error("""
            Missing necessary permissions to execute a deployment.

            The current user """ ~ has_create_cluster.rows[0][1] ~ """ needs CREATECLUSTER privlidges
        """) }}
    {% endif %}
{% endif %}
{% endmacro %}

{% macro internal_ensure_cluster_ownership(clusters=[]) %}
{% set has_cluster_ownership %}
WITH clusters_under_deployment AS (
    SELECT * FROM mz_internal.mz_show_my_cluster_privileges
    WHERE name IN (
    {% if clusters|length > 0 %}
        {% for cluster in clusters %}
            lower(trim('{{ cluster }}')){% if not loop.last %},{% endif %}
        {% endfor %}
    {% else %}
        NULL
    {% endif %}
    )
),

cluster_with_ownership AS (
    SELECT * FROM clusters_under_deployment
    WHERE privilege_type = 'OWNER'
)

SELECT name, current_user()
FROM cluster_with_ownership

EXCEPT

SELECT name, current_user()
FROM clusters_under_deployment
{% endset %}

{% set cluster_ownership = run_query(has_cluster_ownership) %}
{% if execute %}
    {% if cluster_ownership|length > 0 %}
        {{ exceptions.raise_compiler_error("""
            Missing necessary permissions to execute a deployment.

            The current user """ ~ cluster_ownership.rows[0][1] ~ """ needs OWNER privlidges
            on cluster """ ~ cluster_ownership.rows[0][0]
        )}}
    {% endif %}
{% endif %}
{% endmacro %}
