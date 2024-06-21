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

{% macro deploy_init(ignore_existing_objects=False) %}

{% set current_target_name = target.name %}
{% set deployment = var('deployment') %}
{% set target_config = deployment[current_target_name] %}

-- Check if the target-specific configuration exists
{% if not target_config %}
    {{ exceptions.raise_compiler_error("No deployment configuration found for target " ~ current_target_name) }}
{% endif %}

{{ log("Creating deployment environment for target " ~ current_target_name, info=True) }}

{% set clusters = target_config.get('clusters', []) %}
{% set schemas = target_config.get('schemas', []) %}

-- Check that all production schemas
-- and clusters already exist
{% for schema in schemas %}
    {% if not schema_exists(schema) %}
        {{ exceptions.raise_compiler_error("Production schema " ~ schema ~ " does not exist") }}
    {% endif %}
    {% if schema_contains_sinks(schema) %}
        {{ exceptions.raise_compiler_error("""
        Production schema " ~ schema ~ " contains sinks.
        Blue/green deployments require sinks to be in a dedicated schema.
        """) }}
    {% endif %}
{% endfor %}

{% for cluster in clusters %}
    {% if not cluster_exists(cluster) %}
        {{ exceptions.raise_compiler_error("Production cluster " ~ cluster ~ " does not exist") }}
    {% endif %}
    {% if cluster_contains_sinks(cluster) %}
        {{ exceptions.raise_compiler_error("""
        Production cluster " ~ cluster ~ " contains sinks.
        Blue/green deployments require sinks to be in a dedicated cluster.
        """) }}
    {% endif %}
{% endfor %}

{{ deploy_validate_permissions(clusters, schemas) }}

{% for schema in schemas %}
    {% set deploy_schema = schema ~ "_dbt_deploy" %}
    {% if schema_exists(deploy_schema) %}
        {{ log("Deployment schema " ~ deploy_schema ~ " already exists", info=True)}}
        {% set schema_empty %}
            SELECT count(*)
            FROM mz_objects
            JOIN mz_schemas ON mz_objects.schema_id = mz_schemas.id
            JOIN mz_databases ON mz_databases.id = mz_schemas.database_id
            WHERE mz_schemas.name = {{ dbt.string_literal(deploy_schema) }}
                AND mz_objects.id LIKE 'u%'
                AND mz_databases.name = current_database()
        {% endset %}

        {% set schema_object_count = run_query(schema_empty) %}
        {% if execute %}
            {% if schema_object_count and schema_object_count.columns[0] and schema_object_count.rows[0][0] > 0 %}
                {% if check_schema_ci_tag(deploy_schema) %}
                    {{ log("Schema " ~ deploy_schema ~ " was already created for this pull request", info=True) }}
                {% elif ignore_existing_objects %}
                    {{ log("[Warning] Deployment schema " ~ deploy_schema ~ " is not empty", info=True) }}
                    {{ log("[Warning] Confirm the objects it contains are expected before deployment", info=True) }}
                {% else %}
                    {{ exceptions.raise_compiler_error("""
                        Deployment schema """ ~ deploy_schema ~ """ already exists and is not empty.
                        This is potentially dangerous as you may end up deploying objects to production you
                        do not intend.

                        If you are certain the objects in this schema are supposed to exist, you can ignore this
                        error by setting ignore_existing_objects to True.

                        dbt run-operation create_deployment_environment --args '{ignore_existing_objects: True}'
                    """) }}
                {% endif %}
            {% endif %}
        {% endif %}

    {% else %}
        {{ log("Creating deployment schema " ~ deploy_schema, info=True)}}
        {% set create_schema %}
        CREATE SCHEMA {{ deploy_schema }};
        {% endset %}
        {{ run_query(create_schema) }}
        {{ set_schema_ci_tag() }}
        {{ internal_copy_schema_default_privs(schema, deploy_schema) }}
        {{ internal_copy_schema_grants(schema, deploy_schema) }}
    {% endif %}
{% endfor %}

{% for cluster in clusters %}
    {% set deploy_cluster = adapter.generate_final_cluster_name(cluster, force_deploy_suffix=True) %}
    {% if cluster_exists(deploy_cluster) %}
        {{ log("Deployment cluster " ~ deploy_cluster ~ " already exists", info=True) }}
        {% set cluster_empty %}
            WITH dataflows AS (
                SELECT mz_indexes.id
                FROM mz_indexes
                JOIN mz_clusters ON mz_indexes.cluster_id = mz_clusters.id
                WHERE mz_clusters.name = {{ dbt.string_literal(deploy_cluster) }}

                UNION ALL

                SELECT mz_materialized_views.id
                FROM mz_materialized_views
                JOIN mz_clusters ON mz_materialized_views.cluster_id = mz_clusters.id
                WHERE mz_clusters.name = {{ dbt.string_literal(deploy_cluster) }}

                UNION ALL

                SELECT mz_sources.id
                FROM mz_sources
                JOIN mz_clusters ON mz_clusters.id = mz_sources.cluster_id
                WHERE mz_clusters.name = {{ dbt.string_literal(deploy_cluster) }}

                UNION ALL

                SELECT mz_sinks.id
                FROM mz_sinks
                JOIN mz_clusters ON mz_clusters.id = mz_sinks.cluster_id
                WHERE mz_clusters.name = {{ dbt.string_literal(deploy_cluster) }}
            )

            SELECT count(*)
            FROM dataflows
            WHERE id LIKE 'u%'
        {% endset %}


        {% set cluster_object_count = run_query(cluster_empty) %}
        {% if execute %}
            {% if cluster_object_count and cluster_object_count.columns[0] and cluster_object_count.rows[0][0] > 0 %}
                {% if check_cluster_ci_tag(deploy_cluster) %}
                    {{ log("Cluster " ~ deploy_cluster ~ " was already created for this pull request", info=True) }}
                {% elif ignore_existing_objects %}
                    {{ log("[Warning] Deployment cluster " ~ deploy_cluster ~ " is not empty", info=True) }}
                    {{ log("[Warning] Confirm the objects it contains are expected before deployment", info=True) }}
                {% else %}
                    {{ exceptions.raise_compiler_error("""
                        Deployment cluster """ ~ deploy_cluster ~ """ already exists and is not empty.
                        This is potentially dangerous as you may end up deploying objects to production you
                        do not intend.

                        If you are certain the objects in this cluster are supposed to exist, you can ignore this
                        error by setting ignore_existing_objects to True.

                        dbt run-operation create_deployment_environment --args '{ignore_existing_objects: True}'
                    """) }}
                {% endif %}
            {% endif %}
        {% endif %}

    {% else %}
        {{ log("Creating deployment cluster " ~ deploy_cluster ~ " like cluster " ~ cluster, info=True)}}
        {% set cluster_configuration %}
            SELECT
                c.managed,
                c.size,
                c.replication_factor,
                c.id AS cluster_id,
                c.name AS cluster_name,
                cs.type AS schedule_type,
                cs.refresh_rehydration_time_estimate
            FROM mz_clusters c
            LEFT JOIN mz_internal.mz_cluster_schedules cs ON cs.cluster_id = c.id
            WHERE c.name = {{ dbt.string_literal(cluster) }}
        {% endset %}

        {% set cluster_config_results = run_query(cluster_configuration) %}

        {% if execute %}
            {% set results = cluster_config_results.rows[0] %}

            {% set managed = results[0] %}
            {% set size = results[1] %}
            {% set replication_factor = results[2] %}
            {% set schedule_type = results[5] %}
            {% set refresh_rehydration_time_estimate = results[6] %}

            {% if not managed %}
                {{ exceptions.raise_compiler_error("Production cluster " ~ cluster ~ " is not managed") }}
            {% endif %}

            {% set create_cluster %}
                CREATE CLUSTER {{ deploy_cluster }} (
                    SIZE = {{ dbt.string_literal(size) }}
                    {% if schedule_type == 'manual' %}
                        , REPLICATION FACTOR = {{ replication_factor }}
                    {% elif schedule_type == 'on-refresh' and refresh_rehydration_time_estimate %}
                        , SCHEDULE = ON REFRESH (REHYDRATION TIME ESTIMATE = {{ dbt.string_literal(refresh_rehydration_time_estimate) }})
                    {% endif %}
                );
            {% endset %}
            {{ run_query(create_cluster) }}
            {{ set_cluster_ci_tag() }}
            {{ internal_copy_cluster_grants(schema, deploy_schema) }}
        {% endif %}

    {% endif %}
{% endfor %}
{% endmacro %}

{% macro internal_copy_schema_default_privs(from, to) %}

{% set find_default_privs %}
SELECT
  'ALTER DEFAULT PRIVILEGES ' ||
  CASE
    WHEN object_owner = 'PUBLIC' THEN 'FOR ALL ROLES '
    ELSE 'FOR ROLE ' || quote_ident(object_owner) || ' '
  END ||
  'IN SCHEMA {{ to }} '         ||
  'GRANT '  || privilege_type   || ' ' ||
  'ON '     || object_type      || 's ' ||
  'TO '     || quote_ident(grantee)
FROM mz_internal.mz_show_default_privileges
WHERE database = current_database() AND schema = {{ dbt.string_literal(from) }}
    AND object_owner <> 'none' AND grantee <> 'none'
{% endset %}

{% set alter_defaults = run_query(find_default_privs) %}
{% if execute %}
    {% for alter in alter_defaults.rows %}
        {{ run_query(alter[0]) }}
    {% endfor %}
{% endif %}
{% endmacro %}

{% macro internal_copy_schema_grants(from, to) %}
{% set find_revokes %}
WITH schema_privilege AS (
    SELECT mz_internal.mz_aclexplode(s.privileges).*
    FROM mz_schemas s
    JOIN mz_databases d ON s.database_id = d.id
    WHERE d.name = current_database()
        AND s.name = {{ dbt.string_literal(to) }}
)

SELECT 'REVOKE '    || s.privilege_type || ' ' ||
       'ON SCHEMA ' || quote_ident({{ dbt.string_literal(to) }}) || ' ' ||
       'FROM '      || quote_ident(grantee.name)
FROM schema_privilege AS s
JOIN mz_roles AS grantee ON s.grantee = grantee.id
WHERE grantee.name NOT IN ('none', 'mz_system', 'mz_support', current_role)
{% endset %}

{% set revokes = run_query(find_revokes) %}
{% if execute %}
    {% for revoke in revokes.rows %}
        {{ run_query(revoke[0]) }}
    {% endfor %}
{% endif %}

{% set find_grants %}
WITH schema_privilege AS (
    SELECT mz_internal.mz_aclexplode(s.privileges).*
    FROM mz_schemas s
    JOIN mz_databases d ON s.database_id = d.id
    WHERE d.name = current_database()
        AND s.name = {{ dbt.string_literal(from) }}
)

SELECT 'GRANT '     || s.privilege_type || ' ' ||
       'ON SCHEMA ' || quote_ident({{ dbt.string_literal(to) }}) || ' ' ||
       'TO '        || quote_ident(grantee.name)
FROM schema_privilege AS s
JOIN mz_roles AS grantee ON s.grantee = grantee.id
WHERE grantee.name NOT IN ('none', 'mz_system', 'mz_support', current_role)
{% endset %}

{% set grants = run_query(find_grants) %}
{% if execute %}
    {% for grant in grants.rows %}
        {{ run_query(grant[0]) }}
    {% endfor %}
{% endif %}

{% endmacro %}

{% macro internal_copy_cluster_grants(from, to) %}
{% set find_revokes %}
WITH cluster_privilege AS (
    SELECT mz_internal.mz_aclexplode(privileges).*
    FROM mz_clusters
    WHERE name = {{ dbt.string_literal(to) }}
)

SELECT 'REVOKE '     || c.privilege_type || ' ' ||
       'ON CLUSTER ' || quote_ident({{ dbt.string_literal(to) }}) || ' ' ||
       'FROM '       || quote_ident(grantee.name)
FROM cluster_privilege AS c
JOIN mz_roles AS grantee ON c.grantee = grantee.id
WHERE grantee.name NOT IN ('none', 'mz_system', 'mz_support')
    AND grantee.name <> current_role
{% endset %}

{% set revokes = run_query(find_revokes) %}
{% if execute %}
    {% for revoke in revokes.rows %}
        {{ run_query(revoke[0]) }}
    {% endfor %}
{% endif %}

{% set find_grants %}
WITH cluster_privilege AS (
    SELECT mz_internal.mz_aclexplode(privileges).*
    FROM mz_clusters
    WHERE name = {{ dbt.string_literal(from) }}
)

SELECT 'GRANT '      || c.privilege_type || ' ' ||
       'ON CLUSTER ' || quote_ident({{ dbt.string_literal(to) }}) || ' ' ||
       'TO '         || quote_ident(grantee.name)
FROM cluster_privilege AS c
JOIN mz_roles AS grantee ON c.grantee = grantee.id
WHERE grantee.name NOT IN ('none', 'mz_system', 'mz_support')
{% endset %}

{% set grants = run_query(find_grants) %}
{% if execute %}
    {% for grant in grants.rows %}
        {{ run_query(grant[0]) }}
    {% endfor %}
{% endif %}

{% endmacro %}
