// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Built-in catalog items for the `information_schema` schema.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use mz_pgrepr::oid;
use mz_repr::namespaces::INFORMATION_SCHEMA;
use mz_repr::{RelationDesc, SqlScalarType};

use super::{BuiltinView, PUBLIC_SELECT};

pub static INFORMATION_SCHEMA_APPLICABLE_ROLES: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "applicable_roles",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_APPLICABLE_ROLES_OID,
        desc: RelationDesc::builder()
            .with_column("grantee", SqlScalarType::String.nullable(false))
            .with_column("role_name", SqlScalarType::String.nullable(false))
            .with_column("is_grantable", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    member.name AS grantee,
    role.name AS role_name,
    -- ADMIN OPTION isn't implemented.
    'NO' AS is_grantable
FROM mz_catalog.mz_role_members membership
JOIN mz_catalog.mz_roles role ON membership.role_id = role.id
JOIN mz_catalog.mz_roles member ON membership.member = member.id
WHERE mz_catalog.mz_is_superuser() OR pg_has_role(current_role, member.oid, 'USAGE')",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static INFORMATION_SCHEMA_COLUMNS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "columns",
    schema: INFORMATION_SCHEMA,
    oid: oid::VIEW_COLUMNS_OID,
    desc: RelationDesc::builder()
        .with_column("table_catalog", SqlScalarType::String.nullable(false))
        .with_column("table_schema", SqlScalarType::String.nullable(false))
        .with_column("table_name", SqlScalarType::String.nullable(false))
        .with_column("column_name", SqlScalarType::String.nullable(false))
        .with_column("ordinal_position", SqlScalarType::Int64.nullable(false))
        .with_column("column_default", SqlScalarType::String.nullable(true))
        .with_column("is_nullable", SqlScalarType::String.nullable(false))
        .with_column("data_type", SqlScalarType::String.nullable(false))
        .with_column(
            "character_maximum_length",
            SqlScalarType::Int32.nullable(true),
        )
        .with_column("numeric_precision", SqlScalarType::Int32.nullable(true))
        .with_column("numeric_scale", SqlScalarType::Int32.nullable(true))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    current_database() as table_catalog,
    s.name AS table_schema,
    o.name AS table_name,
    c.name AS column_name,
    c.position::int8 AS ordinal_position,
    c.default AS column_default,
    CASE WHEN c.nullable THEN 'YES' ELSE 'NO' END AS is_nullable,
    c.type AS data_type,
    NULL::pg_catalog.int4 AS character_maximum_length,
    NULL::pg_catalog.int4 AS numeric_precision,
    NULL::pg_catalog.int4 AS numeric_scale
FROM mz_catalog.mz_columns c
JOIN mz_catalog.mz_objects o ON o.id = c.id
JOIN mz_catalog.mz_schemas s ON s.id = o.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static INFORMATION_SCHEMA_ENABLED_ROLES: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "enabled_roles",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_ENABLED_ROLES_OID,
        desc: RelationDesc::builder()
            .with_column("role_name", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT name AS role_name
FROM mz_catalog.mz_roles
WHERE mz_catalog.mz_is_superuser() OR pg_has_role(current_role, oid, 'USAGE')",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static INFORMATION_SCHEMA_ROLE_TABLE_GRANTS: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "role_table_grants",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_ROLE_TABLE_GRANTS_OID,
        desc: RelationDesc::builder()
            .with_column("grantor", SqlScalarType::String.nullable(false))
            .with_column("grantee", SqlScalarType::String.nullable(true))
            .with_column("table_catalog", SqlScalarType::String.nullable(true))
            .with_column("table_schema", SqlScalarType::String.nullable(false))
            .with_column("table_name", SqlScalarType::String.nullable(false))
            .with_column("privilege_type", SqlScalarType::String.nullable(true))
            .with_column("is_grantable", SqlScalarType::String.nullable(false))
            .with_column("with_hierarchy", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT grantor, grantee, table_catalog, table_schema, table_name, privilege_type, is_grantable, with_hierarchy
FROM information_schema.table_privileges
WHERE
    grantor IN (SELECT role_name FROM information_schema.enabled_roles)
    OR grantee IN (SELECT role_name FROM information_schema.enabled_roles)",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub static INFORMATION_SCHEMA_KEY_COLUMN_USAGE: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "key_column_usage",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_KEY_COLUMN_USAGE_OID,
        desc: RelationDesc::builder()
            .with_column("constraint_catalog", SqlScalarType::String.nullable(false))
            .with_column("constraint_schema", SqlScalarType::String.nullable(false))
            .with_column("constraint_name", SqlScalarType::String.nullable(false))
            .with_column("table_catalog", SqlScalarType::String.nullable(false))
            .with_column("table_schema", SqlScalarType::String.nullable(false))
            .with_column("table_name", SqlScalarType::String.nullable(false))
            .with_column("column_name", SqlScalarType::String.nullable(false))
            .with_column("ordinal_position", SqlScalarType::Int32.nullable(false))
            .with_column(
                "position_in_unique_constraint",
                SqlScalarType::Int32.nullable(false),
            )
            .with_key(vec![])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    NULL::text AS constraint_catalog,
    NULL::text AS constraint_schema,
    NULL::text AS constraint_name,
    NULL::text AS table_catalog,
    NULL::text AS table_schema,
    NULL::text AS table_name,
    NULL::text AS column_name,
    NULL::integer AS ordinal_position,
    NULL::integer AS position_in_unique_constraint
WHERE false",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static INFORMATION_SCHEMA_REFERENTIAL_CONSTRAINTS: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "referential_constraints",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_REFERENTIAL_CONSTRAINTS_OID,
        desc: RelationDesc::builder()
            .with_column("constraint_catalog", SqlScalarType::String.nullable(false))
            .with_column("constraint_schema", SqlScalarType::String.nullable(false))
            .with_column("constraint_name", SqlScalarType::String.nullable(false))
            .with_column(
                "unique_constraint_catalog",
                SqlScalarType::String.nullable(false),
            )
            .with_column(
                "unique_constraint_schema",
                SqlScalarType::String.nullable(false),
            )
            .with_column(
                "unique_constraint_name",
                SqlScalarType::String.nullable(false),
            )
            .with_column("match_option", SqlScalarType::String.nullable(false))
            .with_column("update_rule", SqlScalarType::String.nullable(false))
            .with_column("delete_rule", SqlScalarType::String.nullable(false))
            .with_key(vec![])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    NULL::text AS constraint_catalog,
    NULL::text AS constraint_schema,
    NULL::text AS constraint_name,
    NULL::text AS unique_constraint_catalog,
    NULL::text AS unique_constraint_schema,
    NULL::text AS unique_constraint_name,
    NULL::text AS match_option,
    NULL::text AS update_rule,
    NULL::text AS delete_rule
WHERE false",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static INFORMATION_SCHEMA_ROUTINES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "routines",
    schema: INFORMATION_SCHEMA,
    oid: oid::VIEW_ROUTINES_OID,
    desc: RelationDesc::builder()
        .with_column("routine_catalog", SqlScalarType::String.nullable(false))
        .with_column("routine_schema", SqlScalarType::String.nullable(false))
        .with_column("routine_name", SqlScalarType::String.nullable(false))
        .with_column("routine_type", SqlScalarType::String.nullable(false))
        .with_column("routine_definition", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    current_database() as routine_catalog,
    s.name AS routine_schema,
    f.name AS routine_name,
    'FUNCTION' AS routine_type,
    NULL::text AS routine_definition
FROM mz_catalog.mz_functions f
JOIN mz_catalog.mz_schemas s ON s.id = f.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static INFORMATION_SCHEMA_SCHEMATA: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "schemata",
    schema: INFORMATION_SCHEMA,
    oid: oid::VIEW_SCHEMATA_OID,
    desc: RelationDesc::builder()
        .with_column("catalog_name", SqlScalarType::String.nullable(false))
        .with_column("schema_name", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    current_database() as catalog_name,
    s.name AS schema_name
FROM mz_catalog.mz_schemas s
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static INFORMATION_SCHEMA_TABLES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "tables",
    schema: INFORMATION_SCHEMA,
    oid: oid::VIEW_TABLES_OID,
    desc: RelationDesc::builder()
        .with_column("table_catalog", SqlScalarType::String.nullable(false))
        .with_column("table_schema", SqlScalarType::String.nullable(false))
        .with_column("table_name", SqlScalarType::String.nullable(false))
        .with_column("table_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    current_database() as table_catalog,
    s.name AS table_schema,
    r.name AS table_name,
    CASE r.type
        WHEN 'materialized-view' THEN 'MATERIALIZED VIEW'
        WHEN 'table' THEN 'BASE TABLE'
        ELSE pg_catalog.upper(r.type)
    END AS table_type
FROM mz_catalog.mz_relations r
JOIN mz_catalog.mz_schemas s ON s.id = r.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static INFORMATION_SCHEMA_TABLE_CONSTRAINTS: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "table_constraints",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_TABLE_CONSTRAINTS_OID,
        desc: RelationDesc::builder()
            .with_column("constraint_catalog", SqlScalarType::String.nullable(false))
            .with_column("constraint_schema", SqlScalarType::String.nullable(false))
            .with_column("constraint_name", SqlScalarType::String.nullable(false))
            .with_column("table_catalog", SqlScalarType::String.nullable(false))
            .with_column("table_schema", SqlScalarType::String.nullable(false))
            .with_column("table_name", SqlScalarType::String.nullable(false))
            .with_column("constraint_type", SqlScalarType::String.nullable(false))
            .with_column("is_deferrable", SqlScalarType::String.nullable(false))
            .with_column("initially_deferred", SqlScalarType::String.nullable(false))
            .with_column("enforced", SqlScalarType::String.nullable(false))
            .with_column("nulls_distinct", SqlScalarType::String.nullable(false))
            .with_key(vec![])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    NULL::text AS constraint_catalog,
    NULL::text AS constraint_schema,
    NULL::text AS constraint_name,
    NULL::text AS table_catalog,
    NULL::text AS table_schema,
    NULL::text AS table_name,
    NULL::text AS constraint_type,
    NULL::text AS is_deferrable,
    NULL::text AS initially_deferred,
    NULL::text AS enforced,
    NULL::text AS nulls_distinct
WHERE false",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static INFORMATION_SCHEMA_TABLE_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "table_privileges",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_TABLE_PRIVILEGES_OID,
        desc: RelationDesc::builder()
            .with_column("grantor", SqlScalarType::String.nullable(false))
            .with_column("grantee", SqlScalarType::String.nullable(true))
            .with_column("table_catalog", SqlScalarType::String.nullable(true))
            .with_column("table_schema", SqlScalarType::String.nullable(false))
            .with_column("table_name", SqlScalarType::String.nullable(false))
            .with_column("privilege_type", SqlScalarType::String.nullable(true))
            .with_column("is_grantable", SqlScalarType::String.nullable(false))
            .with_column("with_hierarchy", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    grantor,
    grantee,
    table_catalog,
    table_schema,
    table_name,
    privilege_type,
    is_grantable,
    CASE privilege_type
        WHEN 'SELECT' THEN 'YES'
        ELSE 'NO'
    END AS with_hierarchy
FROM
    (SELECT
        grantor.name AS grantor,
        CASE mz_internal.mz_aclitem_grantee(privileges)
            WHEN 'p' THEN 'PUBLIC'
            ELSE grantee.name
        END AS grantee,
        table_catalog,
        table_schema,
        table_name,
        unnest(mz_internal.mz_format_privileges(mz_internal.mz_aclitem_privileges(privileges))) AS privilege_type,
        -- ADMIN OPTION isn't implemented.
        'NO' AS is_grantable
    FROM
        (SELECT
            unnest(relations.privileges) AS privileges,
            CASE
                WHEN schemas.database_id IS NULL THEN current_database()
                ELSE databases.name
            END AS table_catalog,
            schemas.name AS table_schema,
            relations.name AS table_name
        FROM mz_catalog.mz_relations AS relations
        JOIN mz_catalog.mz_schemas AS schemas ON relations.schema_id = schemas.id
        LEFT JOIN mz_catalog.mz_databases AS databases ON schemas.database_id = databases.id
        WHERE schemas.database_id IS NULL OR databases.name = current_database())
    JOIN mz_catalog.mz_roles AS grantor ON mz_internal.mz_aclitem_grantor(privileges) = grantor.id
    LEFT JOIN mz_catalog.mz_roles AS grantee ON mz_internal.mz_aclitem_grantee(privileges) = grantee.id)
WHERE
    -- WHERE clause is not guaranteed to short-circuit and 'PUBLIC' will cause an error when passed
    -- to pg_has_role. Therefore we need to use a CASE statement.
    CASE
        WHEN grantee = 'PUBLIC' THEN true
        ELSE mz_catalog.mz_is_superuser()
            OR pg_has_role(current_role, grantee, 'USAGE')
            OR pg_has_role(current_role, grantor, 'USAGE')
    END",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub static INFORMATION_SCHEMA_TRIGGERS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "triggers",
    schema: INFORMATION_SCHEMA,
    oid: oid::VIEW_TRIGGERS_OID,
    desc: RelationDesc::builder()
        .with_column("trigger_catalog", SqlScalarType::String.nullable(false))
        .with_column("trigger_schema", SqlScalarType::String.nullable(false))
        .with_column("trigger_name", SqlScalarType::String.nullable(false))
        .with_column("event_manipulation", SqlScalarType::String.nullable(false))
        .with_column(
            "event_object_catalog",
            SqlScalarType::String.nullable(false),
        )
        .with_column("event_object_schema", SqlScalarType::String.nullable(false))
        .with_column("event_object_table", SqlScalarType::String.nullable(false))
        .with_column("action_order", SqlScalarType::Int32.nullable(false))
        .with_column("action_condition", SqlScalarType::String.nullable(false))
        .with_column("action_statement", SqlScalarType::String.nullable(false))
        .with_column("action_orientation", SqlScalarType::String.nullable(false))
        .with_column("action_timing", SqlScalarType::String.nullable(false))
        .with_column(
            "action_reference_old_table",
            SqlScalarType::String.nullable(false),
        )
        .with_column(
            "action_reference_new_table",
            SqlScalarType::String.nullable(false),
        )
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    NULL::text as trigger_catalog,
    NULL::text AS trigger_schema,
    NULL::text AS trigger_name,
    NULL::text AS event_manipulation,
    NULL::text AS event_object_catalog,
    NULL::text AS event_object_schema,
    NULL::text AS event_object_table,
    NULL::integer AS action_order,
    NULL::text AS action_condition,
    NULL::text AS action_statement,
    NULL::text AS action_orientation,
    NULL::text AS action_timing,
    NULL::text AS action_reference_old_table,
    NULL::text AS action_reference_new_table
WHERE FALSE",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static INFORMATION_SCHEMA_VIEWS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "views",
    schema: INFORMATION_SCHEMA,
    oid: oid::VIEW_VIEWS_OID,
    desc: RelationDesc::builder()
        .with_column("table_catalog", SqlScalarType::String.nullable(false))
        .with_column("table_schema", SqlScalarType::String.nullable(false))
        .with_column("table_name", SqlScalarType::String.nullable(false))
        .with_column("view_definition", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    current_database() as table_catalog,
    s.name AS table_schema,
    v.name AS table_name,
    v.definition AS view_definition
FROM mz_catalog.mz_views v
JOIN mz_catalog.mz_schemas s ON s.id = v.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static INFORMATION_SCHEMA_CHARACTER_SETS: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "character_sets",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_CHARACTER_SETS_OID,
        desc: RelationDesc::builder()
            .with_column(
                "character_set_catalog",
                SqlScalarType::String.nullable(true),
            )
            .with_column("character_set_schema", SqlScalarType::String.nullable(true))
            .with_column("character_set_name", SqlScalarType::String.nullable(false))
            .with_column(
                "character_repertoire",
                SqlScalarType::String.nullable(false),
            )
            .with_column("form_of_use", SqlScalarType::String.nullable(false))
            .with_column(
                "default_collate_catalog",
                SqlScalarType::String.nullable(false),
            )
            .with_column(
                "default_collate_schema",
                SqlScalarType::String.nullable(false),
            )
            .with_column(
                "default_collate_name",
                SqlScalarType::String.nullable(false),
            )
            .with_key(vec![])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    NULL as character_set_catalog,
    NULL as character_set_schema,
    'UTF8' as character_set_name,
    'UCS' as character_repertoire,
    'UTF8' as form_of_use,
    current_database() as default_collate_catalog,
    'pg_catalog' as default_collate_schema,
    'en_US.utf8' as default_collate_name",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });
