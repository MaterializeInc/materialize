// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Constant builtin views exposing information about builtin objects.

use std::collections::BTreeMap;

use itertools::Itertools;
use mz_ore::collections::CollectionExt;
use mz_ore::iter::IteratorExt;
use mz_pgrepr::oid;
use mz_repr::adt::mz_acl_item::MzAclItem;
use mz_repr::namespaces::{MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA};
use mz_repr::{RelationDesc, SemanticType, SqlScalarType};
use mz_sql::ast::Statement;
use mz_sql::ast::display::{AstDisplay, escaped_string_literal};
use mz_sql::catalog::{NameReference, ObjectType};
use mz_sql::rbac;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;

use crate::builtin::{
    Builtin, BuiltinLog, BuiltinMaterializedView, BuiltinSource, BuiltinView, Cardinality,
    LinkProperties, Ontology, OntologyLink, PUBLIC_SELECT,
};

/// Generate builtin views reporting the given builtins.
///
/// Used in the [`super::BUILTINS_STATIC`] initializer.
pub(super) fn builtins(
    builtin_items: &[Builtin<NameReference>],
) -> impl Iterator<Item = Builtin<NameReference>> {
    let source_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::Source(x) => Some(*x),
        _ => None,
    });
    let log_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::Log(x) => Some(*x),
        _ => None,
    });
    let mv_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::MaterializedView(x) => Some(*x),
        _ => None,
    });

    let sources = make_builtin_sources(source_iter, log_iter);
    let materialized_views = make_builtin_materialized_views(mv_iter);

    [sources, materialized_views].into_iter().map(|v| {
        let static_ref = Box::leak(Box::new(v));
        Builtin::View(static_ref)
    })
}

fn make_builtin_sources(
    source_iter: impl Iterator<Item = &'static BuiltinSource>,
    log_iter: impl Iterator<Item = &'static BuiltinLog>,
) -> BuiltinView {
    let owner_priv = rbac::owner_privilege(ObjectType::Source, MZ_SYSTEM_ROLE_ID);
    let source_values = source_iter.map(|src| {
        let privileges = make_privileges_sql(&src.access, &owner_priv);
        format!(
            "({}::oid, '{}', '{}', 'source', {})",
            src.oid, src.schema, src.name, privileges
        )
    });
    let log_values = log_iter.map(|log| {
        let privileges = make_privileges_sql(&log.access, &owner_priv);
        format!(
            "({}::oid, '{}', '{}', 'log', {})",
            log.oid, log.schema, log.name, privileges
        )
    });
    let values = source_values.chain(log_values).join(",");
    let sql = format!(
        "
SELECT oid, schema_name, name, type, privileges
FROM (VALUES {values}) AS v(oid, schema_name, name, type, privileges)"
    );

    BuiltinView {
        name: "mz_builtin_sources",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_BUILTIN_SOURCES_OID,
        desc: RelationDesc::builder()
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_name", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("type", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_key(vec![0])
            .with_key(vec![2])
            .finish(),
        column_comments: Default::default(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

fn make_builtin_materialized_views<'a>(
    iter: impl Iterator<Item = &'a BuiltinMaterializedView>,
) -> BuiltinView {
    let owner_priv = rbac::owner_privilege(ObjectType::MaterializedView, MZ_SYSTEM_ROLE_ID);
    let values = iter
        .map(|mv| {
            let stmt = mz_sql::parse::parse(&mv.create_sql())
                .expect("valid sql")
                .into_element()
                .ast;
            let Statement::CreateMaterializedView(stmt) = stmt else {
                panic!("invalid builtin MV SQL");
            };

            let definition = format!("{};", stmt.query.to_ast_string_stable());
            let definition = escaped_string_literal(&definition);
            let create_sql = stmt.to_ast_string_stable();
            let create_sql = escaped_string_literal(&create_sql);

            let cluster_name = stmt.in_cluster.expect("builtin MV has cluster").to_string();
            let cluster_name = escaped_string_literal(&cluster_name);
            let schema = escaped_string_literal(mv.schema);
            let name = escaped_string_literal(mv.name);
            let privileges = make_privileges_sql(&mv.access, &owner_priv);

            format!(
                "({}::oid, {}, {}, {}, {}, {}, {})",
                mv.oid, schema, name, cluster_name, definition, privileges, create_sql
            )
        })
        .join(",");
    let sql = format!(
        "
SELECT oid, schema_name, name, cluster_name, definition, privileges, create_sql
FROM (VALUES {values}) AS v(oid, schema_name, name, cluster_name, definition, privileges, create_sql)"
    );

    BuiltinView {
        name: "mz_builtin_materialized_views",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_BUILTIN_MATERIALIZED_VIEWS_OID,
        desc: RelationDesc::builder()
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_name", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("cluster_name", SqlScalarType::String.nullable(false))
            .with_column("definition", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .with_key(vec![0])
            .with_key(vec![2])
            .with_key(vec![4])
            .with_key(vec![6])
            .finish(),
        column_comments: Default::default(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

/// Convert the given list of [`MzAclItem`] to the equivalent SQL syntax.
fn make_privileges_sql(privs: &[MzAclItem], owner_priv: &MzAclItem) -> String {
    let privs = privs.iter().chain_one(owner_priv);
    let mut parts = privs.map(|acl| {
        let mode = acl.acl_mode.explode().join(",");
        format!(
            "mz_internal.make_mz_aclitem('{}', '{}', '{}')",
            acl.grantee, acl.grantor, mode
        )
    });
    format!("ARRAY[{}]", parts.join(","))
}

/// Generate the `mz_catalog.mz_sources` builtin materialized view with builtin
/// source entries inlined as a VALUES clause.
///
/// Inlining the values means the MV's SQL fingerprint changes whenever a builtin
/// source or log is added or removed, which forces a `MigrationStep::replacement`
/// for `mz_sources` and guarantees stale data is never silently served.
pub(super) fn make_mz_sources(
    source_iter: impl Iterator<Item = &'static BuiltinSource>,
    log_iter: impl Iterator<Item = &'static BuiltinLog>,
) -> BuiltinMaterializedView {
    let owner_priv = rbac::owner_privilege(ObjectType::Source, MZ_SYSTEM_ROLE_ID);
    let source_values = source_iter.map(|src| {
        let privileges = make_privileges_sql(&src.access, &owner_priv);
        format!(
            "({}::oid, '{}', '{}', 'source', {})",
            src.oid, src.schema, src.name, privileges
        )
    });
    let log_values = log_iter.map(|log| {
        let privileges = make_privileges_sql(&log.access, &owner_priv);
        format!(
            "({}::oid, '{}', '{}', 'log', {})",
            log.oid, log.schema, log.name, privileges
        )
    });
    let builtin_values = source_values.chain(log_values).join(",");

    let sql = format!("
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL oid,
    ASSERT NOT NULL schema_id,
    ASSERT NOT NULL name,
    ASSERT NOT NULL type,
    ASSERT NOT NULL owner_id,
    ASSERT NOT NULL privileges
) AS
WITH
    user_sources AS (
        SELECT
            mz_internal.parse_catalog_id(data->'key'->'gid') AS id,
            (data->'value'->>'oid')::oid AS oid,
            mz_internal.parse_catalog_id(data->'value'->'schema_id') AS schema_id,
            data->'value'->>'name' AS name,
            parsed->>'source_type' AS type,
            parsed->>'connection_id' AS connection_id,
            NULL AS size,
            parsed->>'envelope_type' AS envelope_type,
            parsed->>'key_format' AS key_format,
            parsed->>'value_format' AS value_format,
            COALESCE(
                parsed->>'cluster_id',
                (
                    SELECT mz_internal.parse_catalog_create_sql(p.data->'value'->'definition'->'V1'->>'create_sql')->>'cluster_id'
                    FROM mz_internal.mz_catalog_raw p
                    WHERE
                        p.data->>'kind' = 'Item' AND
                        mz_internal.parse_catalog_id(p.data->'key'->'gid') = parsed->>'of_source_id'
                )
            ) AS cluster_id,
            mz_internal.parse_catalog_id(data->'value'->'owner_id') AS owner_id,
            mz_internal.parse_catalog_privileges(data->'value'->'privileges') AS privileges,
            data->'value'->'definition'->'V1'->>'create_sql' AS create_sql,
            mz_internal.redact_sql(data->'value'->'definition'->'V1'->>'create_sql') AS redacted_create_sql
        FROM
            mz_internal.mz_catalog_raw
            CROSS JOIN LATERAL (
                SELECT mz_internal.parse_catalog_create_sql(data->'value'->'definition'->'V1'->>'create_sql')
            ) AS l(parsed)
        WHERE
            data->>'kind' = 'Item' AND
            parsed->>'type' IN ('source', 'subsource')
    ),
    builtin_mappings AS (
        SELECT
            data->'key'->>'schema_name' AS schema_name,
            data->'key'->>'object_name' AS name,
            's' || (data->'value'->>'catalog_id') AS id
        FROM mz_internal.mz_catalog_raw
        WHERE
            data->>'kind' = 'GidMapping' AND
            data->'key'->>'object_type' = '2'
    ),
    builtin_sources AS (
        SELECT
            m.id,
            src.oid,
            s.id AS schema_id,
            src.name,
            src.type,
            NULL AS connection_id,
            NULL AS size,
            NULL AS envelope_type,
            NULL AS key_format,
            NULL AS value_format,
            NULL AS cluster_id,
            '{MZ_SYSTEM_ROLE_ID}' AS owner_id,
            src.privileges,
            NULL AS create_sql,
            NULL AS redacted_create_sql
        FROM (VALUES {builtin_values}) AS src(oid, schema_name, name, type, privileges)
        JOIN builtin_mappings m USING (schema_name, name)
        JOIN mz_schemas s ON s.name = src.schema_name
        WHERE s.database_id IS NULL
    )
SELECT * FROM user_sources
UNION ALL
SELECT * FROM builtin_sources");

    BuiltinMaterializedView {
        name: "mz_sources",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_SOURCES_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_id", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("type", SqlScalarType::String.nullable(false))
            .with_column("connection_id", SqlScalarType::String.nullable(true))
            .with_column("size", SqlScalarType::String.nullable(true))
            .with_column("envelope_type", SqlScalarType::String.nullable(true))
            .with_column("key_format", SqlScalarType::String.nullable(true))
            .with_column("value_format", SqlScalarType::String.nullable(true))
            .with_column("cluster_id", SqlScalarType::String.nullable(true))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_column("create_sql", SqlScalarType::String.nullable(true))
            .with_column("redacted_create_sql", SqlScalarType::String.nullable(true))
            .with_key(vec![0])
            .with_key(vec![1])
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "Materialize's unique ID for the source."),
            ("oid", "A PostgreSQL-compatible OID for the source."),
            (
                "schema_id",
                "The ID of the schema to which the source belongs. Corresponds to `mz_schemas.id`.",
            ),
            ("name", "The name of the source."),
            (
                "type",
                "The type of the source: `kafka`, `mysql`, `postgres`, `load-generator`, `progress`, or `subsource`.",
            ),
            (
                "connection_id",
                "The ID of the connection associated with the source, if any. Corresponds to `mz_connections.id`.",
            ),
            ("size", "*Deprecated* The size of the source."),
            (
                "envelope_type",
                "For Kafka sources, the envelope type: `none`, `upsert`, or `debezium`. `NULL` for other source types.",
            ),
            (
                "key_format",
                "For Kafka sources, the format of the Kafka message key: `avro`, `csv`, `regex`, `bytes`, `json`, `text`, or `NULL`.",
            ),
            (
                "value_format",
                "For Kafka sources, the format of the Kafka message value: `avro`, `csv`, `regex`, `bytes`, `json`, `text`. `NULL` for other source types.",
            ),
            (
                "cluster_id",
                "The ID of the cluster maintaining the source. Corresponds to `mz_clusters.id`.",
            ),
            (
                "owner_id",
                "The role ID of the owner of the source. Corresponds to `mz_roles.id`.",
            ),
            ("privileges", "The privileges granted on the source."),
            ("create_sql", "The `CREATE` SQL statement for the source."),
            (
                "redacted_create_sql",
                "The redacted `CREATE` SQL statement for the source.",
            ),
        ]),
        sql: Box::leak(sql.into_boxed_str()),
        is_retained_metrics_object: true,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "source",
            description: "An external data source ingested into Materialize (e.g., Kafka, Postgres)",
            links: &const {
                [
                    OntologyLink {
                        name: "in_schema",
                        target: "schema",
                        properties: LinkProperties::fk("schema_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "owned_by",
                        target: "role",
                        properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "runs_on_cluster",
                        target: "cluster",
                        properties: LinkProperties::fk_nullable(
                            "cluster_id",
                            "id",
                            Cardinality::ManyToOne,
                        ),
                    },
                    OntologyLink {
                        name: "uses_connection",
                        target: "connection",
                        properties: LinkProperties::fk_nullable(
                            "connection_id",
                            "id",
                            Cardinality::ManyToOne,
                        ),
                    },
                ]
            },
            column_semantic_types: &const {
                [
                    ("id", SemanticType::CatalogItemId),
                    ("oid", SemanticType::OID),
                    ("schema_id", SemanticType::SchemaId),
                    ("type", SemanticType::SourceType),
                    ("connection_id", SemanticType::CatalogItemId),
                    ("cluster_id", SemanticType::ClusterId),
                    ("owner_id", SemanticType::RoleId),
                    ("create_sql", SemanticType::SqlDefinition),
                    ("redacted_create_sql", SemanticType::RedactedSqlDefinition),
                ]
            },
        }),
    }
}
