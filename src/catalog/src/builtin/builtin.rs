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
use mz_catalog_protos::objects::CatalogItemType;
use mz_ore::collections::CollectionExt;
use mz_ore::iter::IteratorExt;
use mz_pgrepr::oid;
use mz_repr::adt::char::CharLength;
use mz_repr::adt::mz_acl_item::MzAclItem;
use mz_repr::namespaces::MZ_INTERNAL_SCHEMA;
use mz_repr::{RelationDesc, SqlScalarType};
use mz_sql::ast::Statement;
use mz_sql::ast::display::{AstDisplay, escaped_string_literal};
use mz_sql::catalog::{CatalogType, NameReference, ObjectType, TypeCategory};
use mz_sql::rbac;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;

use crate::builtin::{Builtin, BuiltinType, BuiltinView, PUBLIC_SELECT, assert_safe_builtin_name};

/// Generate builtin views reporting the given builtins.
///
/// Used in the [`super::BUILTINS_STATIC`] initializer, which keeps the
/// `Builtin::Type`s in `builtin_types` apart from the other `builtin_items`.
pub(super) fn builtins(
    builtin_types: &[Builtin<NameReference>],
    builtin_items: &[Builtin<NameReference>],
) -> impl Iterator<Item = Builtin<NameReference>> {
    let sources: &'static BuiltinView = Box::leak(Box::new(make_builtin_sources(builtin_items)));
    let materialized_views: &'static BuiltinView =
        Box::leak(Box::new(make_builtin_materialized_views(builtin_items)));
    let tables: &'static BuiltinView = Box::leak(Box::new(make_builtin_tables(builtin_items)));
    let indexes: &'static BuiltinView = Box::leak(Box::new(make_builtin_indexes(builtin_items)));
    let log_indexes: &'static BuiltinView =
        Box::leak(Box::new(make_builtin_log_indexes(builtin_items)));
    let types: &'static BuiltinView = Box::leak(Box::new(make_builtin_types(builtin_types)));

    // The generated views above, and `mz_builtin_views` itself, are listed in
    // `mz_builtin_views` with placeholder SQL rather than their real
    // definitions. See `make_builtin_views`.
    let views: &'static BuiltinView = Box::leak(Box::new(make_builtin_views(
        builtin_items,
        [
            log_indexes,
            sources,
            materialized_views,
            tables,
            indexes,
            types,
        ],
    )));

    // Creation order: `mz_builtin_sources` reads `mz_builtin_log_indexes`, so
    // the latter has to exist first.
    [
        log_indexes,
        sources,
        materialized_views,
        tables,
        indexes,
        types,
        views,
    ]
    .into_iter()
    .map(Builtin::View)
}

fn make_builtin_sources(builtin_items: &[Builtin<NameReference>]) -> BuiltinView {
    let source_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::Source(x) => Some(*x),
        _ => None,
    });
    let owner_priv = rbac::owner_privilege(ObjectType::Source, MZ_SYSTEM_ROLE_ID);
    let source_values = source_iter
        .map(|src| {
            let privileges = make_privileges_sql(&src.access, &owner_priv);
            format!(
                "({}::oid, '{}', '{}', 'source', {}, {})",
                src.oid, src.schema, src.name, privileges, src.is_retained_metrics_object
            )
        })
        .join(",");
    // Builtin logs are never retained metrics objects, hence the constant
    // `false` in their rows.
    let object_type = gid_mapping_object_type(CatalogItemType::Source);
    let sql = format!(
        "
SELECT oid, schema_name, name, type, privileges, is_retained_metrics_object, {object_type} AS object_type
FROM (VALUES {source_values}) AS v(oid, schema_name, name, type, privileges, is_retained_metrics_object)
UNION ALL
SELECT oid, schema_name, name, 'log', privileges, false, {object_type}
FROM mz_internal.mz_builtin_log_indexes"
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
            .with_column(
                "is_retained_metrics_object",
                SqlScalarType::Bool.nullable(false),
            )
            .with_column("object_type", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: Default::default(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

fn make_builtin_materialized_views(builtin_items: &[Builtin<NameReference>]) -> BuiltinView {
    let iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::MaterializedView(x) => Some(*x),
        _ => None,
    });
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
    let object_type = gid_mapping_object_type(CatalogItemType::MaterializedView);
    // `is_retained_metrics_object` is `false` for every row, because the catalog
    // does not act on the flag for a materialized view.
    // https://github.com/MaterializeInc/materialize/pull/36072 wired the flag
    // through but it doesn't actually work. Reporting `false` keeps the column consistent.
    let sql = format!(
        "
SELECT oid, schema_name, name, cluster_name, definition, privileges, create_sql, false AS is_retained_metrics_object, {object_type} AS object_type
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
            .with_column(
                "is_retained_metrics_object",
                SqlScalarType::Bool.nullable(false),
            )
            .with_column("object_type", SqlScalarType::String.nullable(false))
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

fn make_builtin_tables(builtin_items: &[Builtin<NameReference>]) -> BuiltinView {
    let iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::Table(x) => Some(*x),
        _ => None,
    });
    let owner_priv = rbac::owner_privilege(ObjectType::Table, MZ_SYSTEM_ROLE_ID);
    let values = iter
        .map(|table| {
            let schema = escaped_string_literal(table.schema);
            let name = escaped_string_literal(table.name);
            let privileges = make_privileges_sql(&table.access, &owner_priv);
            format!(
                "({}::oid, {}, {}, {}, {})",
                table.oid, schema, name, privileges, table.is_retained_metrics_object
            )
        })
        .join(",");
    let object_type = gid_mapping_object_type(CatalogItemType::Table);
    let sql = format!(
        "
SELECT oid, schema_name, name, privileges, is_retained_metrics_object, {object_type} AS object_type
FROM (VALUES {values}) AS v(oid, schema_name, name, privileges, is_retained_metrics_object)"
    );

    BuiltinView {
        name: "mz_builtin_tables",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_BUILTIN_TABLES_OID,
        desc: RelationDesc::builder()
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_name", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_column(
                "is_retained_metrics_object",
                SqlScalarType::Bool.nullable(false),
            )
            .with_column("object_type", SqlScalarType::String.nullable(false))
            // NOTE: The declared keys must exactly match the keys the
            // optimizer derives from the generated VALUES list
            // (`verify_builtin_descs` enforces this). Table names happen to
            // be unique across builtin schemas today, so `name` is a key. If
            // a table is ever added whose bare name collides with another
            // schema's, drop the `name` key here.
            .with_key(vec![0])
            .with_key(vec![2])
            .finish(),
        column_comments: Default::default(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

/// Generates `mz_internal.mz_builtin_indexes`, which `mz_catalog.mz_indexes`
/// reads to report builtin indexes.
fn make_builtin_indexes(builtin_items: &[Builtin<NameReference>]) -> BuiltinView {
    let iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::Index(x) => Some(*x),
        _ => None,
    });
    let values = iter
        .map(|index| {
            assert_safe_builtin_name(index.name, "index");
            let create_sql_str = index.create_sql();
            let stmt = mz_sql::parse::parse(&create_sql_str)
                .unwrap_or_else(|e| panic!("invalid sql for builtin index {}: {e}", index.name))
                .into_element()
                .ast;
            let Statement::CreateIndex(idx_stmt) = stmt else {
                panic!("expected CreateIndex for builtin index {}", index.name);
            };
            let mz_sql::ast::RawItemName::Name(on_name) = idx_stmt.on_name else {
                panic!("expected Name for on_name in builtin index {}", index.name);
            };
            assert_eq!(
                on_name.0.len(),
                2,
                "expected schema.name format for on_name in builtin index {}",
                index.name
            );
            let on_schema = on_name.0[0].as_str();
            let on_name_str = on_name.0[1].as_str();
            assert_safe_builtin_name(on_schema, "index `on` schema");
            assert_safe_builtin_name(on_name_str, "index `on` object");
            let key_exprs = idx_stmt
                .key_parts
                .unwrap_or_else(|| {
                    panic!("builtin index {} must have explicit key parts", index.name)
                })
                .iter()
                .map(|e| e.to_ast_string_stable())
                .join(", ");
            // Unlike the identifier names above, key expressions are arbitrary
            // SQL (column refs, casts, string literals) that can legitimately
            // contain single quotes — so escape them rather than asserting
            // them away with `assert_safe_builtin_name`.
            let key_exprs_escaped = escaped_string_literal(&key_exprs);
            format!(
                "({}::oid, '{}', '{}', '{}', '{}', {key_exprs_escaped}, {})",
                index.oid,
                index.schema,
                index.name,
                on_schema,
                on_name_str,
                index.is_retained_metrics_object
            )
        })
        .join(",");
    let object_type = gid_mapping_object_type(CatalogItemType::Index);
    let sql = format!(
        "
SELECT oid, schema_name, name, on_schema_name, on_name, key_exprs, is_retained_metrics_object, {object_type} AS object_type
FROM (VALUES {values}) AS v(oid, schema_name, name, on_schema_name, on_name, key_exprs, is_retained_metrics_object)"
    );

    BuiltinView {
        name: "mz_builtin_indexes",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_BUILTIN_INDEXES_OID,
        desc: RelationDesc::builder()
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_name", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("on_schema_name", SqlScalarType::String.nullable(false))
            .with_column("on_name", SqlScalarType::String.nullable(false))
            .with_column("key_exprs", SqlScalarType::String.nullable(false))
            .with_column(
                "is_retained_metrics_object",
                SqlScalarType::Bool.nullable(false),
            )
            .with_column("object_type", SqlScalarType::String.nullable(false))
            // NOTE: The declared keys must exactly match the keys the
            // optimizer derives from the generated VALUES list
            // (`verify_builtin_descs` enforces this).
            .with_key(vec![0])
            .with_key(vec![2])
            .finish(),
        column_comments: Default::default(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

/// Generates `mz_internal.mz_builtin_log_indexes`: per builtin log, the key of
/// the introspection index each cluster maintains on it, and its privileges.
/// `mz_catalog.mz_indexes` reads the keys to report those indexes, and
/// `mz_builtin_sources` reads the privileges for its log rows. See the
/// `Fingerprint` impl for `BuiltinMaterializedView` for what that means for migrations.
fn make_builtin_log_indexes(builtin_items: &[Builtin<NameReference>]) -> BuiltinView {
    let iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::Log(x) => Some(*x),
        _ => None,
    });
    // A log is a source for RBAC purposes; this is the owner privilege the
    // catalog grants when it applies builtin logs.
    let owner_priv = rbac::owner_privilege(ObjectType::Source, MZ_SYSTEM_ROLE_ID);
    let values = iter
        .map(|log| {
            assert_safe_builtin_name(log.name, "log");
            let desc = log.variant.desc();
            let index_by = log.variant.index_by();
            let col_list = index_by
                .iter()
                .map(|&i| match desc.get_unambiguous_name(i) {
                    Some(name) => {
                        assert_safe_builtin_name(name, "log column");
                        format!("\"{}\"", name)
                    }
                    None => (i + 1).to_string(),
                })
                .join(", ");
            let privileges = make_privileges_sql(&log.access, &owner_priv);
            format!(
                "({}::oid, '{}', '{}', '{}', {})",
                log.oid, log.schema, log.name, col_list, privileges
            )
        })
        .join(",");
    let sql = format!(
        "
SELECT oid, schema_name, name, col_list, privileges
FROM (VALUES {values}) AS v(oid, schema_name, name, col_list, privileges)"
    );

    BuiltinView {
        name: "mz_builtin_log_indexes",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_BUILTIN_LOG_INDEXES_OID,
        desc: RelationDesc::builder()
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_name", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("col_list", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            // NOTE: The declared keys must exactly match the keys the
            // optimizer derives from the generated VALUES list
            // (`verify_builtin_descs` enforces this).
            .with_key(vec![0])
            .with_key(vec![2])
            .finish(),
        column_comments: Default::default(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

/// Generates `mz_internal.mz_builtin_types`, which `mz_catalog.mz_types`, the
/// per-kind `mz_catalog.mz_*_types` views and `mz_internal.mz_type_pg_metadata`
/// read to report builtin types. [`builtin_types_ctes`] is how they join it to
/// catalog ids.
///
/// `type_kind` names the per-kind view a type belongs to: `array`, `pseudo`,
/// or `base` for everything else, range types included. `element_schema_name`
/// and `element_name` name an array type's element type. `typlen` is NULL for
/// a type with no scalar type, see [`builtin_scalar_type`].
fn make_builtin_types(builtin_types: &[Builtin<NameReference>]) -> BuiltinView {
    let types: Vec<&BuiltinType<NameReference>> = builtin_types
        .iter()
        .filter_map(|b| match b {
            Builtin::Type(t) => Some(*t),
            _ => None,
        })
        .collect();
    let by_name: BTreeMap<&str, &BuiltinType<NameReference>> =
        types.iter().map(|t| (t.name, *t)).collect();
    // The privileges `apply_system_object_mapping_update` grants every builtin
    // type.
    let privileges = make_privileges_sql(
        &[rbac::default_builtin_object_privilege(ObjectType::Type)],
        &rbac::owner_privilege(ObjectType::Type, MZ_SYSTEM_ROLE_ID),
    );
    let values = types
        .iter()
        .map(|t| {
            assert_safe_builtin_name(t.name, "type");
            let (type_kind, element) = match &t.details.typ {
                CatalogType::Array { element_reference } => ("array", Some(*element_reference)),
                CatalogType::Pseudo => ("pseudo", None),
                // `mz_list_types` and `mz_map_types` read only user-defined
                // types, so a builtin list or map would go unreported.
                CatalogType::List { .. } | CatalogType::Map { .. } => panic!(
                    "builtin type {} is a list or map, which mz_builtin_types cannot report",
                    t.name
                ),
                _ => ("base", None),
            };
            let (element_schema_name, element_name) = match element {
                Some(name) => {
                    let element = by_name.get(name).unwrap_or_else(|| {
                        panic!(
                            "builtin type {} references unknown builtin type {name}",
                            t.name
                        )
                    });
                    (
                        escaped_string_literal(element.schema).to_string(),
                        escaped_string_literal(name).to_string(),
                    )
                }
                None => ("NULL::text".to_string(), "NULL::text".to_string()),
            };
            let category = TypeCategory::from_catalog_type(&t.details.typ).to_string();
            let (typinput, typreceive, typsend) = match &t.details.pg_metadata {
                Some(pg) => (
                    format!("{}::oid", pg.typinput_oid),
                    format!("{}::oid", pg.typreceive_oid),
                    format!("{}::oid", pg.typsend_oid),
                ),
                None => (
                    "NULL::oid".to_string(),
                    "NULL::oid".to_string(),
                    "NULL::oid".to_string(),
                ),
            };
            let typlen = match builtin_scalar_type(&t.details.typ, &by_name) {
                Some(scalar_type) => {
                    format!("{}::int2", mz_pgrepr::Type::from(&scalar_type).typlen())
                }
                None => "NULL::int2".to_string(),
            };
            format!(
                "({}::oid, {}, {}, {}, '{type_kind}', {privileges}, {element_schema_name}, {element_name}, {typinput}, {typreceive}, {typsend}, {typlen})",
                t.oid,
                escaped_string_literal(t.schema),
                escaped_string_literal(t.name),
                escaped_string_literal(&category),
            )
        })
        .join(",");
    let sql = format!(
        "
SELECT oid, schema_name, name, category, type_kind, privileges, element_schema_name, element_name, typinput, typreceive, typsend, typlen
FROM (VALUES {values}) AS v(oid, schema_name, name, category, type_kind, privileges, element_schema_name, element_name, typinput, typreceive, typsend, typlen)"
    );

    BuiltinView {
        name: "mz_builtin_types",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_BUILTIN_TYPES_OID,
        desc: RelationDesc::builder()
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_name", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("category", SqlScalarType::String.nullable(false))
            .with_column("type_kind", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_column("element_schema_name", SqlScalarType::String.nullable(true))
            .with_column("element_name", SqlScalarType::String.nullable(true))
            .with_column("typinput", SqlScalarType::Oid.nullable(true))
            .with_column("typreceive", SqlScalarType::Oid.nullable(true))
            .with_column("typsend", SqlScalarType::Oid.nullable(true))
            .with_column("typlen", SqlScalarType::Int16.nullable(true))
            // NOTE: The declared keys must exactly match the keys the
            // optimizer derives from the generated VALUES list
            // (`verify_builtin_descs` enforces this). Builtin type names are
            // unique across schemas, which `test_builtins_static_dependency_order`
            // asserts, so `name` is a key.
            .with_key(vec![0])
            .with_key(vec![2])
            .finish(),
        column_comments: Default::default(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

/// The CTEs through which the type views reach builtin types: `builtin_mappings`
/// maps a builtin type's schema and name to its catalog id via its `GidMapping`
/// record, and `builtin_types` is `mz_builtin_types` extended with that id.
pub(super) fn builtin_types_ctes() -> String {
    let object_type = gid_mapping_object_type(CatalogItemType::Type);
    format!(
        "builtin_mappings AS (
        SELECT
            data->'key'->>'schema_name' AS schema_name,
            data->'key'->>'object_name' AS name,
            's' || (data->'value'->>'catalog_id') AS id
        FROM mz_internal.mz_catalog_raw
        WHERE
            data->>'kind' = 'GidMapping' AND
            data->'key'->>'object_type' = {object_type}
    ),
    builtin_types AS (
        SELECT
            m.id,
            t.oid,
            t.schema_name,
            t.name,
            t.category,
            t.type_kind,
            t.privileges,
            t.element_schema_name,
            t.element_name,
            t.typinput,
            t.typreceive,
            t.typsend,
            t.typlen
        FROM mz_internal.mz_builtin_types t
        JOIN builtin_mappings m USING (schema_name, name)
    )"
    )
}

/// The `SqlScalarType` of a column of the builtin type `typ`, with element
/// references resolved through `by_name`. Mirrors `scalar_type_from_catalog`
/// for the modifier-free builtin types, so that `mz_pgrepr::Type::from` on the
/// result yields the `typlen` pgwire sends for the type. `None` for
/// pseudo-types and containers of them, which have no scalar type.
fn builtin_scalar_type(
    typ: &CatalogType<NameReference>,
    by_name: &BTreeMap<&str, &BuiltinType<NameReference>>,
) -> Option<SqlScalarType> {
    let resolve = |name: &str| {
        let element = by_name
            .get(name)
            .unwrap_or_else(|| panic!("builtin type references unknown builtin type {name}"));
        builtin_scalar_type(&element.details.typ, by_name)
    };
    Some(match typ {
        CatalogType::Pseudo => return None,
        CatalogType::Array { element_reference } => {
            SqlScalarType::Array(Box::new(resolve(element_reference)?))
        }
        CatalogType::Range { element_reference } => SqlScalarType::Range {
            element_type: Box::new(resolve(element_reference)?),
        },
        // These scalar types carry the catalog id of their defining type, which
        // a builtin type does not have statically.
        CatalogType::List { .. } | CatalogType::Map { .. } | CatalogType::Record { .. } => {
            panic!("builtin types cannot be lists, maps or records")
        }
        CatalogType::AclItem => SqlScalarType::AclItem,
        CatalogType::Bool => SqlScalarType::Bool,
        CatalogType::Bytes => SqlScalarType::Bytes,
        CatalogType::Char => SqlScalarType::Char {
            length: Some(CharLength::ONE),
        },
        CatalogType::Date => SqlScalarType::Date,
        CatalogType::Float32 => SqlScalarType::Float32,
        CatalogType::Float64 => SqlScalarType::Float64,
        CatalogType::Int16 => SqlScalarType::Int16,
        CatalogType::Int32 => SqlScalarType::Int32,
        CatalogType::Int64 => SqlScalarType::Int64,
        CatalogType::UInt16 => SqlScalarType::UInt16,
        CatalogType::UInt32 => SqlScalarType::UInt32,
        CatalogType::UInt64 => SqlScalarType::UInt64,
        CatalogType::MzTimestamp => SqlScalarType::MzTimestamp,
        CatalogType::Interval => SqlScalarType::Interval,
        CatalogType::Jsonb => SqlScalarType::Jsonb,
        CatalogType::Numeric => SqlScalarType::Numeric { max_scale: None },
        CatalogType::Oid => SqlScalarType::Oid,
        CatalogType::PgLegacyChar => SqlScalarType::PgLegacyChar,
        CatalogType::PgLegacyName => SqlScalarType::PgLegacyName,
        CatalogType::RegClass => SqlScalarType::RegClass,
        CatalogType::RegProc => SqlScalarType::RegProc,
        CatalogType::RegType => SqlScalarType::RegType,
        CatalogType::String => SqlScalarType::String,
        CatalogType::Time => SqlScalarType::Time,
        CatalogType::Timestamp => SqlScalarType::Timestamp { precision: None },
        CatalogType::TimestampTz => SqlScalarType::TimestampTz { precision: None },
        CatalogType::Uuid => SqlScalarType::Uuid,
        CatalogType::VarChar => SqlScalarType::VarChar { max_length: None },
        CatalogType::Int2Vector => SqlScalarType::Int2Vector,
        CatalogType::MzAclItem => SqlScalarType::MzAclItem,
    })
}

/// Generates `mz_internal.mz_builtin_views`, listing every builtin view,
/// including itself and the `generated` views.
///
/// Views from `iter` are listed with their real definition and create SQL.
/// The generated views are instead listed with a short placeholder query.
/// Real SQL is impossible for `mz_builtin_views` itself, its definition would
/// have to contain its own text. It is impractical for the other generated
/// views, whose SQL embeds metadata about every builtin object.
/// `mz_builtin_materialized_views` for example carries the SQL of every
/// builtin materialized view, so re-embedding its definition here would
/// produce enormous rows that make `SELECT * FROM mz_views` unusable.
///
/// The placeholder is a valid SQL statement, because `mz_views` applies
/// `mz_internal.redact_sql` to the `create_sql` column and that function
/// errors on unparseable input, which would poison the whole materialized
/// view. The placeholder also embeds the view's qualified name so that the
/// `definition` and `create_sql` columns stay unique across rows, which the
/// declared keys rely on.
fn make_builtin_views(
    builtin_items: &[Builtin<NameReference>],
    generated: [&BuiltinView; 6],
) -> BuiltinView {
    let iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::View(x) => Some(*x),
        _ => None,
    });
    let owner_priv = rbac::owner_privilege(ObjectType::View, MZ_SYSTEM_ROLE_ID);

    let make_row = |oid: u32, schema: &str, name: &str, access: &[MzAclItem], create_sql: &str| {
        let stmt = mz_sql::parse::parse(create_sql)
            .expect("valid sql")
            .into_element()
            .ast;
        let Statement::CreateView(stmt) = stmt else {
            panic!("invalid builtin view SQL");
        };

        let definition = format!("{};", stmt.definition.query.to_ast_string_stable());
        let definition = escaped_string_literal(&definition);
        let create_sql = stmt.to_ast_string_stable();
        let create_sql = escaped_string_literal(&create_sql);

        let schema = escaped_string_literal(schema);
        let name = escaped_string_literal(name);
        let privileges = make_privileges_sql(access, &owner_priv);

        format!(
            "({}::oid, {}, {}, {}, {}, {})",
            oid, schema, name, definition, privileges, create_sql
        )
    };

    let mut view = BuiltinView {
        name: "mz_builtin_views",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_BUILTIN_VIEWS_OID,
        desc: RelationDesc::builder()
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_name", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("definition", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            // NOTE: The declared keys must exactly match the keys the
            // optimizer derives from the generated VALUES list
            // (`verify_builtin_descs` enforces this).
            .with_key(vec![0])
            .with_key(vec![2])
            .with_key(vec![3])
            .with_key(vec![5])
            .finish(),
        column_comments: Default::default(),
        sql: "",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    };

    let full_values = iter.map(|v| make_row(v.oid, v.schema, v.name, &v.access, &v.create_sql()));
    let placeholder_values = generated.iter().copied().chain([&view]).map(|v| {
        let create_sql = format!(
            "CREATE VIEW {}.{} AS SELECT '<generated builtin view {}.{}: definition elided>'",
            v.schema, v.name, v.schema, v.name
        );
        make_row(v.oid, v.schema, v.name, &v.access, &create_sql)
    });
    let values = full_values.chain(placeholder_values).join(",");
    let sql = format!(
        "
SELECT oid, schema_name, name, definition, privileges, create_sql
FROM (VALUES {values}) AS v(oid, schema_name, name, definition, privileges, create_sql)"
    );

    view.sql = Box::leak(sql.into_boxed_str());
    view
}

/// The `object_type` of a builtin's `GidMapping` record in `mz_catalog_raw`, as a
/// SQL literal spelled the way `->>` renders it, so that catalog views can join
/// the record on it without hardcoding the enum's discriminants.
fn gid_mapping_object_type(item_type: CatalogItemType) -> String {
    // `mz_catalog_raw` rows are built with `serde_json::to_value`, so this is
    // the exact value `->>` renders.
    let repr =
        serde_json::to_value(item_type).expect("CatalogItemType serializes as a plain integer");
    format!("'{repr}'")
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
