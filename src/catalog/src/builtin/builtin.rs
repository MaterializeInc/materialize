// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Constant builtin views exposing information about builtin objects.

use std::borrow::Cow;
use std::collections::BTreeMap;

use itertools::Itertools;
use mz_catalog_protos::objects::CatalogItemType;
use mz_ore::collections::CollectionExt;
use mz_ore::iter::IteratorExt;
use mz_pgrepr::oid;
use mz_repr::adt::mz_acl_item::MzAclItem;
use mz_repr::namespaces::MZ_INTERNAL_SCHEMA;
use mz_repr::{RelationDesc, SqlScalarType};
use mz_sql::ast::Statement;
use mz_sql::ast::display::{AstDisplay, escaped_string_literal};
use mz_sql::catalog::{NameReference, ObjectType};
use mz_sql::rbac;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;

use crate::builtin::{Builtin, BuiltinView, PUBLIC_SELECT, assert_safe_builtin_name};

/// Generate builtin views reporting the given builtins.
///
/// Used in the [`super::BUILTINS_STATIC`] initializer.
pub(super) fn builtins(
    builtin_items: &[Builtin<NameReference>],
) -> impl Iterator<Item = Builtin<NameReference>> {
    let sources: &'static BuiltinView = Box::leak(Box::new(make_builtin_sources(builtin_items)));
    let materialized_views: &'static BuiltinView =
        Box::leak(Box::new(make_builtin_materialized_views(builtin_items)));
    let tables: &'static BuiltinView = Box::leak(Box::new(make_builtin_tables(builtin_items)));
    let indexes: &'static BuiltinView = Box::leak(Box::new(make_builtin_indexes(builtin_items)));
    let log_indexes: &'static BuiltinView =
        Box::leak(Box::new(make_builtin_log_indexes(builtin_items)));
    let index_columns: &'static BuiltinView =
        Box::leak(Box::new(make_builtin_index_columns(builtin_items)));

    // The generated views above, and `mz_builtin_views` itself, are listed in
    // `mz_builtin_views` with placeholder SQL rather than their real
    // definitions. See `make_builtin_views`.
    //
    // `mz_builtin_columns` lists the columns of every builtin relation,
    // `mz_builtin_views` and itself included, and `mz_builtin_views` lists it.
    // Both need only the other's identity and declared desc, so the columns
    // view is built in two steps around the views view.
    let mut columns = builtin_columns_view();
    let views: &'static BuiltinView = Box::leak(Box::new(make_builtin_views(
        builtin_items,
        [
            log_indexes,
            sources,
            materialized_views,
            tables,
            indexes,
            index_columns,
            &columns,
        ],
    )));
    let relations = builtin_items
        .iter()
        .filter_map(|b| match b {
            Builtin::Table(t) => Some((t.schema, t.name, Cow::Borrowed(&t.desc), true)),
            Builtin::Source(s) => Some((s.schema, s.name, Cow::Borrowed(&s.desc), false)),
            Builtin::Log(l) => Some((l.schema, l.name, Cow::Owned(l.variant.desc()), false)),
            Builtin::View(v) => Some((v.schema, v.name, Cow::Borrowed(&v.desc), false)),
            Builtin::MaterializedView(mv) => {
                Some((mv.schema, mv.name, Cow::Borrowed(&mv.desc), false))
            }
            Builtin::Type(_) | Builtin::Func(_) | Builtin::Index(_) | Builtin::Connection(_) => {
                None
            }
        })
        .chain(
            [
                log_indexes,
                sources,
                materialized_views,
                tables,
                indexes,
                index_columns,
                views,
                &columns,
            ]
            .into_iter()
            .map(|v| (v.schema, v.name, Cow::Borrowed(&v.desc), false)),
        );
    columns.sql = Box::leak(builtin_columns_sql(relations).into_boxed_str());
    let columns: &'static BuiltinView = Box::leak(Box::new(columns));

    // Creation order: `mz_builtin_sources` reads `mz_builtin_log_indexes`, so
    // the latter has to exist first.
    [
        log_indexes,
        sources,
        materialized_views,
        tables,
        indexes,
        index_columns,
        columns,
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
/// `mz_builtin_sources` reads the privileges for its log rows. See
/// `MZ_INDEXES` for what that means for migrations.
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
    generated: [&BuiltinView; 7],
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

/// `mz_internal.mz_builtin_columns` without its SQL, which
/// `builtin_columns_sql` generates.
fn builtin_columns_view() -> BuiltinView {
    BuiltinView {
        name: "mz_builtin_columns",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_BUILTIN_COLUMNS_OID,
        desc: RelationDesc::builder()
            .with_column("schema_name", SqlScalarType::String.nullable(false))
            .with_column("relation_name", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("position", SqlScalarType::UInt64.nullable(false))
            .with_column("nullable", SqlScalarType::Bool.nullable(false))
            .with_column("type", SqlScalarType::String.nullable(false))
            .with_column("default", SqlScalarType::String.nullable(true))
            .with_column("type_oid", SqlScalarType::Oid.nullable(false))
            .with_column("type_mod", SqlScalarType::Int32.nullable(false))
            // No single column is unique, so the optimizer derives no key
            // from the generated VALUES list (`verify_builtin_descs`
            // enforces that the declared keys match).
            .finish(),
        column_comments: Default::default(),
        sql: "",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

/// The SQL of `mz_internal.mz_builtin_columns`: one VALUES row per column of
/// each `(schema, name, desc, is_table)` relation, as `mz_columns` presents
/// it. Every column of a builtin table has the `NULL` default that
/// `TableDataSource::TableWrites` gives it, and no other relation has one.
fn builtin_columns_sql<'a>(
    relations: impl Iterator<Item = (&'a str, &'a str, Cow<'a, RelationDesc>, bool)>,
) -> String {
    let values = relations
        .flat_map(|(schema, relation, desc, is_table)| {
            let schema = escaped_string_literal(schema);
            let relation = escaped_string_literal(relation);
            desc.iter()
                .enumerate()
                .map(|(i, (name, typ))| {
                    let pg_type = mz_pgrepr::Type::from(&typ.scalar_type);
                    format!(
                        "({schema}, {relation}, {}, {}, {}, {}, {}, {}, {})",
                        escaped_string_literal(name.as_str()),
                        i + 1,
                        typ.nullable,
                        escaped_string_literal(pg_type.name()),
                        if is_table { "'NULL'" } else { "NULL" },
                        pg_type.oid(),
                        pg_type.typmod(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .join(",");
    format!(
        "
SELECT schema_name, relation_name, name, position::uint8 AS position, nullable, type, \"default\", type_oid::oid AS type_oid, type_mod
FROM (VALUES {values}) AS v(schema_name, relation_name, name, position, nullable, type, \"default\", type_oid, type_mod)"
    )
}

/// Generates `mz_internal.mz_builtin_index_columns`: one row per key of each
/// builtin index and of the introspection index each cluster maintains on a
/// builtin log, with the key's position in the indexed relation and its
/// nullability as `mz_catalog.mz_index_columns` presents them. `name` is the
/// index's or the log's name, which `type` tells apart as `'index'` or
/// `'log'`.
///
/// The keys are resolved against the declared descs of the relations they
/// index. Every builtin index keys on bare columns. An expression key would
/// need its nullability from the planner, which is not available here.
fn make_builtin_index_columns(builtin_items: &[Builtin<NameReference>]) -> BuiltinView {
    let descs: BTreeMap<(&str, &str), Cow<RelationDesc>> = builtin_items
        .iter()
        .filter_map(|b| match b {
            Builtin::Table(t) => Some(((t.schema, t.name), Cow::Borrowed(&t.desc))),
            Builtin::Source(s) => Some(((s.schema, s.name), Cow::Borrowed(&s.desc))),
            Builtin::Log(l) => Some(((l.schema, l.name), Cow::Owned(l.variant.desc()))),
            Builtin::View(v) => Some(((v.schema, v.name), Cow::Borrowed(&v.desc))),
            Builtin::MaterializedView(mv) => Some(((mv.schema, mv.name), Cow::Borrowed(&mv.desc))),
            Builtin::Type(_) | Builtin::Func(_) | Builtin::Index(_) | Builtin::Connection(_) => {
                None
            }
        })
        .collect();

    let index_values = builtin_items
        .iter()
        .filter_map(|b| match b {
            Builtin::Index(index) => Some(*index),
            _ => None,
        })
        .flat_map(|index| {
            assert_safe_builtin_name(index.name, "index");
            let create_sql = index.create_sql();
            let stmt = mz_sql::parse::parse(&create_sql)
                .unwrap_or_else(|e| panic!("invalid sql for builtin index {}: {e}", index.name))
                .into_element()
                .ast;
            let Statement::CreateIndex(stmt) = stmt else {
                panic!("expected CreateIndex for builtin index {}", index.name);
            };
            let mz_sql::ast::RawItemName::Name(on_name) = stmt.on_name else {
                panic!("expected Name for on_name in builtin index {}", index.name);
            };
            let [on_schema, on_relation] = &on_name.0[..] else {
                panic!(
                    "expected schema.name format for on_name in builtin index {}",
                    index.name
                );
            };
            let desc = descs
                .get(&(on_schema.as_str(), on_relation.as_str()))
                .unwrap_or_else(|| {
                    panic!(
                        "builtin index {} is on unknown relation {on_schema}.{on_relation}",
                        index.name
                    )
                });
            let key_parts = stmt
                .key_parts
                .unwrap_or_else(|| panic!("builtin index {} must have explicit key parts", index.name));
            key_parts
                .into_iter()
                .enumerate()
                .map(|(i, expr)| {
                    let mz_sql::ast::Expr::Identifier(parts) = &expr else {
                        panic!("builtin index {} has an expression key {expr}", index.name);
                    };
                    let [column] = &parts[..] else {
                        panic!("builtin index {} has a qualified key {expr}", index.name);
                    };
                    let position = desc
                        .iter_names()
                        .position(|name| name.as_str() == column.as_str())
                        .unwrap_or_else(|| {
                            panic!(
                                "builtin index {} keys on unknown column {column} of {on_schema}.{on_relation}",
                                index.name
                            )
                        });
                    let nullable = desc.typ().column_types[position].nullable;
                    format!(
                        "('{}', {}, {}, {}, 'index')",
                        index.name,
                        i + 1,
                        position + 1,
                        nullable
                    )
                })
                .collect::<Vec<_>>()
        });

    let log_values = builtin_items
        .iter()
        .filter_map(|b| match b {
            Builtin::Log(log) => Some(*log),
            _ => None,
        })
        .flat_map(|log| {
            assert_safe_builtin_name(log.name, "log");
            let desc = log.variant.desc();
            log.variant
                .index_by()
                .into_iter()
                .enumerate()
                .map(|(i, column)| {
                    let nullable = desc.typ().column_types[column].nullable;
                    format!(
                        "('{}', {}, {}, {}, 'log')",
                        log.name,
                        i + 1,
                        column + 1,
                        nullable
                    )
                })
                .collect::<Vec<_>>()
        });

    let values = index_values.chain(log_values).join(",");
    let sql = format!(
        "
SELECT name, index_position::uint8 AS index_position, on_position::uint8 AS on_position, nullable, type
FROM (VALUES {values}) AS v(name, index_position, on_position, nullable, type)"
    );

    BuiltinView {
        name: "mz_builtin_index_columns",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_BUILTIN_INDEX_COLUMNS_OID,
        desc: RelationDesc::builder()
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("index_position", SqlScalarType::UInt64.nullable(false))
            .with_column("on_position", SqlScalarType::UInt64.nullable(false))
            .with_column("nullable", SqlScalarType::Bool.nullable(false))
            .with_column("type", SqlScalarType::String.nullable(false))
            // No single column is unique, so the optimizer derives no key
            // from the generated VALUES list (`verify_builtin_descs`
            // enforces that the declared keys match).
            .finish(),
        column_comments: Default::default(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
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
