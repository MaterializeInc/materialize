// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Constant builtin views exposing information about builtin objects.

use itertools::Itertools;
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

use crate::builtin::{
    Builtin, BuiltinIndex, BuiltinLog, BuiltinMaterializedView, BuiltinSource, BuiltinTable,
    BuiltinView, PUBLIC_SELECT, assert_safe_builtin_name,
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
    let table_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::Table(x) => Some(*x),
        _ => None,
    });
    let index_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::Index(x) => Some(*x),
        _ => None,
    });

    let sources: &'static BuiltinView = Box::leak(Box::new(make_builtin_sources(source_iter)));
    let materialized_views: &'static BuiltinView =
        Box::leak(Box::new(make_builtin_materialized_views(mv_iter)));
    let tables: &'static BuiltinView = Box::leak(Box::new(make_builtin_tables(table_iter)));
    let indexes: &'static BuiltinView = Box::leak(Box::new(make_builtin_indexes(index_iter)));
    let log_indexes: &'static BuiltinView = Box::leak(Box::new(make_builtin_log_indexes(log_iter)));

    // The generated views above, and `mz_builtin_views` itself, are listed in
    // `mz_builtin_views` with placeholder SQL rather than their real
    // definitions. See `make_builtin_views`.
    let view_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::View(x) => Some(*x),
        _ => None,
    });
    let views: &'static BuiltinView = Box::leak(Box::new(make_builtin_views(
        view_iter,
        [log_indexes, sources, materialized_views, tables, indexes],
    )));

    // Creation order: `mz_builtin_sources` reads `mz_builtin_log_indexes`, so
    // the latter has to exist first.
    [
        log_indexes,
        sources,
        materialized_views,
        tables,
        indexes,
        views,
    ]
    .into_iter()
    .map(Builtin::View)
}

fn make_builtin_sources(source_iter: impl Iterator<Item = &'static BuiltinSource>) -> BuiltinView {
    let owner_priv = rbac::owner_privilege(ObjectType::Source, MZ_SYSTEM_ROLE_ID);
    let source_values = source_iter
        .map(|src| {
            let privileges = make_privileges_sql(&src.access, &owner_priv);
            format!(
                "({}::oid, '{}', '{}', 'source', {})",
                src.oid, src.schema, src.name, privileges
            )
        })
        .join(",");
    let sql = format!(
        "
SELECT oid, schema_name, name, type, privileges
FROM (VALUES {source_values}) AS v(oid, schema_name, name, type, privileges)
UNION ALL
SELECT oid, schema_name, name, 'log', privileges
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

fn make_builtin_tables(iter: impl Iterator<Item = &'static BuiltinTable>) -> BuiltinView {
    let owner_priv = rbac::owner_privilege(ObjectType::Table, MZ_SYSTEM_ROLE_ID);
    let values = iter
        .map(|table| {
            let schema = escaped_string_literal(table.schema);
            let name = escaped_string_literal(table.name);
            let privileges = make_privileges_sql(&table.access, &owner_priv);
            format!("({}::oid, {}, {}, {})", table.oid, schema, name, privileges)
        })
        .join(",");
    let sql = format!(
        "
SELECT oid, schema_name, name, privileges
FROM (VALUES {values}) AS v(oid, schema_name, name, privileges)"
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
fn make_builtin_indexes(iter: impl Iterator<Item = &'static BuiltinIndex>) -> BuiltinView {
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
                "({}::oid, '{}', '{}', '{}', '{}', {key_exprs_escaped})",
                index.oid, index.schema, index.name, on_schema, on_name_str
            )
        })
        .join(",");
    let sql = format!(
        "
SELECT oid, schema_name, name, on_schema_name, on_name, key_exprs
FROM (VALUES {values}) AS v(oid, schema_name, name, on_schema_name, on_name, key_exprs)"
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
fn make_builtin_log_indexes(iter: impl Iterator<Item = &'static BuiltinLog>) -> BuiltinView {
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
fn make_builtin_views<'a>(
    iter: impl Iterator<Item = &'a BuiltinView>,
    generated: [&BuiltinView; 5],
) -> BuiltinView {
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
