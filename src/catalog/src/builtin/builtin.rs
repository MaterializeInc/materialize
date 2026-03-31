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
use mz_sql::ast::display::{AstDisplay, escaped_string_literal};
use mz_sql::ast::{ContinualTaskStmt, RawItemName, Statement};
use mz_sql::catalog::{NameReference, ObjectType};
use mz_sql::rbac;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;

use crate::builtin::{
    Builtin, BuiltinConnection, BuiltinContinualTask, BuiltinIndex, BuiltinLog,
    BuiltinMaterializedView, BuiltinSource, BuiltinView, PUBLIC_SELECT,
};

/// Generate builtin views reporting the given builtins.
///
/// Used in the [`super::BUILTINS_STATIC`] initializer.
pub(super) fn builtins(
    builtin_items: &[Builtin<NameReference>],
) -> impl Iterator<Item = Builtin<NameReference>> {
    let conn_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::Connection(x) => Some(*x),
        _ => None,
    });
    let source_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::Source(x) => Some(*x),
        _ => None,
    });
    let log_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::Log(x) => Some(*x),
        _ => None,
    });
    let idx_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::Index(x) => Some(*x),
        _ => None,
    });
    let mv_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::MaterializedView(x) => Some(*x),
        _ => None,
    });
    let ct_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::ContinualTask(x) => Some(*x),
        _ => None,
    });

    let connections = make_builtin_connections(conn_iter);
    let sources = make_builtin_sources(source_iter, log_iter);
    let indexes = make_builtin_indexes(idx_iter);
    let materialized_views = make_builtin_materialized_views(mv_iter);
    let continual_tasks = make_builtin_continual_tasks(ct_iter);

    [
        connections,
        sources,
        indexes,
        materialized_views,
        continual_tasks,
    ]
    .into_iter()
    .map(|v| {
        let static_ref = Box::leak(Box::new(v));
        Builtin::View(static_ref)
    })
}

fn make_builtin_connections(iter: impl Iterator<Item = &'static BuiltinConnection>) -> BuiltinView {
    let values = iter
        .map(|conn| {
            let stmt = mz_sql::parse::parse(conn.sql)
                .expect("valid sql")
                .into_element()
                .ast;
            let Statement::CreateConnection(stmt) = stmt else {
                panic!("invalid builtin connection SQL");
            };

            let create_sql = stmt.to_ast_string_stable();
            let create_sql = escaped_string_literal(&create_sql);

            let type_ = stmt.connection_type.as_str();

            let owner_priv = rbac::owner_privilege(ObjectType::Connection, conn.owner_id.clone());
            let privileges = make_privileges_sql(conn.access, &owner_priv);

            format!(
                "({}::oid, '{}', '{}', '{}', '{}', {}, {})",
                conn.oid, conn.schema, conn.name, type_, conn.owner_id, privileges, create_sql
            )
        })
        .join(",");
    let sql = format!(
        "
SELECT oid, schema_name, name, type, owner_id, privileges, create_sql
FROM (VALUES {values}) AS v(oid, schema_name, name, type, owner_id, privileges, create_sql)"
    );

    BuiltinView {
        name: "mz_builtin_connections",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_BUILTIN_CONNECTIONS_OID,
        desc: RelationDesc::builder()
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_name", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("type", SqlScalarType::String.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .with_key(vec![])
            .finish(),
        column_comments: Default::default(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
    }
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
    }
}

fn make_builtin_indexes(iter: impl Iterator<Item = &'static BuiltinIndex>) -> BuiltinView {
    let values = iter
        .map(|idx| {
            let create_sql_str = idx.create_sql();
            let stmt = mz_sql::parse::parse(&create_sql_str)
                .expect("valid sql")
                .into_element()
                .ast;
            let Statement::CreateIndex(stmt) = stmt else {
                panic!("invalid builtin index SQL");
            };

            let create_sql = stmt.to_ast_string_stable();
            let create_sql = escaped_string_literal(&create_sql);

            let cluster_name = stmt.in_cluster.expect("builtin index has cluster");

            let RawItemName::Name(on_name) = stmt.on_name else {
                panic!("builtin index SQL must have unresolved ON name");
            };
            let Ok([on_schema_name, on_name]) = <[_; 2]>::try_from(on_name.0) else {
                panic!("builtin index ON name must be schema-qualified");
            };

            format!(
                "({}::oid, '{}', '{}', '{}', '{}', '{}', {})",
                idx.oid, idx.schema, idx.name, on_schema_name, on_name, cluster_name, create_sql
            )
        })
        .join(",");
    let sql = format!(
        "
SELECT oid, schema_name, name, on_schema_name, on_name, cluster_name, create_sql
FROM (VALUES {values}) AS v(oid, schema_name, name, on_schema_name, on_name, cluster_name, create_sql)"
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
            .with_column("cluster_name", SqlScalarType::String.nullable(false))
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .with_key(vec![0])
            .with_key(vec![2])
            .with_key(vec![4])
            .with_key(vec![6])
            .finish(),
        column_comments: Default::default(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
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

            let cluster_name = stmt.in_cluster.expect("builtin MV has cluster");
            let privileges = make_privileges_sql(&mv.access, &owner_priv);

            format!(
                "({}::oid, '{}', '{}', '{}', {}, {}, {})",
                mv.oid, mv.schema, mv.name, cluster_name, definition, privileges, create_sql
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
    }
}

fn make_builtin_continual_tasks<'a>(
    iter: impl Iterator<Item = &'a BuiltinContinualTask>,
) -> BuiltinView {
    let owner_priv = rbac::owner_privilege(ObjectType::ContinualTask, MZ_SYSTEM_ROLE_ID);
    let values = iter
        .map(|ct| {
            let stmt = mz_sql::parse::parse(&ct.create_sql())
                .expect("valid sql")
                .into_element()
                .ast;
            let Statement::CreateContinualTask(stmt) = stmt else {
                panic!("invalid builtin CT SQL");
            };

            let create_sql = stmt.to_ast_string_stable();
            let create_sql = escaped_string_literal(&create_sql);

            let stmts = stmt.stmts.iter();
            let definition = stmts
                .map(|stmt| match stmt {
                    ContinualTaskStmt::Delete(s) => s.to_ast_string_stable(),
                    ContinualTaskStmt::Insert(s) => s.to_ast_string_stable(),
                })
                .join("; ");
            let definition = escaped_string_literal(&definition);

            let privileges = make_privileges_sql(&ct.access, &owner_priv);
            let cluster_name = stmt.in_cluster.expect("builtin CT has cluster");

            format!(
                "({}::oid, '{}', '{}', '{}', {}, {}, {})",
                ct.oid, ct.schema, ct.name, cluster_name, definition, privileges, create_sql
            )
        })
        .join(",");
    let sql = format!(
        "
SELECT oid, schema_name, name, cluster_name, definition, privileges, create_sql
FROM (VALUES {values}) AS v(oid, schema_name, name, cluster_name, definition, privileges, create_sql)"
    );

    BuiltinView {
        name: "mz_builtin_continual_tasks",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_BUILTIN_CONTINUAL_TASKS_OID,
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
