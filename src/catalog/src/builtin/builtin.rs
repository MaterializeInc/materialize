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
use mz_pgrepr::oid;
use mz_repr::namespaces::MZ_INTERNAL_SCHEMA;
use mz_repr::{RelationDesc, SqlScalarType};
use mz_sql::ast::Statement;
use mz_sql::ast::display::{AstDisplay, escaped_string_literal};
use mz_sql::catalog::NameReference;

use crate::builtin::{Builtin, BuiltinMaterializedView, BuiltinView, PUBLIC_SELECT};

/// Generate builtin views reporting the given builtins.
///
/// Used in the [`super::BUILTINS_STATIC`] initializer.
pub(super) fn builtins(
    builtin_items: &[Builtin<NameReference>],
) -> impl Iterator<Item = Builtin<NameReference>> {
    let mv_iter = builtin_items.iter().filter_map(|b| match b {
        Builtin::MaterializedView(x) => Some(*x),
        _ => None,
    });
    let materialized_views = make_builtin_materialized_views(mv_iter);

    [materialized_views].into_iter().map(|v| {
        let static_ref = Box::leak(Box::new(v));
        Builtin::View(static_ref)
    })
}

fn make_builtin_materialized_views<'a>(
    iter: impl Iterator<Item = &'a BuiltinMaterializedView>,
) -> BuiltinView {
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

            let mut privs = mv.access.iter().map(|acl| {
                let mode = acl.acl_mode.explode().join(",");
                format!(
                    "mz_internal.make_mz_aclitem('{}', '{}', '{}')",
                    acl.grantee, acl.grantor, mode
                )
            });
            let priv_array = format!("ARRAY[{}]", privs.join(","));

            format!(
                "({}::oid, '{}', '{}', '{}', {}, {}, {})",
                mv.oid, mv.schema, mv.name, cluster_name, definition, priv_array, create_sql
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
