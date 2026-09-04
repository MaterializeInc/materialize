// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A golden dump of the RBAC policy table.
//!
//! `mz_sql::rbac::generate_rbac_requirements` is the authorization policy, and it is about 1,200
//! lines of `match`. This test renders what it produces for a given statement, so the policy can
//! be reviewed as a policy and so that a change to it shows up as a diff in the same pull request
//! that causes it.
//!
//! The dump is deliberately unfiltered. It is what the statement requires, not what a particular
//! session would have to satisfy, so superuser and RBAC-disabled sessions do not change it.
//!
//! Run with `cargo test -p mz-adapter --test rbac`, and rewrite with `REWRITE=1`.

#![recursion_limit = "256"]

use std::sync::Arc;

use mz_adapter::catalog::{Catalog, Op};
use mz_adapter::session::{DEFAULT_DATABASE_NAME, Session};
use mz_catalog::SYSTEM_CONN_ID;
use mz_catalog::memory::objects::{CatalogItem, Table, TableDataSource};
use mz_repr::{RelationDesc, RelationVersion, SqlScalarType, VersionedRelationDesc};
use mz_sql::DEFAULT_SCHEMA;
use mz_sql::catalog::{ErrorMessageObjectDescription, SessionCatalog};
use mz_sql::names::{
    ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds, SystemObjectId,
};
use mz_sql::plan::{Params, PlanContext};
use mz_sql::rbac::{self, RbacRequirementsDescription};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use tokio::sync::Mutex;

/// Renders a requirement record in a form that reads as policy rather than as ids.
fn format_requirements(
    reqs: &RbacRequirementsDescription,
    catalog: &impl SessionCatalog,
) -> String {
    let mut out = String::new();

    let role_name = |role_id| match catalog.try_get_role(role_id) {
        Some(role) => role.name().to_string(),
        None => format!("{role_id}"),
    };
    let object_name =
        |id: &SystemObjectId| ErrorMessageObjectDescription::from_sys_id(id, catalog).to_string();

    if let Some(action) = &reqs.superuser_action {
        out.push_str(&format!("superuser only: {action}\n"));
    }

    if reqs.role_membership.is_empty() {
        out.push_str("membership: none\n");
    } else {
        let names: Vec<_> = reqs.role_membership.iter().map(role_name).collect();
        out.push_str(&format!("membership: {}\n", names.join(", ")));
    }

    if reqs.ownership.is_empty() {
        out.push_str("ownership: none\n");
    } else {
        out.push_str("ownership:\n");
        for object_id in &reqs.ownership {
            let described = SystemObjectId::Object(object_id.clone());
            out.push_str(&format!("  {}\n", object_name(&described)));
        }
    }

    if reqs.privileges.is_empty() {
        out.push_str("privileges: none\n");
    } else {
        // Insertion order is preserved rather than sorted. The order is part of the policy: it
        // decides which denial a user is told about first.
        out.push_str("privileges:\n");
        for (object_id, acl_mode, role_id) in &reqs.privileges {
            out.push_str(&format!(
                "  {} on {} for {}\n",
                acl_mode.to_error_string(),
                object_name(object_id),
                role_name(role_id),
            ));
        }
    }

    if reqs.item_usage.is_empty() {
        out.push_str("item usage: none\n");
    } else {
        let types: Vec<_> = reqs
            .item_usage
            .iter()
            .map(|item_type| item_type.to_string())
            .collect();
        out.push_str(&format!("item usage: {}\n", types.join(", ")));
    }

    out
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn datadriven() {
    datadriven::walk_async("tests/testdata/rbac", |mut f| async {
        Catalog::with_debug(|catalog| async move {
            let catalog = Arc::new(Mutex::new(catalog));
            f.run_async(|test_case| {
                let catalog = Arc::clone(&catalog);
                async move {
                    let mut catalog = catalog.lock().await;
                    match test_case.directive.as_str() {
                        // Creates a single-column table directly, so that statements below have
                        // something to reference. Real DDL would need the sequencer.
                        "add-table" => {
                            let (id, global_id) =
                                catalog.allocate_user_id_for_test().await.unwrap();
                            let database = catalog.resolve_database(DEFAULT_DATABASE_NAME).unwrap();
                            let database_name = database.name.clone();
                            let database_spec =
                                ResolvedDatabaseSpecifier::Id(database.id());
                            let schema = catalog
                                .resolve_schema_in_database(
                                    &database_spec,
                                    DEFAULT_SCHEMA,
                                    &SYSTEM_CONN_ID,
                                )
                                .unwrap();
                            let schema_name = schema.name.schema.clone();
                            let schema_spec = schema.id.clone();
                            let name = test_case.input.trim_end().to_string();
                            let commit_ts = catalog.current_upper().await;
                            catalog
                                .transact(
                                    None,
                                    commit_ts,
                                    None,
                                    vec![Op::CreateItem {
                                        id,
                                        name: QualifiedItemName {
                                            qualifiers: ItemQualifiers {
                                                database_spec,
                                                schema_spec,
                                            },
                                            item: name.clone(),
                                        },
                                        item: CatalogItem::Table(Table {
                                            create_sql: Some(format!(
                                                "CREATE TABLE {database_name}.{schema_name}.{name} (a bigint)"
                                            )),
                                            desc: VersionedRelationDesc::new(
                                                RelationDesc::builder()
                                                    .with_column(
                                                        "a",
                                                        SqlScalarType::Int64.nullable(true),
                                                    )
                                                    .finish(),
                                            ),
                                            collections: [(RelationVersion::root(), global_id)]
                                                .into_iter()
                                                .collect(),
                                            conn_id: None,
                                            resolved_ids: ResolvedIds::empty(),
                                            custom_logical_compaction_window: None,
                                            is_retained_metrics_object: false,
                                            data_source: TableDataSource::TableWrites {
                                                defaults: vec![],
                                            },
                                        }),
                                        owner_id: MZ_SYSTEM_ROLE_ID,
                                    }],
                                )
                                .await
                                .unwrap();
                            format!("{id}\n")
                        }
                        // Plans the statement and dumps what RBAC would require of it.
                        "requirements" => {
                            let session = Session::dummy();
                            let conn_catalog = catalog.for_session(&session);
                            let role_id = session.role_metadata().current_role;

                            let parsed = match mz_sql::parse::parse(&test_case.input) {
                                Ok(parsed) => parsed,
                                Err(e) => return format!("parse error: {e}\n"),
                            };
                            let stmt = parsed[0].ast.clone();
                            let (stmt, resolved_ids) =
                                match mz_sql::names::resolve(&conn_catalog, stmt) {
                                    Ok(resolved) => resolved,
                                    Err(e) => return format!("resolve error: {e}\n"),
                                };
                            let pcx = PlanContext::zero();
                            let (plan, _resolved_ids) = match mz_sql::plan::plan(
                                Some(&pcx),
                                &conn_catalog,
                                stmt,
                                &Params::empty(),
                                &resolved_ids,
                            ) {
                                Ok(planned) => planned,
                                Err(e) => return format!("plan error: {e}\n"),
                            };

                            let reqs = rbac::describe_rbac_requirements(
                                &conn_catalog,
                                &plan,
                                None,
                                None,
                                role_id,
                            );
                            format_requirements(&reqs, &conn_catalog)
                        }
                        dir => panic!("unhandled directive {dir}"),
                    }
                }
            })
            .await;
            if let Some(catalog) = Arc::into_inner(catalog) {
                let catalog = catalog.into_inner();
                catalog.expire().await;
            }
            f
        })
        .await
    })
    .await;
}
