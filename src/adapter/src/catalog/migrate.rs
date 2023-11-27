// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use mz_catalog::durable::Transaction;
use mz_ore::collections::CollectionExt;
use mz_ore::now::{EpochMillis, NowFn};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::Raw;
use mz_sql::catalog::SessionCatalog;
use mz_sql::session::user::MZ_SUPPORT_ROLE_ID;
use mz_storage_types::connections::ConnectionContext;
use semver::Version;
use tracing::info;

use crate::catalog::{Catalog, CatalogState, ConnCatalog};

async fn rewrite_items<F>(
    tx: &mut Transaction<'_>,
    cat: Option<&ConnCatalog<'_>>,
    mut f: F,
) -> Result<(), anyhow::Error>
where
    F: for<'a> FnMut(
        &'a mut Transaction<'_>,
        &'a Option<&ConnCatalog<'_>>,
        &'a mut mz_sql::ast::Statement<Raw>,
    ) -> BoxFuture<'a, Result<(), anyhow::Error>>,
{
    let mut updated_items = BTreeMap::new();
    let items = tx.loaded_items();
    for mut item in items {
        let mut stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;

        f(tx, &cat, &mut stmt).await?;

        item.create_sql = stmt.to_ast_string_stable();

        updated_items.insert(item.id, item);
    }
    tx.update_items(updated_items)?;
    Ok(())
}

pub(crate) async fn migrate(
    state: &CatalogState,
    txn: &mut Transaction<'_>,
    now: NowFn,
    _connection_context: &ConnectionContext,
) -> Result<(), anyhow::Error> {
    let catalog_version = txn.get_catalog_content_version();
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };

    info!("migrating from catalog version {:?}", catalog_version);

    let _now = now();
    // First, do basic AST -> AST transformations.
    // rewrite_items(&mut tx, None, |_tx, _cat, _stmt| Box::pin(async { Ok(()) })).await?;

    // Then, load up a temporary catalog with the rewritten items, and perform
    // some transformations that require introspecting the catalog. These
    // migrations are *weird*: they're rewriting the catalog while looking at
    // it. You probably should be adding a basic AST migration above, unless
    // you are really certain you want one of these crazy migrations.
    let state = Catalog::load_catalog_items(txn, state)?;
    let conn_cat = state.for_system_session();
    rewrite_items(txn, Some(&conn_cat), |_tx, cat, item| {
        let catalog_version = catalog_version.clone();
        Box::pin(async move {
            let _conn_cat = cat.expect("must provide access to conn catalog");
            ast_rewrite_create_connection_options_0_77_0(item)?;
            if catalog_version <= Version::new(0, 77, u64::MAX) {
                ast_rewrite_create_connection_options_0_78_0(item)?;
            }
            Ok(())
        })
    })
    .await?;

    mz_support_read_progress_sources(txn, &conn_cat)?;

    info!(
        "migration from catalog version {:?} complete",
        catalog_version
    );
    Ok(())
}

// Add new migrations below their appropriate heading, and precede them with a
// short summary of the migration's purpose and optional additional commentary
// about safety or approach.
//
// The convention is to name the migration function using snake case:
// > <category>_<description>_<version>
//
// Note that:
// - The sum of all migrations must be idempotent because all migrations run
//   every time the catalog opens, unless migrations are explicitly disabled.
//   This might mean changing code outside the migration itself, or only
//   executing some migrations when encountering certain versions.
// - Migrations must preserve backwards compatibility with all past releases of
//   Materialize.
//
// Please include @benesch on any code reviews that add or edit migrations.

// ****************************************************************************
// AST migrations -- Basic AST -> AST transformations
// ****************************************************************************

/// Remove any durably recorded `WITH (VALIDATE = true|false)` from `CREATE
/// CONNECTION` statements.
///
/// This `WITH` option only has an effect when creating the connection, and the
/// fact that it was persisted in the item's definition is unclear, especially
/// in light of `ALTER CONNECTION`, which also offers a `WITH (VALIDATE =
/// true|false)` option, but cannot alter the `VALIDATE` clause on the `CREATE
/// CONNECTION` statement.
fn ast_rewrite_create_connection_options_0_77_0(
    stmt: &mut mz_sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::visit_mut::VisitMut;
    struct CreateConnectionRewriter;
    impl<'ast> VisitMut<'ast, Raw> for CreateConnectionRewriter {
        fn visit_create_connection_statement_mut(
            &mut self,
            node: &'ast mut mz_sql::ast::CreateConnectionStatement<Raw>,
        ) {
            node.with_options
                .retain(|o| o.name != mz_sql::ast::CreateConnectionOptionName::Validate);
        }
    }

    CreateConnectionRewriter.visit_statement_mut(stmt);
    Ok(())
}

/// Add `SECURITY PROTOCOL` to all existing Kafka connections, based on the
fn ast_rewrite_create_connection_options_0_78_0(
    stmt: &mut mz_sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::visit_mut::VisitMut;
    use mz_sql::ast::ConnectionOptionName::*;
    use mz_sql::ast::{
        ConnectionOption, CreateConnectionStatement, CreateConnectionType, Value, WithOptionValue,
    };

    struct CreateConnectionRewriter;
    impl<'ast> VisitMut<'ast, Raw> for CreateConnectionRewriter {
        fn visit_create_connection_statement_mut(
            &mut self,
            node: &'ast mut CreateConnectionStatement<Raw>,
        ) {
            if node.connection_type != CreateConnectionType::Kafka {
                return;
            }
            let has_security_protocol = node
                .values
                .iter()
                .any(|o| matches!(o.name, SecurityProtocol));
            let is_ssl = node
                .values
                .iter()
                .any(|o| matches!(o.name, SslCertificate | SslKey));
            let is_sasl = node
                .values
                .iter()
                .any(|o| matches!(o.name, SaslMechanisms | SaslUsername | SaslPassword));
            if has_security_protocol {
                panic!(
                    "Kafka connection has security protocol pre-v0.78.0: {:#?}",
                    node.values
                );
            }
            let security_protocol = match (is_ssl, is_sasl) {
                (false, false) => "PLAINTEXT",
                (true, false) => "SSL",
                (false, true) => "SASL_SSL", // Pre-v0.78, any SASL option always meant SASL_SSL.
                (true, true) => panic!(
                    "impossible Kafka connection with both SSL and SASL options: {:#?}",
                    node.values
                ),
            };
            node.values.push(ConnectionOption {
                name: SecurityProtocol,
                value: Some(WithOptionValue::Value(Value::String(
                    security_protocol.into(),
                ))),
            });
        }
    }

    CreateConnectionRewriter.visit_statement_mut(stmt);
    Ok(())
}

// ****************************************************************************
// Semantic migrations -- Weird migrations that require access to the catalog
// ****************************************************************************

/// Grant SELECT on all progress sources to the mz_support role.
fn mz_support_read_progress_sources(
    txn: &mut Transaction<'_>,
    conn_catalog: &ConnCatalog,
) -> Result<(), anyhow::Error> {
    let mut updated_items = BTreeMap::new();
    for mut item in txn.loaded_items() {
        let catalog_item = conn_catalog.get_item(&item.id);
        if catalog_item.is_progress_source() {
            let privileges = catalog_item.privileges();
            match privileges.get_acl_item(&MZ_SUPPORT_ROLE_ID, &catalog_item.owner_id()) {
                Some(acl_item) if acl_item.acl_mode.contains(AclMode::SELECT) => {}
                _ => {
                    let mut new_privileges = privileges.clone();
                    new_privileges.grant(MzAclItem {
                        grantee: MZ_SUPPORT_ROLE_ID,
                        grantor: catalog_item.owner_id(),
                        acl_mode: AclMode::SELECT,
                    });
                    item.privileges = new_privileges.into_all_values().collect();
                    updated_items.insert(item.id, item);
                }
            }
        }
    }
    txn.update_items(updated_items)?;
    Ok(())
}

fn _add_to_audit_log(
    tx: &mut Transaction,
    event_type: mz_audit_log::EventType,
    object_type: mz_audit_log::ObjectType,
    details: mz_audit_log::EventDetails,
    occurred_at: EpochMillis,
) -> Result<(), anyhow::Error> {
    let id = tx.get_and_increment_id(mz_catalog::durable::AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
    let event =
        mz_audit_log::VersionedEvent::new(id, event_type, object_type, details, None, occurred_at);
    tx.insert_audit_log_event(event);
    Ok(())
}
