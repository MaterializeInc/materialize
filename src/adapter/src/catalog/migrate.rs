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
use mz_ore::collections::CollectionExt;
use mz_ore::now::EpochMillis;
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::Raw;
use mz_sql_parser::ast::visit_mut::VisitMut;
use mz_sql_parser::ast::{Function, Ident};
use mz_storage_client::types::connections::ConnectionContext;
use semver::Version;
use tracing::info;

use crate::catalog::storage::Transaction;
use crate::catalog::{storage, Catalog, ConnCatalog, SerializedCatalogItem};

/// A flag to indicate whether or not we've already run the migration to rename existing uses of
/// the AVG(...) function. Runs in versions <=0.66.
const RENAME_AVG_FUNCTION_MIGRATION_FLAG: &str = "rename_avg_function_migration_flag";

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
        let create_sql = match &item.definition {
            SerializedCatalogItem::V1 { create_sql } => create_sql,
        };
        let mut stmt = mz_sql::parse::parse(create_sql)?.into_element().ast;

        f(tx, &cat, &mut stmt).await?;

        let serialized_item = SerializedCatalogItem::V1 {
            create_sql: stmt.to_ast_string_stable(),
        };
        item.definition = serialized_item;

        updated_items.insert(item.id, item);
    }
    tx.update_items(updated_items)?;
    Ok(())
}

pub(crate) async fn migrate(
    catalog: &mut Catalog,
    _connection_context: Option<ConnectionContext>,
) -> Result<(), anyhow::Error> {
    let mut storage = catalog.storage().await;
    let catalog_version = storage.get_catalog_content_version().await?;
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };

    info!("migrating from catalog version {:?}", catalog_version);

    let _now = (catalog.config().now)();
    let mut tx = storage.transaction().await?;
    // First, do basic AST -> AST transformations.
    // rewrite_items(&mut tx, None, |_tx, _cat, _stmt| Box::pin(async { Ok(()) })).await?;

    // Only run this migration if we haven't run it already.
    let avg_function_has_run =
        tx.check_migration_has_run(RENAME_AVG_FUNCTION_MIGRATION_FLAG.to_string())?;
    info!("{RENAME_AVG_FUNCTION_MIGRATION_FLAG}, already_run: {avg_function_has_run}");

    if !avg_function_has_run {
        rewrite_items(&mut tx, None, |_tx, _cat, stmt| {
            Box::pin(async { ast_rename_avg_function_0_66_0(stmt) })
        })
        .await?;
        // Important: Mark the migration as being completed in the same transaction that we used
        // to do the migration.
        tx.mark_migration_has_run(RENAME_AVG_FUNCTION_MIGRATION_FLAG.to_string())?;
    }

    // Then, load up a temporary catalog with the rewritten items, and perform
    // some transformations that require introspecting the catalog. These
    // migrations are *weird*: they're rewriting the catalog while looking at
    // it. You probably should be adding a basic AST migration above, unless
    // you are really certain you want one of these crazy migrations.
    let cat = Catalog::load_catalog_items(&mut tx, catalog)?;
    let conn_cat = cat.for_system_session();
    rewrite_items(&mut tx, Some(&conn_cat), |_tx, cat, _item| {
        Box::pin(async move {
            let _conn_cat = cat.expect("must provide access to conn catalog");
            Ok(())
        })
    })
    .await?;

    tx.commit().await?;
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

fn ast_rename_avg_function_0_66_0(
    stmt: &mut mz_sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    struct AvgFunctionRenamer;
    impl<'ast> VisitMut<'ast, Raw> for AvgFunctionRenamer {
        fn visit_function_mut(&mut self, node: &'ast mut Function<Raw>) {
            let components = &mut node.name.name_mut().0;
            let len = components.len();
            if len < 2 {
                return;
            }
            let Some(last_two) = components.get_mut(len - 2..) else {
                return;
            };

            // Rewrite the the function to be "avg_internal_v1".
            if last_two[0].as_str() == "pg_catalog" && last_two[1].as_str() == "avg" {
                last_two[0] = Ident::new("mz_catalog");
                last_two[1] = Ident::new("avg_internal_v1");
            }

            // No one should have an object with this function as a dependency, but we check for it
            // here just to be safe.
            if last_two[0].as_str() == "mz_internal" && last_two[1].as_str() == "mz_avg_promotion" {
                last_two[1] = Ident::new("mz_avg_promotion_internal_v1");
            }

            // Continue to recurse.
            self.visit_function_args_mut(&mut node.args);
            if let Some(v) = &mut node.filter {
                self.visit_expr_mut(&mut *v);
            }
            if let Some(v) = &mut node.over {
                self.visit_window_spec_mut(&mut *v);
            }
        }
    }

    AvgFunctionRenamer.visit_statement_mut(stmt);
    Ok(())
}

// ****************************************************************************
// Semantic migrations -- Weird migrations that require access to the catalog
// ****************************************************************************

fn _add_to_audit_log(
    tx: &mut Transaction,
    event_type: mz_audit_log::EventType,
    object_type: mz_audit_log::ObjectType,
    details: mz_audit_log::EventDetails,
    occurred_at: EpochMillis,
) -> Result<(), anyhow::Error> {
    let id = tx.get_and_increment_id(storage::AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
    let event =
        mz_audit_log::VersionedEvent::new(id, event_type, object_type, details, None, occurred_at);
    tx.insert_audit_log_event(event);
    Ok(())
}
