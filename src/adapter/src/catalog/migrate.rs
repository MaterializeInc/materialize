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
use mz_catalog::builtin::{BuiltinTable, MZ_CLUSTER_REPLICA_FRONTIERS};
use mz_catalog::durable::{SystemObjectDescription, Transaction};
use mz_catalog::memory::objects::StateUpdate;
use mz_ore::collections::CollectionExt;
use mz_ore::now::NowFn;
use mz_repr::namespaces::{MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA};
use mz_repr::{GlobalId, Timestamp};
use mz_sql::ast::display::AstDisplay;
use mz_sql_parser::ast::{Raw, Statement};
use semver::Version;
use tracing::info;
// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::open::into_consolidatable_updates_startup;
use crate::catalog::{BuiltinTableUpdate, CatalogState, ConnCatalog};

async fn rewrite_ast_items<F>(tx: &mut Transaction<'_>, mut f: F) -> Result<(), anyhow::Error>
where
    F: for<'a> FnMut(
        &'a mut Transaction<'_>,
        GlobalId,
        &'a mut Statement<Raw>,
    ) -> BoxFuture<'a, Result<(), anyhow::Error>>,
{
    let mut updated_items = BTreeMap::new();

    for mut item in tx.get_items() {
        let mut stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;
        f(tx, item.id, &mut stmt).await?;

        item.create_sql = stmt.to_ast_string_stable();

        updated_items.insert(item.id, item);
    }
    tx.update_items(updated_items)?;
    Ok(())
}

async fn rewrite_items<F>(
    tx: &mut Transaction<'_>,
    cat: &ConnCatalog<'_>,
    mut f: F,
) -> Result<(), anyhow::Error>
where
    F: for<'a> FnMut(
        &'a mut Transaction<'_>,
        &'a &ConnCatalog<'_>,
        GlobalId,
        &'a mut Statement<Raw>,
    ) -> BoxFuture<'a, Result<(), anyhow::Error>>,
{
    let mut updated_items = BTreeMap::new();
    let items = tx.get_items();
    for mut item in items {
        let mut stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;

        f(tx, &cat, item.id, &mut stmt).await?;

        item.create_sql = stmt.to_ast_string_stable();

        updated_items.insert(item.id, item);
    }
    tx.update_items(updated_items)?;
    Ok(())
}

/// Migrates all user items and loads them into `state`.
///
/// Returns the builtin updates corresponding to all user items.
pub(crate) async fn migrate(
    state: &mut CatalogState,
    tx: &mut Transaction<'_>,
    item_updates: Vec<StateUpdate>,
    _now: NowFn,
    _boot_ts: Timestamp,
) -> Result<Vec<BuiltinTableUpdate<&'static BuiltinTable>>, anyhow::Error> {
    let catalog_version = tx.get_catalog_content_version();
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };

    info!(
        "migrating statements from catalog version {:?}",
        catalog_version
    );

    rewrite_ast_items(tx, |_tx, _id, _stmt| {
        Box::pin(async move {
            // Add per-item AST migrations below.
            //
            // Each migration should be a function that takes `stmt` (the AST
            // representing the creation SQL for the item) as input. Any
            // mutations to `stmt` will be staged for commit to the catalog.
            //
            // Migration functions may also take `tx` as input to stage
            // arbitrary changes to the catalog.
            Ok(())
        })
    })
    .await?;

    // Load items into catalog. We make sure to consolidate the old updates with the new updates to
    // avoid trying to apply unmigrated items.
    let commit_ts = tx.commit_ts();
    let mut item_updates = into_consolidatable_updates_startup(item_updates, commit_ts);
    let op_item_updates = tx.get_and_commit_op_updates();
    let op_item_updates = into_consolidatable_updates_startup(op_item_updates, commit_ts);
    item_updates.extend(op_item_updates);
    differential_dataflow::consolidation::consolidate_updates(&mut item_updates);
    let item_updates = item_updates
        .into_iter()
        .map(|(kind, ts, diff)| StateUpdate {
            kind: kind.into(),
            ts,
            diff: diff.try_into().expect("valid diff"),
        })
        .collect();
    let mut ast_builtin_table_updates = state.apply_updates_for_bootstrap(item_updates).await;

    info!("migrating from catalog version {:?}", catalog_version);

    let conn_cat = state.for_system_session();

    rewrite_items(tx, &conn_cat, |_tx, _conn_cat, _id, _stmt| {
        let _catalog_version = catalog_version.clone();
        Box::pin(async move {
            // Add per-item, post-planning AST migrations below. Most
            // migrations should be in the above `rewrite_ast_items` block.
            //
            // Each migration should be a function that takes `item` (the AST
            // representing the creation SQL for the item) as input. Any
            // mutations to `item` will be staged for commit to the catalog.
            //
            // Be careful if you reference `conn_cat`. Doing so is *weird*,
            // as you'll be rewriting the catalog while looking at it. If
            // possible, make your migration independent of `conn_cat`, and only
            // consider a single item at a time.
            //
            // Migration functions may also take `tx` as input to stage
            // arbitrary changes to the catalog.
            Ok(())
        })
    })
    .await?;

    // Add whole-catalog migrations below.
    //
    // Each migration should be a function that takes `tx` and `conn_cat` as
    // input and stages arbitrary transformations to the catalog on `tx`.

    let op_item_updates = tx.get_and_commit_op_updates();
    let item_builtin_table_updates = state.apply_updates_for_bootstrap(op_item_updates).await;

    ast_builtin_table_updates.extend(item_builtin_table_updates);

    info!(
        "migration from catalog version {:?} complete",
        catalog_version
    );
    Ok(ast_builtin_table_updates)
}

// Add new migrations below their appropriate heading, and precede them with a
// short summary of the migration's purpose and optional additional commentary
// about safety or approach.
//
// The convention is to name the migration function using snake case:
// > <category>_<description>_<version>
//
// Please include the adapter team on any code reviews that add or edit
// migrations.

// Durable migrations

/// Migrations that run only on the durable catalog before any data is loaded into memory.
pub(crate) fn durable_migrate(
    tx: &mut Transaction,
    _boot_ts: Timestamp,
) -> Result<(), anyhow::Error> {
    // We're promoting mz_cluster_replica_frontiers from mz_internal to
    // mz_catalog in this release. As a special case, if a mapping for
    // mz_cluster_replica_frontiers exists in the mz_internal schema, rewrite it
    // in place for the mz_catalog schema instead. This keeps the migration
    // machinery from creating a new shard for the relation, which is important
    // because the 0dt machinery relies on this shard to determine when to cut
    // over to the new version.
    //
    // TODO(benesch): remove this for the next release (27 September 2024).
    let mut mappings = tx.get_system_object_mappings().collect::<Vec<_>>();
    if let Some(object) = mappings.iter_mut().find(|o| {
        o.description
            == SystemObjectDescription {
                schema_name: MZ_INTERNAL_SCHEMA.into(),
                object_name: MZ_CLUSTER_REPLICA_FRONTIERS.name.into(),
                object_type: mz_sql::catalog::CatalogItemType::Source,
            }
    }) {
        object.description.schema_name = MZ_CATALOG_SCHEMA.into();
    }
    tx.set_system_object_mappings(mappings)?;

    Ok(())
}

// Add new migrations below their appropriate heading, and precede them with a
// short summary of the migration's purpose and optional additional commentary
// about safety or approach.
//
// The convention is to name the migration function using snake case:
// > <category>_<description>_<version>
//
// Please include the adapter team on any code reviews that add or edit
// migrations.
