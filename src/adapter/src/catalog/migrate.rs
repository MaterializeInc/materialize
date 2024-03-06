// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use futures::future::BoxFuture;
use mz_catalog::durable::Transaction;
use mz_ore::collections::CollectionExt;
use mz_ore::now::{EpochMillis, NowFn};
use mz_repr::GlobalId;
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{CreateSubsourceOption, CreateSubsourceOptionName, Ident};
use mz_sql::catalog::SessionCatalog;
use mz_sql_parser::ast::{Raw, Statement};
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::sources::GenericSourceConnection;
use semver::Version;
use tracing::info;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::{Catalog, CatalogState, ConnCatalog};

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
    let items = tx.loaded_items();
    for mut item in items {
        let mut stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;

        f(tx, &cat, item.id, &mut stmt).await?;

        item.create_sql = stmt.to_ast_string_stable();

        updated_items.insert(item.id, item);
    }
    tx.update_items(updated_items)?;
    Ok(())
}

pub(crate) async fn migrate(
    state: &CatalogState,
    tx: &mut Transaction<'_>,
    _now: NowFn,
    _connection_context: &ConnectionContext,
) -> Result<(), anyhow::Error> {
    let catalog_version = tx.get_catalog_content_version();
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };

    info!("migrating from catalog version {:?}", catalog_version);

    // Load up a temporary catalog.
    let state = Catalog::load_catalog_items(tx, state)?;

    // Perform per-item AST migrations.
    let conn_cat = state.for_system_session();
    rewrite_items(tx, &conn_cat, |_tx, _conn_cat, _id, _stmt| {
        let _catalog_version = catalog_version.clone();
        Box::pin(async move {
            // Add per-item AST migrations below.
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
    catalog_clean_up_stash_state_v_0_92_0(tx)?;
    let needs_new_ids = subsource_rewrite(tx, &conn_cat);
    assign_new_global_ids(tx, needs_new_ids)?;

    info!(
        "migration from catalog version {:?} complete",
        catalog_version
    );
    Ok(())
}

// Add the `col_num` to all PG column descriptions.
//
// TODO(migration): delete in version v.50 (released in v0.48 + 1 additional
// release)
fn subsource_rewrite(txn: &mut Transaction<'_>, conn_catalog: &ConnCatalog) -> BTreeSet<GlobalId> {
    use mz_sql::ast::UnresolvedItemName;
    use mz_sql::catalog::CatalogItemType;
    use mz_sql_parser::ast::RawItemName;
    use mz_storage_types::connections::Connection;

    let mut needs_new_id = BTreeSet::new();
    let mut source_ids_of_updated_subsources = BTreeSet::new();

    let mut source_map: BTreeMap<_, _> = txn
        .loaded_items()
        .into_iter()
        .filter(|item| conn_catalog.get_item(&item.id).item_type() == CatalogItemType::Source)
        .map(|item| (item.id, item))
        .collect();

    for source in conn_catalog
        .get_items()
        .iter()
        .filter(|item| item.item_type() == CatalogItemType::Source)
    {
        let mut exports = source.source_exports();
        exports.retain(|id, _| *id != source.id());
        if exports.is_empty() {
            continue;
        }

        let desc = source
            .source_desc()
            .expect("item is source with desc")
            .expect("item is source with desc");

        let tables: Vec<_> = match &desc.connection {
            GenericSourceConnection::Postgres(pg) => {
                let connection = conn_catalog.get_item(&pg.connection_id);
                let conn = connection
                    .connection()
                    .expect("generic source connection has connection details");
                let database = match conn {
                    Connection::Postgres(p) => p.database.clone(),
                    _ => unreachable!("PG sources must have PG connections"),
                };

                pg.publication_details
                    .tables
                    .iter()
                    .map(|t| {
                        UnresolvedItemName(vec![
                            Ident::new_unchecked(database.clone()),
                            Ident::new_unchecked(t.namespace.clone()),
                            Ident::new_unchecked(t.name.clone()),
                        ])
                    })
                    .collect()
            }
            GenericSourceConnection::MySql(mysql) => mysql
                .details
                .tables
                .iter()
                .map(|t| {
                    UnresolvedItemName(vec![
                        Ident::new_unchecked("mysql"),
                        Ident::new_unchecked(t.schema_name.clone()),
                        Ident::new_unchecked(t.name.clone()),
                    ])
                })
                .collect(),
            _ => unreachable!("only PG and MySQL sources have non-self source exports"),
        };

        let full_name = conn_catalog.resolve_full_name(source.name());
        let source_name = mz_sql::normalize::unresolve(full_name);

        for (id, output_idx) in exports {
            let item = conn_catalog.get_item(&id);
            let mut stmt = mz_sql_parser::parser::parse_statements(item.create_sql())
                .expect("parsing persisted create_sql must succeed")
                .into_element()
                .ast;

            match &mut stmt {
                Statement::CreateSubsource(create_subsource_stmt) => {
                    create_subsource_stmt.of_source = Some(RawItemName::Name(source_name.clone()));

                    let pos = create_subsource_stmt
                        .with_options
                        .iter()
                        .position(|o| o.name == CreateSubsourceOptionName::References)
                        .expect("subsources must be of references type");

                    create_subsource_stmt.with_options.swap_remove(pos);

                    create_subsource_stmt
                        .with_options
                        .push(CreateSubsourceOption {
                            name: CreateSubsourceOptionName::ExternalReference,
                            value: Some(mz_sql::ast::WithOptionValue::UnresolvedItemName(
                                // output indices are 1 greater than their table
                                // index to account for the idiom of using 0 for
                                // the primary source output.
                                tables[output_idx - 1].clone(),
                            )),
                        })
                }
                _ => unreachable!("subsource items must correlate to subsources"),
            }

            let mut item = source_map.remove(&id).expect("item exists");
            item.create_sql = stmt.to_ast_string_stable();
            txn.update_item(id, item).expect("txn must succeed");

            if id < source.id() {
                needs_new_id.insert(id);
                source_ids_of_updated_subsources.insert(source.id());
            }
        }

        // Update source definition to no longer include list of referenced
        // subsources.
        let id = source.id();
        let mut item = source_map.remove(&id).expect("source exists");
        let mut stmt = mz_sql_parser::parser::parse_statements(&item.create_sql)
            .expect("parsing persisted create_sql must succeed")
            .into_element()
            .ast;

        match &mut stmt {
            Statement::CreateSource(create_source_stmt) => {
                create_source_stmt.referenced_subsources = None;
            }
            _ => unreachable!("subsource items must correlate to subsources"),
        }

        item.create_sql = stmt.to_ast_string_stable();
        txn.update_item(id, item).expect("txn must succeed");
    }

    let mut remaining_updates = VecDeque::from_iter(needs_new_id.iter().cloned());
    // If this item's ID must be moved forward, so too must every item that
    // depends on it except for its primary source ID.
    while let Some(id) = remaining_updates.pop_front() {
        needs_new_id.insert(id);
        remaining_updates.extend(
            conn_catalog
                .state()
                .get_entry(&id)
                .used_by()
                .iter()
                .filter(|id| !source_ids_of_updated_subsources.contains(id))
                .cloned(),
        );
    }

    needs_new_id
}

/// Grant SELECT on all progress sources to the mz_support role.
fn assign_new_global_ids(
    txn: &mut Transaction<'_>,
    needs_new_id: BTreeSet<GlobalId>,
) -> Result<(), anyhow::Error> {
    use itertools::Itertools;
    use mz_ore::cast::CastFrom;
    use mz_storage_client::controller::StorageTxn;

    let update_items: Vec<_> = txn
        .loaded_items()
        .into_iter()
        .filter_map(|item| match needs_new_id.contains(&item.id) {
            true => Some(item),
            false => None,
        })
        .collect();

    let new_ids = txn.allocate_user_item_ids(u64::cast_from(update_items.len()))?;

    txn.remove_items(needs_new_id.clone())?;

    let mut deleted_metadata: BTreeMap<_, _> = txn
        .delete_storage_metadata(needs_new_id)
        .into_iter()
        .collect();

    let mut updated_storage_metadata = BTreeMap::new();

    for (item, new_id) in update_items.into_iter().zip_eq(new_ids.into_iter()) {
        tracing::info!("reassigning {} to {}", item.id, new_id);
        txn.insert_item(
            new_id,
            item.oid,
            item.schema_id,
            &item.name,
            item.create_sql,
            item.owner_id,
            item.privileges,
        )?;
        if let Some(metadata) = deleted_metadata.remove(&item.id) {
            tracing::info!(
                "reassigning {}'s storage metadata to {}: {}",
                item.id,
                new_id,
                metadata
            );
            updated_storage_metadata.insert(new_id, metadata);
        }
    }

    txn.insert_storage_metadata(updated_storage_metadata)?;

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

// Removes any stash specific state from the catalog.
fn catalog_clean_up_stash_state_v_0_92_0(tx: &mut Transaction) -> Result<(), anyhow::Error> {
    tx.clean_up_stash_catalog()?;
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
