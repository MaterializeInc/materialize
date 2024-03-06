// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::str::FromStr;

use futures::future::BoxFuture;
use mz_catalog::durable::Transaction;
use mz_ore::collections::CollectionExt;
use mz_ore::now::{EpochMillis, NowFn};
use mz_repr::{GlobalId, Timestamp};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::visit_mut::VisitMut;
use mz_sql::ast::{CreateSubsourceOption, CreateSubsourceOptionName, Ident};
use mz_sql::catalog::SessionCatalog;
use mz_sql_parser::ast::{Raw, Statement};
use mz_storage_types::connections::ConnectionContext;
use semver::Version;
use tracing::info;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::{Catalog, CatalogState, ConnCatalog};

async fn rewrite_ast_items<F>(tx: &mut Transaction<'_>, mut f: F) -> Result<(), anyhow::Error>
where
    F: for<'a> FnMut(
        &'a mut Transaction<'_>,
        GlobalId,
        &'a mut Statement<Raw>,
    ) -> BoxFuture<'a, Result<(), anyhow::Error>>,
{
    let mut updated_items = BTreeMap::new();
    let items = tx.loaded_items();
    for mut item in items {
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
    now: NowFn,
    _boot_ts: Timestamp,
    _connection_context: &ConnectionContext,
) -> Result<(), anyhow::Error> {
    let catalog_version = tx.get_catalog_content_version();
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };

    info!(
        "migrating statements from catalog version {:?}",
        catalog_version
    );

    rewrite_ast_items(tx, |_tx, _id, stmt| {
        let _catalog_version = catalog_version.clone();
        Box::pin(async move {
            // Add per-item AST migrations below.
            //
            // Each migration should be a function that takes `item` (the AST
            // representing the creation SQL for the item) as input. Any
            // mutations to `item` will be staged for commit to the catalog.
            //
            // Migration functions may also take `tx` as input to stage
            // arbitrary changes to the catalog.
            ast_rewrite_create_source_loadgen_options_0_92_0(stmt)?;
            Ok(())
        })
    })
    .await?;

    // Load up a temporary catalog.
    let state = Catalog::load_catalog_items(tx, state)?;

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
    subsource_rewrite_v0_96(tx, &conn_cat, now)?;

    info!(
        "migration from catalog version {:?} complete",
        catalog_version
    );
    Ok(())
}

/// Inverts the dependency structure of subsources.
///
/// In previous versions of MZ, a primary source depended on its subsources.
/// This migration inverts that relationship so that subsources depend on their
/// primary source.
///
/// This function also adjusts items' `GlobalId`s such that the dependency graph
/// can be inferred using the ordering of `GlobalId`s, i.e. all of an item's
/// dependencies have `GlobalId`s less than its own.
fn subsource_rewrite_v0_96(
    tx: &mut Transaction<'_>,
    conn_catalog: &ConnCatalog,
    now: NowFn,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::UnresolvedItemName;
    use mz_sql::catalog::CatalogItemType;
    use mz_sql_parser::ast::RawItemName;
    use mz_storage_types::connections::Connection;
    use mz_storage_types::sources::GenericSourceConnection;

    let mut needs_new_id = vec![];
    let mut source_ids_of_updated_subsources = BTreeSet::new();

    let mut source_map: BTreeMap<_, _> = tx
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
            tx.update_item(id, item).expect("txn must succeed");

            if id < source.id() {
                needs_new_id.push(id);
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
        tx.update_item(id, item).expect("txn must succeed");
    }

    let mut remaining_updates = VecDeque::from_iter(needs_new_id.iter().cloned());
    // If this item's ID must be moved forward, so too must every item that
    // depends on it except for its primary source ID.
    while let Some(id) = remaining_updates.pop_front() {
        needs_new_id.push(id);
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

    let id_map = assign_new_user_global_ids(tx, conn_catalog, now, needs_new_id)?;

    struct IdUpdater {
        id_map: BTreeMap<GlobalId, GlobalId>,
        err: Result<(), anyhow::Error>,
    }

    let mut id_updater = IdUpdater {
        id_map,
        err: Ok(()),
    };

    impl VisitMut<'_, Raw> for IdUpdater {
        fn visit_item_name_mut(&mut self, node: &'_ mut <Raw as mz_sql::ast::AstInfo>::ItemName) {
            if let RawItemName::Id(id, _) = node {
                match GlobalId::from_str(id.as_str()) {
                    Ok(curr_id) => {
                        if let Some(new_id) = self.id_map.get(&curr_id) {
                            *id = new_id.to_string();
                        }
                    }
                    Err(e) => {
                        if self.err.is_ok() {
                            self.err = Err(e);
                        }
                    }
                }
            }
        }
    }

    for item in tx.loaded_items().into_iter() {
        let mut stmt = mz_sql_parser::parser::parse_statements(&item.create_sql)
            .expect("parsing persisted create_sql must succeed")
            .into_element()
            .ast;

        id_updater.visit_statement_mut(&mut stmt);
        if id_updater.err.is_err() {
            return id_updater.err;
        }

        let new_ast_string = stmt.to_ast_string_stable();
        if item.create_sql != new_ast_string {
            tx.remove_items(BTreeSet::from_iter([item.id]))?;
            tx.insert_item(
                item.id,
                item.oid,
                item.schema_id,
                &item.name,
                new_ast_string,
                item.owner_id,
                item.privileges,
            )?;
        }
    }

    Ok(())
}

/// Assigns new `GlobalId`s to the items in `needs_new_id`.
///
/// The IDs will be assigned in relative order to `needs_new_id`. For example,
/// the first element of `needs_new_id` will have the smallest ID and the last
/// element will have the greatest.
///
/// # Notes
/// This function:
/// - Does not analyze dependencies. Callers must provide all items whose
///   `GlobalId`s they wish to reassign to `needs_new_id`.
/// - Assumes all items in `needs_new_id` are present in `conn_catalog` and that
///   their types do not change.
fn assign_new_user_global_ids(
    tx: &mut Transaction<'_>,
    conn_catalog: &ConnCatalog,
    now: NowFn,
    needs_new_id: Vec<GlobalId>,
) -> Result<BTreeMap<GlobalId, GlobalId>, anyhow::Error> {
    use itertools::Itertools;
    use mz_audit_log::{FromPreviousIdV1, ToNewIdV1};
    use mz_ore::cast::CastFrom;
    use mz_storage_client::controller::StorageTxn;

    let update_items: Vec<_> = tx
        .loaded_items()
        .into_iter()
        .filter_map(|item| match needs_new_id.contains(&item.id) {
            true => {
                assert!(item.id.is_user());
                Some(item)
            }
            false => None,
        })
        .collect();

    let new_ids = tx.allocate_user_item_ids(u64::cast_from(update_items.len()))?;

    tx.remove_items(needs_new_id.iter().cloned().collect())?;

    let mut deleted_metadata: BTreeMap<_, _> = tx
        .delete_collection_metadata(needs_new_id.iter().cloned().collect())
        .into_iter()
        .collect();

    let mut updated_storage_collection_metadata = BTreeMap::new();

    let occurred_at = now();

    let mut new_id_mapping = BTreeMap::new();

    for (item, new_id) in update_items.into_iter().zip_eq(new_ids.into_iter()) {
        new_id_mapping.insert(item.id, new_id);

        let entry = conn_catalog.get_item(&item.id);

        tracing::info!("reassigning {} to {}", item.id, new_id);
        tx.insert_item(
            new_id,
            item.oid,
            item.schema_id,
            &item.name,
            item.create_sql,
            item.owner_id,
            item.privileges,
        )?;

        let object_type = entry.item_type().into();

        add_to_audit_log(
            tx,
            mz_audit_log::EventType::Create,
            object_type,
            mz_audit_log::EventDetails::FromPreviousIdV1(FromPreviousIdV1 {
                previous_id: item.id.to_string(),
                id: new_id.to_string(),
            }),
            occurred_at,
        )?;

        add_to_audit_log(
            tx,
            mz_audit_log::EventType::Drop,
            object_type,
            mz_audit_log::EventDetails::ToNewIdV1(ToNewIdV1 {
                id: item.id.to_string(),
                new_id: new_id.to_string(),
            }),
            occurred_at,
        )?;

        if let Some(metadata) = deleted_metadata.remove(&item.id) {
            tracing::info!(
                "reassigning {}'s storage metadata to {}: {}",
                item.id,
                new_id,
                metadata
            );
            updated_storage_collection_metadata.insert(new_id, metadata);
        }
    }

    tx.insert_collection_metadata(updated_storage_collection_metadata)?;

    Ok(new_id_mapping)
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

fn add_to_audit_log(
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

fn ast_rewrite_create_source_loadgen_options_0_92_0(
    stmt: &mut Statement<Raw>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::{
        CreateSourceConnection, CreateSourceStatement, LoadGenerator, LoadGeneratorOptionName::*,
    };

    struct Rewriter;

    impl<'ast> VisitMut<'ast, Raw> for Rewriter {
        fn visit_create_source_statement_mut(
            &mut self,
            node: &'ast mut CreateSourceStatement<Raw>,
        ) {
            match &mut node.connection {
                CreateSourceConnection::LoadGenerator { generator, options } => {
                    let permitted_options: &[_] = match generator {
                        LoadGenerator::Auction => &[TickInterval],
                        LoadGenerator::Counter => &[TickInterval, MaxCardinality],
                        LoadGenerator::Marketing => &[TickInterval],
                        LoadGenerator::Datums => &[TickInterval],
                        LoadGenerator::Tpch => &[TickInterval, ScaleFactor],
                        LoadGenerator::KeyValue => &[
                            TickInterval,
                            Keys,
                            SnapshotRounds,
                            TransactionalSnapshot,
                            ValueSize,
                            Seed,
                            Partitions,
                            BatchSize,
                        ],
                    };

                    options.retain(|o| permitted_options.contains(&o.name));
                }
                _ => {}
            }
        }
    }

    Rewriter.visit_statement_mut(stmt);

    Ok(())
}

// Durable migrations

/// Migrations that run only on the durable catalog before any data is loaded into memory.
pub(crate) fn durable_migrate(
    tx: &mut Transaction,
    boot_ts: Timestamp,
) -> Result<(), anyhow::Error> {
    let boot_ts = boot_ts.into();
    catalog_fix_system_cluster_replica_ids_v_0_95_0(tx, boot_ts)?;
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

fn catalog_fix_system_cluster_replica_ids_v_0_95_0(
    tx: &mut Transaction,
    boot_ts: EpochMillis,
) -> Result<(), anyhow::Error> {
    use mz_audit_log::{
        CreateClusterReplicaV1, DropClusterReplicaV1, EventDetails, EventType, ObjectType,
        VersionedEvent,
    };
    use mz_catalog::durable::ReplicaLocation;

    let updated_replicas: Vec<_> = tx
        .get_cluster_replicas()
        .filter(|replica| replica.cluster_id.is_system() && replica.replica_id.is_user())
        .map(|replica| (replica.replica_id, replica))
        .collect();
    for (replica_id, mut updated_replica) in updated_replicas {
        let sys_id = tx.allocate_system_replica_id()?;
        updated_replica.replica_id = sys_id;
        tx.remove_cluster_replica(replica_id)?;
        tx.insert_cluster_replica(
            updated_replica.cluster_id,
            updated_replica.replica_id,
            &updated_replica.name,
            updated_replica.config.clone(),
            updated_replica.owner_id,
        )?;

        // Update audit log.
        if let ReplicaLocation::Managed {
            size,
            disk,
            billed_as,
            internal,
            ..
        } = &updated_replica.config.location
        {
            let cluster = tx
                .get_clusters()
                .filter(|cluster| cluster.id == updated_replica.cluster_id)
                .next()
                .expect("missing cluster");
            let drop_audit_id = tx.allocate_audit_log_id()?;
            let remove_event = VersionedEvent::new(
                drop_audit_id,
                EventType::Drop,
                ObjectType::ClusterReplica,
                EventDetails::DropClusterReplicaV1(DropClusterReplicaV1 {
                    cluster_id: updated_replica.cluster_id.to_string(),
                    cluster_name: cluster.name.clone(),
                    replica_id: Some(replica_id.to_string()),
                    replica_name: updated_replica.name.clone(),
                }),
                None,
                boot_ts,
            );
            let create_audit_id = tx.allocate_audit_log_id()?;
            let create_event = VersionedEvent::new(
                create_audit_id,
                EventType::Create,
                ObjectType::ClusterReplica,
                EventDetails::CreateClusterReplicaV1(CreateClusterReplicaV1 {
                    cluster_id: updated_replica.cluster_id.to_string(),
                    cluster_name: cluster.name.clone(),
                    replica_id: Some(updated_replica.replica_id.to_string()),
                    replica_name: updated_replica.name,
                    logical_size: size.clone(),
                    disk: *disk,
                    billed_as: billed_as.clone(),
                    internal: *internal,
                }),
                None,
                boot_ts,
            );
            tx.insert_audit_log_events([remove_event, create_event]);
        }
    }
    Ok(())
}
