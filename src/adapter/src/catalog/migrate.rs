// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use futures::future::BoxFuture;
use maplit::btreeset;
use mz_catalog::durable::{Item, Transaction};
use mz_catalog::memory::objects::StateUpdate;
use mz_ore::collections::CollectionExt;
use mz_ore::now::{EpochMillis, NowFn};
use mz_repr::{GlobalId, Timestamp};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::visit_mut::VisitMut;
use mz_sql::ast::{CreateSubsourceOption, CreateSubsourceOptionName, UnresolvedItemName};
use mz_sql::catalog::SessionCatalog;
use mz_sql_parser::ast::{Raw, Statement};
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::sources::GenericSourceConnection;
use semver::Version;
use tracing::info;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::{CatalogState, ConnCatalog};

async fn rewrite_ast_items<F>(tx: &mut Transaction<'_>, mut f: F) -> Result<(), anyhow::Error>
where
    F: for<'a> FnMut(
        &'a mut Transaction<'_>,
        GlobalId,
        &'a mut Statement<Raw>,
    ) -> BoxFuture<'a, Result<(), anyhow::Error>>,
{
    let mut updated_items = BTreeMap::new();
    let items = tx.get_items();
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

pub(crate) async fn migrate(
    state: &CatalogState,
    tx: &mut Transaction<'_>,
    item_updates: Vec<StateUpdate>,
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

    rewrite_ast_items(tx, |_tx, _id, _stmt| {
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
            Ok(())
        })
    })
    .await?;

    // Load up a temporary catalog.
    let mut state = state.clone();
    // The catalog is temporary, so we can throw out the builtin updates.
    let _ = state.apply_updates_for_bootstrap(item_updates).await;

    info!("migrating from catalog version {:?}", catalog_version);

    let conn_cat = state.for_system_session();

    rewrite_items(tx, &conn_cat, |_tx, conn_cat, _id, stmt| {
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
            ast_rewrite_create_source_pg_database_details(conn_cat, stmt)?;
            Ok(())
        })
    })
    .await?;

    // Add whole-catalog migrations below.
    //
    // Each migration should be a function that takes `tx` and `conn_cat` as
    // input and stages arbitrary transformations to the catalog on `tx`.
    mysql_subsources_remove_unnecessary_db_name(tx, &conn_cat)?;
    fix_dependency_order_0_99_2(tx, &conn_cat, now)?;

    info!(
        "migration from catalog version {:?} complete",
        catalog_version
    );
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
) -> Result<(), anyhow::Error> {
    use itertools::Itertools;
    use mz_audit_log::{FromPreviousIdV1, ToNewIdV1};
    use mz_ore::cast::CastFrom;
    use mz_sql::names::CommentObjectId;
    use mz_sql_parser::ast::RawItemName;
    use mz_storage_client::controller::StorageTxn;

    // !WARNING!
    //
    // double-check that using this fn doesn't interfere with any other
    // migrations

    // Convert the IDs we need into a set with constant-time lookup.
    let news_new_id_set: BTreeSet<_> = needs_new_id
        .iter()
        .map(|id| {
            assert!(id.is_user(), "cannot assign new ID to non-user ID {:?}", id);
            id
        })
        .cloned()
        .collect();

    // Create a catalog of the items we're updating.
    let mut update_items: BTreeMap<_, _> = tx
        .get_items()
        .into_iter()
        .filter_map(|item| match news_new_id_set.contains(&item.id) {
            true => Some((item.id, item)),
            false => None,
        })
        .collect();

    // Allocate the new IDs.
    let new_ids = tx.allocate_user_item_ids(u64::cast_from(needs_new_id.len()))?;

    // Before updating the item by inserting its new state, ensure we remove its
    // old state.
    tx.remove_items(&news_new_id_set)?;

    // Delete the storage metadata alongside removing the item––this will return
    // the metadata for the deleted entries, which we'll re-associate with the
    // new IDs.
    let mut deleted_metadata: BTreeMap<_, _> = tx
        .delete_collection_metadata(news_new_id_set)
        .into_iter()
        .collect();

    // Collect all storage metadata we need to update.
    let mut updated_storage_collection_metadata = BTreeMap::new();
    // Track old and new IDs
    let mut new_id_mapping = BTreeMap::new();

    let occurred_at = now();

    // We know that we've allocated as many new IDs as we have items that need
    // new IDs, so `zip_eq` is appropriate.
    for (old_id, new_id) in needs_new_id.into_iter().zip_eq(new_ids.into_iter()) {
        let item = update_items.remove(&old_id).expect("known to be an entry");

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

    // Provisionally save new storage metadata.
    tx.insert_collection_metadata(updated_storage_collection_metadata)?;

    // Check for comments which will need to be updated.
    for (old_id, new_id) in new_id_mapping.iter() {
        if conn_catalog.get_item_comments(old_id).is_some() {
            tracing::info!("reassigning {}'s comments to {}", old_id, new_id);

            let mut comment_id = conn_catalog
                .state()
                .get_comment_id(mz_sql::names::ObjectId::Item(*old_id));
            let curr_id = comment_id.clone();
            match &mut comment_id {
                CommentObjectId::Table(id)
                | CommentObjectId::View(id)
                | CommentObjectId::MaterializedView(id)
                | CommentObjectId::Source(id)
                | CommentObjectId::Sink(id)
                | CommentObjectId::Index(id)
                | CommentObjectId::Func(id)
                | CommentObjectId::Connection(id)
                | CommentObjectId::Type(id)
                | CommentObjectId::Secret(id) => *id = *new_id,
                // These comments do not use `GlobalId`s.
                id @ (CommentObjectId::Role(_)
                | CommentObjectId::Database(_)
                | CommentObjectId::Schema(_)
                | CommentObjectId::Cluster(_)
                | CommentObjectId::ClusterReplica(_)) => {
                    anyhow::bail!("unexpected comment ID {:?}", id)
                }
            };

            let comments = tx.drop_comments(&btreeset! { curr_id })?;
            for (_id, subcomponent, comment) in comments {
                tx.update_comment(comment_id, subcomponent, Some(comment))?;
            }
        }
    }

    /// Struct to update any referenced IDs.
    struct IdUpdater<'a> {
        new_id_mapping: &'a BTreeMap<GlobalId, GlobalId>,
        err: Result<(), anyhow::Error>,
    }

    let mut id_updater = IdUpdater {
        new_id_mapping: &new_id_mapping,
        err: Ok(()),
    };

    impl<'a> VisitMut<'_, Raw> for IdUpdater<'a> {
        fn visit_item_name_mut(&mut self, node: &'_ mut <Raw as mz_sql::ast::AstInfo>::ItemName) {
            if let RawItemName::Id(id, _) = node {
                match GlobalId::from_str(id.as_str()) {
                    Ok(curr_id) => {
                        if let Some(new_id) = self.new_id_mapping.get(&curr_id) {
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

    // Collect all items whose serialized AST strings have changed.
    let mut updated_items = BTreeMap::new();
    for item in tx.get_items() {
        let mut stmt = mz_sql_parser::parser::parse_statements(&item.create_sql)
            .expect("parsing persisted create_sql must succeed")
            .into_element()
            .ast;

        let original_redacted_sql = stmt.to_ast_string_redacted();

        id_updater.visit_statement_mut(&mut stmt);

        if id_updater.err.is_err() {
            return id_updater.err;
        }

        let new_ast_string = stmt.to_ast_string_stable();
        if item.create_sql != new_ast_string {
            tracing::info!(
                "{}'s `create_sql` string changed because of updated GlobalId\nwas: {}\n\nnow: {}",
                item.id,
                original_redacted_sql,
                stmt.to_ast_string_redacted()
            );
            updated_items.insert(
                item.id,
                Item {
                    id: item.id,
                    oid: item.oid,
                    schema_id: item.schema_id,
                    name: item.name,
                    create_sql: new_ast_string,
                    owner_id: item.owner_id,
                    privileges: item.privileges,
                },
            );
        }
    }
    tx.update_items(updated_items)?;

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

fn ast_rewrite_create_source_pg_database_details(
    cat: &ConnCatalog<'_>,
    stmt: &mut Statement<Raw>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::{
        CreateSourceConnection, CreateSourceStatement, PgConfigOptionName, RawItemName, Value,
        WithOptionValue,
    };
    use mz_storage_types::sources::postgres::ProtoPostgresSourcePublicationDetails;
    use prost::Message;

    struct Rewriter<'a> {
        cat: &'a ConnCatalog<'a>,
    }

    impl<'ast> VisitMut<'ast, Raw> for Rewriter<'_> {
        fn visit_create_source_statement_mut(
            &mut self,
            node: &'ast mut CreateSourceStatement<Raw>,
        ) {
            match &mut node.connection {
                CreateSourceConnection::Postgres {
                    connection,
                    options,
                } => {
                    let details = options
                        .iter_mut()
                        .find(|o| o.name == PgConfigOptionName::Details)
                        .expect("PG sources must have details");

                    let details_val = match &mut details.value {
                        Some(WithOptionValue::Value(Value::String(details))) => details,
                        _ => unreachable!("PG source details' value must be a string"),
                    };

                    let details = hex::decode(details_val.clone())
                        .expect("PG source details must be a hex-encoded string");
                    let mut details = ProtoPostgresSourcePublicationDetails::decode(&*details)
                        .expect("PG source details must be a hex-encoded protobuf");

                    let conn = match connection {
                        RawItemName::Name(connection) => {
                            let connection =
                                mz_sql::normalize::unresolved_item_name(connection.clone())
                                    .expect("PG source connection name must be valid");
                            self.cat
                                .resolve_item(&connection)
                                .expect("PG source connection must exist")
                        }
                        RawItemName::Id(id, _) => {
                            let gid = id
                                .parse()
                                .expect("RawItenName::Id must be uncorrupted GlobalId");
                            self.cat.state().get_entry(&gid)
                        }
                    };

                    let conn = conn
                        .connection()
                        .expect("PG source connection must reference a connection");

                    match &conn {
                        mz_storage_types::connections::Connection::Postgres(pg) => {
                            // Store the connection's database in the details.
                            details.database.clone_from(&pg.database);
                        }
                        _ => unreachable!("PG sources must use PG connections"),
                    };

                    *details_val = hex::encode(details.encode_to_vec());
                }
                _ => {}
            }
        }
    }

    Rewriter { cat }.visit_statement_mut(stmt);

    Ok(())
}

fn mysql_subsources_remove_unnecessary_db_name(
    tx: &mut Transaction<'_>,
    conn_catalog: &ConnCatalog,
) -> Result<(), anyhow::Error> {
    let items = conn_catalog.get_items();
    let mysql_subsources_to_update: Vec<_> = items
        .into_iter()
        .filter_map(|item| match item.subsource_details() {
            Some((ingestion_id, external_reference)) => {
                match conn_catalog
                    .state()
                    .get_entry(&ingestion_id)
                    .source_desc()
                    .expect("exports refer to ingestions")
                    .expect("exports refer to ingestions")
                    .connection
                {
                    GenericSourceConnection::MySql(_) => {
                        let mut updated_reference = external_reference.0.clone();
                        if updated_reference.len() == 3 {
                            let fake_database_name = updated_reference.remove(0);
                            if fake_database_name.as_str() != "mysql" {
                                return Some(Err(anyhow::anyhow!("MySQL subsource's left-most qualification should have been 'mysql' but was {}", fake_database_name)));
                            }

                            Some(Ok((item, updated_reference)))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            None => None,
        }).collect::<Result<Vec<_>, _>>()?;

    let mut updated_items = BTreeMap::new();
    for (item, updated_reference) in mysql_subsources_to_update {
        let mut subsource_stmt = mz_sql_parser::parser::parse_statements(item.create_sql())
            .expect("parsing persisted create_sql must succeed")
            .into_element()
            .ast;

        match &mut subsource_stmt {
            Statement::CreateSubsource(create_subsource_stmt) => {
                create_subsource_stmt
                    .with_options
                    .retain(|o| o.name != CreateSubsourceOptionName::ExternalReference);

                create_subsource_stmt
                    .with_options
                    .push(CreateSubsourceOption {
                        name: CreateSubsourceOptionName::ExternalReference,
                        value: Some(mz_sql::ast::WithOptionValue::UnresolvedItemName(
                            UnresolvedItemName(updated_reference),
                        )),
                    });
            }
            _ => unreachable!("must be subsource"),
        }

        updated_items.insert(item.id(), subsource_stmt.to_ast_string_stable());
    }

    let updated_items: BTreeMap<_, _> = tx
        .get_items()
        .filter_map(|item| {
            updated_items.remove(&item.id).map(|new_create_sql| {
                (
                    item.id,
                    Item {
                        id: item.id,
                        oid: item.oid,
                        schema_id: item.schema_id,
                        name: item.name,
                        create_sql: new_create_sql,
                        owner_id: item.owner_id,
                        privileges: item.privileges,
                    },
                )
            })
        })
        .collect();

    tx.update_items(updated_items)?;

    Ok(())
}

/// In v0.98.x, we inverted the dependency order of sources + subsources and
/// migrated those items, as well as their dependencies, to new IDs.
/// Unfortunately, because of a bug, this did not place items in the correct
/// order and it was possible that some items depend on items with IDs less than
/// its own.
///
/// This PR goes back and fixes any IDs with that broken relationship.
fn fix_dependency_order_0_99_2(
    tx: &mut Transaction<'_>,
    conn_catalog: &ConnCatalog,
    now: NowFn,
) -> Result<(), anyhow::Error> {
    // A vector in the _new_ dependency order. Because the `vec` doesn't have
    // any built-in de-duplication, we will need to deduplicate these values
    // elsewhere.
    let mut needs_new_id = vec![];

    for item in conn_catalog
        .get_items()
        .into_iter()
        // Only consider user items
        .filter(|item| item.id().is_user())
    {
        for dependent_id in item.used_by() {
            // If an item has a dependent with an ID less than its own, that
            // dependent needs an ID that will be pushed forward ahead of this
            // ID.
            if *dependent_id < item.id() {
                needs_new_id.push(*dependent_id);
            }
        }
    }

    let mut remaining_updates = std::collections::VecDeque::from_iter(needs_new_id.drain(..));

    let state = conn_catalog.state();

    // If this item's ID must be moved forward, so too must every item that
    // depends on it except for its primary source ID.
    while let Some(id) = remaining_updates.pop_front() {
        needs_new_id.push(id);
        // If we push this item forward, we must all push forward all IDs that
        // refer to this ID (i.e. we need to ensure that all of an item's
        // dependents have IDs greater than its own).
        remaining_updates.extend(state.get_entry(&id).used_by().iter().cloned());
    }

    // Ensure that each ID is present only once and that it is in the greatest
    // position.
    let mut id_dedup = BTreeSet::new();
    needs_new_id = needs_new_id
        .into_iter()
        .rev()
        .filter(|id| id_dedup.insert(*id))
        .collect();

    // Flip this back around in the right order.
    needs_new_id = needs_new_id.into_iter().rev().collect();

    assign_new_user_global_ids(tx, conn_catalog, now, needs_new_id)?;

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
    catalog_rename_mz_introspection_cluster_v_0_103_0(tx, boot_ts)?;
    catalog_add_new_unstable_schemas_v_0_106_0(tx)?;
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

/// This migration applies the rename of the built-in `mz_introspection` cluster to
/// `mz_catalog_server` to the durable catalog state.
fn catalog_rename_mz_introspection_cluster_v_0_103_0(
    tx: &mut Transaction,
    boot_ts: EpochMillis,
) -> Result<(), anyhow::Error> {
    use mz_audit_log::{EventDetails, EventType, ObjectType, RenameClusterV1, VersionedEvent};

    let found_cluster_id = tx
        .get_clusters()
        .find(|cluster| cluster.id.is_system() && cluster.name == "mz_introspection")
        .map(|cluster| cluster.id);
    let Some(cluster_id) = found_cluster_id else {
        return Ok(());
    };

    tx.rename_cluster(cluster_id, "mz_introspection", "mz_catalog_server")?;

    // Update audit log.
    let audit_id = tx.allocate_audit_log_id()?;
    let rename_event = VersionedEvent::new(
        audit_id,
        EventType::Alter,
        ObjectType::Cluster,
        EventDetails::RenameClusterV1(RenameClusterV1 {
            id: cluster_id.to_string(),
            old_name: "mz_introspection".into(),
            new_name: "mz_catalog_server".into(),
        }),
        None,
        boot_ts,
    );
    tx.insert_audit_log_event(rename_event);

    Ok(())
}

/// This migration applies the rename of the built-in `mz_introspection` cluster to
/// `mz_catalog_server` to the durable catalog state.
fn catalog_add_new_unstable_schemas_v_0_106_0(tx: &mut Transaction) -> Result<(), anyhow::Error> {
    use mz_catalog::durable::initialize::{
        MZ_CATALOG_UNSTABLE_SCHEMA_ID, MZ_INTROSPECTION_SCHEMA_ID,
    };
    use mz_pgrepr::oid::{SCHEMA_MZ_CATALOG_UNSTABLE_OID, SCHEMA_MZ_INTROSPECTION_OID};
    use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
    use mz_repr::namespaces::{MZ_CATALOG_UNSTABLE_SCHEMA, MZ_INTROSPECTION_SCHEMA};
    use mz_sql::names::SchemaId;
    use mz_sql::rbac;
    use mz_sql::session::user::{MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID};

    let schema_ids: BTreeSet<_> = tx
        .get_schemas()
        .filter_map(|schema| match schema.id {
            SchemaId::User(_) => None,
            SchemaId::System(id) => Some(id),
        })
        .collect();
    let schema_privileges = vec![
        rbac::default_builtin_object_privilege(mz_sql::catalog::ObjectType::Schema),
        MzAclItem {
            grantee: MZ_SUPPORT_ROLE_ID,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE,
        },
        rbac::owner_privilege(mz_sql::catalog::ObjectType::Schema, MZ_SYSTEM_ROLE_ID),
    ];

    if !schema_ids.contains(&MZ_CATALOG_UNSTABLE_SCHEMA_ID) {
        tx.insert_system_schema(
            MZ_CATALOG_UNSTABLE_SCHEMA_ID,
            MZ_CATALOG_UNSTABLE_SCHEMA,
            MZ_SYSTEM_ROLE_ID,
            schema_privileges.clone(),
            SCHEMA_MZ_CATALOG_UNSTABLE_OID,
        )?;
    }
    if !schema_ids.contains(&MZ_INTROSPECTION_SCHEMA_ID) {
        tx.insert_system_schema(
            MZ_INTROSPECTION_SCHEMA_ID,
            MZ_INTROSPECTION_SCHEMA,
            MZ_SYSTEM_ROLE_ID,
            schema_privileges.clone(),
            SCHEMA_MZ_INTROSPECTION_OID,
        )?;
    }

    Ok(())
}
