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
use mz_audit_log::{
    CreateClusterReplicaV1, DropClusterReplicaV1, EventDetails, EventType, ObjectType,
    VersionedEvent,
};
use mz_catalog::durable::{ReplicaLocation, Transaction};
use mz_catalog::memory::objects::{StateUpdate, StateUpdateKind};
use mz_ore::collections::CollectionExt;
use mz_ore::now::{EpochMillis, NowFn};
use mz_repr::{GlobalId, Timestamp};
use mz_sql::ast::display::AstDisplay;
use mz_sql_parser::ast::{Raw, Statement};
use mz_storage_types::connections::ConnectionContext;
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
    _now: NowFn,
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
    let mut state = state.clone();
    let item_updates = tx
        .get_items()
        .map(|item| StateUpdate {
            kind: StateUpdateKind::Item(item),
            diff: 1,
        })
        .collect();
    state.apply_updates_for_bootstrap(item_updates)?;

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
// Please include the adapter team on any code reviews that add or edit
// migrations.

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

fn ast_rewrite_create_source_loadgen_options_0_92_0(
    stmt: &mut Statement<Raw>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::visit_mut::VisitMut;
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

fn ast_rewrite_create_source_pg_database_details(
    cat: &ConnCatalog<'_>,
    stmt: &mut Statement<Raw>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::visit_mut::VisitMut;
    use mz_sql::ast::{
        CreateSourceConnection, CreateSourceStatement, PgConfigOptionName, RawItemName, Value,
        WithOptionValue,
    };
    use mz_sql::catalog::SessionCatalog;
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
                            details.database = pg.database.clone();
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
