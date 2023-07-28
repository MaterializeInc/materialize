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
use mz_sql::ast::{Raw, Statement, Value};
use mz_sql::catalog::{CatalogCluster, CatalogClusterReplica, CatalogItem, CatalogItemType};
use mz_sql::names::ObjectId;
use mz_storage_client::types::connections::ConnectionContext;
use semver::Version;
use tracing::info;

use crate::catalog::storage::Transaction;
use crate::catalog::{storage, Catalog, ConnCatalog, SerializedCatalogItem};

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
    connection_context: Option<ConnectionContext>,
) -> Result<(), anyhow::Error> {
    let mut storage = catalog.storage().await;
    let catalog_version = storage.get_catalog_content_version().await?;
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };

    info!("migrating from catalog version {:?}", catalog_version);

    let now = (catalog.config().now)();
    let mut tx = storage.transaction().await?;
    // First, do basic AST -> AST transformations.
    // rewrite_items(&mut tx, None, |_tx, _cat, _stmt| Box::pin(async { Ok(()) })).await?;

    // Then, load up a temporary catalog with the rewritten items, and perform
    // some transformations that require introspecting the catalog. These
    // migrations are *weird*: they're rewriting the catalog while looking at
    // it. You probably should be adding a basic AST migration above, unless
    // you are really certain you want one of these crazy migrations.
    let cat = Catalog::load_catalog_items(&mut tx, catalog)?;
    let conn_cat = cat.for_system_session();
    rewrite_items(&mut tx, Some(&conn_cat), |_tx, cat, item| {
        let connection_context = connection_context.clone();
        Box::pin(async move {
            let conn_cat = cat.expect("must provide access to conn catalog");
            if let Some(conn_cx) = connection_context {
                pg_source_table_metadata_rewrite(conn_cat, &conn_cx, item).await;
            }
            Ok(())
        })
    })
    .await?;

    sync_cluster_replica_owners(&cat, &mut tx, now)?;
    sync_source_owners(&cat, &mut tx, now)?;

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

// ****************************************************************************
// Semantic migrations -- Weird migrations that require access to the catalog
// ****************************************************************************

// Add the `col_num` to all PG column descriptions.
//
// Note that this will also populate column and table constraints from the
// upstream database that were never used, so we must clear them out because
// they were not used to generate the DDL for the subsources and it is now too
// late to apply them.
//
// TODO(migration): delete in version v.52 (released in v0.50 + 1 additional
// release)
async fn pg_source_table_metadata_rewrite(
    catalog: &ConnCatalog<'_>,
    connection_context: &ConnectionContext,
    stmt: &mut mz_sql::ast::Statement<Raw>,
) {
    use mz_proto::RustType;
    use mz_sql::ast::{CreateSourceConnection, PgConfigOption, PgConfigOptionName};
    use mz_sql::plan::StatementContext;
    use mz_storage_client::types::sources::{
        PostgresSourcePublicationDetails, ProtoPostgresSourcePublicationDetails,
    };
    use prost::Message;
    use tracing::warn;

    if let Statement::CreateSource(mz_sql::ast::CreateSourceStatement {
        name,
        connection:
            CreateSourceConnection::Postgres {
                connection,
                options,
            },
        ..
    }) = stmt
    {
        // Find the option containing the serialized details.
        let details_idx = options
            .iter()
            .position(|PgConfigOption { name, .. }| name == &PgConfigOptionName::Details)
            .expect("corrupt catalog");

        // Examine the current details
        let details = match &options[details_idx].value {
            Some(mz_sql::ast::WithOptionValue::Value(mz_sql::ast::Value::String(details))) => {
                details
            }
            _ => unreachable!("corrupt catalog"),
        };

        let details = hex::decode(details).expect("valid catalog");
        let details =
            ProtoPostgresSourcePublicationDetails::decode(&*details).expect("valid catalog");
        let mut publication_details =
            PostgresSourcePublicationDetails::from_proto(details).expect("valid catalog");

        if publication_details
            .tables
            .iter()
            .all(|t| t.columns.iter().all(|c| c.col_num.is_some()))
        {
            mz_ore::soft_assert!(
                publication_details
                    .tables
                    .iter()
                    .all(|t| t.columns.iter().all(|c| c.col_num != Some(0))),
                "PG does not use attnum 0"
            );
            // If every column is present, then no need for this migration.
            return;
        }

        // Get details to connect to the upstream PG instance, which we need to
        // get the schema details.
        let scx = StatementContext::new(None, &*catalog);
        let connection = {
            let item = scx.resolve_item(connection.clone()).expect("valid catalog");
            match item.connection().expect("valid catalog") {
                mz_storage_client::types::connections::Connection::Postgres(connection) => {
                    connection
                }
                _ => unreachable!("corrupt catalog"),
            }
        };

        let publication = options
            .iter()
            .find(|o| matches!(o.name, PgConfigOptionName::Publication))
            .expect("valid catalog")
            .value
            .as_ref()
            .expect("valid catalog");

        let publication = match publication {
            mz_sql::ast::WithOptionValue::Value(mz_sql::ast::Value::String(publication)) => {
                &*publication
            }
            mz_sql::ast::WithOptionValue::Ident(ident) => ident.as_str(),
            _ => unreachable!("corrupt catalog"),
        };

        // verify that we can connect upstream and snapshot publication metadata
        let config = connection
            .config(&*connection_context.secrets_reader)
            .await
            .expect("valid config");

        // Get the current publication tables from the upstream PG source.
        let mut current_publication_tables =
            match mz_postgres_util::publication_info(&config, publication, None).await {
                Ok(v) => v,
                Err(_) => {
                    warn!(
                        "could not perform migration of PG source {name} due \
                    to external dependency; this will render the source useless, \
                    but might be fixable by restarting Materialize"
                    );
                    return;
                }
            };

        // Convert current tables into map because we only care that the tables
        // we know about about are the same.
        let mut cur_tables: BTreeMap<_, _> = current_publication_tables
            .iter_mut()
            .map(|t| (t.oid, t))
            .collect();

        for prev_table in publication_details.tables.into_iter() {
            match cur_tables.get_mut(&prev_table.oid) {
                Some(cur_table) => {
                    // We must undergo this torturous equality check because we
                    // want to check equality for all fields except for the new
                    // col_num field.
                    if prev_table.namespace != cur_table.namespace
                        || prev_table.name != cur_table.name
                        // Error if current table has fewer columns
                        || cur_table.columns.len() != prev_table.columns.len()
                        // Only match columns that are joined prefix of LHS
                        || prev_table.columns.iter().zip(&cur_table.columns).any(
                            |(prev_col, cur_col)| {
                                prev_col.name != cur_col.name
                                    || prev_col.type_oid != cur_col.type_oid
                                    || prev_col.type_mod != cur_col.type_mod
                            },
                        )
                    {
                        warn!(
                            "could not perform migration of PG source {name} due \
                        to schema change; this source must be recreated, but the \
                        schema in the warning where this occurs will have the wrong col_num."
                        );
                        return;
                    }
                    // No table in any previous version of Materialize was
                    // defined with any keys, nor are we planning to test their
                    // data sets to see if they can be retroactively applied.
                    cur_table.keys.clear();
                    // No column in any previous version of Materialize had a
                    // NOT NULL constraint meaningfully applied.
                    for c in cur_table.columns.iter_mut() {
                        c.nullable = true;
                    }
                }
                None => {
                    warn!(
                        "could not perform migration of PG source {name} due \
                    to schema change; this source must be recreated, but the \
                    schema in the warning where this occurs will have the wrong col_num."
                    );
                    return;
                }
            }
        }

        let _ = options.remove(details_idx).value.expect("valid catalog");

        publication_details.tables = current_publication_tables;

        options.push(PgConfigOption {
            name: PgConfigOptionName::Details,
            value: Some(mz_sql::ast::WithOptionValue::Value(Value::String(
                hex::encode(publication_details.into_proto().encode_to_vec()),
            ))),
        });
    }
}

// Update the owners of all linked clusters, linked cluster replicas, and subsources so that their
// owners match their linked object or primary source.
//
// TODO(migration): delete in version v.64 (released in v0.63 + 1 additional
// release)
fn sync_source_owners(
    catalog: &Catalog,
    tx: &mut Transaction,
    now: EpochMillis,
) -> Result<(), anyhow::Error> {
    let mut updated_clusters = BTreeMap::new();
    let mut updated_cluster_replicas = BTreeMap::new();
    let mut updated_items = BTreeMap::new();

    for cluster in catalog.user_clusters() {
        if let Some(linked_id) = cluster.linked_object_id() {
            let linked_owner_id = catalog.get_entry(&linked_id).owner_id();
            let old_owner = cluster.owner_id();
            if &old_owner != linked_owner_id {
                let mut new_cluster = cluster.clone();
                new_cluster.owner_id = *linked_owner_id;
                updated_clusters.insert(cluster.id(), new_cluster);
                add_to_audit_log(
                    tx,
                    mz_audit_log::EventType::Alter,
                    mz_audit_log::ObjectType::Cluster,
                    mz_audit_log::EventDetails::UpdateOwnerV1(mz_audit_log::UpdateOwnerV1 {
                        object_id: ObjectId::Cluster(cluster.id()).to_string(),
                        old_owner_id: old_owner.to_string(),
                        new_owner_id: linked_owner_id.to_string(),
                    }),
                    now,
                )?;
            }
        }
    }

    for cluster_replica in catalog.user_cluster_replicas() {
        let cluster = catalog.get_cluster(cluster_replica.cluster_id());
        if let Some(linked_id) = cluster.linked_object_id() {
            let linked_owner_id = catalog.get_entry(&linked_id).owner_id();
            let old_owner = cluster_replica.owner_id();
            if &old_owner != linked_owner_id {
                let mut new_replica = cluster_replica.clone();
                new_replica.owner_id = *linked_owner_id;
                updated_cluster_replicas.insert(
                    cluster_replica.replica_id(),
                    (cluster_replica.cluster_id(), new_replica),
                );
                add_to_audit_log(
                    tx,
                    mz_audit_log::EventType::Alter,
                    mz_audit_log::ObjectType::ClusterReplica,
                    mz_audit_log::EventDetails::UpdateOwnerV1(mz_audit_log::UpdateOwnerV1 {
                        object_id: ObjectId::ClusterReplica((
                            cluster_replica.cluster_id(),
                            cluster_replica.replica_id(),
                        ))
                        .to_string(),
                        old_owner_id: old_owner.to_string(),
                        new_owner_id: linked_owner_id.to_string(),
                    }),
                    now,
                )?;
            }
        }
    }

    for source in catalog.user_sources() {
        if source.is_subsource() {
            let primary_source = source
                .used_by()
                .iter()
                .find(|id| catalog.get_entry(id).item_type() == CatalogItemType::Source)
                .expect("subsource must have primary source");
            let primary_source_owner = catalog.get_entry(primary_source).owner_id();
            let old_owner = source.owner_id();
            if old_owner != primary_source_owner {
                let mut new_source = source.clone();
                new_source.owner_id = *primary_source_owner;
                let definition = SerializedCatalogItem::V1 {
                    create_sql: new_source.create_sql().to_string(),
                };
                updated_items.insert(
                    source.id,
                    storage::Item {
                        id: new_source.id,
                        name: new_source.name,
                        definition,
                        owner_id: new_source.owner_id,
                        privileges: new_source.privileges.all_values_owned().collect(),
                    },
                );
                add_to_audit_log(
                    tx,
                    mz_audit_log::EventType::Alter,
                    mz_audit_log::ObjectType::Source,
                    mz_audit_log::EventDetails::UpdateOwnerV1(mz_audit_log::UpdateOwnerV1 {
                        object_id: ObjectId::Item(source.id()).to_string(),
                        old_owner_id: old_owner.to_string(),
                        new_owner_id: primary_source_owner.to_string(),
                    }),
                    now,
                )?;
            }
        }
    }

    tx.update_clusters(updated_clusters)
        .expect("corrupt catalog");
    tx.update_cluster_replicas(updated_cluster_replicas)
        .expect("corrupt catalog");
    tx.update_items(updated_items).expect("corrupt catalog");

    Ok(())
}

// Update the owners of all cluster replicas so that their owners match their cluster.
//
// TODO(migration): delete in version v.64 (released in v0.63 + 1 additional
// release)
fn sync_cluster_replica_owners(
    catalog: &Catalog,
    tx: &mut Transaction,
    now: EpochMillis,
) -> Result<(), anyhow::Error> {
    let mut updated_cluster_replicas = BTreeMap::new();
    for cluster_replica in catalog.user_cluster_replicas() {
        let cluster = catalog.get_cluster(cluster_replica.cluster_id());
        let old_owner = cluster_replica.owner_id();
        if old_owner != cluster.owner_id() {
            let mut new_replica = cluster_replica.clone();
            new_replica.owner_id = cluster.owner_id();
            updated_cluster_replicas.insert(
                cluster_replica.replica_id(),
                (cluster_replica.cluster_id(), new_replica),
            );
            add_to_audit_log(
                tx,
                mz_audit_log::EventType::Alter,
                mz_audit_log::ObjectType::ClusterReplica,
                mz_audit_log::EventDetails::UpdateOwnerV1(mz_audit_log::UpdateOwnerV1 {
                    object_id: ObjectId::ClusterReplica((
                        cluster_replica.cluster_id(),
                        cluster_replica.replica_id(),
                    ))
                    .to_string(),
                    old_owner_id: old_owner.to_string(),
                    new_owner_id: cluster.owner_id().to_string(),
                }),
                now,
            )?;
        }
    }
    tx.update_cluster_replicas(updated_cluster_replicas)
        .expect("corrupt catalog");
    Ok(())
}

fn add_to_audit_log(
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
