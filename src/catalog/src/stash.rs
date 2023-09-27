// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use derivative::Derivative;
use std::collections::BTreeMap;
use std::iter::once;
use std::num::NonZeroI64;
use std::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use itertools::Itertools;
use postgres_openssl::MakeTlsConnector;

use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::now::NowFn;
use mz_ore::result::ResultExt;
use mz_ore::retry::Retry;
use mz_proto::{ProtoType, RustType};
use mz_repr::adt::mz_acl_item::MzAclItem;
use mz_repr::role_id::RoleId;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::{
    CatalogError as SqlCatalogError, DefaultPrivilegeAclItem, DefaultPrivilegeObject,
};
use mz_sql::names::CommentObjectId;
use mz_stash::objects::proto;
use mz_stash::{AppendBatch, DebugStashFactory, Stash, StashError, StashFactory, TypedCollection};
use mz_storage_types::sources::Timeline;

use crate::initialize::DEPLOY_GENERATION;
use crate::objects::{
    AuditLogKey, Cluster, ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue,
    ClusterKey, ClusterReplica, ClusterReplicaKey, ClusterReplicaValue, ClusterValue, CommentKey,
    CommentValue, Database, DatabaseKey, DatabaseValue, DefaultPrivilegesKey,
    DefaultPrivilegesValue, GidMappingKey, GidMappingValue, IdAllocKey, IdAllocValue,
    ReplicaConfig, Role, RoleKey, RoleValue, Schema, SchemaKey, SchemaValue, StorageUsageKey,
    SystemObjectDescription, SystemObjectMapping, SystemObjectUniqueIdentifier,
    SystemPrivilegesKey, SystemPrivilegesValue, TimestampKey, TimestampValue,
};
use crate::transaction::{
    add_new_builtin_cluster_replicas_migration, add_new_builtin_clusters_migration, Transaction,
    TransactionBatch,
};
use crate::{
    initialize, BootstrapArgs, DurableCatalogState, Error, OpenableDurableCatalogState,
    ReadOnlyDurableCatalogState, AUDIT_LOG_COLLECTION, CLUSTER_COLLECTION,
    CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION, CLUSTER_REPLICA_COLLECTION, COMMENTS_COLLECTION,
    CONFIG_COLLECTION, DATABASES_COLLECTION, DEFAULT_PRIVILEGES_COLLECTION,
    ID_ALLOCATOR_COLLECTION, ITEM_COLLECTION, ROLES_COLLECTION, SCHEMAS_COLLECTION,
    SETTING_COLLECTION, STORAGE_USAGE_COLLECTION, SYSTEM_CONFIGURATION_COLLECTION,
    SYSTEM_GID_MAPPING_COLLECTION, SYSTEM_PRIVILEGES_COLLECTION, TIMESTAMP_COLLECTION,
};

/// Configuration needed to connect to the stash.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct StashConfig {
    pub stash_factory: StashFactory,
    pub stash_url: String,
    pub schema: Option<String>,
    #[derivative(Debug = "ignore")]
    pub tls: MakeTlsConnector,
}

/// A [`OpenableConnection`] represent a struct capable of opening a connection to the stash.
#[derive(Debug)]
pub(crate) struct OpenableConnection {
    stash: Option<Stash>,
    config: StashConfig,
}

impl OpenableConnection {
    pub(crate) fn new(config: StashConfig) -> OpenableConnection {
        OpenableConnection {
            stash: None,
            config,
        }
    }

    /// Opens the inner stash in read-only mode.
    async fn open_stash_read_only(&mut self) -> Result<&mut Stash, StashError> {
        if !matches!(&self.stash, Some(stash) if stash.is_readonly()) {
            self.stash = Some(
                self.config
                    .stash_factory
                    .open_readonly(
                        self.config.stash_url.clone(),
                        self.config.schema.clone(),
                        self.config.tls.clone(),
                    )
                    .await?,
            );
        }
        Ok(self.stash.as_mut().expect("opened above"))
    }

    /// Opens the inner stash in savepoint mode.
    async fn open_stash_savepoint(&mut self) -> Result<&mut Stash, StashError> {
        if !matches!(&self.stash, Some(stash) if stash.is_savepoint()) {
            self.stash = Some(
                self.config
                    .stash_factory
                    .open_savepoint(
                        self.config.stash_url.clone(),
                        self.config.schema.clone(),
                        self.config.tls.clone(),
                    )
                    .await?,
            );
        }
        Ok(self.stash.as_mut().expect("opened above"))
    }

    /// Opens the inner stash in a writeable mode.
    async fn open_stash(&mut self) -> Result<&mut Stash, StashError> {
        if !matches!(&self.stash, Some(stash) if stash.is_writeable()) {
            self.stash = Some(
                self.config
                    .stash_factory
                    .open(
                        self.config.stash_url.clone(),
                        self.config.schema.clone(),
                        self.config.tls.clone(),
                    )
                    .await?,
            );
        }
        Ok(self.stash.as_mut().expect("opened above"))
    }
}

#[async_trait]
impl OpenableDurableCatalogState<Connection> for OpenableConnection {
    #[tracing::instrument(name = "storage::open_check", level = "info", skip_all)]
    async fn open_savepoint(
        &mut self,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Connection, Error> {
        self.open_stash_savepoint().await?;
        let stash = self.stash.take().expect("opened above");
        retry_open(stash, now, bootstrap_args, deploy_generation).await
    }

    #[tracing::instrument(name = "storage::open_read_only", level = "info", skip_all)]
    async fn open_read_only(
        &mut self,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Connection, Error> {
        self.open_stash_read_only().await?;
        let stash = self.stash.take().expect("opened above");
        retry_open(stash, now, bootstrap_args, None).await
    }

    #[tracing::instrument(name = "storage::open", level = "info", skip_all)]
    async fn open(
        &mut self,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Connection, Error> {
        self.open_stash().await?;
        let stash = self.stash.take().expect("opened above");
        retry_open(stash, now, bootstrap_args, deploy_generation).await
    }

    async fn is_initialized(&mut self) -> Result<bool, Error> {
        let stash = match &mut self.stash {
            None => match self.open_stash_read_only().await {
                Ok(stash) => stash,
                Err(e) if e.can_recover_with_write_mode() => return Ok(false),
                Err(e) => return Err(e.into()),
            },
            Some(stash) => stash,
        };
        stash.is_initialized().await.err_into()
    }

    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, Error> {
        let stash = match &mut self.stash {
            None => match self.open_stash_read_only().await {
                Ok(stash) => stash,
                Err(e) if e.can_recover_with_write_mode() => return Ok(None),
                Err(e) => return Err(e.into()),
            },
            Some(stash) => stash,
        };

        let deployment_generation = CONFIG_COLLECTION
            .peek_key_one(
                stash,
                proto::ConfigKey {
                    key: DEPLOY_GENERATION.into(),
                },
            )
            .await?
            .map(|v| v.value);

        Ok(deployment_generation)
    }
}

/// Opens a new [`Connection`] to the stash, retrying any retryable errors. Optionally
/// initialize the stash if it has not been initialized and perform any migrations needed.
///
/// # Panics
/// If the inner stash has not been opened.
async fn retry_open(
    mut stash: Stash,
    now: NowFn,
    bootstrap_args: &BootstrapArgs,
    deploy_generation: Option<u64>,
) -> Result<Connection, Error> {
    let retry = Retry::default()
        .clamp_backoff(Duration::from_secs(1))
        .max_duration(Duration::from_secs(30))
        .into_retry_stream();
    let mut retry = pin::pin!(retry);

    loop {
        match open_inner(stash, now.clone(), bootstrap_args, deploy_generation).await {
            Ok(conn) => {
                return Ok(conn);
            }
            Err((given_stash, err)) => {
                stash = given_stash;
                let should_retry = matches!(&err, Error::Stash(se) if se.should_retry());
                if !should_retry || retry.next().await.is_none() {
                    return Err(err);
                }
            }
        }
    }
}

#[tracing::instrument(name = "storage::open_inner", level = "info", skip_all)]
async fn open_inner(
    mut stash: Stash,
    now: NowFn,
    bootstrap_args: &BootstrapArgs,
    deploy_generation: Option<u64>,
) -> Result<Connection, (Stash, Error)> {
    // Initialize the Stash if it hasn't been already
    let mut conn = if !match stash.is_initialized().await {
        Ok(is_init) => is_init,
        Err(e) => {
            return Err((stash, e.into()));
        }
    } {
        // Get the current timestamp so we can record when we booted. We don't have to worry
        // about `boot_ts` being less than a previously used timestamp because the stash is
        // uninitialized and there are no previous timestamps.
        let boot_ts = now();

        // Initialize the Stash
        let args = bootstrap_args.clone();
        match stash
            .with_transaction(move |mut tx| {
                Box::pin(async move {
                    initialize::initialize(&mut tx, &args, boot_ts, deploy_generation).await
                })
            })
            .await
        {
            Ok(()) => {}
            Err(e) => return Err((stash, e.into())),
        };
        Connection { stash }
    } else {
        if !stash.is_readonly() {
            // Before we do anything with the Stash, we need to run any pending upgrades and
            // initialize new collections.
            match stash.upgrade().await {
                Ok(()) => {}
                Err(e) => {
                    return Err((stash, e.into()));
                }
            };
        }

        let mut conn = Connection { stash };

        if let Some(deploy_generation) = deploy_generation {
            match conn.set_deploy_generation(deploy_generation).await {
                Ok(()) => {}
                Err(e) => {
                    return Err((conn.stash, e));
                }
            }
        }

        conn
    };

    // Add any new builtin Clusters or Cluster Replicas that may be newly defined.
    if !conn.stash.is_readonly() {
        let mut txn = match conn.transaction().await {
            Ok(txn) => txn,
            Err(e) => {
                return Err((conn.stash, e));
            }
        };

        match add_new_builtin_clusters_migration(&mut txn) {
            Ok(()) => {}
            Err(e) => {
                return Err((conn.stash, e));
            }
        }
        match add_new_builtin_cluster_replicas_migration(&mut txn, bootstrap_args) {
            Ok(()) => {}
            Err(e) => {
                return Err((conn.stash, e));
            }
        }
        match txn.commit().await {
            Ok(()) => {}
            Err(e) => {
                return Err((conn.stash, e));
            }
        }
    }

    Ok(conn)
}

/// A [`Connection`] represent an open connection to the stash. It exposes optimized methods for
/// executing a single operation against the stash. If the consumer needs to execute multiple
/// operations atomically, then they should start a transaction via [`Connection::transaction`].
#[derive(Debug)]
pub struct Connection {
    stash: Stash,
}

impl Connection {
    async fn get_setting(&mut self, key: &str) -> Result<Option<String>, Error> {
        let v = SETTING_COLLECTION
            .peek_key_one(
                &mut self.stash,
                proto::SettingKey {
                    name: key.to_string(),
                },
            )
            .await?;
        Ok(v.map(|v| v.value))
    }

    async fn set_setting(&mut self, key: &str, value: &str) -> Result<(), Error> {
        let key = proto::SettingKey {
            name: key.to_string(),
        };
        let value = proto::SettingValue {
            value: value.into(),
        };
        SETTING_COLLECTION
            .upsert(&mut self.stash, once((key, value)))
            .await
            .map_err(|e| e.into())
    }
}

#[async_trait]
impl ReadOnlyDurableCatalogState for Connection {
    fn epoch(&mut self) -> Option<NonZeroI64> {
        self.stash.epoch()
    }

    async fn get_catalog_content_version(&mut self) -> Result<Option<String>, Error> {
        self.get_setting("catalog_content_version").await
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_clusters(&mut self) -> Result<Vec<Cluster>, Error> {
        let entries = CLUSTER_COLLECTION.peek_one(&mut self.stash).await?;
        let clusters = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (ClusterKey, ClusterValue)| Cluster {
                id: k.id,
                name: v.name,
                linked_object_id: v.linked_object_id,
                owner_id: v.owner_id,
                privileges: v.privileges,
                config: v.config,
            })
            .collect::<Result<_, _>>()?;

        Ok(clusters)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_cluster_replicas(&mut self) -> Result<Vec<ClusterReplica>, Error> {
        let entries = CLUSTER_REPLICA_COLLECTION.peek_one(&mut self.stash).await?;
        let replicas = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(
                |(k, v): (ClusterReplicaKey, ClusterReplicaValue)| ClusterReplica {
                    cluster_id: v.cluster_id,
                    replica_id: k.id,
                    name: v.name,
                    config: v.config,
                    owner_id: v.owner_id,
                },
            )
            .collect::<Result<_, _>>()?;

        Ok(replicas)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_databases(&mut self) -> Result<Vec<Database>, Error> {
        let entries = DATABASES_COLLECTION.peek_one(&mut self.stash).await?;
        let databases = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (DatabaseKey, DatabaseValue)| Database {
                id: k.id,
                name: v.name,
                owner_id: v.owner_id,
                privileges: v.privileges,
            })
            .collect::<Result<_, _>>()?;

        Ok(databases)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_schemas(&mut self) -> Result<Vec<Schema>, Error> {
        let entries = SCHEMAS_COLLECTION.peek_one(&mut self.stash).await?;
        let schemas = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (SchemaKey, SchemaValue)| Schema {
                id: k.id,
                name: v.name,
                database_id: v.database_id,
                owner_id: v.owner_id,
                privileges: v.privileges,
            })
            .collect::<Result<_, _>>()?;

        Ok(schemas)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_system_items(&mut self) -> Result<Vec<SystemObjectMapping>, Error> {
        let entries = SYSTEM_GID_MAPPING_COLLECTION
            .peek_one(&mut self.stash)
            .await?;
        let system_item = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(
                |(k, v): (GidMappingKey, GidMappingValue)| SystemObjectMapping {
                    description: SystemObjectDescription {
                        schema_name: k.schema_name,
                        object_type: k.object_type,
                        object_name: k.object_name,
                    },
                    unique_identifier: SystemObjectUniqueIdentifier {
                        id: GlobalId::System(v.id),
                        fingerprint: v.fingerprint,
                    },
                },
            )
            .collect::<Result<_, _>>()?;
        Ok(system_item)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_introspection_source_indexes(
        &mut self,
        cluster_id: ClusterId,
    ) -> Result<BTreeMap<String, GlobalId>, Error> {
        let entries = CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION
            .peek_one(&mut self.stash)
            .await?;
        let sources = entries
            .into_iter()
            .map(RustType::from_proto)
            .filter_map_ok(
                |(k, v): (
                    ClusterIntrospectionSourceIndexKey,
                    ClusterIntrospectionSourceIndexValue,
                )| {
                    if k.cluster_id == cluster_id {
                        Some((k.name, GlobalId::System(v.index_id)))
                    } else {
                        None
                    }
                },
            )
            .collect::<Result<_, _>>()?;

        Ok(sources)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_roles(&mut self) -> Result<Vec<Role>, Error> {
        let entries = ROLES_COLLECTION.peek_one(&mut self.stash).await?;
        let roles = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (RoleKey, RoleValue)| Role {
                id: k.id,
                name: v.name,
                attributes: v.attributes,
                membership: v.membership,
                vars: v.vars,
            })
            .collect::<Result<_, _>>()?;

        Ok(roles)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_default_privileges(
        &mut self,
    ) -> Result<Vec<(DefaultPrivilegeObject, DefaultPrivilegeAclItem)>, Error> {
        Ok(DEFAULT_PRIVILEGES_COLLECTION
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (DefaultPrivilegesKey, DefaultPrivilegesValue)| {
                (
                    DefaultPrivilegeObject::new(
                        k.role_id,
                        k.database_id,
                        k.schema_id,
                        k.object_type,
                    ),
                    DefaultPrivilegeAclItem::new(k.grantee, v.privileges),
                )
            })
            .collect::<Result<_, _>>()?)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_system_privileges(&mut self) -> Result<Vec<MzAclItem>, Error> {
        Ok(SYSTEM_PRIVILEGES_COLLECTION
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(
                |(k, v): (SystemPrivilegesKey, SystemPrivilegesValue)| MzAclItem {
                    grantee: k.grantee,
                    grantor: k.grantor,
                    acl_mode: v.acl_mode,
                },
            )
            .collect::<Result<_, _>>()?)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_system_configurations(&mut self) -> Result<BTreeMap<String, String>, Error> {
        SYSTEM_CONFIGURATION_COLLECTION
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(|(k, v)| Ok((k.name, v.value)))
            .collect()
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_comments(
        &mut self,
    ) -> Result<Vec<(CommentObjectId, Option<usize>, String)>, Error> {
        let comments = COMMENTS_COLLECTION
            .peek_one(&mut self.stash)
            .await?
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (CommentKey, CommentValue)| (k.object_id, k.sub_component, v.comment))
            .collect::<Result<_, _>>()?;

        Ok(comments)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_timestamps(&mut self) -> Result<BTreeMap<Timeline, Timestamp>, Error> {
        let entries = TIMESTAMP_COLLECTION.peek_one(&mut self.stash).await?;
        let timestamps = entries
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|(k, v): (TimestampKey, TimestampValue)| {
                (k.id.parse().expect("invalid timeline persisted"), v.ts)
            })
            .collect::<Result<_, _>>()?;

        Ok(timestamps)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_timestamp(&mut self, timeline: &Timeline) -> Result<Option<Timestamp>, Error> {
        let key = proto::TimestampKey {
            id: timeline.to_string(),
        };
        let val: Option<TimestampValue> = TIMESTAMP_COLLECTION
            .peek_key_one(&mut self.stash, key)
            .await?
            .map(RustType::from_proto)
            .transpose()?;

        Ok(val.map(|v| v.ts))
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_audit_logs(&mut self) -> Result<Vec<VersionedEvent>, Error> {
        let entries = AUDIT_LOG_COLLECTION.peek_one(&mut self.stash).await?;
        let logs: Vec<_> = entries
            .into_keys()
            .map(AuditLogKey::from_proto)
            .map_ok(|e| e.event)
            .collect::<Result<_, _>>()?;

        Ok(logs)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, Error> {
        ID_ALLOCATOR_COLLECTION
            .peek_key_one(
                &mut self.stash,
                IdAllocKey {
                    name: id_type.to_string(),
                }
                .into_proto(),
            )
            .await
            .map(|x| x.expect("must exist").next_id)
            .map_err(Into::into)
    }
}

#[async_trait]
impl DurableCatalogState for Connection {
    fn is_read_only(&self) -> bool {
        self.stash.is_readonly()
    }

    #[tracing::instrument(name = "storage::transaction", level = "debug", skip_all)]
    async fn transaction(&mut self) -> Result<Transaction, Error> {
        let (
            databases,
            schemas,
            roles,
            items,
            comments,
            clusters,
            cluster_replicas,
            introspection_sources,
            id_allocator,
            configs,
            settings,
            timestamps,
            system_gid_mapping,
            system_configurations,
            default_privileges,
            system_privileges,
        ) = self
            .stash
            .with_transaction(|tx| {
                Box::pin(async move {
                    // Peek the catalog collections in any order and a single transaction.
                    futures::try_join!(
                        tx.peek_one(tx.collection(DATABASES_COLLECTION.name()).await?),
                        tx.peek_one(tx.collection(SCHEMAS_COLLECTION.name()).await?),
                        tx.peek_one(tx.collection(ROLES_COLLECTION.name()).await?),
                        tx.peek_one(tx.collection(ITEM_COLLECTION.name()).await?),
                        tx.peek_one(tx.collection(COMMENTS_COLLECTION.name()).await?),
                        tx.peek_one(tx.collection(CLUSTER_COLLECTION.name()).await?),
                        tx.peek_one(tx.collection(CLUSTER_REPLICA_COLLECTION.name()).await?),
                        tx.peek_one(
                            tx.collection(CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION.name())
                                .await?,
                        ),
                        tx.peek_one(tx.collection(ID_ALLOCATOR_COLLECTION.name()).await?),
                        tx.peek_one(tx.collection(CONFIG_COLLECTION.name()).await?),
                        tx.peek_one(tx.collection(SETTING_COLLECTION.name()).await?),
                        tx.peek_one(tx.collection(TIMESTAMP_COLLECTION.name()).await?),
                        tx.peek_one(tx.collection(SYSTEM_GID_MAPPING_COLLECTION.name()).await?),
                        tx.peek_one(
                            tx.collection(SYSTEM_CONFIGURATION_COLLECTION.name())
                                .await?
                        ),
                        tx.peek_one(tx.collection(DEFAULT_PRIVILEGES_COLLECTION.name()).await?),
                        tx.peek_one(tx.collection(SYSTEM_PRIVILEGES_COLLECTION.name()).await?),
                    )
                })
            })
            .await?;

        Transaction::new(
            self,
            databases,
            schemas,
            roles,
            items,
            comments,
            clusters,
            cluster_replicas,
            introspection_sources,
            id_allocator,
            configs,
            settings,
            timestamps,
            system_gid_mapping,
            system_configurations,
            default_privileges,
            system_privileges,
        )
    }

    #[tracing::instrument(name = "storage::transaction", level = "debug", skip_all)]
    async fn commit_transaction(&mut self, txn_batch: TransactionBatch) -> Result<(), Error> {
        async fn add_batch<'tx, K, V>(
            tx: &'tx mz_stash::Transaction<'tx>,
            batches: &mut Vec<AppendBatch>,
            typed: &'tx TypedCollection<K, V>,
            changes: &[(K, V, mz_stash::Diff)],
        ) -> Result<(), StashError>
        where
            K: mz_stash::Data + 'tx,
            V: mz_stash::Data + 'tx,
        {
            if changes.is_empty() {
                return Ok(());
            }
            let collection = typed.from_tx(tx).await?;
            let mut batch = collection.make_batch_tx(tx).await?;
            for (k, v, diff) in changes {
                collection.append_to_batch(&mut batch, k, v, *diff);
            }
            batches.push(batch);
            Ok(())
        }

        // The with_transaction fn below requires a Fn that can be cloned,
        // meaning anything it closes over must be Clone. TransactionBatch
        // implements Clone, thus, the Arcs here aren't strictly necessary.
        // However, using an Arc means that we never clone the TransactionBatch
        // (which would happen at least one time when the txn starts), and
        // instead only clone the Arc.
        let txn_batch = Arc::new(txn_batch);

        self.stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let mut batches = Vec::new();

                    add_batch(
                        &tx,
                        &mut batches,
                        &DATABASES_COLLECTION,
                        &txn_batch.databases,
                    )
                    .await?;
                    add_batch(&tx, &mut batches, &SCHEMAS_COLLECTION, &txn_batch.schemas).await?;
                    add_batch(&tx, &mut batches, &ITEM_COLLECTION, &txn_batch.items).await?;
                    add_batch(&tx, &mut batches, &COMMENTS_COLLECTION, &txn_batch.comments).await?;
                    add_batch(&tx, &mut batches, &ROLES_COLLECTION, &txn_batch.roles).await?;
                    add_batch(&tx, &mut batches, &CLUSTER_COLLECTION, &txn_batch.clusters).await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &CLUSTER_REPLICA_COLLECTION,
                        &txn_batch.cluster_replicas,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION,
                        &txn_batch.introspection_sources,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &ID_ALLOCATOR_COLLECTION,
                        &txn_batch.id_allocator,
                    )
                    .await?;
                    add_batch(&tx, &mut batches, &CONFIG_COLLECTION, &txn_batch.configs).await?;
                    add_batch(&tx, &mut batches, &SETTING_COLLECTION, &txn_batch.settings).await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &TIMESTAMP_COLLECTION,
                        &txn_batch.timestamps,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &SYSTEM_GID_MAPPING_COLLECTION,
                        &txn_batch.system_gid_mapping,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &SYSTEM_CONFIGURATION_COLLECTION,
                        &txn_batch.system_configurations,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &DEFAULT_PRIVILEGES_COLLECTION,
                        &txn_batch.default_privileges,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &SYSTEM_PRIVILEGES_COLLECTION,
                        &txn_batch.system_privileges,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &AUDIT_LOG_COLLECTION,
                        &txn_batch.audit_log_updates,
                    )
                    .await?;
                    add_batch(
                        &tx,
                        &mut batches,
                        &STORAGE_USAGE_COLLECTION,
                        &txn_batch.storage_usage_updates,
                    )
                    .await?;
                    tx.append(batches).await?;

                    Ok(())
                })
            })
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn confirm_leadership(&mut self) -> Result<(), Error> {
        Ok(self.stash.confirm_leadership().await?)
    }

    async fn set_connect_timeout(&mut self, connect_timeout: Duration) {
        self.stash.set_connect_timeout(connect_timeout).await;
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_catalog_content_version(&mut self, new_version: &str) -> Result<(), Error> {
        self.set_setting("catalog_content_version", new_version)
            .await
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_and_prune_storage_usage(
        &mut self,
        retention_period: Option<Duration>,
        boot_ts: Timestamp,
    ) -> Result<Vec<VersionedStorageUsage>, Error> {
        // If no usage retention period is set, set the cutoff to MIN so nothing
        // is removed.
        let cutoff_ts = match retention_period {
            None => u128::MIN,
            Some(period) => u128::from(boot_ts).saturating_sub(period.as_millis()),
        };
        let is_read_only = self.is_read_only();
        Ok(self
            .stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    let collection = STORAGE_USAGE_COLLECTION.from_tx(&tx).await?;
                    let rows = tx.peek_one(collection).await?;
                    let mut events = Vec::with_capacity(rows.len());
                    let mut batch = collection.make_batch_tx(&tx).await?;
                    for ev in rows.into_keys() {
                        let event: StorageUsageKey = ev.clone().into_rust()?;
                        if u128::from(event.metric.timestamp()) >= cutoff_ts {
                            events.push(event.metric);
                        } else if retention_period.is_some() {
                            collection.append_to_batch(&mut batch, &ev, &(), -1);
                        }
                    }
                    // Delete things only if a retention period is
                    // specified (otherwise opening readonly catalogs
                    // can fail).
                    if retention_period.is_some() && !is_read_only {
                        tx.append(vec![batch]).await?;
                    }
                    Ok(events)
                })
            })
            .await?)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_system_items(&mut self, mappings: Vec<SystemObjectMapping>) -> Result<(), Error> {
        if mappings.is_empty() {
            return Ok(());
        }

        let mappings = mappings
            .into_iter()
            .map(|mapping| {
                let id = if let GlobalId::System(id) = mapping.unique_identifier.id {
                    id
                } else {
                    panic!("non-system id provided")
                };
                (
                    GidMappingKey {
                        schema_name: mapping.description.schema_name,
                        object_type: mapping.description.object_type,
                        object_name: mapping.description.object_name,
                    },
                    GidMappingValue {
                        id,
                        fingerprint: mapping.unique_identifier.fingerprint,
                    },
                )
            })
            .map(|e| RustType::into_proto(&e));
        SYSTEM_GID_MAPPING_COLLECTION
            .upsert(&mut self.stash, mappings)
            .await
            .map_err(|e| e.into())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_introspection_source_indexes(
        &mut self,
        mappings: Vec<(ClusterId, &str, GlobalId)>,
    ) -> Result<(), Error> {
        if mappings.is_empty() {
            return Ok(());
        }

        let mappings = mappings
            .into_iter()
            .map(|(cluster_id, name, index_id)| {
                let index_id = if let GlobalId::System(id) = index_id {
                    id
                } else {
                    panic!("non-system id provided")
                };
                (
                    ClusterIntrospectionSourceIndexKey {
                        cluster_id,
                        name: name.to_string(),
                    },
                    ClusterIntrospectionSourceIndexValue { index_id },
                )
            })
            .map(|e| RustType::into_proto(&e));
        CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION
            .upsert(&mut self.stash, mappings)
            .await
            .map_err(|e| e.into())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_replica_config(
        &mut self,
        replica_id: ReplicaId,
        cluster_id: ClusterId,
        name: String,
        config: ReplicaConfig,
        owner_id: RoleId,
    ) -> Result<(), Error> {
        let key = ClusterReplicaKey { id: replica_id }.into_proto();
        let val = ClusterReplicaValue {
            cluster_id,
            name,
            config,
            owner_id,
        }
        .into_proto();
        CLUSTER_REPLICA_COLLECTION
            .upsert_key(&mut self.stash, key, |_| Ok::<_, Error>(val))
            .await??;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_timestamp(
        &mut self,
        timeline: &Timeline,
        timestamp: Timestamp,
    ) -> Result<(), Error> {
        let key = proto::TimestampKey {
            id: timeline.to_string(),
        };
        let (prev, next) = TIMESTAMP_COLLECTION
            .upsert_key(&mut self.stash, key, move |_| {
                Ok::<_, Error>(TimestampValue { ts: timestamp }.into_proto())
            })
            .await??;
        if let Some(prev) = prev {
            assert!(next >= prev, "global timestamp must always go up");
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_deploy_generation(&mut self, deploy_generation: u64) -> Result<(), Error> {
        CONFIG_COLLECTION
            .upsert_key(
                &mut self.stash,
                proto::ConfigKey {
                    key: DEPLOY_GENERATION.into(),
                },
                move |_| {
                    Ok::<_, Error>(proto::ConfigValue {
                        value: deploy_generation,
                    })
                },
            )
            .await??;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn allocate_id(&mut self, id_type: &str, amount: u64) -> Result<Vec<u64>, Error> {
        if amount == 0 {
            return Ok(Vec::new());
        }
        let key = IdAllocKey {
            name: id_type.to_string(),
        }
        .into_proto();
        let (prev, next) = ID_ALLOCATOR_COLLECTION
            .upsert_key(&mut self.stash, key, move |prev| {
                let id = prev.expect("must exist").next_id;
                match id.checked_add(amount) {
                    Some(next_gid) => Ok(IdAllocValue { next_id: next_gid }.into_proto()),
                    None => Err(Error::from(SqlCatalogError::IdExhaustion)),
                }
            })
            .await??;
        let id = prev.expect("must exist").next_id;
        Ok((id..next.next_id).collect())
    }
}

pub const ALL_COLLECTIONS: &[&str] = &[
    AUDIT_LOG_COLLECTION.name(),
    CLUSTER_COLLECTION.name(),
    CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION.name(),
    CLUSTER_REPLICA_COLLECTION.name(),
    CONFIG_COLLECTION.name(),
    DATABASES_COLLECTION.name(),
    DEFAULT_PRIVILEGES_COLLECTION.name(),
    ID_ALLOCATOR_COLLECTION.name(),
    ITEM_COLLECTION.name(),
    ROLES_COLLECTION.name(),
    SCHEMAS_COLLECTION.name(),
    SETTING_COLLECTION.name(),
    STORAGE_USAGE_COLLECTION.name(),
    SYSTEM_CONFIGURATION_COLLECTION.name(),
    SYSTEM_GID_MAPPING_COLLECTION.name(),
    SYSTEM_PRIVILEGES_COLLECTION.name(),
    TIMESTAMP_COLLECTION.name(),
];

/// A [`DebugOpenableConnection`] represent a struct capable of opening a debug connection to the
/// stash for usage in tests.
#[derive(Debug)]
pub struct DebugOpenableConnection<'a> {
    _debug_stash_factory: &'a DebugStashFactory,
    openable_connection: OpenableConnection,
}

impl DebugOpenableConnection<'_> {
    pub(crate) fn new(debug_stash_factory: &DebugStashFactory) -> DebugOpenableConnection {
        DebugOpenableConnection {
            _debug_stash_factory: debug_stash_factory,
            openable_connection: OpenableConnection {
                stash: None,
                config: StashConfig {
                    stash_factory: debug_stash_factory.stash_factory().clone(),
                    stash_url: debug_stash_factory.url().to_string(),
                    schema: Some(debug_stash_factory.schema().to_string()),
                    tls: debug_stash_factory.tls().clone(),
                },
            },
        }
    }
}

#[async_trait]
impl OpenableDurableCatalogState<Connection> for DebugOpenableConnection<'_> {
    async fn open_savepoint(
        &mut self,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Connection, Error> {
        self.openable_connection
            .open_savepoint(now, bootstrap_args, deploy_generation)
            .await
    }

    async fn open_read_only(
        &mut self,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Connection, Error> {
        self.openable_connection
            .open_read_only(now, bootstrap_args)
            .await
    }

    async fn open(
        &mut self,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Connection, Error> {
        self.openable_connection
            .open(now, bootstrap_args, deploy_generation)
            .await
    }

    async fn is_initialized(&mut self) -> Result<bool, Error> {
        self.openable_connection.is_initialized().await
    }

    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, Error> {
        self.openable_connection.get_deployment_generation().await
    }
}
