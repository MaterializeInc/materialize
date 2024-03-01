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
use mz_storage_types::controller::PersistTxnTablesImpl;
use std::collections::{BTreeMap, BTreeSet};
use std::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use itertools::Itertools;
use postgres_openssl::MakeTlsConnector;
use tracing::error;

use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_ore::metrics::MetricsFutureExt;
use mz_ore::now::EpochMillis;
use mz_ore::result::ResultExt;
use mz_ore::retry::Retry;
use mz_ore::soft_assert_eq_no_log;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::Timestamp;
use mz_sql::catalog::CatalogError as SqlCatalogError;
use mz_sql::session::vars::CatalogKind;
use mz_stash::{AppendBatch, DebugStashFactory, Diff, Stash, StashFactory, TypedCollection};
use mz_stash_types::StashError;

use crate::durable::debug::{Collection, CollectionTrace, Trace};
use crate::durable::initialize::{
    CATALOG_KIND_KEY, DEPLOY_GENERATION, PERSIST_TXN_TABLES, SYSTEM_CONFIG_SYNCED_KEY,
    TOMBSTONE_KEY, USER_VERSION_KEY,
};
use crate::durable::objects::serialization::proto;
use crate::durable::objects::{AuditLogKey, IdAllocKey, IdAllocValue, Snapshot, StorageUsageKey};
use crate::durable::transaction::{Transaction, TransactionBatch};
use crate::durable::upgrade::stash::upgrade;
use crate::durable::{
    initialize, BootstrapArgs, CatalogError, DebugCatalogState, DurableCatalogError,
    DurableCatalogState, Epoch, OpenableDurableCatalogState, ReadOnlyDurableCatalogState,
};

pub const SETTING_COLLECTION: TypedCollection<proto::SettingKey, proto::SettingValue> =
    TypedCollection::new("setting");
pub const SYSTEM_GID_MAPPING_COLLECTION: TypedCollection<
    proto::GidMappingKey,
    proto::GidMappingValue,
> = TypedCollection::new("system_gid_mapping");
pub const CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION: TypedCollection<
    proto::ClusterIntrospectionSourceIndexKey,
    proto::ClusterIntrospectionSourceIndexValue,
> = TypedCollection::new("compute_introspection_source_index"); // historical name
pub const ROLES_COLLECTION: TypedCollection<proto::RoleKey, proto::RoleValue> =
    TypedCollection::new("role");
pub const DATABASES_COLLECTION: TypedCollection<proto::DatabaseKey, proto::DatabaseValue> =
    TypedCollection::new("database");
pub const SCHEMAS_COLLECTION: TypedCollection<proto::SchemaKey, proto::SchemaValue> =
    TypedCollection::new("schema");
pub const ITEM_COLLECTION: TypedCollection<proto::ItemKey, proto::ItemValue> =
    TypedCollection::new("item");
pub const COMMENTS_COLLECTION: TypedCollection<proto::CommentKey, proto::CommentValue> =
    TypedCollection::new("comments");
pub const TIMESTAMP_COLLECTION: TypedCollection<proto::TimestampKey, proto::TimestampValue> =
    TypedCollection::new("timestamp");
pub const SYSTEM_CONFIGURATION_COLLECTION: TypedCollection<
    proto::ServerConfigurationKey,
    proto::ServerConfigurationValue,
> = TypedCollection::new("system_configuration");
pub const CLUSTER_COLLECTION: TypedCollection<proto::ClusterKey, proto::ClusterValue> =
    TypedCollection::new("compute_instance");
pub const CLUSTER_REPLICA_COLLECTION: TypedCollection<
    proto::ClusterReplicaKey,
    proto::ClusterReplicaValue,
> = TypedCollection::new("compute_replicas");
pub const AUDIT_LOG_COLLECTION: TypedCollection<proto::AuditLogKey, ()> =
    TypedCollection::new("audit_log");
pub const CONFIG_COLLECTION: TypedCollection<proto::ConfigKey, proto::ConfigValue> =
    TypedCollection::new("config");
pub const ID_ALLOCATOR_COLLECTION: TypedCollection<proto::IdAllocKey, proto::IdAllocValue> =
    TypedCollection::new("id_alloc");
pub const STORAGE_USAGE_COLLECTION: TypedCollection<proto::StorageUsageKey, ()> =
    TypedCollection::new("storage_usage");
pub const DEFAULT_PRIVILEGES_COLLECTION: TypedCollection<
    proto::DefaultPrivilegesKey,
    proto::DefaultPrivilegesValue,
> = TypedCollection::new("default_privileges");
pub const SYSTEM_PRIVILEGES_COLLECTION: TypedCollection<
    proto::SystemPrivilegesKey,
    proto::SystemPrivilegesValue,
> = TypedCollection::new("system_privileges");
// If you add a new collection, then don't forget to write a migration that initializes the
// collection either with some initial values or as empty. See
// [`mz_stash::upgrade::v17_to_v18`] as an example.

/// Configuration needed to connect to the stash.
#[derive(Derivative, Clone)]
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
pub struct OpenableConnection {
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
    async fn open_stash_savepoint(
        &mut self,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<&mut Stash, StashError> {
        if !matches!(&self.stash, Some(stash) if stash.is_savepoint()) {
            self.stash = Some(
                self.config
                    .stash_factory
                    .open_savepoint(
                        self.config.stash_url.clone(),
                        self.config.schema.clone(),
                        self.config.tls.clone(),
                        epoch_lower_bound,
                    )
                    .await?,
            );
        }
        Ok(self.stash.as_mut().expect("opened above"))
    }

    /// Opens the inner stash in a writeable mode.
    async fn open_stash(
        &mut self,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<&mut Stash, StashError> {
        if !matches!(&self.stash, Some(stash) if stash.is_writeable()) {
            self.stash = Some(
                self.config
                    .stash_factory
                    .open(
                        self.config.stash_url.clone(),
                        self.config.schema.clone(),
                        self.config.tls.clone(),
                        epoch_lower_bound,
                    )
                    .await?,
            );
        }
        Ok(self.stash.as_mut().expect("opened above"))
    }

    async fn get_config(&mut self, key: String) -> Result<Option<u64>, CatalogError> {
        let stash = match &mut self.stash {
            None => match self.open_stash_read_only().await {
                Ok(stash) => stash,
                Err(e) if e.can_recover_with_write_mode() => return Ok(None),
                Err(e) => return Err(e.into()),
            },
            Some(stash) => stash,
        };

        get_config(stash, key).await
    }
}

#[async_trait]
impl OpenableDurableCatalogState for OpenableConnection {
    #[mz_ore::instrument(name = "storage::open_check", level = "info")]
    async fn open_savepoint(
        mut self: Box<Self>,
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_stash_savepoint(epoch_lower_bound).await?;
        let stash = self.stash.take().expect("opened above");
        retry_open(stash, initial_ts, bootstrap_args, deploy_generation).await
    }

    #[mz_ore::instrument(name = "storage::open_read_only", level = "info")]
    async fn open_read_only(
        mut self: Box<Self>,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_stash_read_only().await?;
        let stash = self.stash.take().expect("opened above");
        retry_open(stash, EpochMillis::MIN, bootstrap_args, None).await
    }

    #[mz_ore::instrument(name = "storage::open", level = "info")]
    async fn open(
        mut self: Box<Self>,
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_stash(epoch_lower_bound).await?;
        let stash = self.stash.take().expect("opened above");
        retry_open(stash, initial_ts, bootstrap_args, deploy_generation).await
    }

    #[mz_ore::instrument(name = "storage::open_debug", level = "info")]
    async fn open_debug(mut self: Box<Self>) -> Result<DebugCatalogState, CatalogError> {
        self.open_stash(None).await?;
        let stash = self.stash.take().expect("opened above");
        Ok(DebugCatalogState::Stash(stash))
    }

    async fn is_initialized(&mut self) -> Result<bool, CatalogError> {
        let stash = match &mut self.stash {
            None => match self.open_stash_read_only().await {
                Ok(stash) => stash,
                Err(e) if e.can_recover_with_write_mode() => return Ok(false),
                Err(e) => return Err(e.into()),
            },
            Some(stash) => stash,
        };
        is_stash_initialized(stash).await.err_into()
    }

    async fn epoch(&mut self) -> Result<Epoch, CatalogError> {
        let stash = match &mut self.stash {
            None => match self.open_stash_read_only().await {
                Ok(stash) => stash,
                Err(e) if e.can_recover_with_write_mode() => {
                    return Err(CatalogError::Durable(DurableCatalogError::Uninitialized))
                }
                Err(e) => return Err(e.into()),
            },
            Some(stash) => stash,
        };
        stash
            .epoch()
            .ok_or(CatalogError::Durable(DurableCatalogError::Uninitialized))
    }

    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, CatalogError> {
        self.get_config(DEPLOY_GENERATION.into()).await
    }

    async fn has_system_config_synced_once(&mut self) -> Result<bool, CatalogError> {
        Ok(self
            .get_config(SYSTEM_CONFIG_SYNCED_KEY.into())
            .await?
            .map(|value| value > 0)
            .unwrap_or(false))
    }

    async fn get_tombstone(&mut self) -> Result<Option<bool>, CatalogError> {
        Ok(self.get_config(TOMBSTONE_KEY.into()).await?.map(|v| v > 0))
    }

    async fn get_catalog_kind_config(&mut self) -> Result<Option<CatalogKind>, CatalogError> {
        let value = self.get_config(CATALOG_KIND_KEY.into()).await?;

        value.map(CatalogKind::try_from).transpose().map_err(|err| {
            DurableCatalogError::from(TryFromProtoError::UnknownEnumVariant(err.to_string())).into()
        })
    }

    #[mz_ore::instrument]
    async fn trace(&mut self) -> Result<Trace, CatalogError> {
        fn stringify<T: Collection>(
            values: Vec<((T::Key, T::Value), mz_stash::Timestamp, Diff)>,
        ) -> CollectionTrace<T> {
            let values = values
                .into_iter()
                .map(|((k, v), ts, diff)| ((k, v), ts.to_string(), diff))
                .collect();
            CollectionTrace { values }
        }

        let stash = match self.open_stash_read_only().await {
            Err(e) if e.can_recover_with_write_mode() => {
                return Err(CatalogError::Durable(DurableCatalogError::Uninitialized))
            }
            res => res?,
        };

        let (
            audit_log,
            clusters,
            introspection_sources,
            cluster_replicas,
            comments,
            configs,
            databases,
            default_privileges,
            id_allocator,
            items,
            roles,
            schemas,
            settings,
            storage_usage,
            timestamps,
            system_object_mappings,
            system_configurations,
            system_privileges,
        ): (
            Vec<((proto::AuditLogKey, ()), _, _)>,
            Vec<((proto::ClusterKey, proto::ClusterValue), _, _)>,
            Vec<(
                (
                    proto::ClusterIntrospectionSourceIndexKey,
                    proto::ClusterIntrospectionSourceIndexValue,
                ),
                _,
                _,
            )>,
            Vec<((proto::ClusterReplicaKey, proto::ClusterReplicaValue), _, _)>,
            Vec<((proto::CommentKey, proto::CommentValue), _, _)>,
            Vec<((proto::ConfigKey, proto::ConfigValue), _, _)>,
            Vec<((proto::DatabaseKey, proto::DatabaseValue), _, _)>,
            Vec<(
                (proto::DefaultPrivilegesKey, proto::DefaultPrivilegesValue),
                _,
                _,
            )>,
            Vec<((proto::IdAllocKey, proto::IdAllocValue), _, _)>,
            Vec<((proto::ItemKey, proto::ItemValue), _, _)>,
            Vec<((proto::RoleKey, proto::RoleValue), _, _)>,
            Vec<((proto::SchemaKey, proto::SchemaValue), _, _)>,
            Vec<((proto::SettingKey, proto::SettingValue), _, _)>,
            Vec<((proto::StorageUsageKey, ()), _, _)>,
            Vec<((proto::TimestampKey, proto::TimestampValue), _, _)>,
            Vec<((proto::GidMappingKey, proto::GidMappingValue), _, _)>,
            Vec<(
                (
                    proto::ServerConfigurationKey,
                    proto::ServerConfigurationValue,
                ),
                _,
                _,
            )>,
            Vec<(
                (proto::SystemPrivilegesKey, proto::SystemPrivilegesValue),
                _,
                _,
            )>,
        ) = stash
            .with_transaction(|tx| {
                Box::pin(async move {
                    // Peek the catalog collections in any order and a single transaction.
                    futures::try_join!(
                        tx.iter(tx.collection(AUDIT_LOG_COLLECTION.name()).await?),
                        tx.iter(tx.collection(CLUSTER_COLLECTION.name()).await?),
                        tx.iter(
                            tx.collection(CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION.name())
                                .await?,
                        ),
                        tx.iter(tx.collection(CLUSTER_REPLICA_COLLECTION.name()).await?),
                        tx.iter(tx.collection(COMMENTS_COLLECTION.name()).await?),
                        tx.iter(tx.collection(CONFIG_COLLECTION.name()).await?),
                        tx.iter(tx.collection(DATABASES_COLLECTION.name()).await?),
                        tx.iter(tx.collection(DEFAULT_PRIVILEGES_COLLECTION.name()).await?),
                        tx.iter(tx.collection(ID_ALLOCATOR_COLLECTION.name()).await?),
                        tx.iter(tx.collection(ITEM_COLLECTION.name()).await?),
                        tx.iter(tx.collection(ROLES_COLLECTION.name()).await?),
                        tx.iter(tx.collection(SCHEMAS_COLLECTION.name()).await?),
                        tx.iter(tx.collection(SETTING_COLLECTION.name()).await?),
                        tx.iter(tx.collection(STORAGE_USAGE_COLLECTION.name()).await?),
                        tx.iter(tx.collection(TIMESTAMP_COLLECTION.name()).await?),
                        tx.iter(tx.collection(SYSTEM_GID_MAPPING_COLLECTION.name()).await?),
                        tx.iter(
                            tx.collection(SYSTEM_CONFIGURATION_COLLECTION.name())
                                .await?
                        ),
                        tx.iter(tx.collection(SYSTEM_PRIVILEGES_COLLECTION.name()).await?),
                    )
                })
            })
            .await?;

        Ok(Trace {
            audit_log: stringify(audit_log),
            clusters: stringify(clusters),
            introspection_sources: stringify(introspection_sources),
            cluster_replicas: stringify(cluster_replicas),
            comments: stringify(comments),
            configs: stringify(configs),
            databases: stringify(databases),
            default_privileges: stringify(default_privileges),
            id_allocator: stringify(id_allocator),
            items: stringify(items),
            roles: stringify(roles),
            schemas: stringify(schemas),
            settings: stringify(settings),
            storage_usage: stringify(storage_usage),
            timestamps: stringify(timestamps),
            system_object_mappings: stringify(system_object_mappings),
            system_configurations: stringify(system_configurations),
            system_privileges: stringify(system_privileges),
        })
    }

    fn set_catalog_kind(&mut self, catalog_kind: CatalogKind) {
        error!("unable to set catalog kind to {catalog_kind:?}");
    }

    async fn expire(self: Box<Self>) {
        // Nothing to release in the stash.
    }
}

/// Opens a new [`Connection`] to the stash, retrying any retryable errors. Optionally
/// initialize the stash if it has not been initialized and perform any migrations needed.
///
/// # Panics
/// If the inner stash has not been opened.
async fn retry_open(
    mut stash: Stash,
    initial_ts: EpochMillis,
    bootstrap_args: &BootstrapArgs,
    deploy_generation: Option<u64>,
) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
    let retry = Retry::default()
        .clamp_backoff(Duration::from_secs(1))
        .max_duration(Duration::from_secs(30))
        .into_retry_stream();
    let mut retry = pin::pin!(retry);

    loop {
        match open_inner(stash, initial_ts.clone(), bootstrap_args, deploy_generation).await {
            Ok(conn) => {
                return Ok(conn);
            }
            Err((given_stash, err)) => {
                stash = given_stash;
                let should_retry = matches!(&err, CatalogError::Durable(e) if e.should_retry());
                if !should_retry || retry.next().await.is_none() {
                    return Err(err);
                }
            }
        }
    }
}

#[mz_ore::instrument(name = "storage::open_inner", level = "info")]
async fn open_inner(
    mut stash: Stash,
    initial_ts: EpochMillis,
    bootstrap_args: &BootstrapArgs,
    deploy_generation: Option<u64>,
) -> Result<Box<Connection>, (Stash, CatalogError)> {
    // Initialize the Stash if it hasn't been already
    let is_init = match is_stash_initialized(&mut stash).await {
        Ok(is_init) => is_init,
        Err(e) => {
            return Err((stash, e.into()));
        }
    };
    let conn = if !is_init {
        // Initialize the Stash
        let args = bootstrap_args.clone();
        let mut conn = Connection { stash };
        let mut tx = match Transaction::new(&mut conn, Snapshot::empty()) {
            Ok(txn) => txn,
            Err(e) => return Err((conn.stash, e)),
        };
        match initialize::initialize(&mut tx, &args, initial_ts, deploy_generation).await {
            Ok(()) => {}
            Err(e) => return Err((conn.stash, e)),
        }
        match tx.commit().await {
            Ok(()) => {}
            Err(e) => return Err((conn.stash, e)),
        }
        conn
    } else {
        if !stash.is_readonly() {
            // Before we do anything with the Stash, we need to run any pending upgrades and
            // initialize new collections.
            match upgrade(&mut stash).await {
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

    Ok(Box::new(conn))
}

/// Returns whether this Stash is initialized. We consider a Stash to be initialized if
/// it contains an entry in the [`CONFIG_COLLECTION`] with the key of [`USER_VERSION_KEY`].
#[mz_ore::instrument(name = "stash::is_initialized", level = "debug")]
pub async fn is_stash_initialized(stash: &mut Stash) -> Result<bool, StashError> {
    // Check to see what collections exist, this prevents us from unnecessarily creating a
    // config collection, if one doesn't yet exist.
    let collections = stash.collections().await?;
    let exists = collections
        .iter()
        .any(|(_id, name)| name == CONFIG_COLLECTION.name());

    // If our config collection exists, then we'll try to read a version number.
    if exists {
        let items = CONFIG_COLLECTION.iter(stash).await?;
        let contains_version = items
            .into_iter()
            .any(|((key, _value), _ts, _diff)| key.key == USER_VERSION_KEY);
        Ok(contains_version)
    } else {
        Ok(false)
    }
}

async fn get_config(stash: &mut Stash, key: String) -> Result<Option<u64>, CatalogError> {
    let value = CONFIG_COLLECTION
        .peek_key_one(stash, proto::ConfigKey { key })
        .await?
        .map(|v| v.value);

    Ok(value)
}

/// A [`Connection`] represent an open connection to the stash. It exposes optimized methods for
/// executing a single operation against the stash. If the consumer needs to execute multiple
/// operations atomically, then they should start a transaction via [`Connection::transaction`].
#[derive(Debug)]
pub struct Connection {
    stash: Stash,
}

impl Connection {
    #[mz_ore::instrument(level = "debug")]
    async fn set_deploy_generation(&mut self, deploy_generation: u64) -> Result<(), CatalogError> {
        CONFIG_COLLECTION
            .upsert_key(
                &mut self.stash,
                proto::ConfigKey {
                    key: DEPLOY_GENERATION.into(),
                },
                move |_| {
                    Ok::<_, CatalogError>(proto::ConfigValue {
                        value: deploy_generation,
                    })
                },
            )
            .await??;
        Ok(())
    }
}

#[async_trait]
impl ReadOnlyDurableCatalogState for Connection {
    fn epoch(&mut self) -> Epoch {
        self.stash
            .epoch()
            .expect("an opened stash should always have an epoch number")
    }

    async fn expire(self: Box<Self>) {
        // Nothing to release in the stash.
    }

    #[mz_ore::instrument]
    async fn get_audit_logs(&mut self) -> Result<Vec<VersionedEvent>, CatalogError> {
        let entries = AUDIT_LOG_COLLECTION.peek_one(&mut self.stash).await?;
        let logs: Vec<_> = entries
            .into_keys()
            .map(AuditLogKey::from_proto)
            .map_ok(|e| e.event)
            .collect::<Result<_, _>>()?;

        Ok(logs)
    }

    #[mz_ore::instrument(level = "debug")]
    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, CatalogError> {
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

    async fn get_persist_txn_tables(
        &mut self,
    ) -> Result<Option<PersistTxnTablesImpl>, CatalogError> {
        let value = get_config(&mut self.stash, PERSIST_TXN_TABLES.into()).await?;

        value
            .map(PersistTxnTablesImpl::try_from)
            .transpose()
            .map_err(|err| {
                DurableCatalogError::from(TryFromProtoError::UnknownEnumVariant(err.to_string()))
                    .into()
            })
    }

    async fn get_tombstone(&mut self) -> Result<Option<bool>, CatalogError> {
        Ok(get_config(&mut self.stash, TOMBSTONE_KEY.into())
            .await?
            .map(|value| value > 0))
    }

    #[mz_ore::instrument(level = "debug")]
    async fn snapshot(&mut self) -> Result<Snapshot, CatalogError> {
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
        ): (
            BTreeMap<proto::DatabaseKey, proto::DatabaseValue>,
            BTreeMap<proto::SchemaKey, proto::SchemaValue>,
            BTreeMap<proto::RoleKey, proto::RoleValue>,
            BTreeMap<proto::ItemKey, proto::ItemValue>,
            BTreeMap<proto::CommentKey, proto::CommentValue>,
            BTreeMap<proto::ClusterKey, proto::ClusterValue>,
            BTreeMap<proto::ClusterReplicaKey, proto::ClusterReplicaValue>,
            BTreeMap<
                proto::ClusterIntrospectionSourceIndexKey,
                proto::ClusterIntrospectionSourceIndexValue,
            >,
            BTreeMap<proto::IdAllocKey, proto::IdAllocValue>,
            BTreeMap<proto::ConfigKey, proto::ConfigValue>,
            BTreeMap<proto::SettingKey, proto::SettingValue>,
            BTreeMap<proto::TimestampKey, proto::TimestampValue>,
            BTreeMap<proto::GidMappingKey, proto::GidMappingValue>,
            BTreeMap<proto::ServerConfigurationKey, proto::ServerConfigurationValue>,
            BTreeMap<proto::DefaultPrivilegesKey, proto::DefaultPrivilegesValue>,
            BTreeMap<proto::SystemPrivilegesKey, proto::SystemPrivilegesValue>,
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

        Ok(Snapshot {
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
            system_object_mappings: system_gid_mapping,
            system_configurations,
            default_privileges,
            system_privileges,
        })
    }

    #[mz_ore::instrument(level = "debug")]
    async fn whole_migration_snapshot(
        &mut self,
    ) -> Result<(Snapshot, Vec<VersionedEvent>, Vec<VersionedStorageUsage>), CatalogError> {
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
            audit_events,
            storage_usages,
        ): (
            BTreeMap<proto::DatabaseKey, proto::DatabaseValue>,
            BTreeMap<proto::SchemaKey, proto::SchemaValue>,
            BTreeMap<proto::RoleKey, proto::RoleValue>,
            BTreeMap<proto::ItemKey, proto::ItemValue>,
            BTreeMap<proto::CommentKey, proto::CommentValue>,
            BTreeMap<proto::ClusterKey, proto::ClusterValue>,
            BTreeMap<proto::ClusterReplicaKey, proto::ClusterReplicaValue>,
            BTreeMap<
                proto::ClusterIntrospectionSourceIndexKey,
                proto::ClusterIntrospectionSourceIndexValue,
            >,
            BTreeMap<proto::IdAllocKey, proto::IdAllocValue>,
            BTreeMap<proto::ConfigKey, proto::ConfigValue>,
            BTreeMap<proto::SettingKey, proto::SettingValue>,
            BTreeMap<proto::TimestampKey, proto::TimestampValue>,
            BTreeMap<proto::GidMappingKey, proto::GidMappingValue>,
            BTreeMap<proto::ServerConfigurationKey, proto::ServerConfigurationValue>,
            BTreeMap<proto::DefaultPrivilegesKey, proto::DefaultPrivilegesValue>,
            BTreeMap<proto::SystemPrivilegesKey, proto::SystemPrivilegesValue>,
            BTreeMap<proto::AuditLogKey, ()>,
            BTreeMap<proto::StorageUsageKey, ()>,
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
                        tx.peek_one(tx.collection(AUDIT_LOG_COLLECTION.name()).await?),
                        tx.peek_one(tx.collection(STORAGE_USAGE_COLLECTION.name()).await?),
                    )
                })
            })
            .await?;

        let audit_events = audit_events
            .into_keys()
            .map(RustType::from_proto)
            .map_ok(|key: AuditLogKey| key.event)
            .collect::<Result<_, _>>()?;
        let storage_usages = storage_usages
            .into_keys()
            .map(RustType::from_proto)
            .map_ok(|key: StorageUsageKey| key.metric)
            .collect::<Result<_, _>>()?;

        Ok((
            Snapshot {
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
                system_object_mappings: system_gid_mapping,
                system_configurations,
                default_privileges,
                system_privileges,
            },
            audit_events,
            storage_usages,
        ))
    }
}

#[async_trait]
impl DurableCatalogState for Connection {
    fn is_read_only(&self) -> bool {
        self.stash.is_readonly()
    }

    #[mz_ore::instrument(name = "storage::transaction", level = "debug")]
    async fn transaction(&mut self) -> Result<Transaction, CatalogError> {
        let snapshot = self.snapshot().await?;
        Transaction::new(self, snapshot)
    }

    #[mz_ore::instrument(level = "debug")]
    async fn whole_migration_transaction(
        &mut self,
    ) -> Result<(Transaction, Vec<VersionedEvent>, Vec<VersionedStorageUsage>), CatalogError> {
        let (snapshot, audit_events, storage_usages) = self.whole_migration_snapshot().await?;
        let transaction = Transaction::new(self, snapshot)?;
        Ok((transaction, audit_events, storage_usages))
    }

    #[mz_ore::instrument(name = "storage::transaction", level = "debug")]
    async fn commit_transaction(
        &mut self,
        txn_batch: TransactionBatch,
    ) -> Result<(), CatalogError> {
        async fn add_batch<'tx, K, V>(
            tx: &'tx mz_stash::Transaction<'tx>,
            batches: &mut Vec<AppendBatch>,
            typed: &'tx TypedCollection<K, V>,
            changes: &[(K, V, mz_stash::Diff)],
            is_initialized: bool,
        ) -> Result<(), StashError>
        where
            K: mz_stash::Data + 'tx,
            V: mz_stash::Data + 'tx,
        {
            // During initialization we want to commit empty batches to all collections.
            if changes.is_empty() && is_initialized {
                return Ok(());
            }
            let collection = typed.from_tx(tx).await?;
            soft_assert_eq_no_log!(
                is_initialized,
                collection.is_initialized(tx).await?,
                "stash initialization status should match collection '{}' initialization status",
                collection.id
            );
            let mut batch = collection.make_batch_tx(tx).await?;
            for (k, v, diff) in changes {
                collection.append_to_batch(&mut batch, k, v, *diff);
            }
            batches.push(batch);
            Ok(())
        }

        async fn commit_transaction_inner(
            catalog: &mut Connection,
            txn_batch: TransactionBatch,
        ) -> Result<(), CatalogError> {
            // The with_transaction fn below requires a Fn that can be cloned,
            // meaning anything it closes over must be Clone. TransactionBatch
            // implements Clone, thus, the Arcs here aren't strictly necessary.
            // However, using an Arc means that we never clone the TransactionBatch
            // (which would happen at least one time when the txn starts), and
            // instead only clone the Arc.
            let txn_batch = Arc::new(txn_batch);
            let is_initialized = is_stash_initialized(&mut catalog.stash).await?;

            // Before doing anything else, set the connection timeout if it changed.
            if let Some(connection_timeout) = txn_batch.connection_timeout {
                catalog.stash.set_connect_timeout(connection_timeout).await;
            }

            catalog
                .stash
                .with_transaction(move |tx| {
                    Box::pin(async move {
                        let mut batches = Vec::new();

                        add_batch(
                            &tx,
                            &mut batches,
                            &DATABASES_COLLECTION,
                            &txn_batch.databases,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &SCHEMAS_COLLECTION,
                            &txn_batch.schemas,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &ITEM_COLLECTION,
                            &txn_batch.items,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &COMMENTS_COLLECTION,
                            &txn_batch.comments,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &ROLES_COLLECTION,
                            &txn_batch.roles,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &CLUSTER_COLLECTION,
                            &txn_batch.clusters,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &CLUSTER_REPLICA_COLLECTION,
                            &txn_batch.cluster_replicas,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION,
                            &txn_batch.introspection_sources,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &ID_ALLOCATOR_COLLECTION,
                            &txn_batch.id_allocator,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &CONFIG_COLLECTION,
                            &txn_batch.configs,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &SETTING_COLLECTION,
                            &txn_batch.settings,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &TIMESTAMP_COLLECTION,
                            &txn_batch.timestamps,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &SYSTEM_GID_MAPPING_COLLECTION,
                            &txn_batch.system_gid_mapping,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &SYSTEM_CONFIGURATION_COLLECTION,
                            &txn_batch.system_configurations,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &DEFAULT_PRIVILEGES_COLLECTION,
                            &txn_batch.default_privileges,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &SYSTEM_PRIVILEGES_COLLECTION,
                            &txn_batch.system_privileges,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &AUDIT_LOG_COLLECTION,
                            &txn_batch.audit_log_updates,
                            is_initialized,
                        )
                        .await?;
                        add_batch(
                            &tx,
                            &mut batches,
                            &STORAGE_USAGE_COLLECTION,
                            &txn_batch.storage_usage_updates,
                            is_initialized,
                        )
                        .await?;
                        drop(tx.append(batches).await?);

                        Ok(())
                    })
                })
                .await?;

            Ok(())
        }
        self.stash.metrics.catalog_transaction_commits.inc();
        let counter = self
            .stash
            .metrics
            .catalog_transaction_commit_latency_seconds
            .clone();
        commit_transaction_inner(self, txn_batch)
            .wall_time()
            .inc_by(counter)
            .await
    }

    #[mz_ore::instrument(level = "debug")]
    async fn confirm_leadership(&mut self) -> Result<(), CatalogError> {
        Ok(self.stash.confirm_leadership().await?)
    }

    #[mz_ore::instrument]
    async fn get_and_prune_storage_usage(
        &mut self,
        retention_period: Option<Duration>,
        boot_ts: Timestamp,
        wait_for_consolidation: bool,
    ) -> Result<Vec<VersionedStorageUsage>, CatalogError> {
        // If no usage retention period is set, set the cutoff to MIN so nothing
        // is removed.
        let cutoff_ts = match retention_period {
            None => u128::MIN,
            Some(period) => u128::from(boot_ts).saturating_sub(period.as_millis()),
        };
        let is_read_only = self.is_read_only();
        let (events, consolidate_notif) = self
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
                    let consolidate_notif = if retention_period.is_some() && !is_read_only {
                        let notif = tx.append(vec![batch]).await?;
                        Some(notif)
                    } else {
                        None
                    };
                    Ok((events, consolidate_notif))
                })
            })
            .await?;

        // Before we consider the pruning complete, we need to wait for the consolidate request to
        // finish. We wait for consolidation because the storage usage collection is very large and
        // it's possible for it to conflict with other Stash transactions, preventing consolidation
        // from ever completing.
        if wait_for_consolidation {
            if let Some(notif) = consolidate_notif {
                let _ = notif.await;
            }
        }

        Ok(events)
    }

    #[mz_ore::instrument(level = "debug")]
    async fn allocate_id(&mut self, id_type: &str, amount: u64) -> Result<Vec<u64>, CatalogError> {
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
                    None => Err(CatalogError::from(SqlCatalogError::IdExhaustion)),
                }
            })
            .await??;
        let id = prev.expect("must exist").next_id;
        Ok((id..next.next_id).collect())
    }
}

// Debug methods.

/// Manually update value of `key` in collection `T` to `value`.
#[mz_ore::instrument]
pub(crate) async fn debug_edit<T: Collection>(
    stash: &mut Stash,
    key: T::Key,
    value: T::Value,
) -> Result<Option<T::Value>, CatalogError>
where
    T::Key: mz_stash::Data + Clone + 'static,
    T::Value: mz_stash::Data + Clone + 'static,
{
    let stash_collection = T::stash_collection();
    let (prev, _next) = stash_collection
        .upsert_key(stash, key, move |_| Ok::<_, CatalogError>(value))
        .await??;
    Ok(prev)
}

/// Manually delete `key` from collection `T`.
#[mz_ore::instrument]
pub(crate) async fn debug_delete<T: Collection>(
    stash: &mut Stash,
    key: T::Key,
) -> Result<(), CatalogError>
where
    T::Key: mz_stash::Data + Clone + 'static,
    T::Value: mz_stash::Data + Clone,
{
    let stash_collection = T::stash_collection();
    stash_collection
        .delete_keys(stash, BTreeSet::from([key]))
        .await?;
    Ok(())
}

pub const ALL_COLLECTIONS: &[&str] = &[
    AUDIT_LOG_COLLECTION.name(),
    CLUSTER_COLLECTION.name(),
    CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION.name(),
    CLUSTER_REPLICA_COLLECTION.name(),
    COMMENTS_COLLECTION.name(),
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

/// A [`TestOpenableConnection`] represent a struct capable of opening a debug connection to the
/// stash for usage in tests.
#[derive(Debug)]
pub struct TestOpenableConnection<'a> {
    _debug_stash_factory: &'a DebugStashFactory,
    openable_connection: Box<OpenableConnection>,
}

impl TestOpenableConnection<'_> {
    pub(crate) fn new(debug_stash_factory: &DebugStashFactory) -> TestOpenableConnection {
        TestOpenableConnection {
            _debug_stash_factory: debug_stash_factory,
            openable_connection: Box::new(OpenableConnection {
                stash: None,
                config: StashConfig {
                    stash_factory: debug_stash_factory.stash_factory().clone(),
                    stash_url: debug_stash_factory.url().to_string(),
                    schema: Some(debug_stash_factory.schema().to_string()),
                    tls: debug_stash_factory.tls().clone(),
                },
            }),
        }
    }
}

#[async_trait]
impl OpenableDurableCatalogState for TestOpenableConnection<'_> {
    async fn open_savepoint(
        mut self: Box<Self>,
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.openable_connection
            .open_savepoint(
                initial_ts,
                bootstrap_args,
                deploy_generation,
                epoch_lower_bound,
            )
            .await
    }

    async fn open_read_only(
        mut self: Box<Self>,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.openable_connection
            .open_read_only(bootstrap_args)
            .await
    }

    async fn open(
        mut self: Box<Self>,
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.openable_connection
            .open(
                initial_ts,
                bootstrap_args,
                deploy_generation,
                epoch_lower_bound,
            )
            .await
    }

    async fn open_debug(mut self: Box<Self>) -> Result<DebugCatalogState, CatalogError> {
        self.openable_connection.open_debug().await
    }

    async fn is_initialized(&mut self) -> Result<bool, CatalogError> {
        self.openable_connection.is_initialized().await
    }

    async fn epoch(&mut self) -> Result<Epoch, CatalogError> {
        self.openable_connection.epoch().await
    }

    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, CatalogError> {
        self.openable_connection.get_deployment_generation().await
    }

    async fn has_system_config_synced_once(&mut self) -> Result<bool, CatalogError> {
        self.openable_connection
            .has_system_config_synced_once()
            .await
    }

    async fn get_tombstone(&mut self) -> Result<Option<bool>, CatalogError> {
        self.openable_connection.get_tombstone().await
    }

    async fn get_catalog_kind_config(&mut self) -> Result<Option<CatalogKind>, CatalogError> {
        self.openable_connection.get_catalog_kind_config().await
    }

    async fn trace(&mut self) -> Result<Trace, CatalogError> {
        self.openable_connection.trace().await
    }

    fn set_catalog_kind(&mut self, catalog_kind: CatalogKind) {
        self.openable_connection.set_catalog_kind(catalog_kind);
    }

    async fn expire(self: Box<Self>) {
        self.openable_connection.expire().await
    }
}
