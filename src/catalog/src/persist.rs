// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::now::NowFn;
use mz_ore::{soft_assert, soft_assert_eq};
use mz_persist_client::read::ReadHandle;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::{
    SimpleDecoder, SimpleEncoder, SimpleSchema, UnitSchema, VecU8Schema,
};
use mz_persist_types::dyn_struct::{ColumnsMut, ColumnsRef, DynStructCfg};
use mz_persist_types::Codec;
use mz_proto::RustType;
use mz_repr::adt::mz_acl_item::MzAclItem;
use mz_repr::role_id::RoleId;
use mz_repr::{Diff, GlobalId};
use mz_stash::USER_VERSION_KEY;
use mz_stash_types::objects::proto;
use mz_stash_types::STASH_VERSION;
use mz_storage_types::sources::Timeline;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use timely::progress::Antichain;
use tracing::debug;
use uuid::Uuid;

use crate::initialize::DEPLOY_GENERATION;
use crate::objects::{
    AuditLogKey, ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue,
    DurableType, IntrospectionSourceIndex, Snapshot, StorageUsageKey, TimestampValue,
};
use crate::transaction::{
    add_new_builtin_cluster_replicas_migration, add_new_builtin_clusters_migration,
    TransactionBatch,
};
use crate::{
    initialize, BootstrapArgs, CatalogError, Cluster, ClusterReplica, Comment, Database,
    DefaultPrivilege, DurableCatalogError, DurableCatalogState, Epoch, OpenableDurableCatalogState,
    ReadOnlyDurableCatalogState, ReplicaConfig, Role, Schema, SystemConfiguration,
    SystemObjectMapping, TimelineTimestamp, Transaction,
};

/// New-type used to represent timestamps in persist.
type Timestamp = mz_repr::Timestamp;

/// Durable catalog mode that dictates the effect of mutable operations.
#[derive(Debug)]
enum Mode {
    /// Mutable operations are prohibited.
    Readonly,
    /// Mutable operations have an effect in-memory, but aren't persisted durably.
    Savepoint,
    /// Mutable operations have an effect in-memory and durably.
    Writable,
}

/// A single update to the catalog state.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateUpdate {
    /// They kind and contents of the state update.
    kind: StateUpdateKind,
    /// The timestamp at which the update occurred.
    ts: Timestamp,
    /// Record count difference for the update.
    diff: Diff,
}

impl StateUpdate {
    /// Convert a [`TransactionBatch`] to a list of [`StateUpdate`]s at timestamp `ts`.
    fn from_txn_batch(txn_batch: TransactionBatch, ts: Timestamp) -> Vec<StateUpdate> {
        fn from_batch<K, V>(
            batch: Vec<(K, V, Diff)>,
            ts: Timestamp,
            kind: fn(K, V) -> StateUpdateKind,
        ) -> impl Iterator<Item = StateUpdate> {
            batch.into_iter().map(move |(k, v, diff)| StateUpdate {
                kind: kind(k, v),
                ts,
                diff,
            })
        }
        let TransactionBatch {
            databases,
            schemas,
            items,
            comments,
            roles,
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
            audit_log_updates,
            storage_usage_updates,
        } = txn_batch;
        let databases = from_batch(databases, ts, StateUpdateKind::Database);
        let schemas = from_batch(schemas, ts, StateUpdateKind::Schema);
        let items = from_batch(items, ts, StateUpdateKind::Item);
        let comments = from_batch(comments, ts, StateUpdateKind::Comment);
        let roles = from_batch(roles, ts, StateUpdateKind::Role);
        let clusters = from_batch(clusters, ts, StateUpdateKind::Cluster);
        let cluster_replicas = from_batch(cluster_replicas, ts, StateUpdateKind::ClusterReplica);
        let introspection_sources = from_batch(
            introspection_sources,
            ts,
            StateUpdateKind::IntrospectionSourceIndex,
        );
        let id_allocators = from_batch(id_allocator, ts, StateUpdateKind::IdAllocator);
        let configs = from_batch(configs, ts, StateUpdateKind::Config);
        let settings = from_batch(settings, ts, StateUpdateKind::Setting);
        let timestamps = from_batch(timestamps, ts, StateUpdateKind::Timestamp);
        let system_object_mappings =
            from_batch(system_gid_mapping, ts, StateUpdateKind::SystemObjectMapping);
        let system_configurations = from_batch(
            system_configurations,
            ts,
            StateUpdateKind::SystemConfiguration,
        );
        let default_privileges =
            from_batch(default_privileges, ts, StateUpdateKind::DefaultPrivilege);
        let system_privileges = from_batch(system_privileges, ts, StateUpdateKind::SystemPrivilege);
        let audit_logs = from_batch(audit_log_updates, ts, StateUpdateKind::AuditLog);
        let storage_usage_updates =
            from_batch(storage_usage_updates, ts, StateUpdateKind::StorageUsage);

        databases
            .chain(schemas)
            .chain(items)
            .chain(comments)
            .chain(roles)
            .chain(clusters)
            .chain(cluster_replicas)
            .chain(introspection_sources)
            .chain(id_allocators)
            .chain(configs)
            .chain(settings)
            .chain(timestamps)
            .chain(system_object_mappings)
            .chain(system_configurations)
            .chain(default_privileges)
            .chain(system_privileges)
            .chain(audit_logs)
            .chain(storage_usage_updates)
            .collect()
    }
}

/// The contents of a single state update.
// TODO(jkosh44) Remove Serde when we switch to proto fully.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StateUpdateKind {
    AuditLog(proto::AuditLogKey, ()),
    Cluster(proto::ClusterKey, proto::ClusterValue),
    ClusterReplica(proto::ClusterReplicaKey, proto::ClusterReplicaValue),
    Comment(proto::CommentKey, proto::CommentValue),
    Config(proto::ConfigKey, proto::ConfigValue),
    Database(proto::DatabaseKey, proto::DatabaseValue),
    DefaultPrivilege(proto::DefaultPrivilegesKey, proto::DefaultPrivilegesValue),
    Epoch(Epoch),
    IdAllocator(proto::IdAllocKey, proto::IdAllocValue),
    IntrospectionSourceIndex(
        proto::ClusterIntrospectionSourceIndexKey,
        proto::ClusterIntrospectionSourceIndexValue,
    ),
    Item(proto::ItemKey, proto::ItemValue),
    Role(proto::RoleKey, proto::RoleValue),
    Schema(proto::SchemaKey, proto::SchemaValue),
    Setting(proto::SettingKey, proto::SettingValue),
    StorageUsage(proto::StorageUsageKey, ()),
    SystemConfiguration(
        proto::ServerConfigurationKey,
        proto::ServerConfigurationValue,
    ),
    SystemObjectMapping(proto::GidMappingKey, proto::GidMappingValue),
    SystemPrivilege(proto::SystemPrivilegesKey, proto::SystemPrivilegesValue),
    Timestamp(proto::TimestampKey, proto::TimestampValue),
}

// TODO(jkosh44) As an initial implementation we simply serialize and deserialize everything as a
// JSON. Before enabling in production, we'd like to utilize Protobuf to serialize and deserialize
// everything.
impl Codec for StateUpdateKind {
    type Schema = VecU8Schema;

    fn codec_name() -> String {
        "StateUpdateJson".to_string()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        let bytes = serde_json::to_vec(&self).expect("failed to encode StateUpdate");
        buf.put(bytes.as_slice());
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        serde_json::from_slice(buf).map_err(|err| err.to_string())
    }
}

impl mz_persist_types::columnar::Schema<StateUpdateKind> for VecU8Schema {
    type Encoder<'a> = SimpleEncoder<'a, StateUpdateKind, Vec<u8>>;

    type Decoder<'a> = SimpleDecoder<'a, StateUpdateKind, Vec<u8>>;

    fn columns(&self) -> DynStructCfg {
        SimpleSchema::<StateUpdateKind, Vec<u8>>::columns(&())
    }

    fn decoder<'a>(&self, cols: ColumnsRef<'a>) -> Result<Self::Decoder<'a>, String> {
        SimpleSchema::<StateUpdateKind, Vec<u8>>::decoder(cols, |val, ret| {
            *ret = StateUpdateKind::decode(val).expect("should be valid StateUpdateKind")
        })
    }

    fn encoder<'a>(&self, cols: ColumnsMut<'a>) -> Result<Self::Encoder<'a>, String> {
        SimpleSchema::<StateUpdateKind, Vec<u8>>::push_encoder(cols, |col, val| {
            let mut buf = Vec::new();
            StateUpdateKind::encode(val, &mut buf);
            mz_persist_types::columnar::ColumnPush::<Vec<u8>>::push(col, &buf)
        })
    }
}

/// Handles and metadata needed to interact with persist.
///
/// Production users should call [`Self::_expire`] before dropping a [`PersistHandle`] so that it
/// can expire its leases. If/when rust gets AsyncDrop, this will be done automatically.
#[derive(Debug)]
pub struct PersistHandle {
    /// Write handle to persist.
    write_handle: WriteHandle<StateUpdateKind, (), Timestamp, Diff>,
    /// Read handle to persist.
    read_handle: ReadHandle<StateUpdateKind, (), Timestamp, Diff>,
}

impl PersistHandle {
    /// Create a new [`PersistHandle`] to the catalog state associated with `environment_id`.
    pub(crate) async fn new(persist_client: PersistClient, environment_id: Uuid) -> PersistHandle {
        // TODO(jkosh44) Using the environment ID directly is sufficient for correctness purposes.
        // However, for observability reasons, it would be much more readble if the shard ID was
        // constructed as `format!("s{}CATALOG", hash(environment_id))`.
        let shard_id = ShardId::from_str(&format!("s{environment_id}")).expect("known to be valid");
        let (write_handle, read_handle) = persist_client
            .open(
                shard_id,
                Arc::new(VecU8Schema::default()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: "catalog".to_string(),
                    handle_purpose: "durable catalog state".to_string(),
                },
            )
            .await
            .expect("invalid usage");
        PersistHandle {
            write_handle,
            read_handle,
        }
    }

    async fn open_inner(
        mut self,
        mode: Mode,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<PersistCatalogState, CatalogError> {
        let (is_initialized, upper) = self.is_initialized_inner().await;

        if !matches!(mode, Mode::Writable) && !is_initialized {
            return Err(CatalogError::Durable(DurableCatalogError::NotWritable(
                format!("catalog tables do not exist; will not create in {mode:?} mode"),
            )));
        }

        let version = self.get_user_version(upper).await;
        let read_only = matches!(mode, Mode::Readonly);

        // Grab the current catalog contents from persist.
        let mut initial_snapshot: Vec<_> = self.snapshot(upper).await.collect();
        // Sniff out the most recent epoch.
        let mut epoch = if is_initialized {
            let epoch_idx = initial_snapshot
                .iter()
                .rposition(|update| {
                    matches!(
                        update,
                        StateUpdate {
                            kind: StateUpdateKind::Epoch(_),
                            ..
                        }
                    )
                })
                .expect("initialized catalog must have an epoch");
            match initial_snapshot.remove(epoch_idx) {
                StateUpdate {
                    kind: StateUpdateKind::Epoch(epoch),
                    diff,
                    ..
                } => {
                    soft_assert_eq!(diff, 1);
                    epoch
                }
                _ => unreachable!("checked above"),
            }
        } else {
            Epoch::new(1).expect("known to be non-zero")
        };
        // Note only writable catalogs attempt to increment the epoch.
        if matches!(mode, Mode::Writable) {
            epoch = Epoch::new(epoch.get() + 1).expect("known to be non-zero");
        }

        let mut catalog = PersistCatalogState {
            mode,
            persist_handle: self,
            upper,
            epoch,
            // Initialize empty in-memory state.
            snapshot: Snapshot::empty(),
            fenced: false,
        };

        let mut txn = if is_initialized {
            if !read_only {
                let version =
                    version.ok_or(CatalogError::Durable(DurableCatalogError::Uninitialized))?;
                if version != STASH_VERSION {
                    // TODO(jkosh44) Implement migrations.
                    panic!("the persist catalog does not know how to perform migrations yet");
                }
            }

            // Update in-memory contents with with persist snapshot.
            catalog.apply_updates(initial_snapshot.into_iter())?;

            let mut txn = catalog.transaction().await?;
            if let Some(deploy_generation) = deploy_generation {
                txn.set_config(DEPLOY_GENERATION.into(), deploy_generation)?;
            }
            txn
        } else {
            // Get the current timestamp so we can record when we booted. We don't have to worry
            // about `boot_ts` being less than a previously used timestamp because the catalog is
            // uninitialized and there are no previous timestamps.
            let boot_ts = now();

            let mut txn = catalog.transaction().await?;
            initialize::initialize(&mut txn, bootstrap_args, boot_ts, deploy_generation).await?;
            txn
        };

        if !read_only {
            add_new_builtin_clusters_migration(&mut txn)?;
            add_new_builtin_cluster_replicas_migration(&mut txn, bootstrap_args)?;
        }

        if read_only {
            let (txn_batch, _) = txn.into_parts();
            // The upper here doesn't matter because we are only apply the updates in memory.
            let updates = StateUpdate::from_txn_batch(txn_batch, catalog.upper);
            catalog.apply_updates(updates.into_iter())?;
        } else {
            txn.commit().await?;
        }

        Ok(catalog)
    }

    /// Fetch the current upper of the catalog state.
    // TODO(jkosh44) This isn't actually guaranteed to be linearizable. Before enabling this in
    //  production we need a new linearizable solution.
    async fn current_upper(&mut self) -> Timestamp {
        self.write_handle
            .fetch_recent_upper()
            .await
            .as_option()
            .cloned()
            .expect("we use a totally ordered time and never finalize the shard")
    }

    /// Reports if the catalog state has been initialized, and the current upper.
    async fn is_initialized_inner(&mut self) -> (bool, Timestamp) {
        let upper = self.current_upper().await;
        (upper > 0.into(), upper)
    }

    /// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
    /// state up to, but not including, `upper`.
    ///
    /// The output is consolidated and sorted by timestamp in ascending order.
    async fn snapshot(
        &mut self,
        upper: Timestamp,
    ) -> impl Iterator<Item = StateUpdate> + DoubleEndedIterator {
        let snapshot = if upper > 0.into() {
            let since = self.read_handle.since().clone();
            let mut as_of = upper.saturating_sub(1);
            as_of.advance_by(since.borrow());
            self.read_handle
                .snapshot_and_fetch(Antichain::from_elem(as_of))
                .await
                .expect("we have advanced the restart_as_of by the since")
        } else {
            Vec::new()
        };
        soft_assert!(
            snapshot.iter().all(|(_, _, diff)| *diff == 1),
            "snapshot_and_fetch guarantees a consolidated result"
        );
        snapshot
            .into_iter()
            .map(|((key, _unit), ts, diff)| StateUpdate {
                kind: key.expect("key decoding error"),
                ts,
                diff,
            })
            .sorted_by(|a, b| Ord::cmp(&b.ts, &a.ts))
    }

    /// Get value of config `key`.
    ///
    /// Some configs need to be read before the catalog is opened for bootstrapping.
    async fn get_config(&mut self, key: &str, upper: Timestamp) -> Option<u64> {
        let mut configs: Vec<_> = self
            .snapshot(upper)
            .await
            .rev()
            .filter_map(
                |StateUpdate {
                     kind,
                     ts: _,
                     diff: _,
                 }| match kind {
                    StateUpdateKind::Config(k, v) if k.key == key => Some(v.value),
                    _ => None,
                },
            )
            .collect();
        soft_assert!(
            configs.len() <= 1,
            "multiple configs should not share the same key: {configs:?}"
        );
        configs.pop()
    }

    /// Get the user version of this instance.
    ///
    /// The user version is used to determine if a migration is needed.
    async fn get_user_version(&mut self, upper: Timestamp) -> Option<u64> {
        self.get_config(USER_VERSION_KEY, upper).await
    }
}

#[async_trait]
impl OpenableDurableCatalogState<PersistCatalogState> for PersistHandle {
    async fn open_savepoint(
        mut self,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<PersistCatalogState, CatalogError> {
        self.open_inner(Mode::Savepoint, now, bootstrap_args, deploy_generation)
            .await
    }

    async fn open_read_only(
        mut self,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<PersistCatalogState, CatalogError> {
        self.open_inner(Mode::Readonly, now, bootstrap_args, None)
            .await
    }

    async fn open(
        mut self,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<PersistCatalogState, CatalogError> {
        self.open_inner(Mode::Writable, now, bootstrap_args, deploy_generation)
            .await
    }

    async fn is_initialized(&mut self) -> Result<bool, CatalogError> {
        Ok(self.is_initialized_inner().await.0)
    }

    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, CatalogError> {
        let upper = self.current_upper().await;
        Ok(self.get_config(DEPLOY_GENERATION, upper).await)
    }

    async fn expire(self) {
        self.read_handle.expire().await;
        self.write_handle.expire().await;
    }
}

/// A durable store of the catalog state using Persist as an implementation.
#[derive(Debug)]
pub struct PersistCatalogState {
    /// The [`Mode`] that this catalog was opened in.
    mode: Mode,
    /// Handles to the persist shard containing the catalog.
    persist_handle: PersistHandle,
    /// The current upper of the persist shard.
    upper: Timestamp,
    /// The epoch of this catalog.
    epoch: Epoch,
    /// A cache of the entire catalogs state.
    snapshot: Snapshot,
    /// True if this catalog has fenced out older catalogs, false otherwise.
    fenced: bool,
}

impl PersistCatalogState {
    /// Applies [`StateUpdate`]s to the in memory catalog cache.
    fn apply_updates(
        &mut self,
        updates: impl Iterator<Item = StateUpdate>,
    ) -> Result<(), DurableCatalogError> {
        fn apply<K: Ord, V: Ord>(map: &mut BTreeMap<K, V>, key: K, value: V, diff: Diff) {
            if diff == 1 {
                map.insert(key, value);
            } else if diff == -1 {
                map.remove(&key);
            }
        }

        for update in updates {
            debug!("applying catalog update: {update:?}");
            match update {
                StateUpdate { kind, ts: _, diff } if diff == 1 || diff == -1 => match kind {
                    StateUpdateKind::AuditLog(_, _) => {
                        // We can ignore audit log updates since it's not cached in memory.
                    }
                    StateUpdateKind::Cluster(key, value) => {
                        apply(&mut self.snapshot.clusters, key, value, diff);
                    }
                    StateUpdateKind::ClusterReplica(key, value) => {
                        apply(&mut self.snapshot.cluster_replicas, key, value, diff);
                    }
                    StateUpdateKind::Comment(key, value) => {
                        apply(&mut self.snapshot.comments, key, value, diff);
                    }
                    StateUpdateKind::Config(key, value) => {
                        apply(&mut self.snapshot.configs, key, value, diff);
                    }
                    StateUpdateKind::Database(key, value) => {
                        apply(&mut self.snapshot.databases, key, value, diff);
                    }
                    StateUpdateKind::DefaultPrivilege(key, value) => {
                        apply(&mut self.snapshot.default_privileges, key, value, diff);
                    }
                    StateUpdateKind::Epoch(epoch) => {
                        return Err(DurableCatalogError::Fence(format!(
                            "current catalog epoch {} fenced by new catalog epoch {}",
                            self.epoch, epoch
                        )));
                    }
                    StateUpdateKind::IdAllocator(key, value) => {
                        apply(&mut self.snapshot.id_allocator, key, value, diff);
                    }
                    StateUpdateKind::IntrospectionSourceIndex(key, value) => {
                        apply(&mut self.snapshot.introspection_sources, key, value, diff);
                    }
                    StateUpdateKind::Item(key, value) => {
                        apply(&mut self.snapshot.items, key, value, diff);
                    }
                    StateUpdateKind::Role(key, value) => {
                        apply(&mut self.snapshot.roles, key, value, diff);
                    }
                    StateUpdateKind::Schema(key, value) => {
                        apply(&mut self.snapshot.schemas, key, value, diff);
                    }
                    StateUpdateKind::Setting(key, value) => {
                        apply(&mut self.snapshot.settings, key, value, diff);
                    }
                    StateUpdateKind::StorageUsage(_, _) => {
                        // We can ignore storage usage since it's not cached in memory.
                    }
                    StateUpdateKind::SystemConfiguration(key, value) => {
                        apply(&mut self.snapshot.system_configurations, key, value, diff);
                    }
                    StateUpdateKind::SystemObjectMapping(key, value) => {
                        apply(&mut self.snapshot.system_object_mappings, key, value, diff);
                    }
                    StateUpdateKind::SystemPrivilege(key, value) => {
                        apply(&mut self.snapshot.system_privileges, key, value, diff);
                    }
                    StateUpdateKind::Timestamp(key, value) => {
                        apply(&mut self.snapshot.timestamps, key, value, diff);
                    }
                },
                invalid_update => {
                    panic!("invalid update in consolidated trace: {:?}", invalid_update);
                }
            }
        }

        Ok(())
    }

    /// Execute and return the results of `f` on the current catalog snapshot.
    ///
    /// Will return an error if the catalog has been fenced out.
    async fn with_snapshot<T>(
        &mut self,
        f: impl FnOnce(&Snapshot) -> Result<T, CatalogError>,
    ) -> Result<T, CatalogError> {
        self.confirm_leadership().await?;
        f(&self.snapshot)
    }
}

#[async_trait]
impl ReadOnlyDurableCatalogState for PersistCatalogState {
    fn epoch(&mut self) -> Epoch {
        self.epoch
    }

    async fn expire(self: Box<Self>) {
        self.persist_handle.expire().await
    }

    async fn get_catalog_content_version(&mut self) -> Result<Option<String>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .settings
                .get(&proto::SettingKey {
                    name: "catalog_content_version".to_string(),
                })
                .map(|value| value.value.clone()))
        })
        .await
    }

    async fn get_clusters(&mut self) -> Result<Vec<Cluster>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .clusters
                .clone()
                .into_iter()
                .map(RustType::from_proto)
                .map_ok(|(k, v)| Cluster::from_key_value(k, v))
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    async fn get_cluster_replicas(&mut self) -> Result<Vec<ClusterReplica>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .cluster_replicas
                .clone()
                .into_iter()
                .map(RustType::from_proto)
                .map_ok(|(k, v)| ClusterReplica::from_key_value(k, v))
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    async fn get_databases(&mut self) -> Result<Vec<Database>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .databases
                .clone()
                .into_iter()
                .map(RustType::from_proto)
                .map_ok(|(k, v)| Database::from_key_value(k, v))
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    async fn get_schemas(&mut self) -> Result<Vec<Schema>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .schemas
                .clone()
                .into_iter()
                .map(RustType::from_proto)
                .map_ok(|(k, v)| Schema::from_key_value(k, v))
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    async fn get_system_items(&mut self) -> Result<Vec<SystemObjectMapping>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .system_object_mappings
                .clone()
                .into_iter()
                .map(RustType::from_proto)
                .map_ok(|(k, v)| SystemObjectMapping::from_key_value(k, v))
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    async fn get_introspection_source_indexes(
        &mut self,
        cluster_id: ClusterId,
    ) -> Result<BTreeMap<String, GlobalId>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .introspection_sources
                .clone()
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
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    async fn get_roles(&mut self) -> Result<Vec<Role>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .roles
                .clone()
                .into_iter()
                .map(RustType::from_proto)
                .map_ok(|(k, v)| Role::from_key_value(k, v))
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    async fn get_default_privileges(&mut self) -> Result<Vec<DefaultPrivilege>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .default_privileges
                .clone()
                .into_iter()
                .map(RustType::from_proto)
                .map_ok(|(k, v)| DefaultPrivilege::from_key_value(k, v))
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    async fn get_system_privileges(&mut self) -> Result<Vec<MzAclItem>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .system_privileges
                .clone()
                .into_iter()
                .map(RustType::from_proto)
                .map_ok(|(k, v)| MzAclItem::from_key_value(k, v))
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    async fn get_system_configurations(
        &mut self,
    ) -> Result<Vec<SystemConfiguration>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .system_configurations
                .clone()
                .into_iter()
                .map(RustType::from_proto)
                .map_ok(|(k, v)| SystemConfiguration::from_key_value(k, v))
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    async fn get_comments(&mut self) -> Result<Vec<Comment>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .comments
                .clone()
                .into_iter()
                .map(RustType::from_proto)
                .map_ok(|(k, v)| Comment::from_key_value(k, v))
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    async fn get_timestamps(&mut self) -> Result<Vec<TimelineTimestamp>, CatalogError> {
        self.with_snapshot(|snapshot| {
            Ok(snapshot
                .timestamps
                .clone()
                .into_iter()
                .map(RustType::from_proto)
                .map_ok(|(k, v)| TimelineTimestamp::from_key_value(k, v))
                .collect::<Result<_, _>>()?)
        })
        .await
    }

    async fn get_timestamp(
        &mut self,
        timeline: &Timeline,
    ) -> Result<Option<mz_repr::Timestamp>, CatalogError> {
        let key = proto::TimestampKey {
            id: timeline.to_string(),
        };
        self.with_snapshot(|snapshot| {
            let val: Option<TimestampValue> = snapshot
                .timestamps
                .get(&key)
                .cloned()
                .map(RustType::from_proto)
                .transpose()?;
            Ok(val.map(|v| v.ts))
        })
        .await
    }

    async fn get_audit_logs(&mut self) -> Result<Vec<VersionedEvent>, CatalogError> {
        self.confirm_leadership().await?;
        // This is only called during bootstrapping and we don't want to cache all
        // audit logs in memory because they can grow quite large. Therefore, we
        // go back to persist and grab everything again.
        Ok(self
            .persist_handle
            .snapshot(self.upper)
            .await
            .filter_map(
                |StateUpdate {
                     kind,
                     ts: _,
                     diff: _,
                 }| match kind {
                    StateUpdateKind::AuditLog(key, ()) => Some(key),
                    _ => None,
                },
            )
            .map(RustType::from_proto)
            .map_ok(|key: AuditLogKey| key.event)
            .collect::<Result<_, _>>()?)
    }

    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, CatalogError> {
        let key = proto::IdAllocKey {
            name: id_type.to_string(),
        };
        self.with_snapshot(|snapshot| {
            Ok(snapshot.id_allocator.get(&key).expect("must exist").next_id)
        })
        .await
    }

    async fn snapshot(&mut self) -> Result<Snapshot, CatalogError> {
        self.with_snapshot(|snapshot| Ok(snapshot.clone())).await
    }
}

#[async_trait]
impl DurableCatalogState for PersistCatalogState {
    fn is_read_only(&self) -> bool {
        matches!(self.mode, Mode::Readonly)
    }

    async fn transaction(&mut self) -> Result<Transaction, CatalogError> {
        let snapshot = self.snapshot().await?;
        Transaction::new(self, snapshot)
    }

    async fn commit_transaction(
        &mut self,
        txn_batch: TransactionBatch,
    ) -> Result<(), CatalogError> {
        if self.is_read_only() {
            return Err(DurableCatalogError::NotWritable(
                "cannot commit a transaction in a read-only catalog".to_string(),
            )
            .into());
        }

        let current_upper = self.upper.clone();
        let next_upper = current_upper.step_forward();

        let updates = StateUpdate::from_txn_batch(txn_batch, current_upper);
        self.apply_updates(updates.clone().into_iter())?;

        if matches!(self.mode, Mode::Writable) {
            let mut batch_builder = self
                .persist_handle
                .write_handle
                .builder(Antichain::from_elem(current_upper));

            for StateUpdate { kind, ts, diff } in updates {
                batch_builder
                    .add(&kind, &(), &ts, &diff)
                    .await
                    .expect("invalid usage");
            }
            // If we haven't fenced the previous catalog state, do that now.
            if !self.fenced {
                batch_builder
                    .add(&StateUpdateKind::Epoch(self.epoch), &(), &current_upper, &1)
                    .await
                    .expect("invalid usage");
            }
            let mut batch = batch_builder
                .finish(Antichain::from_elem(next_upper))
                .await
                .expect("invalid usage");
            match self
                .persist_handle
                .write_handle
                .compare_and_append_batch(
                    &mut [&mut batch],
                    Antichain::from_elem(current_upper),
                    Antichain::from_elem(next_upper),
                )
                .await
                .expect("invalid usage")
            {
                Ok(()) => {
                    self.persist_handle
                        .read_handle
                        .downgrade_since(&Antichain::from_elem(current_upper))
                        .await;
                    self.upper = next_upper;
                }
                Err(upper_mismatch) => {
                    return Err(DurableCatalogError::Fence(format!(
                        "current catalog upper {:?} fenced by new catalog upper {:?}",
                        upper_mismatch.expected, upper_mismatch.current
                    ))
                    .into())
                }
            }
        }

        self.fenced = true;
        Ok(())
    }

    async fn confirm_leadership(&mut self) -> Result<(), CatalogError> {
        let upper = self.persist_handle.current_upper().await;
        if upper == self.upper {
            Ok(())
        } else {
            Err(DurableCatalogError::Fence(format!(
                "current catalog upper {:?} fenced by new catalog upper {:?}",
                self.upper, upper
            ))
            .into())
        }
    }

    async fn set_connect_timeout(&mut self, _connect_timeout: Duration) {
        // Persist is able to set this timeout internally so this is a no-op. The connect timeout
        // passed to this function in production and the timeout Persist uses are both derived
        // from the "crdb_connect_timeout" system variable.
    }

    // TODO(jkosh44) For most modifications we delegate to transactions to avoid duplicate code.
    // This is slightly inefficient because we have to clone an entire snapshot when we usually
    // only need one part of the snapshot. This is mostly OK because most non-transaction
    // modifications are only called once during bootstrap. The exceptions are `allocate_id` and
    // `set_timestamp` Potential mitigations against these performance hits are:
    //
    //   - Perform all modifications during bootstrap in a single transaction.
    //   - Utilize `CoW`s in `Transaction`s to avoid cloning unnecessary state.

    async fn set_catalog_content_version(&mut self, new_version: &str) -> Result<(), CatalogError> {
        let mut txn = self.transaction().await?;
        txn.set_setting(
            "catalog_content_version".to_string(),
            Some(new_version.to_string()),
        )?;
        txn.commit().await
    }

    async fn get_and_prune_storage_usage(
        &mut self,
        retention_period: Option<Duration>,
        boot_ts: mz_repr::Timestamp,
    ) -> Result<Vec<VersionedStorageUsage>, CatalogError> {
        // If no usage retention period is set, set the cutoff to MIN so nothing
        // is removed.
        let cutoff_ts = match retention_period {
            None => u128::MIN,
            Some(period) => u128::from(boot_ts).saturating_sub(period.as_millis()),
        };
        let storage_usage = self
            .persist_handle
            .snapshot(self.upper)
            .await
            .filter_map(
                |StateUpdate {
                     kind,
                     ts: _,
                     diff: _,
                 }| match kind {
                    StateUpdateKind::StorageUsage(key, ()) => Some(key),
                    _ => None,
                },
            )
            .map(RustType::from_proto)
            .map_ok(|key: StorageUsageKey| key.metric);
        let mut events = Vec::new();
        let mut expired = Vec::new();

        for event in storage_usage {
            let event = event?;
            if u128::from(event.timestamp()) >= cutoff_ts {
                events.push(event);
            } else if retention_period.is_some() {
                expired.push(event);
            }
        }

        if !self.is_read_only() {
            let mut txn = self.transaction().await?;
            txn.remove_storage_usage_events(expired);
            txn.commit().await?;
        } else {
            self.confirm_leadership().await?;
        }

        Ok(events)
    }

    async fn set_system_items(
        &mut self,
        mappings: Vec<SystemObjectMapping>,
    ) -> Result<(), CatalogError> {
        let mut txn = self.transaction().await?;
        for mapping in mappings {
            txn.set_system_object_mapping(mapping)?;
        }
        txn.commit().await
    }

    async fn set_introspection_source_indexes(
        &mut self,
        mappings: Vec<IntrospectionSourceIndex>,
    ) -> Result<(), CatalogError> {
        let mut txn = self.transaction().await?;
        for mapping in mappings {
            txn.set_introspection_source_index(mapping)?;
        }
        txn.commit().await
    }

    async fn set_replica_config(
        &mut self,
        replica_id: ReplicaId,
        cluster_id: ClusterId,
        name: String,
        config: ReplicaConfig,
        owner_id: RoleId,
    ) -> Result<(), CatalogError> {
        let mut txn = self.transaction().await?;
        txn.set_replica(replica_id, cluster_id, name, config, owner_id)?;
        txn.commit().await
    }

    async fn set_timestamp(
        &mut self,
        timeline: &Timeline,
        timestamp: mz_repr::Timestamp,
    ) -> Result<(), CatalogError> {
        let mut txn = self.transaction().await?;
        txn.set_timestamp(timeline.clone(), timestamp)?;
        txn.commit().await
    }

    async fn set_deploy_generation(&mut self, deploy_generation: u64) -> Result<(), CatalogError> {
        let mut txn = self.transaction().await?;
        txn.set_config(DEPLOY_GENERATION.into(), deploy_generation)?;
        txn.commit().await
    }

    async fn allocate_id(&mut self, id_type: &str, amount: u64) -> Result<Vec<u64>, CatalogError> {
        if amount == 0 {
            return Ok(Vec::new());
        }
        let mut txn = self.transaction().await?;
        let ids = txn.get_and_increment_id_by(id_type.to_string(), amount)?;
        txn.commit().await?;
        Ok(ids)
    }
}
