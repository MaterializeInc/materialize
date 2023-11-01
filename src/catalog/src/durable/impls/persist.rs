// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use futures::StreamExt;
use itertools::Itertools;
use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_ore::now::EpochMillis;
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
use mz_repr::Diff;
use mz_stash::USER_VERSION_KEY;
use mz_stash_types::objects::proto;
use mz_stash_types::STASH_VERSION;
use mz_storage_types::sources::Timeline;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tracing::debug;
use uuid::Uuid;

use crate::durable::debug::{Collection, DebugCatalogState, Trace};
use crate::durable::initialize::DEPLOY_GENERATION;
use crate::durable::objects::{AuditLogKey, DurableType, Snapshot, StorageUsageKey};
use crate::durable::transaction::TransactionBatch;
use crate::durable::{
    initialize, BootstrapArgs, CatalogError, DurableCatalogError, DurableCatalogState, Epoch,
    OpenableDurableCatalogState, ReadOnlyDurableCatalogState, TimelineTimestamp, Transaction,
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
            // Persist implementation does not use the connection timeout.
            connection_timeout: _,
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
/// Production users should call [`Self::expire`] before dropping a [`PersistHandle`] so that it
/// can expire its leases. If/when rust gets AsyncDrop, this will be done automatically.
#[derive(Debug)]
pub struct PersistHandle {
    /// Write handle to persist.
    write_handle: WriteHandle<StateUpdateKind, (), Timestamp, Diff>,
    /// Read handle to persist.
    read_handle: ReadHandle<StateUpdateKind, (), Timestamp, Diff>,
}

impl PersistHandle {
    /// Deterministically generate the a ID for the given `organization_id` and `seed`.
    fn shard_id(organization_id: Uuid, seed: usize) -> ShardId {
        let hash = sha2::Sha256::digest(format!("{organization_id}{seed}")).to_vec();
        soft_assert_eq!(hash.len(), 32, "SHA256 returns 32 bytes (256 bits)");
        let uuid = Uuid::from_slice(&hash[0..16]).expect("from_slice accepts exactly 16 bytes");
        ShardId::from_str(&format!("s{uuid}")).expect("known to be valid")
    }

    /// Create a new [`PersistHandle`] to the catalog state associated with `organization_id`.
    #[tracing::instrument(level = "debug", skip(persist_client))]
    pub(crate) async fn new(persist_client: PersistClient, organization_id: Uuid) -> PersistHandle {
        const SEED: usize = 1;
        let shard_id = Self::shard_id(organization_id, SEED);
        debug!(?shard_id, "new persist backed catalog state");
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

    #[tracing::instrument(level = "debug", skip(self))]
    async fn open_inner(
        mut self,
        mode: Mode,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let (is_initialized, upper) = self.is_initialized_inner().await;

        if !matches!(mode, Mode::Writable) && !is_initialized {
            return Err(CatalogError::Durable(DurableCatalogError::NotWritable(
                format!("catalog tables do not exist; will not create in {mode:?} mode"),
            )));
        }

        let user_version = self.get_user_version(upper).await;
        let read_only = matches!(mode, Mode::Readonly);

        // Grab the current catalog contents from persist.
        let mut initial_snapshot: Vec<_> = self.snapshot(upper).await.collect();

        // Sniff out the most recent epoch.
        let prev_epoch = if is_initialized {
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
                    Some(epoch)
                }
                _ => unreachable!("checked above"),
            }
        } else {
            None
        };
        let mut current_epoch = prev_epoch.map(|epoch| epoch.get()).unwrap_or(1);
        // Note only writable catalogs attempt to increment the epoch.
        if matches!(mode, Mode::Writable) {
            current_epoch = current_epoch + 1;
        }
        let current_epoch = Epoch::new(current_epoch).expect("known to be non-zero");

        debug!(
            ?is_initialized,
            ?upper,
            ?user_version,
            ?prev_epoch,
            ?current_epoch,
            "open inner"
        );

        let mut catalog = PersistCatalogState {
            mode,
            persist_handle: self,
            upper,
            epoch: current_epoch,
            // Initialize empty in-memory state.
            snapshot: Snapshot::empty(),
            fence: Some(Fence { prev_epoch }),
        };

        let txn = if is_initialized {
            if !read_only {
                let user_version = user_version
                    .ok_or(CatalogError::Durable(DurableCatalogError::Uninitialized))?;
                if user_version != STASH_VERSION {
                    // TODO(jkosh44) Implement migrations.
                    panic!("the persist catalog does not know how to perform migrations yet");
                }
            }

            debug!("initial snapshot: {initial_snapshot:?}");

            // Update in-memory contents with with persist snapshot.
            catalog.apply_updates(initial_snapshot.into_iter())?;

            let mut txn = catalog.transaction().await?;
            if let Some(deploy_generation) = deploy_generation {
                txn.set_config(DEPLOY_GENERATION.into(), deploy_generation)?;
            }
            txn
        } else {
            soft_assert_eq!(
                initial_snapshot,
                Vec::new(),
                "snapshot should not contain anything for an uninitialized catalog"
            );
            let mut txn = catalog.transaction().await?;
            initialize::initialize(&mut txn, bootstrap_args, boot_ts, deploy_generation).await?;
            txn
        };

        if read_only {
            let (txn_batch, _) = txn.into_parts();
            // The upper here doesn't matter because we are only apply the updates in memory.
            let updates = StateUpdate::from_txn_batch(txn_batch, catalog.upper);
            catalog.apply_updates(updates.into_iter())?;
        } else {
            txn.commit().await?;
        }

        Ok(Box::new(catalog))
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
    #[tracing::instrument(level = "debug", skip(self))]
    async fn snapshot(
        &mut self,
        upper: Timestamp,
    ) -> impl Iterator<Item = StateUpdate> + DoubleEndedIterator {
        let snapshot = if upper > Timestamp::minimum() {
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

    /// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
    /// state up to, but not including, `upper`.
    async fn snapshot_unconsolidated(
        &mut self,
        upper: Timestamp,
    ) -> impl Iterator<Item = StateUpdate> + DoubleEndedIterator {
        let snapshot = if upper > Timestamp::minimum() {
            let since = self.read_handle.since().clone();
            let mut as_of = upper.saturating_sub(1);
            as_of.advance_by(since.borrow());
            let mut snapshot = Vec::new();
            let mut stream = Box::pin(
                self.read_handle
                    .snapshot_and_stream(Antichain::from_elem(as_of))
                    .await
                    .expect("we have advanced the restart_as_of by the since"),
            );
            while let Some(update) = stream.next().await {
                snapshot.push(update)
            }
            snapshot
        } else {
            Vec::new()
        };
        snapshot
            .into_iter()
            .map(|((key, _unit), ts, diff)| StateUpdate {
                kind: key.expect("key decoding error"),
                ts,
                diff,
            })
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
    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_user_version(&mut self, upper: Timestamp) -> Option<u64> {
        self.get_config(USER_VERSION_KEY, upper).await
    }

    /// Appends `updates` to the catalog state and downgrades the catalog's upper to `next_upper`
    /// iff the current global upper of the catalog is `current_upper`.
    async fn compare_and_append(
        &mut self,
        updates: Vec<StateUpdate>,
        current_upper: Timestamp,
        next_upper: Timestamp,
    ) -> Result<(), CatalogError> {
        let updates = updates
            .into_iter()
            .map(|update| ((update.kind, ()), update.ts, update.diff));
        match self
            .write_handle
            .compare_and_append(
                updates,
                Antichain::from_elem(current_upper),
                Antichain::from_elem(next_upper),
            )
            .await
            .expect("invalid usage")
        {
            Ok(()) => {
                self.read_handle
                    .downgrade_since(&Antichain::from_elem(current_upper))
                    .await;
                Ok(())
            }
            Err(upper_mismatch) => Err(DurableCatalogError::Fence(format!(
                "current catalog upper {:?} fenced by new catalog upper {:?}",
                upper_mismatch.expected, upper_mismatch.current
            ))
            .into()),
        }
    }
}

#[async_trait]
impl OpenableDurableCatalogState for PersistHandle {
    #[tracing::instrument(level = "debug", skip(self))]
    async fn open_savepoint(
        mut self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_inner(Mode::Savepoint, boot_ts, bootstrap_args, deploy_generation)
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn open_read_only(
        mut self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_inner(Mode::Readonly, boot_ts, bootstrap_args, None)
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn open(
        mut self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_inner(Mode::Writable, boot_ts, bootstrap_args, deploy_generation)
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn open_debug(mut self: Box<Self>) -> Result<DebugCatalogState, CatalogError> {
        Ok(DebugCatalogState::Persist(*self))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn is_initialized(&mut self) -> Result<bool, CatalogError> {
        Ok(self.is_initialized_inner().await.0)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, CatalogError> {
        let upper = self.current_upper().await;
        Ok(self.get_config(DEPLOY_GENERATION, upper).await)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn trace(&mut self) -> Result<Trace, CatalogError> {
        let (is_initialized, upper) = self.is_initialized_inner().await;
        if !is_initialized {
            Err(CatalogError::Durable(DurableCatalogError::Uninitialized))
        } else {
            let snapshot = self.snapshot_unconsolidated(upper).await;
            Ok(Trace::from_snapshot(snapshot))
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
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
    /// `Some` indicates that we need to fence the previous catalog, None indicates that we've
    /// already fenced the previous catalog.
    fence: Option<Fence>,
}

impl PersistCatalogState {
    /// Applies [`StateUpdate`]s to the in memory catalog cache.
    #[tracing::instrument(level = "debug", skip_all)]
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

    #[tracing::instrument(level = "debug", skip(self))]
    async fn expire(self: Box<Self>) {
        self.persist_handle.expire().await
    }

    #[tracing::instrument(level = "debug", skip(self))]
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

    #[tracing::instrument(level = "debug", skip(self))]
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

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, CatalogError> {
        let key = proto::IdAllocKey {
            name: id_type.to_string(),
        };
        self.with_snapshot(|snapshot| {
            Ok(snapshot.id_allocator.get(&key).expect("must exist").next_id)
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn snapshot(&mut self) -> Result<Snapshot, CatalogError> {
        self.with_snapshot(|snapshot| Ok(snapshot.clone())).await
    }
}

#[async_trait]
impl DurableCatalogState for PersistCatalogState {
    fn is_read_only(&self) -> bool {
        matches!(self.mode, Mode::Readonly)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn transaction(&mut self) -> Result<Transaction, CatalogError> {
        let snapshot = self.snapshot().await?;
        Transaction::new(self, snapshot)
    }

    #[tracing::instrument(level = "debug", skip(self))]
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

        let mut updates = StateUpdate::from_txn_batch(txn_batch, current_upper);
        debug!("committing updates: {updates:?}");
        self.apply_updates(updates.clone().into_iter())?;

        if matches!(self.mode, Mode::Writable) {
            // If we haven't fenced the previous catalog state, do that now.
            if let Some(Fence { prev_epoch }) = self.fence.take() {
                debug!(
                    "fencing previous catalogs prev_epoch={:?} current_epoch={:?}",
                    prev_epoch, self.epoch
                );
                if let Some(prev_epoch) = prev_epoch {
                    updates.push(StateUpdate {
                        kind: StateUpdateKind::Epoch(prev_epoch),
                        ts: current_upper,
                        diff: -1,
                    });
                }
                updates.push(StateUpdate {
                    kind: StateUpdateKind::Epoch(self.epoch),
                    ts: current_upper,
                    diff: 1,
                });
            }
            self.persist_handle
                .compare_and_append(updates, current_upper, next_upper)
                .await?;
            self.upper = next_upper;
            debug!(
                "commit successful, upper advance from {:?} to {:?}, since advanced from {:?} to {:?}",
                current_upper,
                next_upper,
                self.persist_handle.read_handle.since(),
                current_upper
            );
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
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

    // TODO(jkosh44) For most modifications we delegate to transactions to avoid duplicate code.
    // This is slightly inefficient because we have to clone an entire snapshot when we usually
    // only need one part of the snapshot. A Potential mitigation against these performance hits is
    // to utilize `CoW`s in `Transaction`s to avoid cloning unnecessary state.

    #[tracing::instrument(level = "debug", skip(self))]
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
                debug!("pruning storage event {event:?}");
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

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_timestamp(
        &mut self,
        timeline: &Timeline,
        timestamp: mz_repr::Timestamp,
    ) -> Result<(), CatalogError> {
        let mut txn = self.transaction().await?;
        txn.set_timestamp(timeline.clone(), timestamp)?;
        txn.commit().await
    }

    #[tracing::instrument(level = "debug", skip(self))]
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

/// Extra metadata needed to fence previous catalog.
#[derive(Debug)]
pub struct Fence {
    /// Previous epoch, if one existed.
    prev_epoch: Option<Epoch>,
}

// Debug methods.
impl Trace {
    /// Generates a [`Trace`] from snapshot.
    fn from_snapshot(snapshot: impl Iterator<Item = StateUpdate>) -> Trace {
        let mut trace = Trace::new();
        for StateUpdate { kind, ts, diff } in snapshot {
            match kind {
                StateUpdateKind::AuditLog(k, v) => {
                    trace.audit_log.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Cluster(k, v) => {
                    trace.clusters.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::ClusterReplica(k, v) => {
                    trace
                        .cluster_replicas
                        .values
                        .push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Comment(k, v) => {
                    trace.comments.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Config(k, v) => {
                    trace.configs.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Database(k, v) => {
                    trace.databases.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::DefaultPrivilege(k, v) => {
                    trace
                        .default_privileges
                        .values
                        .push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Epoch(_) => {
                    // Epoch not included in trace.
                }
                StateUpdateKind::IdAllocator(k, v) => {
                    trace
                        .id_allocator
                        .values
                        .push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::IntrospectionSourceIndex(k, v) => trace
                    .introspection_sources
                    .values
                    .push(((k, v), ts.to_string(), diff)),
                StateUpdateKind::Item(k, v) => {
                    trace.items.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Role(k, v) => {
                    trace.roles.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Schema(k, v) => {
                    trace.schemas.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Setting(k, v) => {
                    trace.settings.values.push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::StorageUsage(k, v) => {
                    trace
                        .storage_usage
                        .values
                        .push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::SystemConfiguration(k, v) => trace
                    .system_configurations
                    .values
                    .push(((k, v), ts.to_string(), diff)),
                StateUpdateKind::SystemObjectMapping(k, v) => trace
                    .system_object_mappings
                    .values
                    .push(((k, v), ts.to_string(), diff)),
                StateUpdateKind::SystemPrivilege(k, v) => {
                    trace
                        .system_privileges
                        .values
                        .push(((k, v), ts.to_string(), diff))
                }
                StateUpdateKind::Timestamp(k, v) => {
                    trace.timestamps.values.push(((k, v), ts.to_string(), diff))
                }
            }
        }
        trace
    }
}

impl PersistHandle {
    /// Manually update value of `key` in collection `T` to `value`.
    #[tracing::instrument(level = "info", skip(self))]
    pub(crate) async fn debug_edit<T: Collection>(
        &mut self,
        key: T::Key,
        value: T::Value,
    ) -> Result<Option<T::Value>, CatalogError>
    where
        T::Key: PartialEq + Eq + Debug + Clone,
        T::Value: Debug + Clone,
    {
        let current_upper = self.current_upper().await;
        let next_upper = current_upper.step_forward();
        let snapshot = self.snapshot(current_upper).await;
        let trace = Trace::from_snapshot(snapshot);
        let collection_trace = T::collection_trace(trace);
        let prev_values: Vec<_> = collection_trace
            .values
            .into_iter()
            .filter(|((k, _), _, diff)| {
                soft_assert_eq!(*diff, 1, "trace is consolidated");
                &key == k
            })
            .collect();

        let prev_value = match &prev_values[..] {
            [] => None,
            [((_, v), _, _)] => Some(v.clone()),
            prev_values => panic!("multiple values found for key {key:?}: {prev_values:?}"),
        };

        let mut updates: Vec<_> = prev_values
            .into_iter()
            .map(|((k, v), _, _)| StateUpdate {
                kind: T::persist_update(k, v),
                ts: current_upper,
                diff: -1,
            })
            .collect();
        updates.push(StateUpdate {
            kind: T::persist_update(key, value),
            ts: current_upper,
            diff: 1,
        });
        self.compare_and_append(updates, current_upper, next_upper)
            .await?;
        Ok(prev_value)
    }

    /// Manually delete `key` from collection `T`.
    #[tracing::instrument(level = "info", skip(self))]
    pub(crate) async fn debug_delete<T: Collection>(
        &mut self,
        key: T::Key,
    ) -> Result<(), CatalogError>
    where
        T::Key: PartialEq + Eq + Debug,
    {
        let current_upper = self.current_upper().await;
        let next_upper = current_upper.step_forward();
        let snapshot = self.snapshot(current_upper).await;
        let trace = Trace::from_snapshot(snapshot);
        let collection_trace = T::collection_trace(trace);
        let retractions = collection_trace
            .values
            .into_iter()
            .filter(|((k, _), _, diff)| {
                soft_assert_eq!(*diff, 1, "trace is consolidated");
                &key == k
            })
            .map(|((k, v), _, _)| StateUpdate {
                kind: T::persist_update(k, v),
                ts: current_upper,
                diff: -1,
            })
            .collect();
        self.compare_and_append(retractions, current_upper, next_upper)
            .await?;
        Ok(())
    }
}
