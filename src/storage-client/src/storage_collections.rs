// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An abstraction for dealing with storage collections.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::num::NonZeroI64;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{Future, FutureExt, StreamExt};
use itertools::Itertools;

use mz_ore::collections::CollectionExt;
use mz_ore::instrument;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::txn::TxnsCodec;
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, RelationDesc, TimestampManipulation};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::inline::InlinedConnection;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::{
    CollectionMetadata, StorageError, TxnWalTablesImpl, TxnsCodecRow,
};
use mz_storage_types::dyncfgs::STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_holds::{ReadHold, ReadHoldError};
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sources::{
    ExportReference, GenericSourceConnection, IngestionDescription, SourceData, SourceDesc,
    SourceExport,
};
use mz_txn_wal::metrics::Metrics as TxnMetrics;
use mz_txn_wal::txn_read::{DataSnapshot, TxnsRead};
use mz_txn_wal::txns::TxnsHandle;
use timely::order::TotalOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp as TimelyTimestamp};
use timely::PartialOrder;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, trace, warn};

use crate::controller::{
    CollectionDescription, DataSource, DataSourceOther, PersistEpoch, StorageMetadata, StorageTxn,
};

/// An abstraction for keeping track of storage collections and managing access
/// to them.
///
/// Responsibilities:
///
/// - Keeps a critical persist handle for holding the since of collections
///   where it need to be.
///
/// - Drives the since forward based on the upper of a collection and a
///   [ReadPolicy].
///
/// - Hands out [ReadHolds](ReadHold) that prevent a collection's since from
/// advancing while it needs to be read at a specific time.
#[async_trait]
pub trait StorageCollections: Debug {
    type Timestamp: TimelyTimestamp;

    /// On boot, reconcile this [StorageCollections] with outside state. We get
    /// a [StorageTxn] where we can record any durable state that we need.
    ///
    /// We get `init_ids`, which tells us about all collections that currently
    /// exist, so that we can record durable state for those that _we_ don't
    /// know yet about.
    ///
    /// We also get `drop_ids`, which tells us about all collections that we
    /// might have known about before and have now been dropped.
    async fn initialize_state(
        &self,
        txn: &mut (dyn StorageTxn<Self::Timestamp> + Send),
        init_ids: BTreeSet<GlobalId>,
        drop_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Update storage configuration with new parameters.
    fn update_parameters(&self, config_params: StorageParameters);

    /// Returns the [CollectionMetadata] of the collection identified by `id`.
    fn collection_metadata(
        &self,
        id: GlobalId,
    ) -> Result<CollectionMetadata, StorageError<Self::Timestamp>>;

    /// Acquire an iterator over [CollectionMetadata] for all active
    /// collections.
    ///
    /// A collection is "active" when it has a non empty frontier of read
    /// capabilties.
    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)>;

    /// Returns the frontiers of the identified collection.
    fn collection_frontiers(
        &self,
        id: GlobalId,
    ) -> Result<CollectionFrontiers<Self::Timestamp>, StorageError<Self::Timestamp>> {
        let frontiers = self
            .collections_frontiers(vec![id])?
            .expect_element(|| "known to exist");

        Ok(frontiers)
    }

    /// Atomically gets and returns the frontiers of all the identified
    /// collections.
    fn collections_frontiers(
        &self,
        id: Vec<GlobalId>,
    ) -> Result<Vec<CollectionFrontiers<Self::Timestamp>>, StorageError<Self::Timestamp>>;

    /// Atomically gets and returns the frontiers of all active collections.
    ///
    /// A collection is "active" when it has a non empty frontier of read
    /// capabilties.
    fn active_collection_frontiers(&self) -> Vec<CollectionFrontiers<Self::Timestamp>>;

    /// Checks whether a collection exists under the given `GlobalId`. Returns
    /// an error if the collection does not exist.
    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError<Self::Timestamp>>;

    /// Returns aggregate statistics about the contents of the local input named
    /// `id` at `as_of`.
    async fn snapshot_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> Result<SnapshotStats, StorageError<Self::Timestamp>>;

    /// Returns aggregate statistics about the contents of the local input named
    /// `id` at `as_of`.
    ///
    /// Note that this async function itself returns a future. We may
    /// need to block on the stats being available, but don't want to hold a reference
    /// to the controller for too long... so the outer future holds a reference to the
    /// controller but returns quickly, and the inner future is slow but does not
    /// reference the controller.
    async fn snapshot_parts_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> BoxFuture<'static, Result<SnapshotPartsStats, StorageError<Self::Timestamp>>>;

    /// Update the given [`StorageTxn`] with the appropriate metadata given the
    /// IDs to add and drop.
    ///
    /// The data modified in the `StorageTxn` must be made available in all
    /// subsequent calls that require [`StorageMetadata`] as a parameter.
    async fn prepare_state(
        &self,
        txn: &mut (dyn StorageTxn<Self::Timestamp> + Send),
        ids_to_add: BTreeSet<GlobalId>,
        ids_to_drop: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Create the collections described by the individual
    /// [CollectionDescriptions](CollectionDescription).
    ///
    /// Each command carries the source id, the source description, and any
    /// associated metadata needed to ingest the particular source.
    ///
    /// This command installs collection state for the indicated sources, and
    /// they are now valid to use in queries at times beyond the initial `since`
    /// frontiers. Each collection also acquires a read capability at this
    /// frontier, which will need to be repeatedly downgraded with
    /// `allow_compaction()` to permit compaction.
    ///
    /// This method is NOT idempotent; It can fail between processing of
    /// different collections and leave the [StorageCollections] in an
    /// inconsistent state. It is almost always wrong to do anything but abort
    /// the process on `Err`.
    ///
    /// The `register_ts` is used as the initial timestamp that tables are
    /// available for reads. (We might later give non-tables the same treatment,
    /// but hold off on that initially.) Callers must provide a Some if any of
    /// the collections is a table. A None may be given if none of the
    /// collections are a table (i.e. all materialized views, sources, etc).
    async fn create_collections(
        &self,
        storage_metadata: &StorageMetadata,
        register_ts: Option<Self::Timestamp>,
        mut collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Alters the identified ingestion to use the provided [`SourceDesc`].
    ///
    /// NOTE: Ideally, [StorageCollections] would not care about these, but we
    /// have to learn about changes such that when new subsources are created we
    /// can correctly determine a since based on its depenencies' sinces. This
    /// is really only relevant because newly created subsources depend on the
    /// remap shard, and we can't just have them start at since 0.
    async fn alter_ingestion_source_desc(
        &self,
        ingestion_id: GlobalId,
        source_desc: SourceDesc,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Alters each identified collection to use the correlated
    /// [`GenericSourceConnection`].
    ///
    /// See NOTE on [StorageCollections::alter_ingestion_source_desc].
    async fn alter_ingestion_connections(
        &self,
        source_connections: BTreeMap<GlobalId, GenericSourceConnection<InlinedConnection>>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Drops the read capability for the sources and allows their resources to
    /// be reclaimed.
    ///
    /// TODO(jkosh44): This method does not validate the provided identifiers.
    /// Currently when the controller starts/restarts it has no durable state.
    /// That means that it has no way of remembering any past commands sent. In
    /// the future we plan on persisting state for the controller so that it is
    /// aware of past commands. Therefore this method is for dropping sources
    /// that we know to have been previously created, but have been forgotten by
    /// the controller due to a restart. Once command history becomes durable we
    /// can remove this method and use the normal `drop_sources`.
    fn drop_collections_unvalidated(
        &self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
    );

    /// Assigns a read policy to specific identifiers.
    ///
    /// The policies are assigned in the order presented, and repeated
    /// identifiers should conclude with the last policy. Changing a policy will
    /// immediately downgrade the read capability if appropriate, but it will
    /// not "recover" the read capability if the prior capability is already
    /// ahead of it.
    ///
    /// This [StorageCollections] may include its own overrides on these
    /// policies.
    ///
    /// Identifiers not present in `policies` retain their existing read
    /// policies.
    fn set_read_policies(&self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>);

    /// Acquires and returns the earliest possible read holds for the specified
    /// collections.
    fn acquire_read_holds(
        &self,
        desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold<Self::Timestamp>>, ReadHoldError>;

    /// Applies `updates` and sends any appropriate compaction command.
    ///
    /// This is a legacy interface that should _not_ be used! It is only used by
    /// the compute controller.
    fn update_read_capabilities(
        &self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<Self::Timestamp>>,
    );
}

/// Frontiers of the collection identified by `id`.
pub struct CollectionFrontiers<T> {
    /// The [GlobalId] of the collection that these frontiers belong to.
    pub id: GlobalId,

    /// The upper/write frontier of the collection.
    pub write_frontier: Antichain<T>,

    /// The since frontier that is implied by the collection's existence,
    /// disregarding any read holds.
    ///
    /// Concretely, it is the since frontier that is implied by the combination
    /// of the `write_frontier` and a [ReadPolicy]. The implied capability is
    /// derived from the write frontier using the [ReadPolicy].
    pub implied_capability: Antichain<T>,

    /// The frontier of all oustanding [ReadHolds](ReadHold). This includes the
    /// implied capability.
    pub read_capabilities: Antichain<T>,
}

#[derive(Debug, Clone)]
enum TxnsWal<T> {
    EnabledEager,
    EnabledLazy { txns_read: TxnsRead<T> },
}

impl<T: TimelyTimestamp + Lattice + Codec64> TxnsWal<T> {
    fn expect_enabled_lazy(&self, txns_id: &ShardId) -> &TxnsRead<T> {
        match self {
            TxnsWal::EnabledLazy { txns_read, .. } => {
                assert_eq!(txns_id, txns_read.txns_id());
                txns_read
            }
            TxnsWal::EnabledEager { .. } => {
                panic!("set if txns are enabled and lazy")
            }
        }
    }
}

/// Implementation of [StorageCollections] that is shallow-cloneable and uses a
/// background task for doing work concurrently, in the background.
#[derive(Debug, Clone)]
pub struct StorageCollectionsImpl<
    T: TimelyTimestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
> {
    /// The fencing token for this instance of [StorageCollections], and really
    /// all of the controllers and Coordinator.
    envd_epoch: NonZeroI64,

    /// The set of [ShardIds](ShardId) that we have to finalize. These will have
    /// been persisted by the caller of [StorageCollections::prepare_state].
    finalizable_shards: Arc<std::sync::Mutex<BTreeSet<ShardId>>>,

    /// The set of [ShardIds](ShardId) that we have finalized. We keep track of
    /// shards here until we are given a chance to let our callers know that
    /// these have been finalized, for example via
    /// [StorageCollections::prepare_state].
    finalized_shards: Arc<std::sync::Mutex<BTreeSet<ShardId>>>,

    /// Collections maintained by this [StorageCollections].
    collections: Arc<std::sync::Mutex<BTreeMap<GlobalId, CollectionState<T>>>>,

    /// Whether to use the new txn-wal tables implementation or the legacy one.
    txns: TxnsWal<T>,

    /// Storage configuration parameters.
    config: Arc<Mutex<StorageConfiguration>>,

    /// The persist location where all storage collections are being written to
    persist_location: PersistLocation,

    /// A persist client used to write to storage collections
    persist: Arc<PersistClientCache>,

    /// For sending commands to our internal task.
    cmd_tx: mpsc::UnboundedSender<BackgroundCmd<T>>,

    /// For sending updates about read holds to our internal task.
    holds_tx: mpsc::UnboundedSender<(GlobalId, ChangeBatch<T>)>,

    /// Handles to tasks we own, making sure they're dropped when we are.
    _background_task: Arc<AbortOnDropHandle<()>>,
}

// Supporting methods for implementing [StorageCollections].
//
// Almost all internal methods that are the backing implementation for a trait
// method have the `_inner` suffix.
//
// We follow a pattern where `_inner` methods get a mutable reference to the
// shared collections state, and it's the public-facing method that locks the
// state for the duration of its invocation. This allows calling other `_inner`
// methods from within `_inner` methods.
impl<T> StorageCollectionsImpl<T>
where
    T: TimelyTimestamp
        + Lattice
        + Codec64
        + From<EpochMillis>
        + TimestampManipulation
        + Into<mz_repr::Timestamp>,
{
    /// Creates and returns a new [StorageCollections].
    ///
    /// Note that when creating a new [StorageCollections], you must also
    /// reconcile it with the previous state using
    /// [StorageCollections::initialize_state],
    /// [StorageCollections::prepare_state], and
    /// [StorageCollections::create_collections].
    pub async fn new(
        persist_location: PersistLocation,
        persist_clients: Arc<PersistClientCache>,
        _now: NowFn,
        txns_metrics: Arc<TxnMetrics>,
        envd_epoch: NonZeroI64,
        txn_wal_tables: TxnWalTablesImpl,
        connection_context: ConnectionContext,
        txn: &dyn StorageTxn<T>,
    ) -> Self {
        // This value must be already installed because we must ensure it's
        // durably recorded before it is used, otherwise we risk leaking persist
        // state.
        let txns_id = txn
            .get_txn_wal_shard()
            .expect("must call prepare initialization before creating StorageCollections");
        let txns_id = ShardId::from_str(txns_id.as_str()).expect("shard ID must be valid");

        let txns_client = persist_clients
            .open(persist_location.clone())
            .await
            .expect("location should be valid");

        // We have to initialize, so that TxnsRead::start() below does not
        // block.
        let _txns_handle: TxnsHandle<SourceData, (), T, i64, PersistEpoch, TxnsCodecRow> =
            TxnsHandle::open(
                T::minimum(),
                txns_client.clone(),
                Arc::clone(&txns_metrics),
                txns_id,
                Arc::new(RelationDesc::empty()),
                Arc::new(UnitSchema),
            )
            .await;

        // For handing to the background task, for listening to upper updates.
        let (txns_key_schema, txns_val_schema) = TxnsCodecRow::schemas();
        let txns_write = txns_client
            .open_writer(
                txns_id,
                Arc::new(txns_key_schema),
                Arc::new(txns_val_schema),
                Diagnostics {
                    shard_name: "txns".to_owned(),
                    handle_purpose: "commit txns".to_owned(),
                },
            )
            .await
            .expect("txns schema shouldn't change");

        let txns = match txn_wal_tables {
            TxnWalTablesImpl::Lazy => {
                let txns_read = TxnsRead::start::<TxnsCodecRow>(txns_client.clone(), txns_id).await;
                TxnsWal::EnabledLazy { txns_read }
            }
            TxnWalTablesImpl::Eager => TxnsWal::EnabledEager,
        };

        let collections = Arc::new(std::sync::Mutex::new(BTreeMap::default()));
        let finalizable_shards = Arc::new(std::sync::Mutex::new(BTreeSet::default()));
        let finalized_shards = Arc::new(std::sync::Mutex::new(BTreeSet::default()));
        let config = Arc::new(Mutex::new(StorageConfiguration::new(
            connection_context,
            mz_dyncfgs::all_dyncfgs(),
        )));

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (holds_tx, holds_rx) = mpsc::unbounded_channel();
        let mut background_task = BackgroundTask {
            envd_epoch: envd_epoch.clone(),
            persist_location: persist_location.clone(),
            persist: Arc::clone(&persist_clients),
            config: Arc::clone(&config),
            cmds_tx: cmd_tx.clone(),
            cmds_rx: cmd_rx,
            holds_rx,
            collections: Arc::clone(&collections),
            finalizable_shards: Arc::clone(&finalizable_shards),
            finalized_shards: Arc::clone(&finalized_shards),
            shard_by_id: BTreeMap::new(),
            since_handles: BTreeMap::new(),
            txns_handle: Some(txns_write),
            txns_shards: Default::default(),
        };

        let background_task =
            mz_ore::task::spawn(|| "storage_collections::background_task", async move {
                background_task.run().await
            });

        Self {
            finalizable_shards,
            finalized_shards,
            collections,
            txns,
            envd_epoch,
            config,
            persist_location,
            persist: persist_clients,
            cmd_tx,
            holds_tx,
            _background_task: Arc::new(background_task.abort_on_drop()),
        }
    }

    /// Opens a [WriteHandle] and a [SinceHandleWrapper], for holding back the since.
    ///
    /// `since` is an optional since that the read handle will be forwarded to
    /// if it is less than its current since.
    ///
    /// This will `halt!` the process if we cannot successfully acquire a
    /// critical handle with our current epoch.
    async fn open_data_handles(
        &self,
        id: &GlobalId,
        shard: ShardId,
        since: Option<&Antichain<T>>,
        relation_desc: RelationDesc,
        persist_client: &PersistClient,
    ) -> (WriteHandle<SourceData, (), T, Diff>, SinceHandleWrapper<T>) {
        let write_handle = self
            .open_write_handle(id, shard, relation_desc, persist_client)
            .await;
        let since_handle = self
            .open_critical_handle(id, shard, since, persist_client)
            .await;

        let since_handle = SinceHandleWrapper::Critical(since_handle);

        (write_handle, since_handle)
    }

    /// Opens a write handle for the given `shard`.
    async fn open_write_handle(
        &self,
        id: &GlobalId,
        shard: ShardId,
        relation_desc: RelationDesc,
        persist_client: &PersistClient,
    ) -> WriteHandle<SourceData, (), T, Diff> {
        let diagnostics = Diagnostics {
            shard_name: id.to_string(),
            handle_purpose: format!("controller data for {}", id),
        };

        let mut write = persist_client
            .open_writer(
                shard,
                Arc::new(relation_desc),
                Arc::new(UnitSchema),
                diagnostics.clone(),
            )
            .await
            .expect("invalid persist usage");

        // N.B.
        // Fetch the most recent upper for the write handle. Otherwise, this may
        // be behind the since of the since handle. Its vital this happens AFTER
        // we create the since handle as it needs to be linearized with that
        // operation. It may be true that creating the write handle after the
        // since handle already ensures this, but we do this out of an abundance
        // of caution.
        //
        // Note that this returns the upper, but also sets it on the handle to
        // be fetched later.
        write.fetch_recent_upper().await;

        write
    }

    /// Opens a critical since handle for the given `shard`.
    ///
    /// `since` is an optional since that the read handle will be forwarded to
    /// if it is less than its current since.
    ///
    /// This will `halt!` the process if we cannot successfully acquire a
    /// critical handle with our current epoch.
    async fn open_critical_handle(
        &self,
        id: &GlobalId,
        shard: ShardId,
        since: Option<&Antichain<T>>,
        persist_client: &PersistClient,
    ) -> SinceHandle<SourceData, (), T, Diff, PersistEpoch> {
        let diagnostics = Diagnostics {
            shard_name: id.to_string(),
            handle_purpose: format!("controller data for {}", id),
        };

        // Construct the handle in a separate block to ensure all error paths
        // are diverging
        let since_handle = {
            // This block's aim is to ensure the handle is in terms of our epoch
            // by the time we return it.
            let mut handle: SinceHandle<_, _, _, _, PersistEpoch> = persist_client
                .open_critical_since(
                    shard,
                    PersistClient::CONTROLLER_CRITICAL_SINCE,
                    diagnostics.clone(),
                )
                .await
                .expect("invalid persist usage");

            // Take the join of the handle's since and the provided `since`;
            // this lets materialized views express the since at which their
            // read handles "start."
            let since = handle
                .since()
                .join(since.unwrap_or(&Antichain::from_elem(T::minimum())));

            let our_epoch = self.envd_epoch;

            loop {
                let current_epoch: PersistEpoch = handle.opaque().clone();

                // Ensure the current epoch is <= our epoch.
                let unchecked_success = current_epoch.0.map(|e| e <= our_epoch).unwrap_or(true);

                if unchecked_success {
                    // Update the handle's state so that it is in terms of our
                    // epoch.
                    let checked_success = handle
                        .compare_and_downgrade_since(
                            &current_epoch,
                            (&PersistEpoch::from(our_epoch), &since),
                        )
                        .await
                        .is_ok();
                    if checked_success {
                        break handle;
                    }
                } else {
                    mz_ore::halt!("fenced by envd @ {current_epoch:?}. ours = {our_epoch}");
                }
            }
        };

        since_handle
    }

    /// Opens a leased [ReadHandle], for the purpose of holding back a since,
    /// for the given `shard`.
    ///
    /// `since` is an optional since that the read handle will be forwarded to
    /// if it is less than its current since.
    async fn open_leased_handle(
        &self,
        id: &GlobalId,
        shard: ShardId,
        relation_desc: RelationDesc,
        since: Option<&Antichain<T>>,
        persist_client: &PersistClient,
    ) -> ReadHandle<SourceData, (), T, Diff> {
        let diagnostics = Diagnostics {
            shard_name: id.to_string(),
            handle_purpose: format!("controller data for {}", id),
        };

        let use_critical_since = false;
        let mut handle: ReadHandle<_, _, _, _> = persist_client
            .open_leased_reader(
                shard,
                Arc::new(relation_desc),
                Arc::new(UnitSchema),
                diagnostics.clone(),
                use_critical_since,
            )
            .await
            .expect("invalid persist usage");

        // Take the join of the handle's since and the provided `since`;
        // this lets materialized views express the since at which their
        // read handles "start."
        let since = handle
            .since()
            .join(since.unwrap_or(&Antichain::from_elem(T::minimum())));

        handle.downgrade_since(&since).await;

        handle
    }

    fn register_handles(
        &self,
        id: GlobalId,
        is_in_txns: bool,
        since_handle: SinceHandleWrapper<T>,
        write_handle: WriteHandle<SourceData, (), T, Diff>,
    ) {
        self.send(BackgroundCmd::Register {
            id,
            is_in_txns,
            since_handle,
            write_handle,
        });
    }

    fn send(&self, cmd: BackgroundCmd<T>) {
        let _ = self.cmd_tx.send(cmd);
    }

    async fn snapshot_stats_inner(
        &self,
        id: GlobalId,
        as_of: SnapshotStatsAsOf<T>,
    ) -> Result<SnapshotStats, StorageError<T>> {
        // TODO: Pull this out of BackgroundTask. Unlike the other methods, the
        // caller of this one drives it to completion.
        //
        // We'd need to either share the critical handle somehow or maybe have
        // two instances around, one in the worker and one in the
        // StorageCollections.
        let (tx, rx) = oneshot::channel();
        self.send(BackgroundCmd::SnapshotStats(id, as_of, tx));
        rx.await.expect("BackgroundTask should be live").0.await
    }

    /// If this identified collection has a dependency, install a read hold on
    /// it.
    ///
    /// This is necessary to ensure that the dependency's since does not advance
    /// beyond its dependents'.
    fn install_collection_dependency_read_holds_inner(
        &self,
        self_collections: &mut BTreeMap<GlobalId, CollectionState<T>>,
        id: GlobalId,
    ) -> Result<(), StorageError<T>> {
        let (deps, collection_implied_capability) = match self_collections.get(&id) {
            Some(CollectionState {
                storage_dependencies: deps,
                implied_capability,
                ..
            }) => (deps.clone(), implied_capability),
            _ => return Ok(()),
        };

        for dep in deps.iter() {
            let dep_collection = self_collections
                .get(dep)
                .ok_or(StorageError::IdentifierMissing(id))?;

            mz_ore::soft_assert_or_log!(
                PartialOrder::less_equal(
                    &dep_collection.implied_capability,
                    collection_implied_capability
                ),
                "dependency since cannot be in advance of dependent's since"
            );
        }

        self.install_read_capabilities_inner(
            self_collections,
            id,
            &deps,
            collection_implied_capability.clone(),
        )?;

        Ok(())
    }

    /// Determine if this collection has another dependency.
    ///
    /// Currently, collections have either 0 or 1 dependencies.
    fn determine_collection_dependencies(
        &self,
        self_collections: &mut BTreeMap<GlobalId, CollectionState<T>>,
        data_source: &DataSource,
    ) -> Result<Vec<GlobalId>, StorageError<T>> {
        let dependencies = match &data_source {
            DataSource::Introspection(_)
            | DataSource::Webhook
            | DataSource::Other(DataSourceOther::TableWrites)
            | DataSource::Progress
            | DataSource::Other(DataSourceOther::Compute) => Vec::new(),
            DataSource::IngestionExport { ingestion_id, .. } => {
                // Ingestion exports depend on their primary source's remap
                // collection.
                let source_collection = self_collections
                    .get(ingestion_id)
                    .ok_or(StorageError::IdentifierMissing(*ingestion_id))?;
                match &source_collection.description {
                    CollectionDescription {
                        data_source: DataSource::Ingestion(ingestion_desc),
                        ..
                    } => vec![ingestion_desc.remap_collection_id],
                    _ => unreachable!(
                        "SourceExport must only refer to primary sources that already exist"
                    ),
                }
            }
            // Ingestions depend on their remap collection.
            DataSource::Ingestion(ingestion) => vec![ingestion.remap_collection_id],
        };

        Ok(dependencies)
    }

    /// Install read capabilities on the given `storage_dependencies`.
    #[instrument(level = "debug")]
    fn install_read_capabilities_inner(
        &self,
        self_collections: &mut BTreeMap<GlobalId, CollectionState<T>>,
        from_id: GlobalId,
        storage_dependencies: &[GlobalId],
        read_capability: Antichain<T>,
    ) -> Result<(), StorageError<T>> {
        let mut changes = ChangeBatch::new();
        for time in read_capability.iter() {
            changes.update(time.clone(), 1);
        }

        let user_capabilities = self_collections
            .iter_mut()
            .filter(|(id, _c)| id.is_user())
            .map(|(id, c)| {
                let updates = c.read_capabilities.updates().cloned().collect_vec();
                (*id, c.implied_capability.clone(), updates)
            })
            .collect_vec();

        trace!(
            %from_id,
            ?storage_dependencies,
            ?read_capability,
            ?user_capabilities,
            "install_read_capabilities_inner");

        let mut storage_read_updates = storage_dependencies
            .iter()
            .map(|id| (*id, changes.clone()))
            .collect();

        StorageCollectionsImpl::update_read_capabilities_inner(
            &self.cmd_tx,
            self_collections,
            &mut storage_read_updates,
        );

        let user_capabilities = self_collections
            .iter_mut()
            .filter(|(id, _c)| id.is_user())
            .map(|(id, c)| {
                let updates = c.read_capabilities.updates().cloned().collect_vec();
                (*id, c.implied_capability.clone(), updates)
            })
            .collect_vec();

        trace!(
            %from_id,
            ?storage_dependencies,
            ?read_capability,
            ?user_capabilities,
            "after install_read_capabilities_inner!");

        Ok(())
    }

    async fn read_handle_for_snapshot(
        &self,
        metadata: &CollectionMetadata,
        id: GlobalId,
    ) -> Result<ReadHandle<SourceData, (), T, Diff>, StorageError<T>> {
        let persist_client = self
            .persist
            .open(metadata.persist_location.clone())
            .await
            .unwrap();

        // We create a new read handle every time someone requests a snapshot
        // and then immediately expire it instead of keeping a read handle
        // permanently in our state to avoid having it heartbeat continously.
        // The assumption is that calls to snapshot are rare and therefore worth
        // it to always create a new handle.
        let read_handle = persist_client
            .open_leased_reader::<SourceData, (), _, _>(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: id.to_string(),
                    handle_purpose: format!("snapshot {}", id),
                },
                false,
            )
            .await
            .expect("invalid persist usage");
        Ok(read_handle)
    }

    fn set_read_policies_inner(
        &self,
        collections: &mut BTreeMap<GlobalId, CollectionState<T>>,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) {
        trace!("set_read_policies: {:?}", policies);

        let mut read_capability_changes = BTreeMap::default();

        for (id, policy) in policies.into_iter() {
            let collection = match collections.get_mut(&id) {
                Some(c) => c,
                None => {
                    panic!("Reference to absent collection {id}");
                }
            };

            let mut new_read_capability = policy.frontier(collection.write_frontier.borrow());

            if PartialOrder::less_equal(&collection.implied_capability, &new_read_capability) {
                let mut update = ChangeBatch::new();
                update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));
                if !update.is_empty() {
                    read_capability_changes.insert(id, update);
                }
            }

            collection.read_policy = policy;
        }

        for (id, changes) in read_capability_changes.iter() {
            if id.is_user() {
                trace!(%id, ?changes, "in set_read_policies, capability changes");
            }
        }

        if !read_capability_changes.is_empty() {
            StorageCollectionsImpl::update_read_capabilities_inner(
                &self.cmd_tx,
                collections,
                &mut read_capability_changes,
            );
        }
    }

    // This is not an associated function so that we can share it with the task
    // that updates the persist handles and also has a reference to the shared
    // collections state.
    fn update_read_capabilities_inner(
        cmd_tx: &mpsc::UnboundedSender<BackgroundCmd<T>>,
        collections: &mut BTreeMap<GlobalId, CollectionState<T>>,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>,
    ) {
        // Location to record consequences that we need to act on.
        let mut collections_net = BTreeMap::new();

        // We must not rely on any specific relative ordering of `GlobalId`s.
        // That said, it is reasonable to assume that collections generally have
        // greater IDs than their dependencies, so starting with the largest is
        // a useful optimization.
        while let Some(id) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&id).unwrap();

            if id.is_user() {
                trace!(id = ?id, update = ?update, "update_read_capabilities");
            }

            let collection = if let Some(c) = collections.get_mut(&id) {
                c
            } else {
                let has_positive_updates = update.iter().any(|(_ts, diff)| *diff > 0);
                if has_positive_updates {
                    panic!(
                        "reference to absent collection {id} but we have positive updates: {:?}",
                        update
                    );
                } else {
                    // Continue purely negative updates. Someone has probably
                    // already dropped this collection!
                    continue;
                }
            };

            let current_read_capabilities = collection.read_capabilities.frontier().to_owned();
            for (time, diff) in update.iter() {
                assert!(
                    collection.read_capabilities.count_for(time) + diff >= 0,
                    "update {:?} for collection {id} would lead to negative \
                        read capabilities, read capabilities before applying: {:?}",
                    update,
                    collection.read_capabilities
                );

                if collection.read_capabilities.count_for(time) + diff > 0 {
                    assert!(
                        current_read_capabilities.less_equal(time),
                        "update {:?} for collection {id} is trying to \
                            install read capabilities before the current \
                            frontier of read capabilities, read capabilities before applying: {:?}",
                        update,
                        collection.read_capabilities
                    );
                }
            }

            let changes = collection.read_capabilities.update_iter(update.drain());
            update.extend(changes);

            if id.is_user() {
                trace!(
                %id,
                ?collection.storage_dependencies,
                ?update,
                "forwarding update to storage dependencies");
            }

            for id in collection.storage_dependencies.iter() {
                updates
                    .entry(*id)
                    .or_insert_with(ChangeBatch::new)
                    .extend(update.iter().cloned());
            }

            let (changes, frontier) = collections_net
                .entry(id)
                .or_insert_with(|| (ChangeBatch::new(), Antichain::new()));

            changes.extend(update.drain());
            *frontier = collection.read_capabilities.frontier().to_owned();
        }

        // Translate our net compute actions into downgrades of persist sinces.
        // The actual downgrades are performed by a Tokio task asynchorously.
        let mut persist_compaction_commands = BTreeMap::default();
        for (key, (mut changes, frontier)) in collections_net {
            if !changes.is_empty() {
                if frontier.is_empty() {
                    info!(id = %key, "removing collection state because the since advanced to []!");
                    collections.remove(&key).expect("must still exist");
                }
                persist_compaction_commands.insert(key, frontier);
            }
        }

        let persist_compaction_commands = persist_compaction_commands.into_iter().collect_vec();

        cmd_tx
            .send(BackgroundCmd::DowngradeSince(persist_compaction_commands))
            .expect("cannot fail to send");
    }

    /// Remove any shards that we know are finalized
    fn synchronize_finalized_shards(&self, storage_metadata: &StorageMetadata) {
        self.finalized_shards
            .lock()
            .expect("lock poisoned")
            .retain(|shard| {
                storage_metadata
                    .unfinalized_shards
                    .contains(shard.to_string().as_str())
            });
    }
}

// See comments on the above impl for StorageCollectionsImpl.
#[async_trait]
impl<T> StorageCollections for StorageCollectionsImpl<T>
where
    T: TimelyTimestamp
        + Lattice
        + Codec64
        + From<EpochMillis>
        + TimestampManipulation
        + Into<mz_repr::Timestamp>,
{
    type Timestamp = T;

    async fn initialize_state(
        &self,
        txn: &mut (dyn StorageTxn<T> + Send),
        init_ids: BTreeSet<GlobalId>,
        drop_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<T>> {
        let metadata = txn.get_collection_metadata();

        let processed_metadata: Result<Vec<_>, _> = metadata
            .into_iter()
            .map(|(id, shard)| ShardId::from_str(&shard).map(|shard| (id, shard)))
            .collect();

        let metadata = processed_metadata.map_err(|e| StorageError::Generic(anyhow::anyhow!(e)))?;
        let existing_metadata: BTreeSet<_> = metadata.into_iter().map(|(id, _)| id).collect();

        // Determine which collections we do not yet have metadata for.
        let new_collections: BTreeSet<GlobalId> = init_ids
            .iter()
            .filter(|id| !existing_metadata.contains(id))
            .cloned()
            .collect();

        mz_ore::soft_assert_or_log!(
                new_collections.iter().all(|id| id.is_system()),
                "initializing collections should only be missing metadata for new system objects, but got {:?}",
                new_collections
            );

        self.prepare_state(txn, new_collections, drop_ids).await?;

        // All shards that belong to collections dropped in the last epoch are
        // eligible for finalization. This intentionally includes any built-in
        // collections present in `drop_ids`.
        //
        // n.b. this introduces an unlikely race condition: if a collection is
        // dropped from the catalog, but the dataflow is still running on a
        // worker, assuming the shard is safe to finalize on reboot may cause
        // the cluster to panic.
        let unfinalized_shards = txn
            .get_unfinalized_shards()
            .into_iter()
            .map(|shard| ShardId::from_str(&shard).expect("deserialization corrupted"))
            .collect_vec();

        info!(?unfinalized_shards, "initializing finalizable_shards");

        self.finalizable_shards
            .lock()
            .expect("lock poisoned")
            .extend(unfinalized_shards.into_iter());

        Ok(())
    }

    fn update_parameters(&self, config_params: StorageParameters) {
        // We serialize the dyncfg updates in StorageParameters, but configure
        // persist separately.
        config_params.dyncfg_updates.apply(self.persist.cfg());

        self.config
            .lock()
            .expect("lock poisoned")
            .update(config_params);
    }

    fn collection_metadata(
        &self,
        id: GlobalId,
    ) -> Result<CollectionMetadata, StorageError<Self::Timestamp>> {
        let collections = self.collections.lock().expect("lock poisoned");

        collections
            .get(&id)
            .map(|c| c.collection_metadata.clone())
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)> {
        let collections = self.collections.lock().expect("lock poisoned");

        collections
            .iter()
            .filter(|(_id, c)| !c.is_dropped())
            .map(|(id, c)| (*id, c.collection_metadata.clone()))
            .collect()
    }

    fn collections_frontiers(
        &self,
        ids: Vec<GlobalId>,
    ) -> Result<Vec<CollectionFrontiers<Self::Timestamp>>, StorageError<Self::Timestamp>> {
        let collections = self.collections.lock().expect("lock poisoned");

        let res = ids
            .into_iter()
            .map(|id| {
                collections
                    .get(&id)
                    .map(|c| CollectionFrontiers {
                        id: id.clone(),
                        write_frontier: c.write_frontier.clone(),
                        implied_capability: c.implied_capability.clone(),
                        read_capabilities: c.read_capabilities.frontier().to_owned(),
                    })
                    .ok_or(StorageError::IdentifierMissing(id))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(res)
    }

    fn active_collection_frontiers(&self) -> Vec<CollectionFrontiers<Self::Timestamp>> {
        let collections = self.collections.lock().expect("lock poisoned");

        let res = collections
            .iter()
            .filter(|(_id, c)| !c.is_dropped())
            .map(|(id, c)| CollectionFrontiers {
                id: id.clone(),
                write_frontier: c.write_frontier.clone(),
                implied_capability: c.implied_capability.clone(),
                read_capabilities: c.read_capabilities.frontier().to_owned(),
            })
            .collect_vec();

        res
    }

    async fn snapshot_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> Result<SnapshotStats, StorageError<Self::Timestamp>> {
        let metadata = self.collection_metadata(id)?;

        // See the comments in StorageController::snapshot for what's going on
        // here.
        let as_of = match metadata.txns_shard.as_ref() {
            None => SnapshotStatsAsOf::Direct(as_of),
            Some(txns_id) => {
                let txns_read = self.txns.expect_enabled_lazy(txns_id);
                let as_of = as_of
                    .into_option()
                    .expect("cannot read as_of the empty antichain");
                txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = txns_read
                    .data_snapshot(metadata.data_shard, as_of.clone())
                    .await;
                SnapshotStatsAsOf::Txns(data_snapshot)
            }
        };
        self.snapshot_stats_inner(id, as_of).await
    }

    async fn snapshot_parts_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> BoxFuture<'static, Result<SnapshotPartsStats, StorageError<Self::Timestamp>>> {
        let metadata = {
            let self_collections = self.collections.lock().expect("lock poisoned");

            let collection_metadata = self_collections
                .get(&id)
                .ok_or(StorageError::IdentifierMissing(id))
                .map(|c| c.collection_metadata.clone());

            match collection_metadata {
                Ok(m) => m,
                Err(e) => return Box::pin(async move { Err(e) }),
            }
        };

        // See the comments in StorageController::snapshot for what's going on
        // here.
        let read_handle = self.read_handle_for_snapshot(&metadata, id).await;

        let data_snapshot = match (metadata, as_of.as_option()) {
            (
                CollectionMetadata {
                    txns_shard: Some(txns_id),
                    data_shard,
                    ..
                },
                Some(as_of),
            ) => {
                let txns_read = self.txns.expect_enabled_lazy(&txns_id);
                txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = txns_read.data_snapshot(data_shard, as_of.clone()).await;
                Some(data_snapshot)
            }
            _ => None,
        };

        Box::pin(async move {
            let mut read_handle = read_handle?;
            let result = match data_snapshot {
                Some(data_snapshot) => data_snapshot.snapshot_parts_stats(&mut read_handle).await,
                None => read_handle.snapshot_parts_stats(as_of).await,
            };
            read_handle.expire().await;
            result.map_err(|_| StorageError::ReadBeforeSince(id))
        })
    }

    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError<Self::Timestamp>> {
        let collections = self.collections.lock().expect("lock poisoned");

        if collections.contains_key(&id) {
            Ok(())
        } else {
            Err(StorageError::IdentifierMissing(id))
        }
    }

    async fn prepare_state(
        &self,
        txn: &mut (dyn StorageTxn<Self::Timestamp> + Send),
        ids_to_add: BTreeSet<GlobalId>,
        ids_to_drop: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<T>> {
        txn.insert_collection_metadata(
            ids_to_add
                .into_iter()
                .map(|id| (id, ShardId::new().to_string()))
                .collect(),
        )?;

        // Delete the metadata for any dropped collections.
        let dropped_mappings = txn.delete_collection_metadata(ids_to_drop);

        let dropped_shards = dropped_mappings
            .into_iter()
            .map(|(_id, shard)| shard)
            .collect();

        txn.insert_unfinalized_shards(dropped_shards)?;

        // Reconcile any shards we've successfully finalized with the shard
        // finalization collection.
        let finalized_shards = self
            .finalized_shards
            .lock()
            .expect("lock poisoned")
            .iter()
            .map(|v| v.to_string())
            .collect();
        txn.mark_shards_as_finalized(finalized_shards);

        Ok(())
    }

    // TODO(aljoscha): It would be swell if we could refactor this Leviathan of
    // a method/move individual parts to their own methods.
    #[instrument(level = "debug")]
    async fn create_collections(
        &self,
        storage_metadata: &StorageMetadata,
        register_ts: Option<Self::Timestamp>,
        mut collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        // Validate first, to avoid corrupting state.
        // 1. create a dropped identifier, or
        // 2. create an existing identifier with a new description.
        // Make sure to check for errors within `ingestions` as well.
        collections.sort_by_key(|(id, _)| *id);
        collections.dedup();
        for pos in 1..collections.len() {
            if collections[pos - 1].0 == collections[pos].0 {
                return Err(StorageError::SourceIdReused(collections[pos].0));
            }
        }

        {
            // Early sanity check: if we knew about a collection already it's
            // description must match!
            //
            // NOTE: There could be concurrent modifications to
            // `self.collections`, but this sanity check is better than nothing.
            let self_collections = self.collections.lock().expect("lock poisoned");
            for (id, description) in collections.iter() {
                if let Some(existing_collection) = self_collections.get(id) {
                    if &existing_collection.description != description {
                        return Err(StorageError::SourceIdReused(*id));
                    }
                }
            }
        }

        // We first enrich each collection description with some additional
        // metadata...
        let enriched_with_metadata = collections
            .into_iter()
            .map(|(id, description)| {
                let data_shard = storage_metadata.get_collection_shard::<T>(id)?;

                let get_shard = |id| -> Result<ShardId, StorageError<T>> {
                    let shard = storage_metadata.get_collection_shard::<T>(id)?;
                    Ok(shard)
                };

                let status_shard = match description.status_collection_id {
                    Some(status_collection_id) => Some(get_shard(status_collection_id)?),
                    None => None,
                };

                let remap_shard = match &description.data_source {
                    // Only ingestions can have remap shards.
                    DataSource::Ingestion(IngestionDescription {
                        remap_collection_id,
                        ..
                    }) => {
                        // Iff ingestion has a remap collection, its metadata
                        // must exist (and be correct) by this point.
                        Some(get_shard(*remap_collection_id)?)
                    }
                    _ => None,
                };

                // If the shard is being managed by txn-wal (initially,
                // tables), then we need to pass along the shard id for the txns
                // shard to dataflow rendering.
                let txns_shard = match description.data_source {
                    DataSource::Other(DataSourceOther::TableWrites) => match &self.txns {
                        // If we're not using lazy txn-wal upper (i.e. we're
                        // using eager uppers) then all reads should be done
                        // normally.
                        TxnsWal::EnabledEager { .. } => None,
                        TxnsWal::EnabledLazy { txns_read, .. } => Some(*txns_read.txns_id()),
                    },
                    DataSource::Ingestion(_)
                    | DataSource::IngestionExport { .. }
                    | DataSource::Introspection(_)
                    | DataSource::Progress
                    | DataSource::Webhook
                    | DataSource::Other(DataSourceOther::Compute) => None,
                };

                let metadata = CollectionMetadata {
                    persist_location: self.persist_location.clone(),
                    remap_shard,
                    data_shard,
                    status_shard,
                    relation_desc: description.desc.clone(),
                    txns_shard,
                };

                Ok((id, description, metadata))
            })
            .collect_vec();

        // So that we can open `SinceHandle`s for each collections concurrently.
        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .unwrap();
        let persist_client = &persist_client;
        // Reborrow the `&mut self` as immutable, as all the concurrent work to
        // be processed in this stream cannot all have exclusive access.
        use futures::stream::{StreamExt, TryStreamExt};
        let this = &*self;
        let mut to_register: Vec<_> = futures::stream::iter(enriched_with_metadata)
            .map(|data: Result<_, StorageError<Self::Timestamp>>| {
                let register_ts = register_ts.clone();
                async move {
                let (id, description, metadata) = data?;

                // should be replaced with real introspection
                // (https://github.com/MaterializeInc/materialize/issues/14266)
                // but for now, it's helpful to have this mapping written down
                // somewhere
                debug!(
                    "mapping GlobalId={} to remap shard ({:?}), data shard ({}), status shard ({:?})",
                    id, metadata.remap_shard, metadata.data_shard, metadata.status_shard
                );

                let (write, mut since_handle) = this
                    .open_data_handles(
                        &id,
                        metadata.data_shard,
                        description.since.as_ref(),
                        metadata.relation_desc.clone(),
                        persist_client,
                    )
                    .await;

                // Present tables as springing into existence at the register_ts
                // by advancing the since. Otherwise, we could end up in a
                // situation where a table with a long compaction window appears
                // to exist before the environment (and this the table) existed.
                //
                // We could potentially also do the same thing for other
                // sources, in particular storage's internal sources and perhaps
                // others, but leave them for now.
                match description.data_source {
                    DataSource::Introspection(_)
                    | DataSource::IngestionExport { .. }
                    | DataSource::Webhook
                    | DataSource::Ingestion(_)
                    | DataSource::Progress
                    | DataSource::Other(DataSourceOther::Compute) => {},
                    DataSource::Other(DataSourceOther::TableWrites) => {
                        let register_ts = register_ts.expect("caller should have provided a register_ts when creating a table");
                        if since_handle.since().elements() == &[T::minimum()] {
                            debug!("advancing {} to initial since of {:?}", id, register_ts);
                            let token = since_handle.opaque();
                            let _ = since_handle.compare_and_downgrade_since(&token, (&token, &Antichain::from_elem(register_ts.clone()))).await;
                        }
                    }
                }

                Ok::<_, StorageError<Self::Timestamp>>((id, description, write, since_handle, metadata))
            }})
            // Poll each future for each collection concurrently, maximum of 50 at a time.
            .buffer_unordered(50)
            // HERE BE DRAGONS:
            //
            // There are at least 2 subtleties in using `FuturesUnordered`
            // (which `buffer_unordered` uses underneath:
            // - One is captured here
            //   <https://github.com/rust-lang/futures-rs/issues/2387>
            // - And the other is deadlocking if processing an OUTPUT of a
            //   `FuturesUnordered` stream attempts to obtain an async mutex that
            //   is also obtained in the futures being polled.
            //
            // Both of these could potentially be issues in all usages of
            // `buffer_unordered` in this method, so we stick the standard
            // advice: only use `try_collect` or `collect`!
            .try_collect()
            .await?;

        // Reorder in dependency order.
        to_register.sort_by_key(|(id, ..)| *id);

        // New collections that are being created.
        let mut new_collections = BTreeSet::new();

        // We hold this lock for a very short amount of time, just doing some
        // hashmap inserts and unbounded channel sends.
        let mut self_collections = self.collections.lock().expect("lock poisoned");

        for (id, mut description, write_handle, since_handle, metadata) in to_register {
            new_collections.insert(id);

            // Ensure that the ingestion has an export for its primary source.
            // This is done in an akward spot to appease the borrow checker.
            if let DataSource::Ingestion(ingestion) = &mut description.data_source {
                ingestion.source_exports.insert(
                    id,
                    SourceExport {
                        ingestion_output: None,
                        storage_metadata: (),
                    },
                );
            }

            let write_frontier = write_handle.upper();
            let data_shard_since = since_handle.since().clone();

            // Determine if this collection has any dependencies.
            let storage_dependencies = self.determine_collection_dependencies(
                &mut *self_collections,
                &description.data_source,
            )?;

            // Determine the intial since of the collection.
            let initial_since = match storage_dependencies
                .iter()
                .at_most_one()
                .expect("should have at most one depdendency")
            {
                Some(dep) => {
                    let dependency_collection = self_collections
                        .get(dep)
                        .ok_or(StorageError::IdentifierMissing(*dep))?;
                    let dependency_since = dependency_collection.implied_capability.clone();

                    // If an item has a dependency, its initial since must be
                    // advanced as far as its dependency, i.e. a dependency's
                    // since may never be in advance of its dependents.
                    //
                    // We have to do this every time we initialize the
                    // collection, though––the invariant might have been upheld
                    // correctly in the previous epoch, but the
                    // `data_shard_since` might not have compacted and, on
                    // establishing a new persist connection, still have data we
                    // said _could_ be compacted.
                    if PartialOrder::less_than(&data_shard_since, &dependency_since) {
                        // The dependency since cannot be in advance of the
                        // dependent upper unless the collection is new. If the
                        // dependency since advanced past the dependent's upper,
                        // the dependent cannot read data from the dependency at
                        // its upper.
                        //
                        // Another way of understanding that this is a problem
                        // is that this means that the read hold installed on
                        // the dependency was probably not been upheld––if it
                        // were, the dependency's since could not have advanced
                        // as far the dependent's upper.
                        mz_ore::soft_assert_or_log!(
                            write_frontier.elements() == &[T::minimum()]
                                || PartialOrder::less_than(&dependency_since, write_frontier),
                            "dependency ({dep}) since has advanced past dependent ({id}) upper \n
                            dependent ({id}): since {:?}, upper {:?} \n
                            dependency ({dep}): since {:?}",
                            data_shard_since,
                            write_frontier,
                            dependency_since
                        );

                        dependency_since
                    } else {
                        data_shard_since
                    }
                }
                None => data_shard_since,
            };

            let mut collection_state = CollectionState::new(
                description,
                initial_since,
                write_frontier.clone(),
                storage_dependencies,
                metadata.clone(),
            );

            // Install the collection state in the appropriate spot.
            match &collection_state.description.data_source {
                DataSource::Introspection(_) => {
                    self_collections.insert(id, collection_state);
                }
                DataSource::Webhook => {
                    self_collections.insert(id, collection_state);
                }
                DataSource::IngestionExport {
                    ingestion_id,
                    external_reference,
                } => {
                    // Adjust the source to contain this export.
                    let source_collection = self_collections
                        .get_mut(ingestion_id)
                        .expect("known to exist");
                    match &mut source_collection.description {
                        CollectionDescription {
                            data_source: DataSource::Ingestion(ingestion_desc),
                            ..
                        } => ingestion_desc.source_exports.insert(
                            id,
                            SourceExport {
                                ingestion_output: Some(ExportReference::from(
                                    external_reference.clone(),
                                )),
                                storage_metadata: (),
                            },
                        ),
                        _ => unreachable!(
                            "SourceExport must only refer to primary sources that already exist"
                        ),
                    };

                    self_collections.insert(id, collection_state);
                }
                DataSource::Other(DataSourceOther::TableWrites) => {
                    let register_ts = register_ts
                        .as_ref()
                        .expect("caller should have provided a register_ts when creating a table");
                    // This register call advances the logical upper of the
                    // table. The register call eventually circles that info
                    // back to us, but some tests fail if we don't synchronously
                    // update it in create_collections, so just do that now.
                    let advance_to = mz_persist_types::StepForward::step_forward(register_ts);

                    if let TxnsWal::EnabledLazy { .. } = &self.txns {
                        if collection_state.write_frontier.less_than(&advance_to) {
                            collection_state.write_frontier =
                                Antichain::from_elem(advance_to.clone());
                        }
                    }
                    self_collections.insert(id, collection_state);
                }
                DataSource::Progress | DataSource::Other(DataSourceOther::Compute) => {
                    self_collections.insert(id, collection_state);
                }
                DataSource::Ingestion(_) => {
                    self_collections.insert(id, collection_state);
                }
            }

            self.register_handles(
                id,
                metadata.txns_shard.is_some(),
                since_handle,
                write_handle,
            );

            // If this collection has a dependency, install a read hold on it.
            self.install_collection_dependency_read_holds_inner(&mut *self_collections, id)?;
        }

        drop(self_collections);

        self.synchronize_finalized_shards(storage_metadata);

        Ok(())
    }

    async fn alter_ingestion_source_desc(
        &self,
        ingestion_id: GlobalId,
        source_desc: SourceDesc,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        // The StorageController checks the validity of these. And we just
        // accept them.

        let mut self_collections = self.collections.lock().expect("lock poisoned");
        let collection = self_collections
            .get_mut(&ingestion_id)
            .ok_or(StorageError::IdentifierMissing(ingestion_id))?;

        let curr_ingestion = match &mut collection.description.data_source {
            DataSource::Ingestion(active_ingestion) => active_ingestion,
            _ => unreachable!("verified collection refers to ingestion"),
        };

        curr_ingestion.desc = source_desc;
        debug!("altered {ingestion_id}'s SourceDesc");

        Ok(())
    }

    async fn alter_ingestion_connections(
        &self,
        source_connections: BTreeMap<GlobalId, GenericSourceConnection<InlinedConnection>>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let mut self_collections = self.collections.lock().expect("lock poisoned");

        for (id, conn) in source_connections {
            let collection = self_collections
                .get_mut(&id)
                .ok_or_else(|| StorageError::IdentifierMissing(id))?;

            match &mut collection.description.data_source {
                DataSource::Ingestion(ingestion) => {
                    // If the connection hasn't changed, there's no sense in
                    // re-rendering the dataflow.
                    if ingestion.desc.connection != conn {
                        info!(from = ?ingestion.desc.connection, to = ?conn, "alter_ingestion_connections, updating");
                        ingestion.desc.connection = conn;
                    } else {
                        warn!(
                            "update_source_connection called on {id} but the \
                            connection was the same"
                        );
                    }
                }
                o => {
                    warn!("update_source_connection called on {:?}", o);
                    Err(StorageError::IdentifierInvalid(id))?;
                }
            }
        }

        Ok(())
    }

    fn drop_collections_unvalidated(
        &self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
    ) {
        debug!(?identifiers, "drop_collections_unvalidated");

        let mut self_collections = self.collections.lock().expect("lock poisoned");

        for id in identifiers.iter() {
            let metadata = storage_metadata.get_collection_shard::<T>(*id);
            mz_ore::soft_assert_or_log!(
                matches!(metadata, Err(StorageError::IdentifierMissing(_))),
                "dropping {id}, but drop was not synchronized with storage \
                controller via `synchronize_collections`"
            );

            let dropped_data_source = match self_collections.get(id) {
                Some(col) => col.description.data_source.clone(),
                None => continue,
            };

            // If we are dropping source exports, we need to modify the
            // ingestion that it runs on.
            if let DataSource::IngestionExport { ingestion_id, .. } = dropped_data_source {
                // Adjust the source to remove this export.
                let ingestion = match self_collections.get_mut(&ingestion_id) {
                    Some(ingestion) => ingestion,
                    // Primary ingestion already dropped.
                    None => {
                        tracing::error!(
                            "primary source {ingestion_id} seemingly dropped before subsource {id}",
                        );
                        continue;
                    }
                };

                match &mut ingestion.description {
                    CollectionDescription {
                        data_source: DataSource::Ingestion(ingestion_desc),
                        ..
                    } => {
                        let removed = ingestion_desc.source_exports.remove(id);
                        mz_ore::soft_assert_or_log!(
                            removed.is_some(),
                            "dropped subsource {id} already removed from source exports"
                        );
                    }
                    _ => unreachable!(
                        "SourceExport must only refer to primary sources that already exist"
                    ),
                };
            }
        }

        // Policies that advance the since to the empty antichain. We do still
        // honor outstanding read holds, and collections will only be dropped
        // once those are removed as well.
        //
        // We don't explicitly remove read capabilities! Downgrading the
        // frontier of the source to `[]` (the empty Antichain), will propagate
        // to the storage dependencies.
        let mut finalized_policies = Vec::new();

        for id in identifiers {
            // Make sure it's still there, might already have been deleted.
            if self_collections.contains_key(&id) {
                finalized_policies.push((id, ReadPolicy::ValidFrom(Antichain::new())));
            }
        }
        self.set_read_policies_inner(&mut self_collections, finalized_policies);

        drop(self_collections);

        self.synchronize_finalized_shards(storage_metadata);
    }

    fn set_read_policies(&self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>) {
        let mut collections = self.collections.lock().expect("lock poisoned");

        let user_capabilities = collections
            .iter_mut()
            .filter(|(id, _c)| id.is_user())
            .map(|(id, c)| {
                let updates = c.read_capabilities.updates().cloned().collect_vec();
                (*id, c.implied_capability.clone(), updates)
            })
            .collect_vec();

        trace!(?policies, ?user_capabilities, "set_read_policies");

        self.set_read_policies_inner(&mut collections, policies);

        let user_capabilities = collections
            .iter_mut()
            .filter(|(id, _c)| id.is_user())
            .map(|(id, c)| {
                let updates = c.read_capabilities.updates().cloned().collect_vec();
                (*id, c.implied_capability.clone(), updates)
            })
            .collect_vec();

        trace!(?user_capabilities, "after! set_read_policies");
    }

    fn acquire_read_holds(
        &self,
        desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold<Self::Timestamp>>, ReadHoldError> {
        let mut collections = self.collections.lock().expect("lock poisoned");

        let mut advanced_holds = Vec::new();
        // We advance the holds by our current since frontier. Can't acquire
        // holds for times that have been compacted away!
        //
        // NOTE: We acquire read holds at the earliest possible time rather than
        // at the implied capability. This is so that, for example, adapter can
        // acquire a read hold to hold back the frontier, giving the COMPUTE
        // controller a chance to also acquire a read hold at that early
        // frontier. If/when we change the interplay between adapter and COMPUTE
        // to pass around ReadHold tokens, we might tighten this up and instead
        // acquire read holds at the implied capability.
        for id in desired_holds.iter() {
            let collection = collections
                .get(id)
                .ok_or(ReadHoldError::CollectionMissing(*id))?;
            let since = collection.read_capabilities.frontier().to_owned();
            advanced_holds.push((*id, since));
        }

        let mut updates = advanced_holds
            .iter()
            .map(|(id, hold)| {
                let mut changes = ChangeBatch::new();
                changes.extend(hold.iter().map(|time| (time.clone(), 1)));
                (*id, changes)
            })
            .collect::<BTreeMap<_, _>>();

        StorageCollectionsImpl::update_read_capabilities_inner(
            &self.cmd_tx,
            &mut collections,
            &mut updates,
        );

        let acquired_holds = advanced_holds
            .into_iter()
            .map(|(id, since)| ReadHold::new(id, since, self.holds_tx.clone()))
            .collect_vec();

        trace!(?desired_holds, ?acquired_holds, "acquire_read_holds");

        Ok(acquired_holds)
    }

    fn update_read_capabilities(
        &self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<Self::Timestamp>>,
    ) {
        let mut collections = self.collections.lock().expect("lock poisoned");

        StorageCollectionsImpl::update_read_capabilities_inner(
            &self.cmd_tx,
            &mut collections,
            updates,
        );
    }
}

/// Wraps either a "critical" [SinceHandle] or a leased [ReadHandle].
///
/// When a [StorageCollections] is in read-only mode, we will only ever acquire
/// [ReadHandle], because acquiring the [SinceHandle] and driving forward it's
/// since is considered a write. Conversely, when in read-write mode, we acquire
/// [SinceHandle].
#[derive(Debug)]
enum SinceHandleWrapper<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    Critical(SinceHandle<SourceData, (), T, Diff, PersistEpoch>),
    Leased(ReadHandle<SourceData, (), T, Diff>),
}

impl<T> SinceHandleWrapper<T>
where
    T: TimelyTimestamp + Lattice + Codec64 + TotalOrder,
{
    pub fn since(&self) -> &Antichain<T> {
        match self {
            Self::Critical(handle) => handle.since(),
            Self::Leased(handle) => handle.since(),
        }
    }

    pub fn opaque(&self) -> PersistEpoch {
        match self {
            Self::Critical(handle) => handle.opaque().clone(),
            Self::Leased(_handle) => {
                // The opaque is expected to be used with
                // `compare_and_downgrade_since`, and the leased handle doesn't
                // have a notion of an opaque. We pretend here and in
                // `compare_and_downgrade_since`.
                PersistEpoch(None)
            }
        }
    }

    pub async fn compare_and_downgrade_since(
        &mut self,
        expected: &PersistEpoch,
        new: (&PersistEpoch, &Antichain<T>),
    ) -> Result<Antichain<T>, PersistEpoch> {
        match self {
            Self::Critical(handle) => handle.compare_and_downgrade_since(expected, new).await,
            Self::Leased(handle) => {
                let (opaque, since) = new;
                assert!(opaque.0.is_none());

                handle.downgrade_since(since).await;

                Ok(since.clone())
            }
        }
    }

    pub async fn maybe_compare_and_downgrade_since(
        &mut self,
        expected: &PersistEpoch,
        new: (&PersistEpoch, &Antichain<T>),
    ) -> Option<Result<Antichain<T>, PersistEpoch>> {
        match self {
            Self::Critical(handle) => {
                handle
                    .maybe_compare_and_downgrade_since(expected, new)
                    .await
            }
            Self::Leased(handle) => {
                let (opaque, since) = new;
                assert!(opaque.0.is_none());

                handle.maybe_downgrade_since(since).await;

                Some(Ok(since.clone()))
            }
        }
    }

    pub fn snapshot_stats(
        &self,
        id: GlobalId,
        as_of: Option<Antichain<T>>,
    ) -> BoxFuture<'static, Result<SnapshotStats, StorageError<T>>> {
        match self {
            Self::Critical(handle) => {
                let res = handle
                    .snapshot_stats(as_of)
                    .map(move |x| x.map_err(|_| StorageError::ReadBeforeSince(id)));
                Box::pin(res)
            }
            Self::Leased(handle) => {
                let res = handle
                    .snapshot_stats(as_of)
                    .map(move |x| x.map_err(|_| StorageError::ReadBeforeSince(id)));
                Box::pin(res)
            }
        }
    }

    pub fn snapshot_stats_from_txn(
        &self,
        id: GlobalId,
        data_snapshot: DataSnapshot<T>,
    ) -> BoxFuture<'static, Result<SnapshotStats, StorageError<T>>> {
        match self {
            Self::Critical(handle) => Box::pin(
                data_snapshot
                    .snapshot_stats_from_critical(handle)
                    .map(move |x| x.map_err(|_| StorageError::ReadBeforeSince(id))),
            ),
            Self::Leased(handle) => Box::pin(
                data_snapshot
                    .snapshot_stats_from_leased(handle)
                    .map(move |x| x.map_err(|_| StorageError::ReadBeforeSince(id))),
            ),
        }
    }

    pub async fn expire(self) {
        match self {
            Self::Critical(_handle) => {
                unreachable!("not trying to expire critical handles this way");
            }
            Self::Leased(handle) => {
                handle.expire().await;
            }
        }
    }
}

/// State maintained about individual collections.
#[derive(Debug)]
struct CollectionState<T> {
    /// Description with which the collection was created
    pub description: CollectionDescription<T>,

    /// Accumulation of read capabilities for the collection.
    ///
    /// This accumulation will always contain `self.implied_capability`, but may
    /// also contain capabilities held by others who have read dependencies on
    /// this collection.
    pub read_capabilities: MutableAntichain<T>,

    /// The implicit capability associated with collection creation.  This
    /// should never be less than the since of the associated persist
    /// collection.
    pub implied_capability: Antichain<T>,

    /// The policy to use to downgrade `self.implied_capability`.
    pub read_policy: ReadPolicy<T>,

    /// Storage identifiers on which this collection depends.
    pub storage_dependencies: Vec<GlobalId>,

    /// Reported write frontier.
    pub write_frontier: Antichain<T>,

    pub collection_metadata: CollectionMetadata,
}

impl<T: TimelyTimestamp> CollectionState<T> {
    /// Creates a new collection state, with an initial read policy valid from
    /// `since`.
    pub fn new(
        description: CollectionDescription<T>,
        since: Antichain<T>,
        write_frontier: Antichain<T>,
        storage_dependencies: Vec<GlobalId>,
        metadata: CollectionMetadata,
    ) -> Self {
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));
        Self {
            description,
            read_capabilities,
            implied_capability: since.clone(),
            read_policy: ReadPolicy::NoPolicy {
                initial_since: since,
            },
            storage_dependencies,
            write_frontier,
            collection_metadata: metadata,
        }
    }

    /// Returns whether the collection was dropped.
    pub fn is_dropped(&self) -> bool {
        self.read_capabilities.is_empty()
    }
}

/// A task that keeps persist handles, downgrades sinces when asked,
/// periodically gets recent uppers from them, and updates the shard collection
/// state when needed.
///
/// This shares state with [StorageCollectionsImpl] via `Arcs` and channels.
#[derive(Debug)]
struct BackgroundTask<T: TimelyTimestamp + Lattice + Codec64> {
    envd_epoch: NonZeroI64,
    config: Arc<Mutex<StorageConfiguration>>,
    cmds_tx: mpsc::UnboundedSender<BackgroundCmd<T>>,
    cmds_rx: mpsc::UnboundedReceiver<BackgroundCmd<T>>,
    holds_rx: mpsc::UnboundedReceiver<(GlobalId, ChangeBatch<T>)>,
    finalizable_shards: Arc<std::sync::Mutex<BTreeSet<ShardId>>>,
    finalized_shards: Arc<std::sync::Mutex<BTreeSet<ShardId>>>,
    collections: Arc<std::sync::Mutex<BTreeMap<GlobalId, CollectionState<T>>>>,
    // So we know what shard ID corresponds to what global ID, which we need
    // when re-enqueing futures for determining the next upper update.
    shard_by_id: BTreeMap<GlobalId, ShardId>,
    since_handles: BTreeMap<GlobalId, SinceHandleWrapper<T>>,
    txns_handle: Option<WriteHandle<SourceData, (), T, Diff>>,
    txns_shards: BTreeSet<GlobalId>,
    persist_location: PersistLocation,
    persist: Arc<PersistClientCache>,
}

#[derive(Debug)]
enum BackgroundCmd<T: TimelyTimestamp + Lattice + Codec64> {
    Register {
        id: GlobalId,
        is_in_txns: bool,
        write_handle: WriteHandle<SourceData, (), T, Diff>,
        since_handle: SinceHandleWrapper<T>,
    },
    // This was also dead code in the StorageController. I think we keep around
    // these code paths for when we need to do migrations in the future.
    #[allow(dead_code)]
    Update {
        id: GlobalId,
        is_in_txns: bool,
        write_handle: WriteHandle<SourceData, (), T, Diff>,
        since_handle: SinceHandleWrapper<T>,
    },
    DowngradeSince(Vec<(GlobalId, Antichain<T>)>),
    SnapshotStats(
        GlobalId,
        SnapshotStatsAsOf<T>,
        oneshot::Sender<SnapshotStatsRes<T>>,
    ),
}

/// A newtype wrapper to hang a Debug impl off of.
pub(crate) struct SnapshotStatsRes<T>(BoxFuture<'static, Result<SnapshotStats, StorageError<T>>>);

impl<T> Debug for SnapshotStatsRes<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotStatsRes").finish_non_exhaustive()
    }
}

impl<T> BackgroundTask<T>
where
    T: TimelyTimestamp
        + Lattice
        + Codec64
        + From<EpochMillis>
        + TimestampManipulation
        + Into<mz_repr::Timestamp>,
{
    async fn run(&mut self) {
        // Futures that fetch the recent upper from all other shards.
        let mut upper_futures: FuturesUnordered<
            std::pin::Pin<
                Box<
                    dyn Future<
                            Output = (GlobalId, WriteHandle<SourceData, (), T, i64>, Antichain<T>),
                        > + Send,
                >,
            >,
        > = FuturesUnordered::new();

        let gen_upper_future = |id: GlobalId, mut handle: WriteHandle<SourceData, (), T, i64>| {
            let fut = async move {
                let current_upper = handle.shared_upper();
                handle.wait_for_upper_past(&current_upper).await;
                let new_upper = handle.shared_upper();
                (id, handle, new_upper)
            };

            fut
        };

        let mut txns_upper_future = match self.txns_handle.take() {
            Some(txns_handle) => {
                let txns_upper_future = gen_upper_future(GlobalId::Transient(1), txns_handle);
                txns_upper_future.boxed()
            }
            None => async { std::future::pending().await }.boxed(),
        };

        // We check periodically if there's any shards that we need to finalize
        // and do so if yes.
        let mut finalization_interval = tokio::time::interval(Duration::from_secs(5));
        finalization_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                (id, handle, upper) = &mut txns_upper_future => {
                    trace!("new upper from txns shard: {:?}", upper);
                    let mut uppers = Vec::new();
                    for id in self.txns_shards.iter() {
                        uppers.push((id.clone(), upper.clone()));
                    }
                    self.update_write_frontiers(&uppers).await;

                    let fut = gen_upper_future(id, handle);
                    txns_upper_future = fut.boxed();
                }
                Some((id, handle, upper)) = upper_futures.next() => {
                    if id.is_user() {
                        trace!("new upper for collection {id}: {:?}", upper);
                    }
                    let current_shard = self.shard_by_id.get(&id);
                    if let Some(shard_id) = current_shard {
                        if shard_id == &handle.shard_id() {
                            // Still current, so process the update and enqueue
                            // again!
                            let uppers = vec![(id, upper)];
                            self.update_write_frontiers(&uppers).await;
                            let fut = gen_upper_future(id, handle);
                            upper_futures.push(fut.boxed());
                        } else {
                            // Be polite and expire the write handle. This can
                            // happen when we get an upper update for a write
                            // handle that has since been replaced via Update.
                            handle.expire().await;
                        }
                    }
                }
                cmd = self.cmds_rx.recv() => {
                    let cmd = if let Some(cmd) = cmd {
                        cmd
                    } else {
                        // We're done!
                        break;
                    };

                    match cmd {
                        BackgroundCmd::Register{ id, is_in_txns, write_handle, since_handle } => {
                            debug!("registering handles for {}", id);
                            let previous = self.shard_by_id.insert(id, write_handle.shard_id());
                            if previous.is_some() {
                                panic!("already registered a WriteHandle for collection {id}");
                            }

                            let previous = self.since_handles.insert(id, since_handle);
                            if previous.is_some() {
                                panic!("already registered a SinceHandle for collection {id}");
                            }

                            if is_in_txns {
                                self.txns_shards.insert(id);
                            } else {
                                let fut = gen_upper_future(id, write_handle);
                                upper_futures.push(fut.boxed());
                            }

                        }
                        BackgroundCmd::DowngradeSince(cmds) => {
                            self.downgrade_sinces(cmds).await;
                        }
                        BackgroundCmd::Update { id, is_in_txns, write_handle, since_handle } => {
                            self.shard_by_id.insert(id, write_handle.shard_id()).expect(
                                "BackgroundCmd::Update only valid for updating extant write handles",
                            );

                            self.since_handles.insert(id, since_handle).expect(
                                "BackgroundCmd::Update only valid for updating extant since handles",
                            );

                            if is_in_txns {
                                self.txns_shards.insert(id);
                            } else {
                                let fut = gen_upper_future(id, write_handle);
                                upper_futures.push(fut.boxed());
                            }

                        }
                        BackgroundCmd::SnapshotStats(id, as_of, tx) => {
                            // NB: The requested as_of could be arbitrarily far
                            // in the future. So, in order to avoid blocking
                            // this loop until it's available and the
                            // `snapshot_stats` call resolves, instead return
                            // the future to the caller and await it there.
                            let res = match self.since_handles.get(&id) {
                                Some(x) => {
                                    let fut: BoxFuture<
                                        'static,
                                        Result<SnapshotStats, StorageError<T>>,
                                    > = match as_of {
                                        SnapshotStatsAsOf::Direct(as_of) => {
                                            x.snapshot_stats(id, Some(as_of))
                                        }
                                        SnapshotStatsAsOf::Txns(data_snapshot) => {
                                            x.snapshot_stats_from_txn(id, data_snapshot)
                                        }
                                    };
                                    SnapshotStatsRes(fut)
                                }
                                None => SnapshotStatsRes(Box::pin(futures::future::ready(Err(
                                    StorageError::IdentifierMissing(id),
                                )))),
                            };
                            // It's fine if the listener hung up.
                            let _ = tx.send(res);
                        }
                    }
                }
                Some(holds_changes) = self.holds_rx.recv() => {
                    let mut batched_changes = BTreeMap::new();
                    batched_changes.insert(holds_changes.0, holds_changes.1);

                    while let Ok(mut holds_changes) = self.holds_rx.try_recv() {
                        let entry = batched_changes.entry(holds_changes.0);
                        entry
                            .and_modify(|existing| existing.extend(holds_changes.1.drain()))
                            .or_insert_with(|| holds_changes.1);
                    }

                    let mut collections = self.collections.lock().expect("lock poisoned");

                    let user_changes = batched_changes
                        .iter()
                        .filter(|(id, _c)| id.is_user())
                        .map(|(id, c)| {
                            (id.clone(), c.clone())
                        })
                        .collect_vec();

                    if !user_changes.is_empty() {
                        trace!(?user_changes, "applying holds changes from channel");
                    }

                    StorageCollectionsImpl::update_read_capabilities_inner(
                        &self.cmds_tx,
                        &mut collections,
                        &mut batched_changes,
                    );
                }
                _time = finalization_interval.tick() => {
                    self.finalize_shards().await;
                }
            }
        }

        warn!("BackgroundTask shutting down");
    }

    #[instrument(level = "debug")]
    async fn update_write_frontiers(&mut self, updates: &[(GlobalId, Antichain<T>)]) {
        let mut read_capability_changes = BTreeMap::default();

        let mut self_collections = self.collections.lock().expect("lock poisoned");

        for (id, new_upper) in updates.iter() {
            let collection = if let Some(c) = self_collections.get_mut(id) {
                c
            } else {
                warn!("Reference to absent collection {id}");
                continue;
            };

            if PartialOrder::less_than(&collection.write_frontier, new_upper) {
                collection.write_frontier.clone_from(new_upper);
            }

            let mut new_read_capability = collection
                .read_policy
                .frontier(collection.write_frontier.borrow());

            if id.is_user() {
                trace!(
                    %id,
                    implied_capability = ?collection.implied_capability,
                    policy = ?collection.read_policy,
                    write_frontier = ?collection.write_frontier,
                    ?new_read_capability,
                    "update_write_frontiers");
            }

            if PartialOrder::less_equal(&collection.implied_capability, &new_read_capability) {
                let mut update = ChangeBatch::new();
                update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));

                if !update.is_empty() {
                    read_capability_changes.insert(*id, update);
                }
            }
        }

        if !read_capability_changes.is_empty() {
            StorageCollectionsImpl::update_read_capabilities_inner(
                &self.cmds_tx,
                &mut self_collections,
                &mut read_capability_changes,
            );
        }
    }

    async fn downgrade_sinces(&mut self, cmds: Vec<(GlobalId, Antichain<T>)>) {
        for (id, new_since) in cmds {
            let since_handle = if let Some(c) = self.since_handles.get_mut(&id) {
                c
            } else {
                // This can happen when someone concurrently drops a collection.
                trace!("downgrade_sinces: reference to absent collection {id}");
                continue;
            };

            if id.is_user() {
                trace!("downgrading since of {} to {:?}", id, new_since);
            }

            let epoch = since_handle.opaque().clone();
            let result = if new_since.is_empty() {
                // A shard's since reaching the empty frontier is a prereq for
                // being able to finalize a shard, so the final downgrade should
                // never be rate-limited.
                let res = Some(
                    since_handle
                        .compare_and_downgrade_since(&epoch, (&epoch, &new_since))
                        .await,
                );

                info!(%id, "removing persist handles because the since advanced to []!");

                let _since_handle = self.since_handles.remove(&id).expect("known to exist");
                let dropped_shard_id = if let Some(shard_id) = self.shard_by_id.remove(&id) {
                    shard_id
                } else {
                    panic!("missing GlobalId -> ShardId mapping for id {id}");
                };

                // We're not responsible for writes to tables, so we also don't
                // de-register them from the txn system. Whoever is responsible
                // will remove them. We only make sure to remove the table from
                // our tracking.
                self.txns_shards.remove(&id);

                if !self
                    .config
                    .lock()
                    .expect("lock poisoned")
                    .parameters
                    .finalize_shards
                {
                    info!("not triggering shard finalization due to dropped storage object because enable_storage_shard_finalization parameter is false");
                    return;
                }

                info!(%id, %dropped_shard_id, "enqueing shard finalization due to dropped collection and dropped persist handle");

                self.finalizable_shards
                    .lock()
                    .expect("lock poisoned")
                    .insert(dropped_shard_id);

                res
            } else {
                since_handle
                    .maybe_compare_and_downgrade_since(&epoch, (&epoch, &new_since))
                    .await
            };

            if let Some(Err(other_epoch)) = result {
                mz_ore::halt!("fenced by envd @ {other_epoch:?}. ours = {epoch:?}");
            }
        }
    }

    /// Attempts to close all shards marked for finalization.
    #[instrument(level = "debug")]
    async fn finalize_shards(&mut self) {
        if !self
            .config
            .lock()
            .expect("lock poisoned")
            .parameters
            .finalize_shards
        {
            debug!("not triggering shard finalization due to dropped storage object because enable_storage_shard_finalization parameter is false");
            return;
        }

        let finalizable_shards = {
            // We hold the lock for as short as possible and pull our cloned set
            // of shards.
            let shared_shards = self.finalizable_shards.lock().expect("lock poisoned");
            shared_shards.iter().cloned().collect_vec()
        };

        if finalizable_shards.is_empty() {
            debug!("no shards to finalize");
            return;
        }

        debug!(?finalizable_shards, "attempting to finalize shards");

        // Open a persist client to delete unused shards.
        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .unwrap();

        let persist_client = &persist_client;
        let diagnostics = &Diagnostics::from_purpose("finalizing shards");

        let force_downgrade_since = STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION
            .get(self.config.lock().expect("lock poisoned").config_set());

        let epoch = &PersistEpoch::from(self.envd_epoch);

        use futures::stream::StreamExt;
        let finalized_shards: BTreeSet<ShardId> = futures::stream::iter(finalizable_shards.clone())
            .map(|shard_id| async move {
                let persist_client = persist_client.clone();
                let diagnostics = diagnostics.clone();
                let epoch = epoch.clone();

                let is_finalized = persist_client
                    .is_finalized::<SourceData, (), T, Diff>(shard_id, diagnostics)
                    .await
                    .expect("invalid persist usage");

                if is_finalized {
                    debug!(%shard_id, "shard is already finalized!");
                    Some(shard_id)
                } else {
                    debug!(%shard_id, "spawning new finalization task");
                    // Finalizing a shard can take a long time cleaning up
                    // existing data. Spawning a task means that we can't
                    // proactively remove this shard from the finalization
                    // register, unfortunately... but a future run of
                    // `finalize_shards` should notice the shard has been
                    // finalized and tidy up.
                    mz_ore::task::spawn(|| format!("finalize_shard({shard_id})"), async move {
                        let finalize = || async move {
                            let empty_batch: Vec<((SourceData, ()), T, Diff)> = vec![];
                            let mut write_handle: WriteHandle<SourceData, (), T, Diff> =
                                persist_client
                                    .open_writer(
                                        shard_id,
                                        Arc::new(RelationDesc::empty()),
                                        Arc::new(UnitSchema),
                                        // TODO: thread the global ID into the shard finalization WAL
                                        Diagnostics::from_purpose("finalizing shards"),
                                    )
                                    .await
                                    .expect("invalid persist usage");

                            let upper = write_handle.upper();

                            if !upper.is_empty() {
                                let append = write_handle
                                    .append(empty_batch, upper.clone(), Antichain::new())
                                    .await?;

                                if let Err(e) = append {
                                    warn!(%shard_id, "tried to finalize a shard with an advancing upper: {e:?}");
                                    return Ok(());
                                }
                            }
                            write_handle.expire().await;

                            if force_downgrade_since {
                                let mut since_handle: SinceHandle<
                                    SourceData,
                                    (),
                                    T,
                                    Diff,
                                    PersistEpoch,
                                > = persist_client
                                    .open_critical_since(
                                        shard_id,
                                        PersistClient::CONTROLLER_CRITICAL_SINCE,
                                        Diagnostics::from_purpose("finalizing shards"),
                                    )
                                    .await
                                    .expect("invalid persist usage");
                                let handle_epoch = since_handle.opaque().clone();
                                let our_epoch = epoch.clone();
                                let epoch = if our_epoch.0 > handle_epoch.0 {
                                    // We're newer, but it's fine to use the
                                    // handle's old epoch to try and downgrade.
                                    handle_epoch
                                } else {
                                    // Good luck, buddy! The downgrade below
                                    // will not succeed. There's a process with
                                    // a newer epoch out there and someone at
                                    // some juncture will fence out this
                                    // process.
                                    our_epoch
                                };
                                let new_since = Antichain::new();
                                let downgrade = since_handle
                                    .compare_and_downgrade_since(&epoch, (&epoch, &new_since))
                                    .await;
                                if let Err(e) = downgrade {
                                    warn!(
                                        "tried to finalize a shard with an advancing epoch: {e:?}"
                                    );
                                    return Ok(());
                                }
                                // Not available now, so finalization is broken.
                                // since_handle.expire().await;
                            }

                            persist_client
                                .finalize_shard::<SourceData, (), T, Diff>(
                                    shard_id,
                                    Diagnostics::from_purpose("finalizing shards"),
                                )
                                .await
                        };

                        match finalize().await {
                            Err(e) => {
                                // Rather than error, just leave this shard as
                                // one to finalize later.
                                warn!("error during background finalization of shard {shard_id}: {e:?}");
                            }
                            Ok(()) => {
                                debug!(%shard_id, "finalize success!");
                            }
                        }
                    });
                    None
                }
            })
            // Poll each future for each collection concurrently, maximum of 10
            // at a time.
            .buffer_unordered(10)
            // HERE BE DRAGONS: see warning on other uses of buffer_unordered
            // before any changes to `collect`
            .collect::<BTreeSet<Option<ShardId>>>()
            .await
            .into_iter()
            .filter_map(|shard| shard)
            .collect();

        let mut shared_finalizable_shards = self.finalizable_shards.lock().expect("lock poisoned");
        let mut shared_finalized_shards = self.finalized_shards.lock().expect("lock poisoned");

        for id in finalized_shards.iter() {
            shared_finalizable_shards.remove(id);
            shared_finalized_shards.insert(*id);
        }

        debug!(
            ?finalizable_shards,
            ?finalized_shards,
            ?shared_finalizable_shards,
            ?shared_finalized_shards,
            "done finalizing shards"
        );
    }
}

#[derive(Debug)]
pub(crate) enum SnapshotStatsAsOf<T: TimelyTimestamp + Lattice + Codec64> {
    /// Stats for a shard with an "eager" upper (one that continually advances
    /// as time passes, even if no writes are coming in).
    Direct(Antichain<T>),
    /// Stats for a shard with a "lazy" upper (one that only physically advances
    /// in response to writes).
    Txns(DataSnapshot<T>),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_dyncfg::ConfigSet;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;
    use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_repr::{RelationDesc, Row};
    use mz_secrets::InMemorySecretsController;

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: integer-to-pointer casts and `ptr::from_exposed_addr`
    async fn test_snapshot_stats(&self) {
        let persist_location = PersistLocation {
            blob_uri: "mem://".to_owned(),
            consensus_uri: "mem://".to_owned(),
        };
        let persist_client = PersistClientCache::new(
            PersistConfig::new_default_configs(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone()),
            &MetricsRegistry::new(),
            |_, _| PubSubClientConnection::noop(),
        );
        let persist_client = Arc::new(persist_client);

        let (cmds_tx, mut background_task) =
            BackgroundTask::new_for_test(persist_location.clone(), Arc::clone(&persist_client));
        let background_task =
            mz_ore::task::spawn(|| "storage_collections::background_task", async move {
                background_task.run().await
            });

        let persist = persist_client.open(persist_location).await.unwrap();

        let shard_id = ShardId::new();
        let since_handle = persist
            .open_critical_since(
                shard_id,
                PersistClient::CONTROLLER_CRITICAL_SINCE,
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();
        let write_handle = persist
            .open_writer::<SourceData, (), mz_repr::Timestamp, i64>(
                shard_id,
                Arc::new(RelationDesc::empty()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();

        cmds_tx
            .send(BackgroundCmd::Register {
                id: GlobalId::User(1),
                is_in_txns: false,
                since_handle: SinceHandleWrapper::Critical(since_handle),
                write_handle,
            })
            .unwrap();

        let mut write_handle = persist
            .open_writer::<SourceData, (), mz_repr::Timestamp, i64>(
                shard_id,
                Arc::new(RelationDesc::empty()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();

        // No stats for unknown GlobalId.
        let stats =
            snapshot_stats(&cmds_tx, GlobalId::User(2), Antichain::from_elem(0.into())).await;
        assert!(stats.is_err());

        // Stats don't resolve for as_of past the upper.
        let stats_fut = snapshot_stats(&cmds_tx, GlobalId::User(1), Antichain::from_elem(1.into()));
        assert!(stats_fut.now_or_never().is_none());

        // // Call it again because now_or_never consumed our future and it's not clone-able.
        let stats_ts1_fut =
            snapshot_stats(&cmds_tx, GlobalId::User(1), Antichain::from_elem(1.into()));

        // Write some data.
        let data = (
            (SourceData(Ok(Row::default())), ()),
            mz_repr::Timestamp::from(0),
            1i64,
        );
        let () = write_handle
            .compare_and_append(
                &[data],
                Antichain::from_elem(0.into()),
                Antichain::from_elem(1.into()),
            )
            .await
            .unwrap()
            .unwrap();

        // Verify that we can resolve stats for ts 0 while the ts 1 stats call is outstanding.
        let stats = snapshot_stats(&cmds_tx, GlobalId::User(1), Antichain::from_elem(0.into()))
            .await
            .unwrap();
        assert_eq!(stats.num_updates, 1);

        // Write more data and unblock the ts 1 call
        let data = (
            (SourceData(Ok(Row::default())), ()),
            mz_repr::Timestamp::from(1),
            1i64,
        );
        let () = write_handle
            .compare_and_append(
                &[data],
                Antichain::from_elem(1.into()),
                Antichain::from_elem(2.into()),
            )
            .await
            .unwrap()
            .unwrap();

        let stats = stats_ts1_fut.await.unwrap();
        assert_eq!(stats.num_updates, 2);

        // Make sure it runs until at least here.
        drop(background_task);
    }

    async fn snapshot_stats<T: TimelyTimestamp + Lattice + Codec64>(
        cmds_tx: &mpsc::UnboundedSender<BackgroundCmd<T>>,
        id: GlobalId,
        as_of: Antichain<T>,
    ) -> Result<SnapshotStats, StorageError<T>> {
        let (tx, rx) = oneshot::channel();
        cmds_tx
            .send(BackgroundCmd::SnapshotStats(
                id,
                SnapshotStatsAsOf::Direct(as_of),
                tx,
            ))
            .unwrap();
        let res = rx.await.expect("BackgroundTask should be live").0;

        res.await
    }

    impl<T: TimelyTimestamp + Lattice + Codec64> BackgroundTask<T> {
        fn new_for_test(
            persist_location: PersistLocation,
            persist_client: Arc<PersistClientCache>,
        ) -> (mpsc::UnboundedSender<BackgroundCmd<T>>, Self) {
            let (cmds_tx, cmds_rx) = mpsc::unbounded_channel();
            let (_holds_tx, holds_rx) = mpsc::unbounded_channel();
            let connection_context =
                ConnectionContext::for_tests(Arc::new(InMemorySecretsController::new()));

            let task = Self {
                envd_epoch: NonZeroI64::new(1).unwrap(),
                config: Arc::new(Mutex::new(StorageConfiguration::new(
                    connection_context,
                    ConfigSet::default(),
                ))),
                cmds_tx: cmds_tx.clone(),
                cmds_rx,
                holds_rx,
                finalizable_shards: Arc::new(Mutex::new(BTreeSet::new())),
                finalized_shards: Arc::new(Mutex::new(BTreeSet::new())),
                collections: Arc::new(Mutex::new(BTreeMap::new())),
                shard_by_id: BTreeMap::new(),
                since_handles: BTreeMap::new(),
                txns_handle: None,
                txns_shards: BTreeSet::new(),
                persist_location,
                persist: persist_client,
            };

            (cmds_tx, task)
        }
    }
}
