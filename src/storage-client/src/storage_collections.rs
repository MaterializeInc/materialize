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

use mz_ore::instrument;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_txn::metrics::Metrics as TxnMetrics;
use mz_persist_txn::txn_read::{DataSnapshot, TxnsRead};
use mz_persist_txn::txns::TxnsHandle;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, RelationDesc, TimestampManipulation};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::inline::InlinedConnection;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::{
    CollectionMetadata, PersistTxnTablesImpl, StorageError, TxnsCodecRow,
};
use mz_storage_types::dyncfgs::STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_holds::{ReadHold, ReadHoldError};
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sources::{
    GenericSourceConnection, IngestionDescription, SourceConnection, SourceData, SourceDesc,
    SourceExport,
};
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
/// - Hands out [ReadHold] that prevent a collection's since from advancing
///   while it needs to be read at a specific time.
#[async_trait(?Send)]
pub trait StorageCollections: Debug {
    type Timestamp: TimelyTimestamp;

    /// On boot, seed metadata/state.
    async fn initialize_state(
        &mut self,
        txn: &mut dyn StorageTxn<Self::Timestamp>,
        init_ids: BTreeSet<GlobalId>,
        drop_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Resets the txns system to a set of invariants necessary for correctness.
    ///
    /// Must be called on boot before create_collections or the various appends.
    /// This is true _regardless_ of whether the persist-txn feature is on or
    /// not. See the big comment in the impl of the method for details. Ideally,
    /// this would have just been folded into `Controller::new`, but it needs
    /// the timestamp and there are boot dependency issues.
    ///
    /// TODO: This can be removed once we've flipped to the new txns system for
    /// good and there is no possibility of the old code running concurrently
    /// with the new code.
    async fn init_txns(
        &mut self,
        init_ts: Self::Timestamp,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Marks the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before
    /// implementing prior commands, and so it is important for a user to invoke
    /// this method as soon as it is comfortable. This method can be invoked
    /// immediately, at the potential expense of performance.
    fn initialization_complete(&mut self);

    /// Update storage configuration with new parameters.
    fn update_parameters(&mut self, config_params: StorageParameters);

    /// Returns the [CollectionMetadata] of the collection identified by `id`.
    fn collection_metadata(
        &self,
        id: GlobalId,
    ) -> Result<CollectionMetadata, StorageError<Self::Timestamp>>;

    /// Returns the since/upper frontiers of the identified collection.
    fn collection_frontiers(
        &self,
        id: GlobalId,
    ) -> Result<
        (Antichain<Self::Timestamp>, Antichain<Self::Timestamp>),
        StorageError<Self::Timestamp>,
    >;

    /// Returns the since/upper frontiers of the identified collections.
    ///
    /// Having a method that returns both frontiers at the same time, for all
    /// requested collections, ensures that we can get a consistent "snapshot"
    /// of collection state. If we had separate methods instead, and/or would
    /// allow getting frontiers for collections one at a time, it could happen
    /// that collection state changes concurrently, while information is
    /// gathered.
    fn collections_frontiers(
        &self,
        id: Vec<GlobalId>,
    ) -> Result<
        Vec<(
            GlobalId,
            Antichain<Self::Timestamp>,
            Antichain<Self::Timestamp>,
        )>,
        StorageError<Self::Timestamp>,
    >;

    /// Acquire an iterator over [CollectionMetadata] for all active
    /// collections.
    ///
    /// A collection is "active" when it has not been dropped via
    /// [StorageCollections::drop_collections]. Collections that have been
    /// dropped by still have outstanding [ReadHolds](ReadHold) are not
    /// considered active for this method.
    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)>;

    /// Returns the frontier of read capabilities and the write frontier for all
    /// active collections.
    ///
    /// This is different from [StorageCollections::collections_frontiers] which
    /// returns the implied capability and the write frontier.
    ///
    /// A collection is "active" when it has a non empty frontier of read
    /// capabilties.
    // WIP: What to do about this difference in frontier reporting? Some of the
    // previous callsites wanted the implied capability and some wanted the
    // frontier of read capabilities.
    fn active_collection_frontiers(
        &self,
    ) -> Vec<(
        GlobalId,
        Antichain<Self::Timestamp>,
        Antichain<Self::Timestamp>,
    )>;

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
    async fn snapshot_parts_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> Result<SnapshotPartsStats, StorageError<Self::Timestamp>>;

    /// Update the given [`StorageTxn`] with the appropriate metadata given the
    /// IDs to add and drop.
    ///
    /// The data modified in the `StorageTxn` must be made available in all
    /// subsequent calls that require [`StorageMetadata`] as a parameter.
    async fn prepare_state(
        &mut self,
        txn: &mut dyn StorageTxn<Self::Timestamp>,
        ids_to_add: BTreeSet<GlobalId>,
        ids_to_drop: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Create the sources described in the individual RunIngestionCommand
    /// commands.
    ///
    /// Each command carries the source id, the source description, and any
    /// associated metadata needed to ingest the particular source.
    ///
    /// This command installs collection state for the indicated sources, and
    /// the are now valid to use in queries at times beyond the initial `since`
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
        &mut self,
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
        &mut self,
        ingestion_id: GlobalId,
        source_desc: SourceDesc,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Alters each identified collection to use the correlated
    /// [`GenericSourceConnection`].
    ///
    /// See NOTE on [StorageCollections::alter_ingestion_source_desc].
    async fn alter_ingestion_connections(
        &mut self,
        source_connections: BTreeMap<GlobalId, GenericSourceConnection<InlinedConnection>>,
    ) -> Result<(), StorageError<Self::Timestamp>>;

    /// Drops the read capability for the collections and allows their resources
    /// to be reclaimed.
    ///
    /// Collection state is only fully dropped, however, once there are not more
    /// outstanding [ReadHolds](ReadHold) for a collection.
    fn drop_collections(
        &mut self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
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
        &mut self,
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
    fn set_read_policies(&mut self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>);

    /// Acquires and returns the earliest possible read holds for the specified
    /// collections.
    fn acquire_read_holds(
        &mut self,
        desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold<Self::Timestamp>>, ReadHoldError>;

    /// Applies `updates` and sends any appropriate compaction command.
    ///
    /// This is a legacy interface that should _not_ be used! It is only used by
    /// the compute controller.
    fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<Self::Timestamp>>,
    );
}

#[derive(Debug, Clone)]
enum PersistTxns<T> {
    EnabledEager,
    EnabledLazy { txns_read: TxnsRead<T> },
}

impl<T: TimelyTimestamp + Lattice + Codec64> PersistTxns<T> {
    fn expect_enabled_lazy(&self, txns_id: &ShardId) -> &TxnsRead<T> {
        match self {
            PersistTxns::EnabledLazy { txns_read, .. } => {
                assert_eq!(txns_id, txns_read.txns_id());
                txns_read
            }
            PersistTxns::EnabledEager { .. } => {
                panic!("set if txns are enabled and lazy")
            }
        }
    }
}

/// Implementation of [StorageCollections] that is shallow-cloneable and uses a
/// [background task](BackgroundTask) for doing work concurrently, in the
/// background.
#[derive(Debug, Clone)]
pub struct StorageCollectionsImpl<
    T: TimelyTimestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
> {
    /// The fencing token for this instance of [StorageCollections], and really
    /// all of the controllers and Coordinator.
    envd_epoch: NonZeroI64,

    /// The set of [ShardIds](ShardId) to finalize.
    ///
    /// This is a separate set from `finalized_shards` because we know that some
    /// environments have many, many finalizable shards that we are struggling
    /// to finalize.
    finalizable_shards: Arc<std::sync::Mutex<BTreeSet<ShardId>>>,

    /// The set of [ShardIds](ShardId) we have finalized.
    ///
    /// This is a separate set from `finalizable_shards` because we know that
    /// some environments have many, many finalizable shards that we are
    /// struggling to finalize.
    finalized_shards: Arc<std::sync::Mutex<BTreeSet<ShardId>>>,

    /// Collections maintained by this [StorageCollections].
    ///
    /// This collection only grows, although individual collections may be
    /// rendered unusable. This is to prevent the re-binding of identifiers to
    /// other descriptions.
    collections: Arc<std::sync::Mutex<BTreeMap<GlobalId, CollectionState<T>>>>,

    /// Whether to use the new persist-txn tables implementation or the legacy
    /// one.
    txns: PersistTxns<T>,
    /// Whether we have run `txns_init` yet (required before create_collections
    /// and the various flavors of append).
    txns_init_run: bool,

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
        persist_txn_tables: PersistTxnTablesImpl,
        connection_context: ConnectionContext,
        txn: &dyn StorageTxn<T>,
    ) -> Self {
        // This value must be already installed because we must ensure it's
        // durably recorded before it is used, otherwise we risk leaking persist
        // state.
        let txns_id = txn
            .get_persist_txn_shard()
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
        let txns_write = txns_client
            .open_writer(
                txns_id,
                Arc::new(RelationDesc::empty()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: "txns".to_owned(),
                    handle_purpose: "commit txns".to_owned(),
                },
            )
            .await
            .expect("txns schema shouldn't change");

        let txns = match persist_txn_tables {
            PersistTxnTablesImpl::Lazy => {
                let txns_read = TxnsRead::start::<TxnsCodecRow>(txns_client.clone(), txns_id).await;
                PersistTxns::EnabledLazy { txns_read }
            }
            PersistTxnTablesImpl::Eager => PersistTxns::EnabledEager,
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
            txns_init_run: false,
            envd_epoch,
            config,
            persist_location,
            persist: persist_clients,
            cmd_tx,
            holds_tx,
            _background_task: Arc::new(background_task.abort_on_drop()),
        }
    }

    /// Validate that a collection exists for all identifiers, and error if any
    /// do not.
    fn validate_collection_ids(
        &self,
        ids: impl Iterator<Item = GlobalId>,
    ) -> Result<(), StorageError<T>> {
        for id in ids {
            self.check_exists(id)?;
        }
        Ok(())
    }

    /// Opens a write and critical since handles for the given `shard`.
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
    ) -> (
        WriteHandle<SourceData, (), T, Diff>,
        SinceHandle<SourceData, (), T, Diff, PersistEpoch>,
    ) {
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

        (write, since_handle)
    }

    fn register_handles(
        &self,
        id: GlobalId,
        is_in_txns: bool,
        since_handle: SinceHandle<SourceData, (), T, Diff, PersistEpoch>,
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
        self.cmd_tx.send(cmd).expect("task unexpectedly shut down");
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

        // Repeatedly extract the maximum id, and updates for it.
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
    fn synchronize_finalized_shards(&mut self, storage_metadata: &StorageMetadata) {
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
#[async_trait(?Send)]
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
        &mut self,
        txn: &mut dyn StorageTxn<T>,
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

    /// See [crate::controller::StorageController::init_txns] and its
    /// implementation.
    async fn init_txns(
        &mut self,
        _init_ts: Self::Timestamp,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        assert_eq!(self.txns_init_run, false);

        // We don't initialize the txns system, the `StorageController` does
        // that. All we care about is that initialization has been run.
        self.txns_init_run = true;
        Ok(())
    }

    fn initialization_complete(&mut self) {
        // Nothing to do for now!
    }

    fn update_parameters(&mut self, config_params: StorageParameters) {
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

    fn collection_frontiers(
        &self,
        id: GlobalId,
    ) -> Result<
        (Antichain<Self::Timestamp>, Antichain<Self::Timestamp>),
        StorageError<Self::Timestamp>,
    > {
        let collections = self.collections.lock().expect("lock poisoned");

        let collection = collections
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))?;

        let res = (
            collection.implied_capability.clone(),
            collection.write_frontier.clone(),
        );

        Ok(res)
    }

    fn collections_frontiers(
        &self,
        ids: Vec<GlobalId>,
    ) -> Result<Vec<(GlobalId, Antichain<T>, Antichain<T>)>, StorageError<Self::Timestamp>> {
        let collections = self.collections.lock().expect("lock poisoned");

        let res = ids
            .into_iter()
            .map(|id| {
                collections
                    .get(&id)
                    .map(|c| {
                        (
                            id.clone(),
                            c.implied_capability.clone(),
                            c.write_frontier.clone(),
                        )
                    })
                    .ok_or(StorageError::IdentifierMissing(id))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(res)
    }

    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)> {
        let collections = self.collections.lock().expect("lock poisoned");

        collections
            .iter()
            .filter(|(_id, c)| !c.is_dropped())
            .map(|(id, c)| (*id, c.collection_metadata.clone()))
            .collect()
    }

    fn active_collection_frontiers(&self) -> Vec<(GlobalId, Antichain<T>, Antichain<T>)> {
        let collections = self.collections.lock().expect("lock poisoned");

        let res = collections
            .iter()
            .filter(|(_id, c)| !c.is_dropped())
            .map(|(id, c)| {
                (
                    id.clone(),
                    c.read_capabilities.frontier().to_owned(),
                    c.write_frontier.clone(),
                )
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
    ) -> Result<SnapshotPartsStats, StorageError<Self::Timestamp>> {
        let metadata = {
            let self_collections = self.collections.lock().expect("lock poisoned");

            let collection = self_collections
                .get(&id)
                .ok_or(StorageError::IdentifierMissing(id))?;
            collection.collection_metadata.clone()
        };

        // See the comments in StorageController::snapshot for what's going on
        // here.
        let result = match metadata.txns_shard.as_ref() {
            None => {
                let mut read_handle = self.read_handle_for_snapshot(&metadata, id).await?;
                let result = read_handle.snapshot_parts_stats(as_of).await;
                read_handle.expire().await;
                result
            }
            Some(txns_id) => {
                let as_of = as_of
                    .into_option()
                    .expect("cannot read as_of the empty antichain");
                let txns_read = self.txns.expect_enabled_lazy(txns_id);
                txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = txns_read
                    .data_snapshot(metadata.data_shard, as_of.clone())
                    .await;
                let mut handle = self.read_handle_for_snapshot(&metadata, id).await?;
                let result = data_snapshot.snapshot_parts_stats(&mut handle).await;
                handle.expire().await;
                result
            }
        };
        result.map_err(|_| StorageError::ReadBeforeSince(id))
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
        &mut self,
        txn: &mut dyn StorageTxn<T>,
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
        &mut self,
        storage_metadata: &StorageMetadata,
        register_ts: Option<Self::Timestamp>,
        mut collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        assert!(self.txns_init_run);
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

                // If the shard is being managed by persist-txn (initially,
                // tables), then we need to pass along the shard id for the txns
                // shard to dataflow rendering.
                let txns_shard = match description.data_source {
                    DataSource::Other(DataSourceOther::TableWrites) => match &self.txns {
                        // If we're not using lazy persist-txn upper (i.e. we're
                        // using eager uppers) then all reads should be done
                        // normally.
                        PersistTxns::EnabledEager { .. } => None,
                        PersistTxns::EnabledLazy { txns_read, .. } => Some(*txns_read.txns_id()),
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
                            let token = since_handle.opaque().clone();
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
                        output_index: 0,
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
                        } => {
                            // DataSource::IngestionExport names the object it
                            // wants to export, so we look up the output index
                            // for that name.
                            let output_index = ingestion_desc
                                .desc
                                .connection
                                .output_idx_for_name(external_reference)
                                .ok_or(StorageError::MissingSubsourceReference {
                                    ingestion_id: *ingestion_id,
                                    reference: external_reference.clone(),
                                })?;

                            ingestion_desc.source_exports.insert(
                                id,
                                SourceExport {
                                    output_index,
                                    storage_metadata: (),
                                },
                            )
                        }
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

                    if let PersistTxns::EnabledLazy { .. } = &self.txns {
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
        &mut self,
        ingestion_id: GlobalId,
        source_desc: SourceDesc,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        // The StorageController checks the validity of these. And we just
        // accept them.

        let mut self_collections = self.collections.lock().expect("lock poisoned");
        let collection = self_collections
            .get(&ingestion_id)
            .ok_or(StorageError::IdentifierMissing(ingestion_id))?;

        let curr_ingestion = match &collection.description.data_source {
            DataSource::Ingestion(active_ingestion) => active_ingestion,
            _ => unreachable!("verified collection refers to ingestion"),
        };

        mz_ore::soft_assert_ne_or_log!(
            curr_ingestion.desc,
            source_desc,
            "alter_ingestion_source_desc should only be called when producing new SourceDesc",
        );

        // Generate new source exports because they might have changed.
        let mut source_exports = BTreeMap::new();
        // Each source includes a `0` output index export "for the
        // primary source", whether it's used or not.
        source_exports.insert(
            ingestion_id,
            SourceExport {
                output_index: 0,
                storage_metadata: (),
            },
        );

        // Get the updated output indices for each source export.
        //
        // TODO(#26766): this could be simpler if the output indices were
        // determined in rendering, e.g. `SourceExport` had an
        // `Option<UnresolvedItemName>` instead of a `usize` and we looked up
        // its output index when we were aligning the rendering outputs.
        for export_id in curr_ingestion.source_exports.keys() {
            if *export_id == ingestion_id {
                // Already inserted above
                continue;
            }

            let DataSource::IngestionExport {
                ingestion_id,
                external_reference,
            } = &self_collections
                .get(export_id)
                .expect("missing source export")
                .description
                .data_source
            else {
                panic!("source exports must be DataSource::IngestionExport")
            };

            let output_index = source_desc
                .connection
                .output_idx_for_name(external_reference)
                .ok_or(StorageError::MissingSubsourceReference {
                    ingestion_id: *ingestion_id,
                    reference: external_reference.clone(),
                })?;

            source_exports.insert(
                *export_id,
                SourceExport {
                    output_index,
                    storage_metadata: (),
                },
            );
        }

        // Update the `SourceDesc` and the source exports simultaneously.
        let collection = self_collections
            .get_mut(&ingestion_id)
            .expect("validated exists");
        let curr_ingestion = match &mut collection.description.data_source {
            DataSource::Ingestion(curr_ingestion) => curr_ingestion,
            _ => unreachable!("verified collection refers to ingestion"),
        };
        curr_ingestion.desc = source_desc;
        curr_ingestion.source_exports = source_exports;
        debug!("altered {ingestion_id}'s SourceDesc");

        // n.b. we do not re-run updated ingestions because updating the source
        // desc is only done in preparation for adding subsources, which will
        // then run the ingestion.
        //
        // If this expectation ever changes, we will almost certainly know
        // because failing to run an altered ingestion means that whatever
        // changes you expect to occur will not be reflected in the running
        // dataflow.

        Ok(())
    }

    async fn alter_ingestion_connections(
        &mut self,
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

    fn drop_collections(
        &mut self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        debug!(?identifiers, "drop_collections");

        self.validate_collection_ids(identifiers.iter().cloned())?;
        self.drop_collections_unvalidated(storage_metadata, identifiers);

        Ok(())
    }

    fn drop_collections_unvalidated(
        &mut self,
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
            let collection = self_collections.get_mut(&id);

            if let Some(collection) = collection {
                assert!(
                    !collection.is_dropped,
                    "collection {} has been dropped already",
                    id
                );
                collection.is_dropped = true;
                finalized_policies.push((id, ReadPolicy::ValidFrom(Antichain::new())));
            }
        }
        self.set_read_policies_inner(&mut self_collections, finalized_policies);

        drop(self_collections);

        self.synchronize_finalized_shards(storage_metadata);
    }

    fn set_read_policies(&mut self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>) {
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
        &mut self,
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
        &mut self,
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

    /// Marks the collection as dropped. Meaning it is no longer active.
    ///
    /// We have this to differentiate between active collections and collections
    /// that have been dropped, but whose state has not been reclaimed yet
    /// because there are outstanding read holds.
    ///
    /// NOTE: We are working around quirks in StorageController here, which
    /// doesn't always drop its read holds when dropping ingestions. Once those
    /// are resolved, we should be able to get rid of this flag.
    pub is_dropped: bool,
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
            is_dropped: false,
        }
    }

    /// Returns whether the collection was dropped.
    pub fn is_dropped(&self) -> bool {
        self.is_dropped
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
    since_handles: BTreeMap<GlobalId, SinceHandle<SourceData, (), T, Diff, PersistEpoch>>,
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
        since_handle: SinceHandle<SourceData, (), T, Diff, PersistEpoch>,
    },
    // This was also dead code in the StorageController. I think we keep around
    // these code paths for when we need to do migrations in the future.
    #[allow(dead_code)]
    Update(
        GlobalId,
        WriteHandle<SourceData, (), T, Diff>,
        SinceHandle<SourceData, (), T, Diff, PersistEpoch>,
    ),
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
        // Futures that fetch the recent upper from the txns shard.
        let mut txns_upper_futures = FuturesUnordered::new();

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

            fut.boxed()
        };

        if let Some(txns_handle) = self.txns_handle.take() {
            let fut = gen_upper_future(GlobalId::Transient(1), txns_handle);
            txns_upper_futures.push(fut);
        }

        // We check periodically if there's any shards that we need to finalize
        // and do so if yes.
        let mut finalization_interval = tokio::time::interval(Duration::from_secs(5));
        finalization_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                Some((id, handle, upper)) = txns_upper_futures.next() => {
                    trace!("new upper from txns shard: {:?}", upper);
                    let mut uppers = Vec::new();
                    for id in self.txns_shards.iter() {
                        uppers.push((id.clone(), upper.clone()));
                    }
                    self.update_write_frontiers(&uppers).await;

                    let fut = gen_upper_future(id, handle);
                    txns_upper_futures.push(fut);
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
                            upper_futures.push(fut);
                        } else {
                            // Be polite and expire the write handle.
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
                            }

                            let fut = gen_upper_future(id, write_handle);
                            upper_futures.push(fut);
                        }
                        BackgroundCmd::DowngradeSince(cmds) => {
                            self.downgrade_sinces(cmds).await;
                        }
                        BackgroundCmd::Update(id, write_handle, since_handle) => {
                            self.shard_by_id.insert(id, write_handle.shard_id()).expect(
                                "BackgroundCmd::Update only valid for updating extant write handles",
                            );

                            self.since_handles.insert(id, since_handle).expect(
                                "BackgroundCmd::Update only valid for updating extant since handles",
                            );

                            let fut = gen_upper_future(id, write_handle);
                            upper_futures.push(fut);

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
                                            Box::pin(x.snapshot_stats(Some(as_of)).map(move |x| {
                                                x.map_err(|_| StorageError::ReadBeforeSince(id))
                                            }))
                                        }
                                        SnapshotStatsAsOf::Txns(data_snapshot) => Box::pin(
                                            data_snapshot.snapshot_stats(x).map(move |x| {
                                                x.map_err(|_| StorageError::ReadBeforeSince(id))
                                            }),
                                        ),
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
