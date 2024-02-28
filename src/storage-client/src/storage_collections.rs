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

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{Future, FutureExt, StreamExt};
use itertools::Itertools;

use mz_ore::instrument;
use mz_ore::metrics::MetricsRegistry;
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
use mz_proto::RustType;
use mz_repr::{Diff, GlobalId, RelationDesc, TimestampManipulation};
use mz_stash::{self, AppendBatch, StashFactory, TypedCollection};
use mz_stash_types::metrics::Metrics as StashMetrics;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::{
    CollectionMetadata, DurableCollectionMetadata, PersistTxnTablesImpl, StorageError, TxnsCodecRow,
};
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sources::{IngestionDescription, SourceData, SourceExport};
use mz_storage_types::{collections as proto, AlterCompatible};
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp as TimelyTimestamp};
use timely::PartialOrder;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::controller::{CollectionDescription, DataSource, DataSourceOther, PersistEpoch};

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

    /// Marks the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before implementing prior commands,
    /// and so it is important for a user to invoke this method as soon as it is comfortable.
    /// This method can be invoked immediately, at the potential expense of performance.
    fn initialization_complete(&mut self);

    /// Update storage configuration with new parameters.
    fn update_parameters(&mut self, config_params: StorageParameters);

    /// Get the current configuration, including parameters updated with `update_parameters`.
    fn config(&self) -> StorageConfiguration;

    /// Returns the `CollectionDescription` for the given `GlobalId`.
    fn collection_description(
        &self,
        id: GlobalId,
    ) -> Result<CollectionDescription<Self::Timestamp>, StorageError>;

    /// Returns the [CollectionMetadata] of the collection identified by `id`.
    fn collection_metadata(&self, id: GlobalId) -> Result<CollectionMetadata, StorageError>;

    /// Returns the since/upper frontiers of the collection identified by `id`.
    fn collection_frontiers(
        &self,
        id: Vec<GlobalId>,
    ) -> Result<
        Vec<(
            GlobalId,
            Antichain<Self::Timestamp>,
            Antichain<Self::Timestamp>,
        )>,
        StorageError,
    >;

    /// Acquire an iterator over [CollectionMetadata] for all active
    /// collections.
    ///
    /// A collection is "active" when it has not been dropped via
    /// [StorageCollections::drop_collections]. Collections that have been
    /// dropped by still have outstanding [ReadHolds](ReadHold) are not
    /// considered active for this method.
    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)>;

    /// Returns the since/upper frontiers for all active collections.
    ///
    /// A collection is "active" when it has not been dropped via
    /// [StorageCollections::drop_collections]. Collections that have been
    /// dropped by still have outstanding [ReadHolds](ReadHold) are not
    /// considered active for this method.
    fn active_collection_frontiers(
        &self,
    ) -> Vec<(
        GlobalId,
        Antichain<Self::Timestamp>,
        Antichain<Self::Timestamp>,
    )>;

    /// Checks whether a collection exists under the given `GlobalId`. Returns
    /// an error if the collection does not exist.
    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError>;

    /// Returns aggregate statistics about the contents of the local input named
    /// `id` at `as_of`.
    async fn snapshot_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> Result<SnapshotStats, StorageError>;

    /// Returns aggregate statistics about the contents of the local input named
    /// `id` at `as_of`.
    async fn snapshot_parts_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> Result<SnapshotPartsStats, StorageError>;

    /// Migrate any storage controller state from previous versions to this
    /// version's expectations.
    ///
    /// This function must "see" the GlobalId of every collection you plan to
    /// create, but can be called with all of the catalog's collections at once.
    async fn migrate_collections(
        &mut self,
        collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError>;

    /// Create the sources described in the individual RunIngestionCommand commands.
    ///
    /// Each command carries the source id, the source description, and any associated metadata
    /// needed to ingest the particular source.
    ///
    /// This command installs collection state for the indicated sources, and the are
    /// now valid to use in queries at times beyond the initial `since` frontiers. Each
    /// collection also acquires a read capability at this frontier, which will need to
    /// be repeatedly downgraded with `allow_compaction()` to permit compaction.
    ///
    /// This method is NOT idempotent; It can fail between processing of different
    /// collections and leave the controller in an inconsistent state. It is almost
    /// always wrong to do anything but abort the process on `Err`.
    ///
    /// The `register_ts` is used as the initial timestamp that tables are available for reads. (We
    /// might later give non-tables the same treatment, but hold off on that initially.) Callers
    /// must provide a Some if any of the collections is a table. A None may be given if none of the
    /// collections are a table (i.e. all materialized views, sources, etc).
    async fn create_collections(
        &mut self,
        register_ts: Option<Self::Timestamp>,
        collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError>;

    /// Check that the collection associated with `id` can be altered to represent the given
    /// `ingestion`.
    ///
    /// Note that this check is optimistic and its return of `Ok(())` does not guarantee that
    /// subsequent calls to `alter_collection` are guaranteed to succeed.
    fn check_alter_collection(
        &mut self,
        collections: &BTreeMap<GlobalId, IngestionDescription>,
    ) -> Result<(), StorageError>;

    /// Alter the identified collection to use the described ingestion.
    async fn alter_collection(
        &mut self,
        collections: BTreeMap<GlobalId, IngestionDescription>,
    ) -> Result<(), StorageError>;

    /// Drops the read capability for the collections and allows their resources to be reclaimed.
    ///
    /// Collection state is only fully dropped, however, once there are not more
    /// outstanding [ReadHolds](ReadHold) for a collection.
    fn drop_collections(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError>;

    /// Drops the read capability for the sources and allows their resources to be reclaimed.
    ///
    /// TODO(jkosh44): This method does not validate the provided identifiers. Currently when the
    ///     controller starts/restarts it has no durable state. That means that it has no way of
    ///     remembering any past commands sent. In the future we plan on persisting state for the
    ///     controller so that it is aware of past commands.
    ///     Therefore this method is for dropping sources that we know to have been previously
    ///     created, but have been forgotten by the controller due to a restart.
    ///     Once command history becomes durable we can remove this method and use the normal
    ///     `drop_sources`.
    fn drop_collections_unvalidated(&mut self, identifiers: Vec<GlobalId>);

    /// Assigns a read policy to specific identifiers.
    ///
    /// The policies are assigned in the order presented, and repeated identifiers should
    /// conclude with the last policy. Changing a policy will immediately downgrade the read
    /// capability if appropriate, but it will not "recover" the read capability if the prior
    /// capability is already ahead of it.
    ///
    /// The `CollectionsController` may include its own overrides on these policies.
    ///
    /// Identifiers not present in `policies` retain their existing read policies.
    fn set_read_policies(&mut self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>);

    /// Acquires the desired read holds, advancing them to the since frontier
    /// when necessary. This will return the holds that have been acquired.
    ///
    /// TODO: More type-safe interface.
    fn acquire_read_holds(
        &mut self,
        desired_holds: Vec<(GlobalId, Antichain<Self::Timestamp>)>,
    ) -> Vec<ReadHold<Self::Timestamp>>;

    /// Applies `updates` and sends any appropriate compaction command.
    ///
    /// This is a legacy interface that should _not_ be used! It is only used by
    /// the compute controller.
    fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<Self::Timestamp>>,
    );

    /// Signal to the controller that the adapter has populated all of its
    /// initial state and the controller can reconcile (i.e. drop) any unclaimed
    /// resources.
    async fn reconcile_state(&mut self);

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
    async fn init_txns(&mut self, init_ts: Self::Timestamp) -> Result<(), StorageError>;
}

pub static METADATA_COLLECTION: TypedCollection<proto::GlobalId, proto::DurableCollectionMetadata> =
    TypedCollection::new("storage-collection-metadata");

pub static PERSIST_TXNS_SHARD: TypedCollection<(), String> =
    TypedCollection::new("persist-txns-shard");

pub static ALL_COLLECTIONS: &[&str] = &[METADATA_COLLECTION.name(), PERSIST_TXNS_SHARD.name()];

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

/// Keeps track of storage collections and can hand out read holds for them.
#[derive(Debug, Clone)]
pub struct StorageCollectionsImpl<
    T: TimelyTimestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
> {
    /// The fencing token for this instance of the controller.
    envd_epoch: NonZeroI64,

    /// Collections maintained by the storage controller.
    ///
    /// This collection only grows, although individual collections may be rendered unusable.
    /// This is to prevent the re-binding of identifiers to other descriptions.
    collections: Arc<std::sync::Mutex<BTreeMap<GlobalId, CollectionState<T>>>>,

    stash: Arc<tokio::sync::Mutex<mz_stash::Stash>>,

    /// Whether to use the new persist-txn tables implementation or the legacy
    /// one.
    txns: PersistTxns<T>,
    /// Whether we have run `txns_init` yet (required before create_collections
    /// and the various flavors of append).
    txns_init_run: bool,

    /// Storage configuration to apply to newly provisioned instances, and use during purification.
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

impl<T> StorageCollectionsImpl<T>
where
    T: TimelyTimestamp
        + Lattice
        + Codec64
        + From<EpochMillis>
        + TimestampManipulation
        + Into<mz_repr::Timestamp>,
{
    /// Create a new storage controller from a client it should wrap.
    ///
    /// Note that when creating a new storage controller, you must also
    /// reconcile it with the previous state.
    pub async fn new(
        postgres_url: String,
        persist_location: PersistLocation,
        persist_clients: Arc<PersistClientCache>,
        _now: NowFn,
        stash_metrics: Arc<StashMetrics>,
        txns_metrics: Arc<TxnMetrics>,
        envd_epoch: NonZeroI64,
        _metrics_registry: MetricsRegistry,
        persist_txn_tables: PersistTxnTablesImpl,
        connection_context: ConnectionContext,
    ) -> Self {
        let tls = mz_tls_util::make_tls(
            &tokio_postgres::config::Config::from_str(&postgres_url)
                .expect("invalid postgres url for storage stash"),
        )
        .expect("could not make storage TLS connection");
        let mut stash = StashFactory::from_metrics(stash_metrics)
            .open(postgres_url, None, tls, None)
            .await
            .expect("could not connect to postgres storage stash");

        // Ensure all collections are initialized, otherwise they cannot
        // be read.
        async fn maybe_get_init_batch<'tx, K, V>(
            tx: &'tx mz_stash::Transaction<'tx>,
            typed: &TypedCollection<K, V>,
        ) -> Option<AppendBatch>
        where
            K: mz_stash::Data,
            V: mz_stash::Data,
        {
            let collection = tx
                .collection::<K, V>(typed.name())
                .await
                .expect("named collection must exist");
            if !collection
                .is_initialized(tx)
                .await
                .expect("collection known to exist")
            {
                Some(
                    collection
                        .make_batch_tx(tx)
                        .await
                        .expect("stash operation must succeed"),
                )
            } else {
                None
            }
        }

        stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    // Query all collections in parallel. Makes for triplicated
                    // names, but runs quick.
                    let (metadata_collection, persist_txns_shard) = futures::join!(
                        maybe_get_init_batch(&tx, &METADATA_COLLECTION),
                        maybe_get_init_batch(&tx, &PERSIST_TXNS_SHARD),
                    );
                    let batches: Vec<AppendBatch> = [metadata_collection, persist_txns_shard]
                        .into_iter()
                        .filter_map(|b| b)
                        .collect();

                    tx.append(batches).await.map(drop)
                })
            })
            .await
            .expect("stash operation must succeed");

        let txns_client = persist_clients
            .open(persist_location.clone())
            .await
            .expect("location should be valid");

        let txns_id = PERSIST_TXNS_SHARD
            .insert_key_without_overwrite(&mut stash, (), ShardId::new().into_proto())
            .await
            .expect("could not get txns shard id")
            .parse::<ShardId>()
            .expect("should be valid shard id");

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

        // For handing to the background task, for listening to upper
        // updates.
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

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (holds_tx, holds_rx) = mpsc::unbounded_channel();
        let mut uppers_task = BackgroundTask {
            cmds_tx: cmd_tx.clone(),
            cmds_rx: cmd_rx,
            holds_rx,
            collections: Arc::clone(&collections),
            shard_by_id: BTreeMap::new(),
            since_handles: BTreeMap::new(),
            txns_handle: Some(txns_write),
            txns_shards: Default::default(),
        };

        let background_task =
            mz_ore::task::spawn(|| "collection_controller::uppers_task", async move {
                uppers_task.run().await
            });

        Self {
            collections,
            stash: Arc::new(tokio::sync::Mutex::new(stash)),
            txns,
            txns_init_run: false,
            envd_epoch,
            config: Arc::new(Mutex::new(StorageConfiguration::new(
                connection_context,
                mz_dyncfgs::all_dyncfgs(),
            ))),
            persist_location,
            persist: persist_clients,
            cmd_tx,
            holds_tx,
            _background_task: Arc::new(background_task.abort_on_drop()),
        }
    }

    /// Validate that a collection exists for all identifiers, and error if any do not.
    fn validate_collection_ids(
        &self,
        ids: impl Iterator<Item = GlobalId>,
    ) -> Result<(), StorageError> {
        for id in ids {
            self.check_exists(id)?;
        }
        Ok(())
    }

    /// Opens a write and critical since handles for the given `shard`.
    ///
    /// `since` is an optional `since` that the read handle will be forwarded to if it is less than
    /// its current since.
    ///
    /// This will `halt!` the process if we cannot successfully acquire a critical handle with our
    /// current epoch.
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

        // Construct the handle in a separate block to ensure all error paths are diverging
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

            // Take the join of the handle's since and the provided `since`; this lets materialized
            // views express the since at which their read handles "start."
            let since = handle
                .since()
                .join(since.unwrap_or(&Antichain::from_elem(T::minimum())));

            let our_epoch = self.envd_epoch;

            loop {
                let current_epoch: PersistEpoch = handle.opaque().clone();

                // Ensure the current epoch is <= our epoch.
                let unchecked_success = current_epoch.0.map(|e| e <= our_epoch).unwrap_or(true);

                if unchecked_success {
                    // Update the handle's state so that it is in terms of our epoch.
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
        // Fetch the most recent upper for the write handle. Otherwise, this may be behind
        // the since of the since handle. Its vital this happens AFTER we create
        // the since handle as it needs to be linearized with that operation. It may be true
        // that creating the write handle after the since handle already ensures this, but we
        // do this out of an abundance of caution.
        //
        // Note that this returns the upper, but also sets it on the handle to be fetched later.
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
    ) -> Result<SnapshotStats, StorageError> {
        // TODO: Pull this out of PersistReadWorker. Unlike the other methods,
        // the caller of this one drives it to completion.
        //
        // We'd need to either share the critical handle somehow or maybe have
        // two instances around, one in the worker and one in the controller.
        let (tx, rx) = oneshot::channel();
        self.send(BackgroundCmd::SnapshotStats(id, as_of, tx));
        rx.await.expect("BackgroundTask should be live").0.await
    }

    // async fn send_blocking<R: std::fmt::Debug>(
    //     &self,
    //     cmd: impl FnOnce(oneshot::Sender<R>) -> ControllerTaskCmd<T>,
    // ) -> R {
    //     let (tx, rx) = oneshot::channel();
    //     let req = cmd(tx);
    //     let () = self.cmd_tx.send(req).expect("task unexpectedly shut down");
    //     rx.await.expect("task unexpectedly shut down")
    // }

    /// A handle to the `Stash` used by this controller.
    pub fn shared_stash(&self) -> Arc<tokio::sync::Mutex<mz_stash::Stash>> {
        Arc::clone(&self.stash)
    }

    fn check_alter_collections_inner(
        &self,
        self_collections: &BTreeMap<GlobalId, CollectionState<T>>,
        collections: &BTreeMap<GlobalId, IngestionDescription>,
    ) -> Result<(), StorageError> {
        for (id, ingestion) in collections {
            self.check_alter_collection_inner(self_collections, *id, ingestion.clone())?;
        }
        Ok(())
    }

    /// Determines if an `ALTER` is valid.
    fn check_alter_collection_inner(
        &self,
        self_collections: &BTreeMap<GlobalId, CollectionState<T>>,
        id: GlobalId,
        mut ingestion: IngestionDescription,
    ) -> Result<(), StorageError> {
        // Take a cloned copy of the description because we are going to treat it as a "scratch
        // space".
        let mut collection_description = self_collections
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))?
            .description
            .clone();

        // Get the previous storage dependencies; we need these to understand if something has
        // changed in what we depend upon.
        let prev_storage_dependencies = collection_description.get_storage_dependencies();

        // We cannot know the metadata of exports yet to be created, so we have
        // to remove them. However, we know that adding source exports is
        // compatible, so still OK to proceed.
        ingestion
            .source_exports
            .retain(|id, _| self_collections.contains_key(id));

        // Describe the ingestion in terms of collection metadata.
        let described_ingestion =
            self.enrich_ingestion_inner(self_collections, id, ingestion.clone())?;

        // Check compatibility between current and new ingestions and install new ingestion in
        // collection description.
        match &mut collection_description.data_source {
            DataSource::Ingestion(cur_ingestion) => {
                let prev_ingestion =
                    self.enrich_ingestion_inner(self_collections, id, cur_ingestion.clone())?;
                prev_ingestion.alter_compatible(id, &described_ingestion)?;

                *cur_ingestion = ingestion;
            }
            o => {
                tracing::info!(
                    "{id:?} inalterable because its data source is {:?} and not an ingestion",
                    o
                );
                return Err(StorageError::InvalidAlter { id });
            }
        };

        let new_storage_dependencies = collection_description.get_storage_dependencies();

        if prev_storage_dependencies != new_storage_dependencies {
            tracing::info!(
                    "{id:?} inalterable because its storage dependencies have changed: were {:?} but are now {:?}",
                    prev_storage_dependencies,
                    new_storage_dependencies
                );
            return Err(StorageError::InvalidAlter { id });
        }

        Ok(())
    }

    /// For each element of `collections`, install a read hold on all of the
    /// `storage_dependencies`.
    ///
    /// Note that this adjustment is only guaranteed to be reflected in memory;
    /// downgrades to persist shards are not guaranteed to occur unless they
    /// close the shard.
    ///
    /// # Panics
    ///
    /// - If any identified collection's since is less than the dependency since
    ///   and:
    ///     - Its read policy is not `ReadPolicy::NoPolicy`
    ///     - Its read policy is `ReadPolicy::NoPolicy(f)` and the dependency
    ///       since is <= `f`.
    ///
    ///     - Its write frontier is neither `T::minimum` nor beyond the
    ///       dependency since.
    /// - If any identified collection's data source is not
    ///   [`DataSource::Ingestion] (primary source) or [`DataSource::Other`]
    ///   (subsources).
    fn install_dependency_read_holds_inner<I: Iterator<Item = GlobalId>>(
        &self,
        self_collections: &mut BTreeMap<GlobalId, CollectionState<T>>,
        collections: I,
        storage_dependencies: &[GlobalId],
    ) -> Result<(), StorageError> {
        let dependency_since =
            self.determine_collection_since_joins_inner(self_collections, storage_dependencies)?;

        for id in collections {
            let collection = self_collections.get(&id).expect("known to exist");
            assert!(
                matches!(collection.description.data_source, DataSource::Other(_) | DataSource::Ingestion(_)),
                "only primary sources w/ subsources and subsources can have dependency read holds installed"
            );

            // Because of the "backward" dependency structure (primary sources
            // depend on subsources, rather than the other way around, which one
            // might expect), we do not know what the initial since of the
            // collection should be. We only find out that information once its
            // primary sources comes along and correlates the subsource to its
            // dependency sinces (e.g. remap shards).
            //
            // Once we find that out, we need ensure that the controller's
            // version of the since is sufficiently advanced so that we may
            // install the read hold.
            //
            // TODO: remove this if statement once we fix the inverse dependency
            // of subsources
            if PartialOrder::less_than(&collection.implied_capability, &dependency_since) {
                assert!(
                    match &collection.read_policy {
                        ReadPolicy::NoPolicy { initial_since } =>
                            PartialOrder::less_than(initial_since, &dependency_since),
                        _ => false,
                    },
                    "subsources should not have external read holds installed until \
                                    their ingestion is created, but {:?} has read policy {:?}",
                    id,
                    collection.read_policy
                );

                // Patch up the implied capability + maybe the persist shard's
                // since.
                self.set_read_policies_inner(
                    self_collections,
                    vec![(
                        id,
                        ReadPolicy::NoPolicy {
                            initial_since: dependency_since.clone(),
                        },
                    )],
                );

                // We have to re-borrow.
                let collection = self_collections.get(&id).expect("known to exist");
                assert!(
                    collection.implied_capability == dependency_since,
                    "monkey patching the implied_capability to {:?} did not work, is still {:?}",
                    dependency_since,
                    collection.implied_capability,
                );
            }

            // Fill in the storage dependencies.
            let collection = self_collections.get_mut(&id).expect("known to exist");

            assert!(
                PartialOrder::less_than(&collection.implied_capability, &collection.write_frontier)
                    // Whenever a collection is being initialized, this state is
                    // acceptable.
                    || *collection.write_frontier == [T::minimum()],
                "{id}:  the implied capability {:?} should be less than the write_frontier {:?}. Collection state dump: {:#?}",
                collection.implied_capability,
                collection.write_frontier,
                collection
            );

            collection
                .storage_dependencies
                .extend(storage_dependencies.iter().cloned());

            assert!(
                !PartialOrder::less_than(
                    &collection.read_capabilities.frontier(),
                    &collection.implied_capability.borrow()
                ),
                "{id}: at this point, there can be no read holds for any time that is not \
                    beyond the implied capability  but we have implied_capability {:?}, \
                    read_capabilities {:?}",
                collection.implied_capability,
                collection.read_capabilities,
            );

            let read_hold = collection.implied_capability.clone();
            self.install_read_capabilities_inner(
                self_collections,
                id,
                storage_dependencies,
                read_hold,
            )?;
        }

        Ok(())
    }

    /// Return the since frontier at which we can read from all the given
    /// collections.
    ///
    /// The outer error is a potentially recoverable internal error, while the
    /// inner error is appropriate to return to the adapter.
    fn determine_collection_since_joins_inner(
        &self,
        self_collections: &mut BTreeMap<GlobalId, CollectionState<T>>,
        collections: &[GlobalId],
    ) -> Result<Antichain<T>, StorageError> {
        let mut joined_since = Antichain::from_elem(T::minimum());

        for id in collections {
            let collection = self_collections
                .get(id)
                .ok_or(StorageError::IdentifierMissing(*id))?;

            let since = collection.implied_capability.clone();
            joined_since.join_assign(&since);
        }

        Ok(joined_since)
    }

    /// Install read capabilities on the given `storage_dependencies`.
    #[instrument(level = "debug")]
    fn install_read_capabilities_inner(
        &self,
        collections: &mut BTreeMap<GlobalId, CollectionState<T>>,
        from_id: GlobalId,
        storage_dependencies: &[GlobalId],
        read_capability: Antichain<T>,
    ) -> Result<(), StorageError> {
        let mut changes = ChangeBatch::new();
        for time in read_capability.iter() {
            changes.update(time.clone(), 1);
        }

        let user_capabilities = collections
            .iter_mut()
            .filter(|(id, _c)| id.is_user())
            .map(|(id, c)| {
                let updates = c.read_capabilities.updates().cloned().collect_vec();
                (*id, c.implied_capability.clone(), updates)
            })
            .collect_vec();

        debug!(
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
            collections,
            &mut storage_read_updates,
        );

        let user_capabilities = collections
            .iter_mut()
            .filter(|(id, _c)| id.is_user())
            .map(|(id, c)| {
                let updates = c.read_capabilities.updates().cloned().collect_vec();
                (*id, c.implied_capability.clone(), updates)
            })
            .collect_vec();

        debug!(
            %from_id,
            ?storage_dependencies,
            ?read_capability,
            ?user_capabilities,
            "after install_read_capabilities_inner!");

        Ok(())
    }

    /// Converts an `IngestionDescription<()>` into `IngestionDescription<CollectionMetadata>`.
    fn enrich_ingestion_inner(
        &self,
        self_collections: &BTreeMap<GlobalId, CollectionState<T>>,
        id: GlobalId,
        ingestion: IngestionDescription,
    ) -> Result<IngestionDescription<CollectionMetadata>, StorageError> {
        // The ingestion metadata is simply the collection metadata of the collection with
        // the associated ingestion
        let collection = self_collections
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))?;
        let ingestion_metadata = collection.collection_metadata.clone();

        let mut source_exports = BTreeMap::new();
        for (id, export) in ingestion.source_exports {
            // Note that these metadata's have been previously enriched with the
            // required `RelationDesc` for each sub-source above!
            let collection = self_collections
                .get(&id)
                .ok_or(StorageError::IdentifierMissing(id))?;
            let storage_metadata = collection.collection_metadata.clone();
            source_exports.insert(
                id,
                SourceExport {
                    storage_metadata,
                    output_index: export.output_index,
                },
            );
        }

        Ok(IngestionDescription {
            source_exports,
            ingestion_metadata,
            // The rest of the fields are identical
            desc: ingestion.desc,
            instance_id: ingestion.instance_id,
            remap_collection_id: ingestion.remap_collection_id,
        })
    }

    async fn read_handle_for_snapshot(
        &self,
        metadata: &CollectionMetadata,
        id: GlobalId,
    ) -> Result<ReadHandle<SourceData, (), T, Diff>, StorageError> {
        let persist_client = self
            .persist
            .open(metadata.persist_location.clone())
            .await
            .unwrap();

        // We create a new read handle every time someone requests a snapshot and then immediately
        // expire it instead of keeping a read handle permanently in our state to avoid having it
        // heartbeat continously. The assumption is that calls to snapshot are rare and therefore
        // worth it to always create a new handle.
        let read_handle = persist_client
            .open_leased_reader::<SourceData, (), _, _>(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: id.to_string(),
                    handle_purpose: format!("snapshot {}", id),
                },
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
        debug!("set_read_policies: {:?}", policies);

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
                debug!(%id, ?changes, "in set_read_policies, capability changes");
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
                tracing::debug!(id = ?id, update = ?update, "update_read_capabilities");
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
                tracing::debug!(
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
}

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

    fn initialization_complete(&mut self) {
        debug!("initialization_complete");
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

    fn config(&self) -> StorageConfiguration {
        self.config.lock().expect("lock poisoned").clone()
    }

    fn collection_description(
        &self,
        id: GlobalId,
    ) -> Result<CollectionDescription<Self::Timestamp>, StorageError> {
        let collections = self.collections.lock().expect("lock poisoned");

        collections
            .get(&id)
            .map(|c| c.description.clone())
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn collection_metadata(&self, id: GlobalId) -> Result<CollectionMetadata, StorageError> {
        let collections = self.collections.lock().expect("lock poisoned");

        collections
            .get(&id)
            .map(|c| c.collection_metadata.clone())
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn collection_frontiers(
        &self,
        ids: Vec<GlobalId>,
    ) -> Result<Vec<(GlobalId, Antichain<T>, Antichain<T>)>, StorageError> {
        let collections = self.collections.lock().expect("lock poisoned");

        let res = ids
            .into_iter()
            .map(|id| {
                collections
                    .get(&id)
                    .map(|c| {
                        (
                            id.clone(),
                            c.read_capabilities.frontier().to_owned(),
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
    ) -> Result<SnapshotStats, StorageError> {
        let metadata = self.collection_metadata(id)?;

        // See the comments in Self::snapshot for what's going on here.
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
    ) -> Result<SnapshotPartsStats, StorageError> {
        let metadata = {
            let self_collections = self.collections.lock().expect("lock poisoned");

            let collection = self_collections
                .get(&id)
                .ok_or(StorageError::IdentifierMissing(id))?;
            collection.collection_metadata.clone()
        };

        // See the comments in Self::snapshot for what's going on here.
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

    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError> {
        let collections = self.collections.lock().expect("lock poisoned");

        if collections.contains_key(&id) {
            Ok(())
        } else {
            Err(StorageError::IdentifierMissing(id))
        }
    }

    async fn migrate_collections(
        &mut self,
        collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError> {
        debug!("migrate_collections: {:?}", collections);

        Ok(())
    }

    // TODO(aljoscha): It would be swell if we could refactor this Leviathan of
    // a method/move individual parts to their own methods.
    #[instrument(level = "debug")]
    async fn create_collections(
        &mut self,
        register_ts: Option<Self::Timestamp>,
        mut collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError> {
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
        for (id, description) in collections.iter() {
            if let Ok(existing_description) = self.collection_description(*id) {
                if &existing_description != description {
                    return Err(StorageError::SourceIdReused(*id));
                }
            }
        }

        // Install collection state for each bound description. Note that this
        // method implementation attempts to do AS MUCH work concurrently as
        // possible. There are inline comments explaining the motivation behind
        // each section.
        let mut entries = Vec::with_capacity(collections.len());

        for (id, _desc) in &collections {
            entries.push((
                *id,
                DurableCollectionMetadata {
                    data_shard: ShardId::new(),
                },
            ))
        }

        // Perform all stash writes in a single transaction, to minimize transaction overhead and
        // the time spent waiting for stash.
        let durable_metadata: BTreeMap<GlobalId, DurableCollectionMetadata> = METADATA_COLLECTION
            .insert_without_overwrite(
                &mut *self.stash.lock().await,
                entries
                    .into_iter()
                    .map(|(key, val)| (key.into_proto(), val.into_proto())),
            )
            .await?
            .into_iter()
            .map(RustType::from_proto)
            .collect::<Result<_, _>>()
            .map_err(|e| StorageError::IOError(e.into()))?;

        // We first enrich each collection description with some additional metadata...
        let enriched_with_metadata = collections
            .into_iter()
            .map(|(id, description)| {
                let collection_shards = durable_metadata.get(&id).expect("inserted above");

                let status_shard =
                    if let Some(status_collection_id) = description.status_collection_id {
                        Some(
                            durable_metadata
                                .get(&status_collection_id)
                                .ok_or(StorageError::IdentifierMissing(status_collection_id))?
                                .data_shard,
                        )
                    } else {
                        None
                    };

                let remap_shard = match &description.data_source {
                    // Only ingestions can have remap shards.
                    DataSource::Ingestion(IngestionDescription {
                        remap_collection_id,
                        ..
                    }) => {
                        // Iff ingestion has a remap collection, its metadata must
                        // exist (and be correct) by this point.
                        Some(
                            durable_metadata
                                .get(remap_collection_id)
                                .ok_or(StorageError::IdentifierMissing(*remap_collection_id))?
                                .data_shard,
                        )
                    }
                    _ => None,
                };

                // If the shard is being managed by persist-txn (initially, tables), then we need to
                // pass along the shard id for the txns shard to dataflow rendering.
                let txns_shard = match description.data_source {
                    DataSource::Other(DataSourceOther::TableWrites) => match &self.txns {
                        // If we're not using lazy persist-txn upper (i.e. we're
                        // using eager uppers) then all reads should be done
                        // normally.
                        PersistTxns::EnabledEager { .. } => None,
                        PersistTxns::EnabledLazy { txns_read, .. } => Some(*txns_read.txns_id()),
                    },
                    DataSource::Ingestion(_)
                    | DataSource::Introspection(_)
                    | DataSource::Progress
                    | DataSource::Webhook
                    | DataSource::Other(DataSourceOther::Compute)
                    | DataSource::Other(DataSourceOther::Source) => None,
                };

                let metadata = CollectionMetadata {
                    persist_location: self.persist_location.clone(),
                    remap_shard,
                    data_shard: collection_shards.data_shard,
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
        // Reborrow the `&mut self` as immutable, as all the concurrent work to be processed in
        // this stream cannot all have exclusive access.
        use futures::stream::{StreamExt, TryStreamExt};
        let this = &*self;
        let to_register: Vec<_> = futures::stream::iter(enriched_with_metadata)
            .map(|data: Result<_, StorageError>| {
                let register_ts = register_ts.clone();
                async move {
                let (id, description, metadata) = data?;

                // should be replaced with real introspection (https://github.com/MaterializeInc/materialize/issues/14266)
                // but for now, it's helpful to have this mapping written down somewhere
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

                // Present tables as springing into existence at the register_ts by advancing
                // the since. Otherwise, we could end up in a situation where a table with a
                // long compaction window appears to exist before the environment (and this the
                // table) existed.
                //
                // We could potentially also do the same thing for other sources, in particular
                // storage's internal sources and perhaps others, but leave them for now.
                match description.data_source {
                    DataSource::Introspection(_)
                    | DataSource::Webhook
                    | DataSource::Ingestion(_)
                    | DataSource::Progress
                    | DataSource::Other(DataSourceOther::Compute)
                    | DataSource::Other(DataSourceOther::Source) => {},
                    DataSource::Other(DataSourceOther::TableWrites) => {
                        let register_ts = register_ts.expect("caller should have provided a register_ts when creating a table");
                        if since_handle.since().elements() == &[T::minimum()] {
                            debug!("advancing {} to initial since of {:?}", id, register_ts);
                            let token = since_handle.opaque().clone();
                            let _ = since_handle.compare_and_downgrade_since(&token, (&token, &Antichain::from_elem(register_ts.clone()))).await;
                        }
                    }
                }

                Ok::<_, StorageError>((id, description, write, since_handle, metadata))
            }})
            // Poll each future for each collection concurrently, maximum of 50 at a time.
            .buffer_unordered(50)
            // HERE BE DRAGONS:
            //
            // There are at least 2 subtleties in using `FuturesUnordered` (which
            // `buffer_unordered` uses underneath:
            // - One is captured here <https://github.com/rust-lang/futures-rs/issues/2387>
            // - And the other is deadlocking if processing an OUTPUT of a `FuturesUnordered`
            // stream attempts to obtain an async mutex that is also obtained in the futures
            // being polled.
            //
            // Both of these could potentially be issues in all usages of `buffer_unordered` in
            // this method, so we stick the standard advice: only use `try_collect` or
            // `collect`!
            .try_collect()
            .await?;

        // We hold this lock for a very short amount of time, just doing some hashmap inserts
        // and unbounded channel sends.
        let mut self_collections = self.collections.lock().expect("lock poisoned");

        let mut to_create = Vec::with_capacity(to_register.len());
        let mut table_registers = Vec::with_capacity(to_register.len());
        // This work mutates the controller state, so must be done serially. Because there
        // is no io-bound work, its very fast.
        {
            for (id, description, write_handle, since_handle, metadata) in to_register {
                let data_shard_since = since_handle.since().clone();

                let collection_state = CollectionState::new(
                    description.clone(),
                    data_shard_since,
                    write_handle.upper().clone(),
                    vec![],
                    metadata.clone(),
                );

                match description.data_source {
                    DataSource::Introspection(_) | DataSource::Webhook => {
                        debug!(desc = ?description, meta = ?metadata, "registering {} with persist monotonic worker", id);
                        self_collections.insert(id, collection_state);
                    }
                    DataSource::Other(DataSourceOther::TableWrites) => {
                        debug!(desc = ?description, meta = ?metadata, "registering {} with persist table worker", id);
                        table_registers.push((id, collection_state));
                    }
                    DataSource::Ingestion(_)
                    | DataSource::Progress
                    | DataSource::Other(DataSourceOther::Compute)
                    | DataSource::Other(DataSourceOther::Source) => {
                        debug!(desc = ?description, meta = ?metadata, "not registering {} with a controller persist worker", id);
                        self_collections.insert(id, collection_state);
                    }
                }
                self.register_handles(
                    id,
                    metadata.txns_shard.is_some(),
                    since_handle,
                    write_handle,
                );

                to_create.push((id, description));
            }
        }

        // Register the tables all in one batch.
        if !table_registers.is_empty() {
            let register_ts = register_ts
                .expect("caller should have provided a register_ts when creating a table");
            let advance_to = mz_persist_types::StepForward::step_forward(&register_ts);
            for (id, mut collection_state) in table_registers {
                if let PersistTxns::EnabledLazy { .. } = &self.txns {
                    if collection_state.write_frontier.less_than(&advance_to) {
                        collection_state.write_frontier = Antichain::from_elem(advance_to.clone());
                    }
                }
                self_collections.insert(id, collection_state);
            }
        }

        // Patch up the since of all subsources (which includes the "main"
        // collection) and install read holds from the subsources on the since
        // of the remap collection. We need to do this here because a) the since
        // of the remap collection might be in advance of the since of the data
        // collections because we lazily forward commands to downgrade the since
        // to persist, and b) at the time the subsources are created we know
        // close to nothing about them, not even that they are subsources.
        //
        // N.B. Patching up the since based on the since of the remap collection
        // is correct because the since of the remap collection can advance iff
        // the storage controller allowed it to, which it only does when it
        // would also allow the since of the data collections to advance. It's
        // just that we need to reconcile outselves to the outside world
        // (persist) here.
        //
        // TODO(aljoscha): We should find a way to put this information and the
        // read holds in place when we create the subsource collections. OR, we
        // could create the subsource collections only as part of creating the
        // main source/ingestion.
        for (_id, description) in to_create.iter() {
            match &description.data_source {
                DataSource::Ingestion(ingestion) => {
                    let storage_dependencies = description.get_storage_dependencies();

                    self.install_dependency_read_holds_inner(
                        &mut self_collections,
                        // N.B. The "main" collection of the source is included in
                        // `source_exports`.
                        ingestion.source_exports.keys().cloned(),
                        &storage_dependencies,
                    )?;
                }
                DataSource::Webhook
                | DataSource::Introspection(_)
                | DataSource::Progress
                | DataSource::Other(_) => {
                    // No since to patch up and no read holds to install on
                    // dependencies!
                }
            }
        }

        Ok(())
    }

    fn check_alter_collection(
        &mut self,
        collections: &BTreeMap<GlobalId, IngestionDescription>,
    ) -> Result<(), StorageError> {
        let self_collections = self.collections.lock().expect("lock poisoned");
        self.check_alter_collections_inner(&self_collections, collections)?;
        Ok(())
    }

    async fn alter_collection(
        &mut self,
        collections: BTreeMap<GlobalId, IngestionDescription>,
    ) -> Result<(), StorageError> {
        let mut self_collections = self.collections.lock().expect("lock poisoned");

        self.check_alter_collections_inner(&self_collections, &collections)
            .expect("error avoided by calling check_alter_collection first");

        for (id, ingestion) in collections {
            // Describe the ingestion in terms of collection metadata.
            let description = self
                .enrich_ingestion_inner(&mut self_collections, id, ingestion.clone())
                .expect("verified valid in check_alter_collection_inner");

            let collection = self_collections.get_mut(&id).expect("validated exists");
            let new_source_exports = match &mut collection.description.data_source {
                DataSource::Ingestion(active_ingestion) => {
                    // Determine which IDs we're adding.
                    let new_source_exports: Vec<_> = description
                        .source_exports
                        .keys()
                        .filter(|id| !active_ingestion.source_exports.contains_key(id))
                        .cloned()
                        .collect();
                    *active_ingestion = ingestion;

                    new_source_exports
                }
                _ => unreachable!("verified collection refers to ingestion"),
            };

            // Assess dependency since, which we have to fast-forward this
            // collection's since to.
            let storage_dependencies = collection.description.get_storage_dependencies();

            // Ensure this new collection's since is aligned with the dependencies.
            // This will likely place its since beyond its upper which is OK because
            // its snapshot will catch it up with the rest of the source, i.e. we
            // will never see its upper at a state beyond 0 and less than its since.
            self.install_dependency_read_holds_inner(
                &mut self_collections,
                new_source_exports.into_iter(),
                &storage_dependencies,
            )?;
        }

        Ok(())
    }

    fn drop_collections(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError> {
        debug!(?identifiers, "drop_collections");

        self.validate_collection_ids(identifiers.iter().cloned())?;
        self.drop_collections_unvalidated(identifiers);

        Ok(())
    }

    fn drop_collections_unvalidated(&mut self, identifiers: Vec<GlobalId>) {
        debug!(?identifiers, "drop_collections_unvalidated");

        let mut collections = self.collections.lock().expect("lock poisoned");

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
            let collection = collections.get_mut(&id);

            if let Some(collection) = collection {
                tracing::info!(%id, "is_dropped = true!");
                assert!(
                    !collection.is_dropped,
                    "collection {} has been dropped already",
                    id
                );
                collection.is_dropped = true;
                finalized_policies.push((id, ReadPolicy::ValidFrom(Antichain::new())));
            }
        }
        self.set_read_policies_inner(&mut collections, finalized_policies);
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

        debug!(?policies, ?user_capabilities, "set_read_policies");

        self.set_read_policies_inner(&mut collections, policies);

        let user_capabilities = collections
            .iter_mut()
            .filter(|(id, _c)| id.is_user())
            .map(|(id, c)| {
                let updates = c.read_capabilities.updates().cloned().collect_vec();
                (*id, c.implied_capability.clone(), updates)
            })
            .collect_vec();

        debug!(?user_capabilities, "after! set_read_policies");
    }

    fn acquire_read_holds(
        &mut self,
        desired_holds: Vec<(GlobalId, Antichain<Self::Timestamp>)>,
    ) -> Vec<ReadHold<Self::Timestamp>> {
        let mut collections = self.collections.lock().expect("lock poisoned");

        // Clone for logging!
        let cloned_desired_holds = desired_holds.clone();

        // We advance the holds by our current since frontier. Can't
        // acquire holds for times that have been compacted away!
        let advanced_holds = desired_holds.into_iter().map(|(id, mut desired_hold)| {
            let collection = collections.get(&id).expect("missing collection");
            desired_hold.join_assign(&collection.read_capabilities.frontier().to_owned());
            (id, desired_hold)
        });

        // Report back the holds that we could acquire.
        let res = advanced_holds.clone().collect_vec();

        let mut updates = advanced_holds
            .into_iter()
            .map(|(id, hold)| {
                let mut changes = ChangeBatch::new();
                changes.extend(hold.into_iter().map(|time| (time, 1)));

                (id, changes)
            })
            .collect::<BTreeMap<_, _>>();

        StorageCollectionsImpl::update_read_capabilities_inner(
            &self.cmd_tx,
            &mut collections,
            &mut updates,
        );

        let res = res
            .into_iter()
            .map(|(id, since)| ReadHold::new(id, since, self.holds_tx.clone()))
            .collect_vec();

        debug!(desired_holds = ?cloned_desired_holds, acquired_holds = ?res, "acquire_read_holds");

        res
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

    async fn reconcile_state(&mut self) {
        debug!("reconcile_state");
    }

    /// See [crate::controller::StorageController::init_txns] and its
    /// implementation.
    async fn init_txns(&mut self, _init_ts: Self::Timestamp) -> Result<(), StorageError> {
        assert_eq!(self.txns_init_run, false);

        // We don't initialize the txns system, the `StorageController` does
        // that. All we care about is that initialization has been run.
        self.txns_init_run = true;
        Ok(())
    }
}

/// State maintained about individual collections.
#[derive(Debug)]
struct CollectionState<T> {
    /// Description with which the collection was created
    pub description: CollectionDescription<T>,

    /// Accumulation of read capabilities for the collection.
    ///
    /// This accumulation will always contain `self.implied_capability`, but may also contain
    /// capabilities held by others who have read dependencies on this collection.
    pub read_capabilities: MutableAntichain<T>,
    /// The implicit capability associated with collection creation.  This should never be less
    /// than the since of the associated persist collection.
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
    /// Creates a new collection state, with an initial read policy valid from `since`.
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

/// A Task that keeps persist handles, downgrades sinces when asked,
/// periodically gets recent uppers from them, and updates the shard collection
/// state when needed.
#[derive(Debug)]
struct BackgroundTask<T: TimelyTimestamp + Lattice + Codec64> {
    cmds_tx: mpsc::UnboundedSender<BackgroundCmd<T>>,
    cmds_rx: mpsc::UnboundedReceiver<BackgroundCmd<T>>,
    holds_rx: mpsc::UnboundedReceiver<(GlobalId, ChangeBatch<T>)>,
    collections: Arc<std::sync::Mutex<BTreeMap<GlobalId, CollectionState<T>>>>,
    // So we know what shard ID corresponds to what global ID, which we need
    // when re-enqueing futures for determining the next upper update.
    shard_by_id: BTreeMap<GlobalId, ShardId>,
    since_handles: BTreeMap<GlobalId, SinceHandle<SourceData, (), T, Diff, PersistEpoch>>,
    txns_handle: Option<WriteHandle<SourceData, (), T, Diff>>,
    txns_shards: BTreeSet<GlobalId>,
}

#[derive(Debug)]
enum BackgroundCmd<T: TimelyTimestamp + Lattice + Codec64> {
    Register {
        id: GlobalId,
        is_in_txns: bool,
        write_handle: WriteHandle<SourceData, (), T, Diff>,
        since_handle: SinceHandle<SourceData, (), T, Diff, PersistEpoch>,
    },
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
        oneshot::Sender<SnapshotStatsRes>,
    ),
}

/// A newtype wrapper to hang a Debug impl off of.
pub(crate) struct SnapshotStatsRes(BoxFuture<'static, Result<SnapshotStats, StorageError>>);

impl Debug for SnapshotStatsRes {
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

        loop {
            tokio::select! {
                Some((id, handle, upper)) = txns_upper_futures.next() => {
                    debug!("new upper from txns shard: {:?}", upper);
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
                        tracing::debug!("new upper for collection {id}: {:?}", upper);
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
                                "ControllerCmd::Update only valid for updating extant write handles",
                            );

                            self.since_handles.insert(id, since_handle).expect(
                                "ControllerCmd::Update only valid for updating extant since handles",
                            );

                            let fut = gen_upper_future(id, write_handle);
                            upper_futures.push(fut);

                        }
                        BackgroundCmd::SnapshotStats(id, as_of, tx) => {
                            // NB: The requested as_of could be arbitrarily far in the future. So,
                            // in order to avoid blocking the PersistReadWorker loop until it's
                            // available and the `snapshot_stats` call resolves, instead return the
                            // future to the caller and await it there.
                            let res = match self.since_handles.get(&id) {
                                Some(x) => {
                                    let fut: BoxFuture<
                                        'static,
                                        Result<SnapshotStats, StorageError>,
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
                    // TODO: We could read all outstanding holds updates and
                    // apply them all in one go.

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
                        tracing::debug!(?user_changes, "applying holds changes from channel");
                    }

                    StorageCollectionsImpl::update_read_capabilities_inner(
                        &self.cmds_tx,
                        &mut collections,
                        &mut batched_changes,
                    );
                }
            }
        }

        warn!("UppersTask shutting down");
    }

    #[instrument(level = "debug")]
    async fn update_write_frontiers(&mut self, updates: &[(GlobalId, Antichain<T>)]) {
        let mut read_capability_changes = BTreeMap::default();

        let mut collections = self.collections.lock().expect("lock poisoned");

        for (id, new_upper) in updates.iter() {
            let collection = if let Some(c) = collections.get_mut(id) {
                c
            } else {
                tracing::warn!("Reference to absent collection {id}");
                continue;
            };

            if PartialOrder::less_than(&collection.write_frontier, new_upper) {
                collection.write_frontier = new_upper.clone();
            }

            let mut new_read_capability = collection
                .read_policy
                .frontier(collection.write_frontier.borrow());

            if id.is_user() {
                tracing::debug!(
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
                &mut collections,
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
                tracing::trace!("downgrade_sinces: reference to absent collection {id}");
                continue;
            };

            if id.is_user() {
                tracing::trace!("downgrading since of {} to {:?}", id, new_since);
            }

            let epoch = since_handle.opaque().clone();
            let result = if new_since.is_empty() {
                // A shard's since reaching the empty frontier is a prereq for being
                // able to finalize a shard, so the final downgrade should never be
                // rate-limited.
                let res = Some(
                    since_handle
                        .compare_and_downgrade_since(&epoch, (&epoch, &new_since))
                        .await,
                );

                info!(%id, "removing persist handles because the since advanced to []!");

                let since_handle = self.since_handles.remove(&id).expect("known to exist");
                // Be nice about expiring!
                since_handle.expire().await;
                if self.shard_by_id.remove(&id).is_none() {
                    panic!("missing GlobalId -> ShardId mapping for id {id}");
                }
                self.txns_shards.remove(&id);

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
