// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the compute instance controller.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use mz_build_info::DUMMY_BUILD_INFO;
use mz_cluster_client::WallclockLagFn;
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_cluster_client::metrics::ControllerMetrics;
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::dyncfgs::{ENABLE_COLUMN_PAGED_BATCHER, ENABLE_MZ_JOIN_CORE};
use mz_controller_types::dyncfgs::{
    ENABLE_WALLCLOCK_FRESHNESS_RECORDING, ENABLE_WALLCLOCK_FRESHNESS_REPORTING,
    WALLCLOCK_FRESHNESS_RESTART_POLICY,
};
use mz_dyncfg::{ConfigSet, ConfigUpdates, ConfigVal};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{Datum, GlobalId, RelationDesc, RelationVersion, Row, Timestamp};
use mz_storage_client::client::TimestamplessUpdateBuilder;
use mz_storage_client::controller::{
    CollectionDescription, IntrospectionType, StorageMetadata, StorageTxn, WallclockLag,
};
use mz_storage_client::freshness::{FreshnessSeeds, Measure, SharedVisibilityLog};
use mz_storage_client::storage_collections::{
    CollectionFrontiers, SnapshotCursor, StorageCollections,
};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::{CollectionMetadata, StorageError};
use mz_storage_types::errors::CollectionMissing;
use mz_storage_types::freshness::histogram::BucketScheme;
use mz_storage_types::freshness::{FreshnessLeaf, FreshnessTree, Ticker, TickerLookup};
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_holds::{ChangeTx, ReadHold};
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sources::SourceData;
use mz_storage_types::time_dependence::{TimeDependence, TimeDependenceError};
use timely::progress::Antichain;
use tokio::sync::mpsc;

use crate::controller::replica::ReplicaConfig;
use crate::controller::{ComputeControllerResponse, IntrospectionUpdates};
use crate::logging::{LogVariant, TimelyLog};
use crate::metrics::ComputeControllerMetrics;
use crate::protocol::command::{ComputeCommand, InstanceConfig};
use crate::protocol::response::{ComputeResponse, FrontiersResponse};

use super::{
    CollectionReadiness, Instance, ReplicaCollectionIntrospection, ReplicaCollectionState,
    ReplicaId, SharedCollectionState, classify_collection_readiness,
};

fn ac(ts: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(Timestamp::new(ts))
}

fn state(id: ReplicaId, as_of: u64, write: u64, output: u64) -> ReplicaCollectionState {
    let (tx, _rx) = mpsc::unbounded_channel();
    let introspection = ReplicaCollectionIntrospection::new(id, GlobalId::User(1), tx, ac(as_of));
    let mut state = ReplicaCollectionState::new(None, ac(as_of), introspection, Vec::new());
    state.update_write_frontier(ac(write));
    state.update_output_frontier(ac(output));
    state
}

fn classify(
    replicas: &[(ReplicaId, ReplicaCollectionState)],
    targets: &[ReplicaId],
    references: &[ReplicaId],
    lag: Option<Timestamp>,
) -> CollectionReadiness {
    classify_collection_readiness(
        replicas.iter().map(|(id, state)| (*id, state)),
        &targets.iter().copied().collect::<BTreeSet<_>>(),
        &references.iter().copied().collect::<BTreeSet<_>>(),
        lag,
    )
}

const LAG: Option<Timestamp> = Some(Timestamp::new(60));

/// The regression the lag gate exists for: a pending replica whose dataflow
/// has produced its first output past the as-of, and so reports hydrated,
/// while its output frontier is still far behind the outgoing replica.
#[mz_ore::test]
fn shared_write_does_not_hide_output_lag() {
    let reference = ReplicaId::User(1);
    let target = ReplicaId::User(2);
    let mut replicas = vec![
        (reference, state(reference, 100, 10_000, 10_000)),
        // The shared MV write frontier is caught up, but this replica has
        // only just produced its first output past the as-of.
        (target, state(target, 100, 10_000, 101)),
    ];
    assert_eq!(
        classify(&replicas, &[target], &[reference], LAG),
        CollectionReadiness::Lagging { lag: Some(9_899) },
    );
    replicas[1].1.update_output_frontier(ac(9_940));
    assert_eq!(
        classify(&replicas, &[target], &[reference], LAG),
        CollectionReadiness::Ready,
    );
}

#[mz_ore::test]
fn refresh_writes_ahead_of_outputs_is_ready() {
    let reference = ReplicaId::User(1);
    let target = ReplicaId::User(2);
    let replicas = vec![
        (reference, state(reference, 100, 20_000, 10_000)),
        (target, state(target, 100, 20_000, 10_000)),
    ];
    assert_eq!(
        classify(&replicas, &[target], &[reference], LAG),
        CollectionReadiness::Ready,
    );
}

#[mz_ore::test]
fn furthest_reference_and_bystander_exclusion() {
    let trailing = ReplicaId::User(1);
    let ahead = ReplicaId::User(2);
    let target = ReplicaId::User(3);
    let bystander = ReplicaId::User(4);
    let replicas = vec![
        (trailing, state(trailing, 100, 9_000, 9_000)),
        (ahead, state(ahead, 100, 10_000, 10_000)),
        (target, state(target, 100, 8_990, 8_990)),
        (bystander, state(bystander, 100, 1_000_000, 1_000_000)),
    ];
    assert_eq!(
        classify(&replicas, &[target], &[trailing], LAG),
        CollectionReadiness::Ready,
    );
    assert_eq!(
        classify(&replicas, &[target], &[trailing, ahead], LAG),
        CollectionReadiness::Lagging { lag: Some(1_010) },
    );
}

#[mz_ore::test]
fn one_ready_target_suffices() {
    let reference = ReplicaId::User(1);
    let unhydrated = ReplicaId::User(2);
    let lagging = ReplicaId::User(3);
    let ready = ReplicaId::User(4);
    let replicas = vec![
        (reference, state(reference, 100, 10_000, 10_000)),
        // This replica is within the lag allowance but has not advanced
        // past its own as-of. It cannot supply progress for the hydrated,
        // lagging replica below.
        (unhydrated, state(unhydrated, 10_000, 10_000, 10_000)),
        (lagging, state(lagging, 100, 1_000, 1_000)),
        (ready, state(ready, 100, 9_990, 9_990)),
    ];
    assert_eq!(
        classify(&replicas, &[unhydrated, lagging, ready], &[reference], LAG),
        CollectionReadiness::Ready,
    );
    assert_eq!(
        classify(&replicas, &[unhydrated, lagging], &[reference], LAG),
        CollectionReadiness::Lagging { lag: Some(9_000) },
    );
}

#[mz_ore::test]
fn no_lag_gate_is_hydration_only() {
    let reference = ReplicaId::User(1);
    let target = ReplicaId::User(2);
    let replicas = vec![
        (reference, state(reference, 100, 10_000, 10_000)),
        (target, state(target, 100, 101, 101)),
    ];
    assert_eq!(
        classify(&replicas, &[target], &[reference], None),
        CollectionReadiness::Ready,
    );
    let unhydrated = vec![(target, state(target, 100, 100, 100))];
    assert_eq!(
        classify(&unhydrated, &[target], &[], None),
        CollectionReadiness::Unhydrated,
    );
}

#[mz_ore::test]
fn lag_reports_the_closest_hydrated_target() {
    let reference = ReplicaId::User(1);
    let far = ReplicaId::User(2);
    let close = ReplicaId::User(3);
    let mut replicas = vec![
        (reference, state(reference, 100, 10_000, 10_000)),
        (far, state(far, 100, 1_000, 1_000)),
        (close, state(close, 100, 9_000, 9_000)),
    ];
    for _ in 0..2 {
        assert_eq!(
            classify(&replicas, &[far, close], &[reference], LAG),
            CollectionReadiness::Lagging { lag: Some(1_000) },
        );
        replicas.reverse();
    }
}

#[mz_ore::test]
fn completed_reference_requires_completed_target() {
    let reference = ReplicaId::User(1);
    let target = ReplicaId::User(2);
    let mut replicas = vec![
        (reference, state(reference, 100, 1_000, 1_000)),
        (target, state(target, 100, 1_000, 1_000)),
    ];
    replicas[0].1.update_output_frontier(Antichain::new());
    assert_eq!(
        classify(&replicas, &[target], &[reference], Some(Timestamp::new(0))),
        CollectionReadiness::Lagging { lag: None },
    );
    replicas[1].1.update_output_frontier(Antichain::new());
    assert_eq!(
        classify(&replicas, &[target], &[reference], Some(Timestamp::new(0))),
        CollectionReadiness::Ready,
    );
}

#[mz_ore::test]
fn no_reference_means_nothing_to_regress() {
    let target = ReplicaId::User(1);
    let replicas = vec![(target, state(target, 100, 5_000, 5_000))];
    assert_eq!(
        classify(&replicas, &[target], &[], Some(Timestamp::new(0))),
        CollectionReadiness::Ready,
    );
    assert_eq!(
        classify(&replicas, &[target], &[target], Some(Timestamp::new(0))),
        CollectionReadiness::Ready,
    );
}

#[mz_ore::test]
fn no_targets_is_unhydrated() {
    let reference = ReplicaId::User(1);
    let replicas = vec![(reference, state(reference, 100, 10_000, 10_000))];
    assert_eq!(
        classify(&replicas, &[], &[reference], LAG),
        CollectionReadiness::Unhydrated,
    );
}

fn create_instance_command() -> ComputeCommand {
    ComputeCommand::CreateInstance(Box::new(InstanceConfig {
        logging: Default::default(),
        expiration_offset: None,
        peek_stash_persist_location: PersistLocation::new_in_mem(),
        arrangement_dictionary_compression: false,
        initial_config: Default::default(),
    }))
}

fn initial_config(cmd: &ComputeCommand) -> &ConfigUpdates {
    match cmd {
        ComputeCommand::CreateInstance(config) => &config.initial_config,
        other => panic!("expected CreateInstance, got {other:?}"),
    }
}

/// `CreateInstance` is specialized with a full snapshot of the instance-wide dyncfg, so the
/// replica seeds its worker config at create time rather than waiting for the first
/// `UpdateConfiguration`. This is the regression guard for create-time setup observing dyncfg
/// defaults.
#[mz_ore::test]
fn create_instance_snapshots_instance_wide_dyncfg() {
    let dyncfg = ConfigSet::default()
        .add(&ENABLE_COLUMN_PAGED_BATCHER)
        .add(&ENABLE_MZ_JOIN_CORE);
    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_COLUMN_PAGED_BATCHER, true);
    updates.add(&ENABLE_MZ_JOIN_CORE, false);
    updates.apply(&dyncfg);

    // A replica without an override sees exactly the instance-wide values.
    let cmd = Instance::specialize_command_for_replica(
        create_instance_command(),
        &ConfigUpdates::default(),
        &dyncfg,
    );
    let snapshot = initial_config(&cmd);
    assert_eq!(
        snapshot.updates.get(ENABLE_COLUMN_PAGED_BATCHER.name()),
        Some(&ConfigVal::Bool(true)),
    );
    assert_eq!(
        snapshot.updates.get(ENABLE_MZ_JOIN_CORE.name()),
        Some(&ConfigVal::Bool(false)),
    );
}

/// A replica-scoped override beats the instance-wide value in the create-time snapshot, so a
/// create-time-frozen scoped flag reaches the replica with its override applied.
#[mz_ore::test]
fn create_instance_snapshot_applies_replica_override() {
    let dyncfg = ConfigSet::default().add(&ENABLE_COLUMN_PAGED_BATCHER);
    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_COLUMN_PAGED_BATCHER, true);
    updates.apply(&dyncfg);

    let mut override_updates = ConfigUpdates::default();
    override_updates.add(&ENABLE_COLUMN_PAGED_BATCHER, false);

    let cmd = Instance::specialize_command_for_replica(
        create_instance_command(),
        &override_updates,
        &dyncfg,
    );
    assert_eq!(
        initial_config(&cmd)
            .updates
            .get(ENABLE_COLUMN_PAGED_BATCHER.name()),
        Some(&ConfigVal::Bool(false)),
        "replica override should win over the instance-wide value",
    );
}

/// `UpdateConfiguration` continues to merge the replica's override into the update.
#[mz_ore::test]
fn update_configuration_merges_replica_override() {
    let dyncfg = ConfigSet::default().add(&ENABLE_COLUMN_PAGED_BATCHER);

    let mut override_updates = ConfigUpdates::default();
    override_updates.add(&ENABLE_COLUMN_PAGED_BATCHER, true);

    let cmd = Instance::specialize_command_for_replica(
        ComputeCommand::UpdateConfiguration(Box::new(Default::default())),
        &override_updates,
        &dyncfg,
    );
    match cmd {
        ComputeCommand::UpdateConfiguration(params) => assert_eq!(
            params
                .dyncfg_updates
                .updates
                .get(ENABLE_COLUMN_PAGED_BATCHER.name()),
            Some(&ConfigVal::Bool(true)),
        ),
        other => panic!("expected UpdateConfiguration, got {other:?}"),
    }
}

/// Resolves every ingestion to an aligned ticker of the given interval.
#[derive(Debug)]
struct FixedTickers(u64);

/// An ingestion that [`FixedTickers`] does not know.
const UNKNOWN_INGESTION: GlobalId = GlobalId::User(999);

impl TickerLookup for FixedTickers {
    fn ingestion_ticker(&self, id: GlobalId) -> Option<Ticker> {
        (id != UNKNOWN_INGESTION).then_some(Ticker::Aligned {
            interval_ms: self.0,
        })
    }
}

/// A `StorageCollections` that only provides ticker lookups, visibility logs and restart seeds.
#[derive(Debug)]
struct TickerStorage(
    Arc<FixedTickers>,
    BTreeMap<GlobalId, SharedVisibilityLog>,
    FreshnessSeeds,
);

#[async_trait]
impl StorageCollections for TickerStorage {
    async fn initialize_state(
        &self,
        _txn: &mut (dyn StorageTxn + Send),
        _init_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError> {
        unimplemented!()
    }

    fn update_parameters(&self, _config_params: StorageParameters) {
        unimplemented!()
    }

    fn collection_metadata(&self, _id: GlobalId) -> Result<CollectionMetadata, CollectionMissing> {
        unimplemented!()
    }

    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)> {
        unimplemented!()
    }

    fn collections_frontiers(
        &self,
        ids: Vec<GlobalId>,
    ) -> Result<Vec<CollectionFrontiers>, CollectionMissing> {
        // The harness's compute collections are not storage collections.
        Err(CollectionMissing(ids[0]))
    }

    fn active_collection_frontiers(&self) -> Vec<CollectionFrontiers> {
        unimplemented!()
    }

    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError> {
        Err(StorageError::IdentifierMissing(id))
    }

    async fn snapshot_stats(
        &self,
        _id: GlobalId,
        _as_of: Antichain<Timestamp>,
    ) -> Result<SnapshotStats, StorageError> {
        unimplemented!()
    }

    async fn snapshot_parts_stats(
        &self,
        _id: GlobalId,
        _as_of: Antichain<Timestamp>,
    ) -> BoxFuture<'static, Result<SnapshotPartsStats, StorageError>> {
        unimplemented!()
    }

    fn snapshot(
        &self,
        _id: GlobalId,
        _as_of: Timestamp,
    ) -> BoxFuture<'static, Result<Vec<(Row, StorageDiff)>, StorageError>> {
        unimplemented!()
    }

    async fn snapshot_latest(&self, _id: GlobalId) -> Result<Vec<Row>, StorageError> {
        unimplemented!()
    }

    fn snapshot_cursor(
        &self,
        _id: GlobalId,
        _as_of: Timestamp,
    ) -> BoxFuture<'static, Result<SnapshotCursor, StorageError>> {
        unimplemented!()
    }

    fn snapshot_and_stream(
        &self,
        _id: GlobalId,
        _as_of: Timestamp,
    ) -> BoxFuture<
        'static,
        Result<BoxStream<'static, (SourceData, Timestamp, StorageDiff)>, StorageError>,
    > {
        unimplemented!()
    }

    fn create_update_builder(
        &self,
        _id: GlobalId,
    ) -> BoxFuture<
        'static,
        Result<TimestamplessUpdateBuilder<SourceData, (), StorageDiff>, StorageError>,
    > {
        unimplemented!()
    }

    async fn prepare_state(
        &self,
        _txn: &mut (dyn StorageTxn + Send),
        _ids_to_add: BTreeSet<GlobalId>,
        _ids_to_drop: BTreeSet<GlobalId>,
        _ids_to_register: BTreeMap<GlobalId, ShardId>,
    ) -> Result<(), StorageError> {
        unimplemented!()
    }

    async fn create_collections_for_bootstrap(
        &self,
        _storage_metadata: &StorageMetadata,
        _register_ts: Option<Timestamp>,
        _collections: Vec<(GlobalId, CollectionDescription)>,
        _migrated_storage_collections: &BTreeSet<GlobalId>,
    ) -> Result<(), StorageError> {
        unimplemented!()
    }

    async fn alter_table_desc(
        &self,
        _existing_collection: GlobalId,
        _new_collection: GlobalId,
        _new_desc: RelationDesc,
        _expected_version: RelationVersion,
    ) -> Result<(), StorageError> {
        unimplemented!()
    }

    fn drop_collections_unvalidated(
        &self,
        _storage_metadata: &StorageMetadata,
        _identifiers: Vec<GlobalId>,
    ) {
        unimplemented!()
    }

    fn set_read_policies(&self, _policies: Vec<(GlobalId, ReadPolicy)>) {
        unimplemented!()
    }

    fn acquire_read_holds(
        &self,
        _desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold>, CollectionMissing> {
        unimplemented!()
    }

    fn determine_time_dependence(
        &self,
        _id: GlobalId,
    ) -> Result<Option<TimeDependence>, TimeDependenceError> {
        unimplemented!()
    }

    fn freshness_tree(&self, _id: GlobalId) -> Option<FreshnessTree> {
        unimplemented!()
    }

    fn set_freshness_tree(&self, _id: GlobalId, _tree: Option<FreshnessTree>) {}

    fn visibility_log(&self, id: GlobalId) -> Option<SharedVisibilityLog> {
        self.1.get(&id).cloned()
    }

    fn ticker_lookup(&self) -> Arc<dyn TickerLookup + Send + Sync> {
        Arc::<FixedTickers>::clone(&self.0)
    }

    fn freshness_seeds(&self) -> FreshnessSeeds {
        self.2.clone()
    }

    fn update_ingestion_timestamp_interval(&self, _id: GlobalId, _interval: Duration) {
        unimplemented!()
    }

    fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        unimplemented!()
    }
}

/// A minute-aligned wall-clock time in ms.
const T0: u64 = 1_800_000_000_000;

/// The storage collection whose visibility log the harness's `StorageCollections` serves.
const STORAGE_INPUT: GlobalId = GlobalId::User(1);

/// A sent freshness histogram row: measure, collection, replica (`None` for global), and period
/// `(start, end]`.
type RecordedKey = (String, String, Option<String>, u64, u64);

/// Sent freshness histogram counts, with the buckets seen per measure, collection and replica.
#[derive(Debug, Default)]
struct Recorded {
    counts: BTreeMap<RecordedKey, i64>,
    buckets: BTreeMap<(String, String, Option<String>), BTreeSet<i32>>,
}

impl Recorded {
    /// The `(start, end, count)` of every end-to-end period recorded for `replica`, or globally.
    fn periods(&self, replica: Option<ReplicaId>) -> Vec<(u64, u64, i64)> {
        self.measure_periods("end_to_end", None, replica)
    }

    /// The `(start, end, count)` of every period of `measure` recorded for `id`, or any
    /// collection, on `replica`, or globally.
    fn measure_periods(
        &self,
        measure: &str,
        id: Option<GlobalId>,
        replica: Option<ReplicaId>,
    ) -> Vec<(u64, u64, i64)> {
        let id = id.map(|id| id.to_string());
        let replica = replica.map(|r| r.to_string());
        self.counts
            .iter()
            .filter(|((m, c, r, _, _), _)| {
                m == measure && id.as_ref().is_none_or(|id| id == c) && *r == replica
            })
            .map(|((_, _, _, start, end), count)| (*start, *end, *count))
            .collect()
    }

    /// The buckets of `measure` recorded for `id` on `replica`, or globally.
    fn buckets(&self, measure: &str, id: GlobalId, replica: Option<ReplicaId>) -> BTreeSet<i32> {
        let key = (
            measure.to_string(),
            id.to_string(),
            replica.map(|r| r.to_string()),
        );
        self.buckets.get(&key).cloned().unwrap_or_default()
    }
}

/// The log collection of a [`FreshnessHarness::with_log`], with a 1 s ingestion leaf.
const LOG: GlobalId = GlobalId::System(1000);
const LOG_VARIANT: LogVariant = LogVariant::Timely(TimelyLog::Operates);

/// An instance whose clock reads `clock` and that resolves every ingestion to a 1 s ticker.
struct FreshnessHarness {
    instance: Instance,
    clock: Arc<AtomicU64>,
    /// The visibility log of [`STORAGE_INPUT`].
    storage_log: SharedVisibilityLog,
    /// The restart seeds the harness's `StorageCollections` serves.
    seeds: FreshnessSeeds,
    introspection_rx: mpsc::UnboundedReceiver<IntrospectionUpdates>,
    _response_rx: mpsc::UnboundedReceiver<ComputeControllerResponse>,
}

impl FreshnessHarness {
    fn new(recording: bool, read_only: bool) -> Self {
        Self::build(recording, read_only, Vec::new())
    }

    /// A recording harness with the log collection [`LOG`].
    fn with_log() -> Self {
        let log = (
            LOG_VARIANT,
            LOG,
            SharedCollectionState::new(ac(0)),
            Some(FreshnessTree::Leaf(FreshnessLeaf::Ingestion(
                GlobalId::User(1),
            ))),
        );
        Self::build(true, false, vec![log])
    }

    fn build(recording: bool, read_only: bool, logs: Vec<super::ArrangedLog>) -> Self {
        let clock = Arc::new(AtomicU64::new(T0));
        let now = {
            let clock = Arc::clone(&clock);
            NowFn::from(move || clock.load(Ordering::SeqCst))
        };
        let dyncfg = mz_dyncfgs::all_dyncfgs();
        let mut updates = ConfigUpdates::default();
        updates.add(&ENABLE_WALLCLOCK_FRESHNESS_RECORDING, recording);
        updates.apply(&dyncfg);

        let registry = MetricsRegistry::new();
        let metrics = ComputeControllerMetrics::new(&registry, ControllerMetrics::new(&registry))
            .for_instance(ComputeInstanceId::User(1));
        // The instance task is not running, so no commands arrive.
        let (_command_tx, command_rx) = mpsc::unbounded_channel();
        let (response_tx, response_rx) = mpsc::unbounded_channel();
        let (introspection_tx, introspection_rx) = mpsc::unbounded_channel();
        let read_hold_tx: ChangeTx = Arc::new(|_, _| Ok(()));

        let storage_log = SharedVisibilityLog::default();
        let seeds = FreshnessSeeds::default();
        let storage = TickerStorage(
            Arc::new(FixedTickers(1000)),
            BTreeMap::from([(STORAGE_INPUT, storage_log.clone())]),
            seeds.clone(),
        );
        let instance = Instance::new(
            &DUMMY_BUILD_INFO,
            Arc::new(storage),
            PersistLocation::new_in_mem(),
            logs,
            metrics,
            now.clone(),
            WallclockLagFn::new(now),
            Arc::new(dyncfg),
            command_rx,
            response_tx,
            read_hold_tx,
            introspection_tx,
            read_only,
        );
        FreshnessHarness {
            instance,
            clock,
            storage_log,
            seeds,
            introspection_rx,
            _response_rx: response_rx,
        }
    }

    fn set_now(&self, now_ms: u64) {
        self.clock.store(now_ms, Ordering::SeqCst);
    }

    /// Switches recording and runs the maintenance step that applies the switch.
    fn set_recording(&mut self, recording: bool) {
        let mut updates = ConfigUpdates::default();
        updates.add(&ENABLE_WALLCLOCK_FRESHNESS_RECORDING, recording);
        updates.apply(&self.instance.dyncfg);
        self.instance.refresh_freshness_config();
    }

    fn add_replica(&mut self, id: ReplicaId) {
        self.add_replica_with_logs(id, BTreeMap::new());
    }

    /// Adds a replica that maintains the log collections `index_logs`.
    fn add_replica_with_logs(&mut self, id: ReplicaId, index_logs: BTreeMap<LogVariant, GlobalId>) {
        let mut logging = crate::logging::LoggingConfig::default();
        logging.index_logs = index_logs;
        let config = ReplicaConfig {
            // Nothing listens there, so the replica task keeps retrying in the background.
            location: ClusterReplicaLocation {
                ctl_addrs: vec!["127.0.0.1:1".into()],
            },
            logging,
            grpc_client: Default::default(),
            expiration_offset: None,
            arrangement_dictionary_compression: false,
        };
        self.instance
            .add_replica(id, config, None, ConfigUpdates::default())
            .expect("new replica");
    }

    /// Adds a collection with a 1 s ingestion leaf and no inputs.
    fn add_collection(&mut self, id: GlobalId, as_of: u64) {
        self.add_collection_with_inputs(id, as_of, &[]);
    }

    /// Adds a collection with a 1 s ingestion leaf that reads `inputs`.
    fn add_collection_with_inputs(&mut self, id: GlobalId, as_of: u64, inputs: &[GlobalId]) {
        let tree = FreshnessTree::Leaf(FreshnessLeaf::Ingestion(STORAGE_INPUT));
        let inputs: Vec<_> = inputs.iter().map(|id| (*id, tree.clone())).collect();
        self.add_collection_with_input_trees(id, as_of, &inputs);
    }

    /// Adds a collection with a 1 s ingestion leaf that reads `inputs` with the given trees.
    fn add_collection_with_input_trees(
        &mut self,
        id: GlobalId,
        as_of: u64,
        inputs: &[(GlobalId, FreshnessTree)],
    ) {
        let as_of = Antichain::from_elem(Timestamp::from(as_of));
        self.instance.add_collection(
            id,
            as_of.clone(),
            SharedCollectionState::new(as_of),
            BTreeMap::new(),
            BTreeMap::new(),
            Vec::new(),
            false,
            false,
            None,
            None,
            None,
            Some(FreshnessTree::Leaf(FreshnessLeaf::Ingestion(STORAGE_INPUT))),
            inputs.iter().cloned().collect(),
        );
    }

    /// Delivers a write frontier report of `replica` for collection `id`.
    fn report(&mut self, replica: ReplicaId, id: GlobalId, write_frontier: u64) {
        let frontier = Antichain::from_elem(Timestamp::from(write_frontier));
        self.report_frontier(replica, id, frontier);
    }

    fn report_frontier(
        &mut self,
        replica: ReplicaId,
        id: GlobalId,
        frontier: Antichain<Timestamp>,
    ) {
        let epoch = self.instance.replicas[&replica].epoch;
        let frontiers = FrontiersResponse {
            write_frontier: Some(frontier),
            input_frontier: None,
            output_frontier: None,
        };
        self.instance
            .handle_response((replica, epoch, ComputeResponse::Frontiers(id, frontiers)));
    }

    /// Drains the freshness histogram updates sent so far.
    fn recorded(&mut self) -> Recorded {
        let mut recorded = Recorded::default();
        while let Ok((type_, updates)) = self.introspection_rx.try_recv() {
            let has_replica = match type_ {
                IntrospectionType::WallclockFreshnessHistogram => false,
                IntrospectionType::ReplicaWallclockFreshnessHistogram => true,
                _ => continue,
            };
            for (row, diff) in updates {
                let datums = row.unpack();
                let ms = |d: Datum| match d {
                    Datum::TimestampTz(t) => u64::try_from(t.timestamp_millis()).expect("positive"),
                    other => panic!("expected a timestamp, got {other:?}"),
                };
                let (replica, rest) = match has_replica {
                    true => (Some(datums[3].unwrap_str().to_string()), &datums[4..]),
                    false => (None, &datums[3..]),
                };
                let measure = rest[0].unwrap_str().to_string();
                let id = datums[2].unwrap_str().to_string();
                assert!(
                    ["end_to_end", "local", "skew"].contains(&measure.as_str()),
                    "{measure}"
                );
                assert_eq!(rest[1], Datum::UInt32(4));
                recorded
                    .buckets
                    .entry((measure.clone(), id.clone(), replica.clone()))
                    .or_default()
                    .insert(rest[2].unwrap_int32());
                assert_eq!(rest[3].unwrap_map().iter().count(), 0, "no workload class");
                let key = (measure, id, replica, ms(datums[0]), ms(datums[1]));
                *recorded.counts.entry(key).or_insert(0) += diff.into_inner();
            }
        }
        recorded
    }
}

/// A 1 s source observed 5 ms after each tick on two replicas, one of which reconnects and
/// reports a frontier below what it reported before.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn freshness_histograms_count_each_arrival_once() {
    let mut h = FreshnessHarness::new(true, false);
    h.instance.initialization_complete();
    let id = GlobalId::User(10);
    h.add_collection(id, T0);
    let (r1, r2) = (ReplicaId::User(1), ReplicaId::User(2));
    h.add_replica(r1);
    h.add_replica(r2);

    let tick = |h: &mut FreshnessHarness, s: u64| {
        h.set_now(T0 + s * 1000 + 5);
        h.report(r1, id, T0 + s * 1000 + 1);
        h.report(r2, id, T0 + s * 1000 + 1);
    };
    for s in 1..=120 {
        tick(&mut h, s);
    }
    // The reconnected replica restarts from the collection's read frontier.
    h.instance.rehydrate_replica(r2);
    h.report(r2, id, T0 + 10_001);
    for s in 121..=180 {
        tick(&mut h, s);
    }

    let recorded = h.recorded();
    let expected = vec![
        (T0, T0 + 60_000, 60_000),
        (T0 + 60_000, T0 + 120_000, 60_000),
        (T0 + 120_000, T0 + 180_000, 60_000),
    ];
    assert_eq!(recorded.periods(None), expected, "global");
    assert_eq!(recorded.periods(Some(r1)), expected, "replica 1");
    assert_eq!(recorded.periods(Some(r2)), expected, "replica 2");
    // Without inputs, local delay is the end-to-end latency.
    for replica in [None, Some(r1), Some(r2)] {
        assert_eq!(
            recorded.measure_periods("local", None, replica),
            expected,
            "local {replica:?}"
        );
    }
    assert_eq!(recorded.counts.len(), 18, "{:?}", recorded.counts);
    let bucket = BucketScheme::new(4).expect("power of two").bucket(1005);
    for replica in [None, Some(r1), Some(r2)] {
        for measure in ["end_to_end", "local"] {
            assert_eq!(
                recorded.buckets(measure, id, replica),
                BTreeSet::from([bucket]),
                "{measure} {replica:?}"
            );
        }
    }
}

/// A chain from a storage collection, whose log the compute controller reads through
/// `StorageCollections`, through two compute collections, each observed a few ms after its
/// input, on two replicas. The second replica is slower for the last collection.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn local_delay_attributes_each_hop() {
    let mut h = FreshnessHarness::new(true, false);
    h.instance.initialization_complete();
    let (a, b) = (GlobalId::User(10), GlobalId::User(11));
    h.add_collection_with_inputs(a, T0, &[STORAGE_INPUT]);
    h.add_collection_with_inputs(b, T0, &[a]);
    let (r1, r2) = (ReplicaId::User(1), ReplicaId::User(2));
    h.add_replica(r1);
    h.add_replica(r2);

    let config =
        mz_storage_client::freshness::recorder_config(&h.instance.dyncfg).expect("recording on");
    let tree = FreshnessTree::Leaf(FreshnessLeaf::Ingestion(STORAGE_INPUT))
        .resolve(&FixedTickers(1000))
        .expect("resolves");
    for s in 1..=120 {
        let tick = T0 + s * 1000;
        // The storage controller observes the input 2 ms after the tick.
        h.set_now(tick + 2);
        let frontier = Antichain::from_elem(Timestamp::from(tick + 1));
        h.storage_log.record(&tree, &frontier, tick + 2, &config);
        h.set_now(tick + 5);
        h.report(r1, a, tick + 1);
        h.report(r2, a, tick + 1);
        h.set_now(tick + 13);
        h.report(r1, b, tick + 1);
        h.set_now(tick + 40);
        h.report(r2, b, tick + 1);
    }

    let recorded = h.recorded();
    let scheme = BucketScheme::new(4).expect("power of two");
    let expected = vec![
        (T0, T0 + 60_000, 60_000),
        (T0 + 60_000, T0 + 120_000, 60_000),
    ];
    for id in [a, b] {
        for replica in [None, Some(r1), Some(r2)] {
            for measure in ["end_to_end", "local"] {
                assert_eq!(
                    recorded.measure_periods(measure, Some(id), replica),
                    expected,
                    "{measure} {id} {replica:?}"
                );
            }
        }
    }
    // A replica's collection waits for its input on the same replica. The global frontier of `b`
    // advances with the faster replica.
    for replica in [None, Some(r1), Some(r2)] {
        assert_eq!(
            recorded.buckets("local", a, replica),
            BTreeSet::from([scheme.bucket(3)]),
            "a waits 3 ms for the storage input on {replica:?}"
        );
    }
    for (replica, delay) in [(None, 8), (Some(r1), 8), (Some(r2), 35)] {
        assert_eq!(
            recorded.buckets("local", b, replica),
            BTreeSet::from([scheme.bucket(delay)]),
            "b waits {delay} ms for a on {replica:?}"
        );
    }
}

/// A collection that reads two compute collections records skew, which the collections with a
/// single input do not.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn skew_needs_two_inputs() {
    let mut h = FreshnessHarness::new(true, false);
    h.instance.initialization_complete();
    let (a, b, c) = (GlobalId::User(10), GlobalId::User(11), GlobalId::User(12));
    h.add_collection_with_inputs(a, T0, &[STORAGE_INPUT]);
    h.add_collection_with_inputs(b, T0, &[STORAGE_INPUT]);
    h.add_collection_with_inputs(c, T0, &[a, b]);
    let r1 = ReplicaId::User(1);
    h.add_replica(r1);

    let config =
        mz_storage_client::freshness::recorder_config(&h.instance.dyncfg).expect("recording on");
    let tree = FreshnessTree::Leaf(FreshnessLeaf::Ingestion(STORAGE_INPUT))
        .resolve(&FixedTickers(1000))
        .expect("resolves");
    for s in 1..=120 {
        let tick = T0 + s * 1000;
        h.set_now(tick + 2);
        let frontier = Antichain::from_elem(Timestamp::from(tick + 1));
        h.storage_log.record(&tree, &frontier, tick + 2, &config);
        h.set_now(tick + 5);
        h.report(r1, a, tick + 1);
        h.set_now(tick + 9);
        h.report(r1, b, tick + 1);
        h.set_now(tick + 30);
        h.report(r1, c, tick + 1);
    }

    let recorded = h.recorded();
    let scheme = BucketScheme::new(4).expect("power of two");
    let expected = vec![
        (T0, T0 + 60_000, 60_000),
        (T0 + 60_000, T0 + 120_000, 60_000),
    ];
    for replica in [None, Some(r1)] {
        assert_eq!(
            recorded.measure_periods("skew", Some(c), replica),
            expected,
            "{replica:?}"
        );
        // Both inputs pass every needed timestamp when they show it, so there is no wait
        // between them and the slower one, `b`, decides the local delay.
        assert_eq!(
            recorded.buckets("skew", c, replica),
            BTreeSet::from([scheme.bucket(1)]),
            "{replica:?}"
        );
        assert_eq!(
            recorded.buckets("local", c, replica),
            BTreeSet::from([scheme.bucket(21)]),
            "{replica:?}"
        );
        for id in [a, b] {
            assert_eq!(
                recorded.measure_periods("skew", Some(id), replica),
                vec![],
                "{id} {replica:?}"
            );
        }
    }
}

/// An input whose tree no longer resolves counts as ready at each observation, and the
/// collection still records its own histograms and visibility for its consumers.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn unresolvable_input_tree_counts_as_ready() {
    let mut h = FreshnessHarness::new(true, false);
    h.instance.initialization_complete();
    let (a, b) = (GlobalId::User(10), GlobalId::User(11));
    let unknown = FreshnessTree::Leaf(FreshnessLeaf::Ingestion(UNKNOWN_INGESTION));
    h.add_collection_with_input_trees(a, T0, &[(STORAGE_INPUT, unknown)]);
    h.add_collection_with_inputs(b, T0, &[a]);
    let r1 = ReplicaId::User(1);
    h.add_replica(r1);
    let config =
        mz_storage_client::freshness::recorder_config(&h.instance.dyncfg).expect("recording on");
    let tree = FreshnessTree::Leaf(FreshnessLeaf::Ingestion(STORAGE_INPUT))
        .resolve(&FixedTickers(1000))
        .expect("resolves");
    for s in 1..=60 {
        let tick = T0 + s * 1000;
        h.set_now(tick + 2);
        let frontier = Antichain::from_elem(Timestamp::from(tick + 1));
        h.storage_log.record(&tree, &frontier, tick + 2, &config);
        h.set_now(tick + 5);
        h.report(r1, a, tick + 1);
        h.set_now(tick + 13);
        h.report(r1, b, tick + 1);
    }
    let recorded = h.recorded();
    let scheme = BucketScheme::new(4).expect("power of two");
    let expected = vec![(T0, T0 + 60_000, 60_000)];
    for replica in [None, Some(r1)] {
        for measure in ["end_to_end", "local"] {
            assert_eq!(
                recorded.measure_periods(measure, Some(a), replica),
                expected,
                "{measure} {replica:?}"
            );
        }
        assert_eq!(
            recorded.buckets("local", a, replica),
            BTreeSet::from([scheme.bucket(1)]),
            "{replica:?}"
        );
        // `b` reads the visibility `a` recorded.
        assert_eq!(
            recorded.buckets("local", b, replica),
            BTreeSet::from([scheme.bucket(8)]),
            "{replica:?}"
        );
    }
}

/// An input with a tree that never recorded its visibility counts as ready at each observation,
/// which records local delay 0 rather than the end-to-end latency.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn local_delay_without_input_entries_is_zero() {
    let mut h = FreshnessHarness::new(true, false);
    h.instance.initialization_complete();
    let a = GlobalId::User(10);
    h.add_collection_with_inputs(a, T0, &[STORAGE_INPUT]);
    let r1 = ReplicaId::User(1);
    h.add_replica(r1);
    for s in 1..=60 {
        h.set_now(T0 + s * 1000 + 5);
        h.report(r1, a, T0 + s * 1000 + 1);
    }
    let recorded = h.recorded();
    let scheme = BucketScheme::new(4).expect("power of two");
    for replica in [None, Some(r1)] {
        assert_eq!(
            recorded.measure_periods("local", Some(a), replica),
            vec![(T0, T0 + 60_000, 60_000)],
            "{replica:?}"
        );
        assert_eq!(
            recorded.buckets("local", a, replica),
            BTreeSet::from([scheme.bucket(0)]),
            "{replica:?}"
        );
    }
}

/// An input that completed is visible from its completion on, but a window is ready no earlier
/// than it starts, so its consumer's local delay is measured from the later of the two.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn local_delay_after_closed_input_counts_from_close_or_window_start() {
    let mut h = FreshnessHarness::new(true, false);
    h.instance.initialization_complete();
    let a = GlobalId::User(10);
    h.add_collection_with_inputs(a, T0, &[STORAGE_INPUT]);
    let r1 = ReplicaId::User(1);
    h.add_replica(r1);
    let config =
        mz_storage_client::freshness::recorder_config(&h.instance.dyncfg).expect("recording on");
    let tree = FreshnessTree::Leaf(FreshnessLeaf::Ingestion(STORAGE_INPUT))
        .resolve(&FixedTickers(1000))
        .expect("resolves");
    // The input closes 500 ms after the consumer is created.
    h.storage_log
        .record(&tree, &Antichain::new(), T0 + 500, &config);
    for s in 1..=60 {
        h.set_now(T0 + s * 1000 + 5);
        h.report(r1, a, T0 + s * 1000 + 1);
    }
    let recorded = h.recorded();
    let scheme = BucketScheme::new(4).expect("power of two");
    // The first window starts before the close, every later one after it.
    let expected = BTreeSet::from([scheme.bucket(505), scheme.bucket(1005)]);
    for replica in [None, Some(r1)] {
        assert_eq!(
            recorded.buckets("local", a, replica),
            expected,
            "{replica:?}"
        );
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn freshness_histograms_need_flag_and_writes() {
    for (recording, read_only) in [(false, false), (true, true)] {
        let mut h = FreshnessHarness::new(recording, read_only);
        h.instance.initialization_complete();
        let id = GlobalId::User(10);
        h.add_collection(id, T0);
        let r1 = ReplicaId::User(1);
        h.add_replica(r1);
        for s in 1..=120 {
            h.set_now(T0 + s * 1000 + 5);
            h.report(r1, id, T0 + s * 1000 + 1);
        }
        let recorded = h.recorded();
        assert!(
            recorded.counts.is_empty(),
            "recording={recording} read_only={read_only}: {:?}",
            recorded.counts,
        );
    }
}

/// Turning recording off discards open periods, and turning it on again starts at the wall-clock
/// time of the next observed frontier.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn freshness_recording_restarts_after_toggle() {
    let mut h = FreshnessHarness::new(true, false);
    h.instance.initialization_complete();
    let id = GlobalId::User(10);
    h.add_collection(id, T0);
    let r1 = ReplicaId::User(1);
    h.add_replica(r1);
    for s in 1..=180 {
        match s {
            31 => h.set_recording(false),
            91 => h.set_recording(true),
            _ => {}
        }
        h.set_now(T0 + s * 1000 + 5);
        h.report(r1, id, T0 + s * 1000 + 1);
    }
    // Recording restarts at the observation at T0 + 91_005, where its first period starts.
    let expected = vec![
        (T0 + 91_005, T0 + 120_000, 28_995),
        (T0 + 120_000, T0 + 180_000, 60_000),
    ];
    let recorded = h.recorded();
    assert_eq!(recorded.periods(None), expected, "global");
    assert_eq!(recorded.periods(Some(r1)), expected, "replica");
}

/// Dropping a collection emits its open period up to the last observed frontier, and its empty
/// frontier afterwards records nothing.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn freshness_recording_finishes_dropped_collections() {
    let mut h = FreshnessHarness::new(true, false);
    h.instance.initialization_complete();
    let id = GlobalId::User(10);
    h.add_collection(id, T0);
    let r1 = ReplicaId::User(1);
    h.add_replica(r1);
    for s in 1..=90 {
        h.set_now(T0 + s * 1000 + 5);
        h.report(r1, id, T0 + s * 1000 + 1);
    }
    h.instance
        .drop_collections(vec![id])
        .expect("collection exists");
    h.report_frontier(r1, id, Antichain::new());
    let expected = vec![
        (T0, T0 + 60_000, 60_000),
        (T0 + 60_000, T0 + 120_000, 30_000),
    ];
    let recorded = h.recorded();
    assert_eq!(recorded.periods(None), expected, "global");
    assert_eq!(recorded.periods(Some(r1)), expected, "replica");
}

/// A collection created with as-of `[0]`, as over a source that is still snapshotting, records
/// only the arrivals after its creation, however small its first frontier is.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn freshness_as_of_zero_starts_at_creation() {
    let mut h = FreshnessHarness::new(true, false);
    h.instance.initialization_complete();
    let id = GlobalId::User(10);
    h.add_collection(id, 0);
    let r1 = ReplicaId::User(1);
    h.add_replica(r1);
    h.set_now(T0 + 5);
    h.report(r1, id, 1);
    for s in 1..=120 {
        h.set_now(T0 + s * 1000 + 5);
        h.report(r1, id, T0 + s * 1000 + 1);
    }
    let recorded = h.recorded();
    let expected = vec![
        (T0, T0 + 60_000, 60_000),
        (T0 + 60_000, T0 + 120_000, 60_000),
    ];
    assert_eq!(recorded.periods(None), expected, "global");
    assert_eq!(recorded.periods(Some(r1)), expected, "replica");
}

/// A cluster loses its only replica for thirty days. The global recording writes nothing while
/// there is no replica and charges the catch-up of the next replica as latency, in pieces of at
/// most a day.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn freshness_replica_outage_is_charged_in_day_pieces() {
    const DAY_MS: u64 = 24 * 60 * 60 * 1000;
    let mut h = FreshnessHarness::new(true, false);
    h.instance.initialization_complete();
    let id = GlobalId::User(10);
    h.add_collection(id, T0);
    let (r1, r2) = (ReplicaId::User(1), ReplicaId::User(2));
    h.add_replica(r1);
    for s in 1..=90 {
        h.set_now(T0 + s * 1000 + 5);
        h.report(r1, id, T0 + s * 1000 + 1);
    }
    let drop_ms = T0 + 90_000;
    h.instance.remove_replica(r1).expect("replica exists");
    let before = h.recorded();
    // Removing the replica ends its recording, while the global one stays open.
    assert_eq!(
        before.periods(None),
        vec![(T0, T0 + 60_000, 60_000)],
        "global"
    );
    assert_eq!(
        before.periods(Some(r1)),
        vec![
            (T0, T0 + 60_000, 60_000),
            (T0 + 60_000, T0 + 120_000, 30_000)
        ],
        "replica"
    );

    let now = drop_ms + 30 * DAY_MS;
    h.set_now(now + 5);
    h.add_replica(r2);
    h.report(r2, id, now + 1);
    let after = h.recorded();
    assert_eq!(
        after.periods(Some(r2)),
        vec![],
        "the new replica starts now"
    );
    let periods = after.periods(None);
    // The first period continues the open one of the drop, and the pieces are contiguous and
    // hold one arrival per ms each, except for the few ms of the outage that fell out of the
    // 30 day retention while the observation was 5 ms late.
    let expired = now + 5 - 30 * DAY_MS - drop_ms;
    let mut pos = T0 + 60_000;
    for (start, end, count) in &periods {
        assert!(*start <= pos && pos < *end, "gap before ({start}, {end}]");
        let expected = match *start == T0 + 60_000 {
            true => end - pos - expired,
            false => end - pos,
        };
        assert_eq!(u64::try_from(*count).expect("positive"), expected);
        assert!(end - start <= DAY_MS);
        pos = *end;
    }
    assert_eq!(pos, now - now % 60_000);
    assert!((30..=33).contains(&periods.len()), "{periods:?}");
    // The rows are the pieces times the buckets they touch.
    assert!(after.counts.len() <= 200, "{} rows", after.counts.len());
}

/// A log collection is recorded per replica only, from the replica's creation until it goes
/// away, when its open periods are emitted.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn freshness_log_collections_record_per_replica_only() {
    let mut h = FreshnessHarness::with_log();
    h.instance.initialization_complete();
    let r1 = ReplicaId::User(1);
    h.add_replica_with_logs(r1, BTreeMap::from([(LOG_VARIANT, LOG)]));
    for s in 1..=90 {
        h.set_now(T0 + s * 1000 + 5);
        h.report(r1, LOG, T0 + s * 1000 + 1);
    }
    h.instance.remove_replica(r1).expect("replica exists");
    let recorded = h.recorded();
    assert_eq!(recorded.periods(None), vec![], "global");
    assert_eq!(
        recorded.periods(Some(r1)),
        vec![
            (T0, T0 + 60_000, 60_000),
            (T0 + 60_000, T0 + 120_000, 30_000)
        ],
        "replica"
    );
}

/// A rehydrated replica's logs start over, so its recording of a log collection ends with the
/// old replica and a new one starts at the rehydration.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn freshness_rehydration_restarts_log_collection_recordings() {
    let mut h = FreshnessHarness::with_log();
    h.instance.initialization_complete();
    let r1 = ReplicaId::User(1);
    h.add_replica_with_logs(r1, BTreeMap::from([(LOG_VARIANT, LOG)]));
    for s in 1..=90 {
        h.set_now(T0 + s * 1000 + 5);
        h.report(r1, LOG, T0 + s * 1000 + 1);
    }
    h.instance.rehydrate_replica(r1);
    h.report(r1, LOG, 1);
    for s in 91..=180 {
        h.set_now(T0 + s * 1000 + 5);
        h.report(r1, LOG, T0 + s * 1000 + 1);
    }
    let recorded = h.recorded();
    assert_eq!(recorded.periods(None), vec![], "global");
    assert_eq!(
        recorded.periods(Some(r1)),
        vec![
            (T0, T0 + 60_000, 60_000),
            (T0 + 60_000, T0 + 120_000, 30_000),
            (T0 + 90_005, T0 + 120_000, 29_995),
            (T0 + 120_000, T0 + 180_000, 60_000),
        ],
        "replica"
    );
}

/// A replica collection whose recording was dropped by switching recording off has none to keep
/// when its replica rehydrates, so it keeps the recording the new replica collection starts with.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn freshness_rehydration_keeps_a_new_recording() {
    let mut h = FreshnessHarness::new(true, false);
    h.instance.initialization_complete();
    let id = GlobalId::User(10);
    h.add_collection(id, T0);
    let r1 = ReplicaId::User(1);
    h.add_replica(r1);
    h.set_recording(false);
    h.set_recording(true);
    h.set_now(T0 + 1000);
    h.instance.rehydrate_replica(r1);
    h.set_now(T0 + 61_005);
    h.report(r1, id, T0 + 61_001);
    // The new replica collection records from its creation at the rehydration, instead of from
    // its first observation.
    let recorded = h.recorded();
    assert_eq!(
        recorded.periods(Some(r1)),
        vec![(T0 + 1000, T0 + 60_000, 59_000)],
        "replica"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn reported_lag_skips_transient_collections() {
    let mut h = FreshnessHarness::new(false, false);
    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_WALLCLOCK_FRESHNESS_REPORTING, true);
    updates.apply(&h.instance.dyncfg);
    h.instance.initialization_complete();
    let r1 = ReplicaId::User(1);
    h.add_replica(r1);
    let user = GlobalId::User(10);
    let transient = GlobalId::Transient(10);
    for id in [user, transient] {
        h.add_collection(id, T0);
        // Lag is only reported for hydrated collections, whose output frontier passed the as-of.
        let frontier = Antichain::from_elem(Timestamp::from(T0 + 1001));
        let epoch = h.instance.replicas[&r1].epoch;
        let frontiers = FrontiersResponse {
            write_frontier: Some(frontier.clone()),
            input_frontier: None,
            output_frontier: Some(frontier),
        };
        h.instance
            .handle_response((r1, epoch, ComputeResponse::Frontiers(id, frontiers)));
    }
    h.set_now(T0 + 1005);
    h.instance.refresh_wallclock_lag();

    let computed = |id| h.instance.collections[&id].assignment_bound.is_computed();
    assert!(computed(user), "user collection reports max(D, S)");
    assert!(!computed(transient), "transient collection computes no D");
    let lag_max = |id| h.instance.replicas[&r1].collections[&id].wallclock_lag_max;
    assert_eq!(lag_max(transient), None);
    assert!(
        matches!(lag_max(user), Some(WallclockLag::Millis(ms)) if ms >= 1000),
        "{:?}",
        lag_max(user),
    );
}

/// A collection and a replica that existed before a restart, whose seeds are published after the
/// first observation. Under `charge` the periods since the seeds are contiguous and complete,
/// under `hole` the recording starts at the first observation.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn restart_policy_charges_outage_or_leaves_hole() {
    let charged = vec![
        (T0 + 60_000, T0 + 120_000, 60_000),
        (T0 + 120_000, T0 + 180_000, 60_000),
    ];
    // The hole recording starts at the first observed frontier, which reflects T0 + 121_000.
    let hole = vec![(T0 + 121_000, T0 + 180_000, 59_000)];
    for (policy, expected) in [("charge", charged), ("hole", hole)] {
        let mut h = FreshnessHarness::new(true, false);
        let mut updates = ConfigUpdates::default();
        updates.add(&WALLCLOCK_FRESHNESS_RESTART_POLICY, policy);
        updates.apply(&h.instance.dyncfg);
        let id = GlobalId::User(10);
        let r1 = ReplicaId::User(1);
        h.add_replica(r1);
        h.add_collection(id, T0 + 60_000);
        h.instance.initialization_complete();

        let tick = |h: &mut FreshnessHarness, s: u64| {
            h.set_now(T0 + s * 1000 + 5);
            h.report(r1, id, T0 + s * 1000 + 1);
        };
        tick(&mut h, 121);
        for replica_id in [None, Some(r1)] {
            let seeds = [Measure::EndToEnd, Measure::Local]
                .map(|measure| ((id, replica_id, measure), T0 + 60_000));
            h.seeds.publish(replica_id.is_some(), seeds.into());
        }
        for s in 122..=180 {
            tick(&mut h, s);
        }

        let recorded = h.recorded();
        for replica in [None, Some(r1)] {
            for measure in ["end_to_end", "local"] {
                assert_eq!(
                    recorded.measure_periods(measure, Some(id), replica),
                    expected,
                    "{policy} {measure} {replica:?}"
                );
            }
        }
    }
}
