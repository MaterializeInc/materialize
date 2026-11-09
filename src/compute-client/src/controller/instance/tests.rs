// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the compute instance controller.

use std::collections::BTreeSet;

use mz_compute_types::dyncfgs::{ENABLE_COLUMN_PAGED_BATCHER, ENABLE_MZ_JOIN_CORE};
use mz_dyncfg::{ConfigSet, ConfigUpdates, ConfigVal};
use mz_persist_types::PersistLocation;
use mz_repr::{GlobalId, Timestamp};
use timely::progress::Antichain;
use tokio::sync::mpsc;

use crate::protocol::command::{ComputeCommand, InstanceConfig};

use super::{
    CollectionReadiness, Instance, ReplicaCollectionIntrospection, ReplicaCollectionState,
    ReplicaId, classify_collection_readiness,
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

/// Tests of the staleness histograms the instance records.
mod freshness {
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
    use mz_controller_types::dyncfgs::ENABLE_WALLCLOCK_FRESHNESS_RECORDING;
    use mz_dyncfg::ConfigUpdates;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::NowFn;
    use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
    use mz_persist_types::{PersistLocation, ShardId};
    use mz_repr::{Datum, GlobalId, RelationDesc, RelationVersion, Row, Timestamp};
    use mz_storage_client::client::TimestamplessUpdateBuilder;
    use mz_storage_client::controller::{
        CollectionDescription, IntrospectionType, StorageMetadata, StorageTxn,
    };
    use mz_storage_client::storage_collections::{
        CollectionFrontiers, SnapshotCursor, StorageCollections,
    };
    use mz_storage_types::StorageDiff;
    use mz_storage_types::controller::{CollectionMetadata, StorageError};
    use mz_storage_types::errors::CollectionMissing;
    use mz_storage_types::freshness::BucketScheme;
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
    use crate::protocol::response::{ComputeResponse, FrontiersResponse};

    use super::super::{Instance, ReplicaId, SharedCollectionState};

    /// A `StorageCollections` that knows no collections.
    #[derive(Debug)]
    struct NoStorage;

    #[async_trait]
    impl StorageCollections for NoStorage {
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

        fn collection_metadata(
            &self,
            _id: GlobalId,
        ) -> Result<CollectionMetadata, CollectionMissing> {
            unimplemented!()
        }

        fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)> {
            unimplemented!()
        }

        fn collections_frontiers(
            &self,
            ids: Vec<GlobalId>,
        ) -> Result<Vec<CollectionFrontiers>, CollectionMissing> {
            match ids.first() {
                Some(id) => Err(CollectionMissing(*id)),
                None => Ok(Vec::new()),
            }
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

        fn determine_freshness_dependence(
            &self,
            _id: GlobalId,
        ) -> Result<Option<TimeDependence>, TimeDependenceError> {
            unimplemented!()
        }

        fn set_freshness_dependence(&self, _id: GlobalId, _dependence: Option<TimeDependence>) {
            unimplemented!()
        }

        fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
            unimplemented!()
        }
    }

    /// A minute-aligned wall-clock time in ms, the harness's boot time.
    const T0: u64 = 1_800_000_000_000;

    /// The log collection of a [`FreshnessHarness::with_log`].
    const LOG: GlobalId = GlobalId::System(1000);
    const LOG_VARIANT: LogVariant = LogVariant::Timely(TimelyLog::Operates);

    /// A sent staleness histogram row: collection, replica (`None` for global), and period
    /// `[start, end)`.
    type RecordedKey = (String, Option<String>, u64, u64);

    /// Sent staleness histogram counts, summed over the buckets of each period, and the
    /// counts per bucket.
    #[derive(Debug, Default)]
    struct Recorded {
        counts: BTreeMap<RecordedKey, i64>,
        buckets: BTreeMap<RecordedKey, BTreeMap<i32, i64>>,
    }

    impl Recorded {
        /// The `(start, end, count)` of every period recorded for `id` on `replica`, or
        /// globally.
        fn periods(&self, id: GlobalId, replica: Option<ReplicaId>) -> Vec<(u64, u64, i64)> {
            let id = id.to_string();
            let replica = replica.map(|r| r.to_string());
            self.counts
                .iter()
                .filter(|((c, r, _, _), _)| *c == id && *r == replica)
                .map(|((_, _, start, end), count)| (*start, *end, *count))
                .collect()
        }

        /// The largest bucket recorded for `id` on `replica`, or globally, in any period.
        fn max_bucket(&self, id: GlobalId, replica: Option<ReplicaId>) -> Option<i32> {
            let id = id.to_string();
            let replica = replica.map(|r| r.to_string());
            self.buckets
                .iter()
                .filter(|((c, r, _, _), _)| *c == id && *r == replica)
                .filter_map(|(_, buckets)| buckets.keys().max().copied())
                .max()
        }
    }

    /// A recording instance booted at [`T0`], whose clock reads `clock`.
    struct FreshnessHarness {
        instance: Instance,
        clock: Arc<AtomicU64>,
        introspection_rx: mpsc::UnboundedReceiver<IntrospectionUpdates>,
        _response_rx: mpsc::UnboundedReceiver<ComputeControllerResponse>,
    }

    impl FreshnessHarness {
        fn new() -> Self {
            Self::build(false, Vec::new())
        }

        /// A harness with the log collection [`LOG`].
        fn with_log() -> Self {
            let as_of = Antichain::from_elem(Timestamp::MIN);
            let log = (LOG_VARIANT, LOG, SharedCollectionState::new(as_of));
            Self::build(false, vec![log])
        }

        fn build(
            read_only: bool,
            logs: Vec<(LogVariant, GlobalId, SharedCollectionState)>,
        ) -> Self {
            let clock = Arc::new(AtomicU64::new(T0));
            let now = {
                let clock = Arc::clone(&clock);
                NowFn::from(move || clock.load(Ordering::SeqCst))
            };
            let dyncfg = mz_dyncfgs::all_dyncfgs();
            let mut updates = ConfigUpdates::default();
            updates.add(&ENABLE_WALLCLOCK_FRESHNESS_RECORDING, true);
            updates.apply(&dyncfg);

            let registry = MetricsRegistry::new();
            let metrics =
                ComputeControllerMetrics::new(&registry, ControllerMetrics::new(&registry))
                    .for_instance(ComputeInstanceId::User(1));
            // The instance task is not running, so no commands arrive.
            let (_command_tx, command_rx) = mpsc::unbounded_channel();
            let (response_tx, response_rx) = mpsc::unbounded_channel();
            let (introspection_tx, introspection_rx) = mpsc::unbounded_channel();
            let read_hold_tx: ChangeTx = Arc::new(|_, _| Ok(()));

            let instance = Instance::new(
                &DUMMY_BUILD_INFO,
                Arc::new(NoStorage),
                PersistLocation::new_in_mem(),
                logs,
                metrics,
                now.clone(),
                Timestamp::from(T0),
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
                introspection_rx,
                _response_rx: response_rx,
            }
        }

        fn set_now(&self, now_ms: u64) {
            self.clock.store(now_ms, Ordering::SeqCst);
        }

        fn add_replica(&mut self, id: ReplicaId) {
            self.add_replica_with_logs(id, BTreeMap::new());
        }

        /// Adds a replica that maintains the log collections `index_logs`.
        fn add_replica_with_logs(
            &mut self,
            id: ReplicaId,
            index_logs: BTreeMap<LogVariant, GlobalId>,
        ) {
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

        /// Adds a collection that follows wall-clock time.
        fn add_collection(&mut self, id: GlobalId, as_of: u64) {
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
                Some(TimeDependence::default()),
            );
        }

        /// Delivers a write frontier report of `replica` for collection `id`.
        fn report(&mut self, replica: ReplicaId, id: GlobalId, write_frontier: u64) {
            let epoch = self.instance.replicas[&replica].epoch;
            let frontiers = FrontiersResponse {
                write_frontier: Some(Antichain::from_elem(Timestamp::from(write_frontier))),
                input_frontier: None,
                output_frontier: None,
            };
            self.instance.handle_response((
                replica,
                epoch,
                ComputeResponse::Frontiers(id, frontiers),
            ));
        }

        /// For each second `s` in `seconds`, advances the clock to `T0 + s * 1000 + 5`, and has
        /// each of `replicas` report each of `ids` at `T0 + s * 1000 + 1`.
        fn run(
            &mut self,
            seconds: std::ops::RangeInclusive<u64>,
            replicas: &[ReplicaId],
            ids: &[GlobalId],
        ) {
            for s in seconds {
                self.set_now(T0 + s * 1000 + 5);
                for replica in replicas {
                    for id in ids {
                        self.report(*replica, *id, T0 + s * 1000 + 1);
                    }
                }
            }
        }

        /// Drains the staleness histogram updates sent so far.
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
                        Datum::TimestampTz(t) => {
                            u64::try_from(t.timestamp_millis()).expect("positive")
                        }
                        other => panic!("expected a timestamp, got {other:?}"),
                    };
                    let (replica, rest) = match has_replica {
                        true => (Some(datums[3].unwrap_str().to_string()), &datums[4..]),
                        false => (None, &datums[3..]),
                    };
                    assert_eq!(rest[0].unwrap_str(), "end_to_end");
                    assert_eq!(rest[1], Datum::UInt32(4));
                    assert_eq!(rest[3].unwrap_map().iter().count(), 0, "no workload class");
                    let id = datums[2].unwrap_str().to_string();
                    let key = (id, replica, ms(datums[0]), ms(datums[1]));
                    *recorded
                        .buckets
                        .entry(key.clone())
                        .or_default()
                        .entry(rest[2].unwrap_int32())
                        .or_default() += diff.into_inner();
                    *recorded.counts.entry(key).or_insert(0) += diff.into_inner();
                }
            }
            recorded
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn global_and_replica_rows_per_period() {
        let mut h = FreshnessHarness::new();
        h.instance.initialization_complete();
        let id = GlobalId::User(10);
        h.add_collection(id, T0);
        let (r1, r2) = (ReplicaId::User(1), ReplicaId::User(2));
        h.add_replica(r1);
        h.add_replica(r2);

        h.run(1..=120, &[r1, r2], &[id]);

        let recorded = h.recorded();
        let expected = vec![
            (T0, T0 + 60_000, 60_000),
            (T0 + 60_000, T0 + 120_000, 60_000),
        ];
        for replica in [None, Some(r1), Some(r2)] {
            assert_eq!(recorded.periods(id, replica), expected, "{replica:?}");
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn log_collection_has_no_global_rows() {
        let mut h = FreshnessHarness::with_log();
        h.instance.initialization_complete();
        let r1 = ReplicaId::User(1);
        h.add_replica_with_logs(r1, BTreeMap::from([(LOG_VARIANT, LOG)]));

        h.run(1..=60, &[r1], &[LOG]);

        let recorded = h.recorded();
        assert_eq!(recorded.periods(LOG, None), vec![], "global");
        assert_eq!(
            recorded.periods(LOG, Some(r1)),
            vec![(T0, T0 + 60_000, 60_000)],
            "replica"
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn drop_emits_partial_period_once() {
        let mut h = FreshnessHarness::new();
        h.instance.initialization_complete();
        let id = GlobalId::User(10);
        h.add_collection(id, T0);
        let r1 = ReplicaId::User(1);
        h.add_replica(r1);

        h.run(1..=30, &[r1], &[id]);
        h.set_now(T0 + 30_500);
        h.instance.drop_collections(vec![id]).expect("exists");
        // The replica keeps reporting until it has dropped the dataflow.
        h.run(31..=120, &[r1], &[id]);
        h.set_now(T0 + 180_000);
        h.instance.tick_freshness();

        let recorded = h.recorded();
        for replica in [None, Some(r1)] {
            assert_eq!(
                recorded.periods(id, replica),
                vec![(T0, T0 + 30_500, 30_500)],
                "{replica:?}"
            );
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn replica_removal_emits_partial_period() {
        let mut h = FreshnessHarness::new();
        h.instance.initialization_complete();
        let id = GlobalId::User(10);
        h.add_collection(id, T0);
        let (r1, r2) = (ReplicaId::User(1), ReplicaId::User(2));
        h.add_replica(r1);
        h.add_replica(r2);

        h.run(1..=30, &[r1, r2], &[id]);
        h.set_now(T0 + 30_500);
        h.instance.remove_replica(r2).expect("exists");
        h.run(31..=60, &[r1], &[id]);
        h.set_now(T0 + 120_000);
        h.instance.tick_freshness();

        let recorded = h.recorded();
        assert_eq!(
            recorded.periods(id, Some(r2)),
            vec![(T0, T0 + 30_500, 30_500)],
            "removed replica"
        );
        let expected = vec![
            (T0, T0 + 60_000, 60_000),
            (T0 + 60_000, T0 + 120_000, 60_000),
        ];
        for replica in [None, Some(r1)] {
            assert_eq!(recorded.periods(id, replica), expected, "{replica:?}");
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn rehydration_keeps_non_log_recordings() {
        let mut h = FreshnessHarness::with_log();
        h.instance.initialization_complete();
        let id = GlobalId::User(10);
        h.add_collection(id, T0);
        let r1 = ReplicaId::User(1);
        h.add_replica_with_logs(r1, BTreeMap::from([(LOG_VARIANT, LOG)]));

        h.run(1..=30, &[r1], &[id, LOG]);
        h.set_now(T0 + 30_500);
        h.instance.rehydrate_replica(r1);
        h.run(31..=59, &[r1], &[id, LOG]);

        let recorded = h.recorded();
        assert_eq!(
            recorded.periods(LOG, Some(r1)),
            vec![(T0, T0 + 30_500, 30_500)],
            "the log's recording ends at the rehydration"
        );
        assert_eq!(recorded.periods(id, Some(r1)), vec![], "period still open");

        h.run(60..=60, &[r1], &[id, LOG]);
        let recorded = h.recorded();
        assert_eq!(
            recorded.periods(id, Some(r1)),
            vec![(T0, T0 + 60_000, 60_000)],
            "the collection's recording continues"
        );
        assert_eq!(
            recorded.periods(LOG, Some(r1)),
            vec![(T0 + 30_500, T0 + 60_000, 29_500)],
            "the log's new recording starts at the rehydration"
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn rehydration_measures_against_reset_frontier() {
        let mut h = FreshnessHarness::new();
        h.instance.initialization_complete();
        let id = GlobalId::User(10);
        h.add_collection(id, T0);
        let r1 = ReplicaId::User(1);
        h.add_replica(r1);

        h.run(1..=30, &[r1], &[id]);
        let before = h.recorded();
        // The rehydrated replica restarts from the collection's read frontier, which stays at
        // `T0` under the harness's read policy, and reports nothing before the period ends.
        h.set_now(T0 + 30_500);
        h.instance.rehydrate_replica(r1);
        h.set_now(T0 + 60_000);
        h.instance.tick_freshness();

        let recorded = h.recorded();
        assert_eq!(before.counts, BTreeMap::new(), "period still open");
        assert_eq!(
            recorded.periods(id, Some(r1)),
            vec![(T0, T0 + 60_000, 60_000)]
        );
        let scheme = BucketScheme::new(4).expect("power of two");
        // Against the frontier before the rehydration, the worst case would be about 30 s.
        let reset = scheme.bucket(Duration::from_secs(50));
        let max = recorded.max_bucket(id, Some(r1)).expect("rows");
        assert!(max >= reset, "max bucket {max} below {reset}");
        // The global frontier does not regress.
        let global = recorded.max_bucket(id, None).expect("rows");
        assert!(global < reset, "global max bucket {global}");
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn replica_added_after_drop_records_nothing() {
        let mut h = FreshnessHarness::new();
        h.instance.initialization_complete();
        let id = GlobalId::User(10);
        h.add_collection(id, T0);

        // The harness's read holds never release, so the collection outlives the drop.
        h.set_now(T0 + 1_000);
        h.instance.drop_collections(vec![id]).expect("exists");
        let r1 = ReplicaId::User(1);
        h.add_replica(r1);
        h.run(2..=60, &[r1], &[id]);
        h.set_now(T0 + 120_000);
        h.instance.tick_freshness();

        let recorded = h.recorded();
        assert_eq!(recorded.periods(id, Some(r1)), vec![], "replica");
        assert_eq!(
            recorded.periods(id, None),
            vec![(T0, T0 + 1_000, 1_000)],
            "global"
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn restart_starts_at_boot() {
        let mut h = FreshnessHarness::new();
        let (a, b) = (GlobalId::User(10), GlobalId::User(11));
        let r1 = ReplicaId::User(1);
        // Bootstrap takes 10 s, and installs `a` and the replica at its end.
        h.set_now(T0 + 10_000);
        h.add_collection(a, T0);
        h.add_replica(r1);
        h.instance.initialization_complete();
        h.set_now(T0 + 20_000);
        h.add_collection(b, T0 + 20_000);

        h.run(21..=60, &[r1], &[a, b]);

        let recorded = h.recorded();
        for replica in [None, Some(r1)] {
            assert_eq!(
                recorded.periods(a, replica),
                vec![(T0, T0 + 60_000, 60_000)],
                "a {replica:?}"
            );
            assert_eq!(
                recorded.periods(b, replica),
                vec![(T0 + 20_000, T0 + 60_000, 40_000)],
                "b {replica:?}"
            );
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn read_only_records_nothing() {
        let mut h = FreshnessHarness::build(true, Vec::new());
        h.instance.initialization_complete();
        let id = GlobalId::User(10);
        h.add_collection(id, T0);
        let r1 = ReplicaId::User(1);
        h.add_replica(r1);

        h.run(1..=120, &[r1], &[id]);
        h.instance.drop_collections(vec![id]).expect("exists");
        h.instance.tick_freshness();

        assert_eq!(h.recorded().counts, BTreeMap::new());
    }
}
