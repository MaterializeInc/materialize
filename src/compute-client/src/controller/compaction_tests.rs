// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use mz_build_info::DUMMY_BUILD_INFO;
use mz_cluster_client::WallclockLagFn;
use mz_cluster_client::metrics::ControllerMetrics;
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::dataflows::{DataflowDescription, IndexDesc, IndexImport};
use mz_compute_types::plan::LirRelationExpr;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, MetricSinkConnection};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{GlobalId, RelationDesc, RelationVersion, ReprRelationType, Row, Timestamp};
use mz_storage_client::client::TimestamplessUpdateBuilder;
use mz_storage_client::controller::{CollectionDescription, StorageMetadata, StorageTxn};
use mz_storage_client::storage_collections::{
    CollectionFrontiers, SnapshotCursor, StorageCollections,
};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::{CollectionMetadata, StorageError};
use mz_storage_types::errors::CollectionMissing;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sources::SourceData;
use mz_storage_types::time_dependence::{TimeDependence, TimeDependenceError};
use timely::progress::Antichain;
use tokio::sync::mpsc;

use super::Instance;
use crate::controller::ComputeController;
use crate::controller::error::{CompactionBoundError, DataflowCreationError};
use crate::logging::{LogVariant, TimelyLog};
use crate::protocol::command::ComputeCommand;

const INSTANCE: ComputeInstanceId = ComputeInstanceId::User(1);
const INPUT: GlobalId = GlobalId::System(1);
const OUTPUT: GlobalId = GlobalId::User(1);
const BASE: GlobalId = GlobalId::User(100);

fn frontier(time: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(time.into())
}

fn controller() -> ComputeController {
    let registry = MetricsRegistry::new();
    ComputeController::new(
        &DUMMY_BUILD_INFO,
        Arc::new(EmptyStorage),
        false,
        false,
        &registry,
        PersistLocation::new_in_mem(),
        ControllerMetrics::new(&registry),
        SYSTEM_TIME.clone(),
        WallclockLagFn::new(SYSTEM_TIME.clone()),
    )
}

fn create_instance(controller: &mut ComputeController) {
    controller
        .create_instance(
            INSTANCE,
            BTreeMap::from([(LogVariant::Timely(TimelyLog::Operates), INPUT)]),
            None,
        )
        .unwrap();
}

fn dataflow(as_of: u64, write_only: bool) -> DataflowDescription<LirRelationExpr> {
    let mut dataflow = DataflowDescription::new("compaction bound test".into());
    dataflow.as_of = Some(frontier(as_of));
    let desc = IndexDesc {
        on_id: BASE,
        key: Vec::new(),
    };
    dataflow.index_imports.insert(
        INPUT,
        IndexImport {
            desc: desc.clone(),
            typ: ReprRelationType::empty(),
            monotonic: false,
            with_snapshot: true,
        },
    );
    if write_only {
        dataflow.sink_exports.insert(
            OUTPUT,
            ComputeSinkDesc {
                from: BASE,
                from_desc: RelationDesc::empty(),
                connection: ComputeSinkConnection::MetricSink(MetricSinkConnection {
                    label: OUTPUT.to_string(),
                }),
                with_snapshot: true,
                up_to: Antichain::new(),
                non_null_assertions: Vec::new(),
                refresh_schedule: None,
            },
        );
    } else {
        dataflow
            .index_exports
            .insert(OUTPUT, (desc, ReprRelationType::empty()));
    }
    dataflow
}

// Read hold changes enqueue more instance commands. Drain them before observing the contract,
// without relying on which task wins a scheduling race against a barrier command.
fn drain(instance: &mut Instance) {
    while let Ok(command) = instance.command_rx.try_recv() {
        command(instance);
    }
}

async fn observe(controller: &ComputeController, expected: Antichain<Timestamp>) {
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(move |instance| {
            drain(instance);
            assert_eq!(
                instance.collection(INPUT).unwrap().read_frontier(),
                expected
            );
            assert_eq!(
                instance.collection(OUTPUT).unwrap().read_frontier(),
                expected
            );
            for id in [INPUT, OUTPUT] {
                // Replay folds compaction into CreateDataflow's as_of and removes completed
                // dataflows. Check the commanded frontier independently of that representation.
                let commanded = instance
                    .history
                    .iter()
                    .filter_map(|cmd| match cmd {
                        ComputeCommand::AllowCompaction { id: cid, frontier } if *cid == id => {
                            Some(frontier)
                        }
                        ComputeCommand::CreateDataflow(dataflow)
                            if dataflow.export_ids().any(|export| export == id) =>
                        {
                            dataflow.as_of.as_ref()
                        }
                        _ => None,
                    })
                    .last()
                    .cloned()
                    .unwrap_or_else(Antichain::new);
                assert_eq!(commanded, expected);
            }
        })
        .await;
}

#[mz_ore::test(tokio::test)]
async fn compaction_permission_caps_inputs_and_commands_without_overriding_holds() {
    let mut controller = controller();
    controller
        .apply_compaction_bound(OUTPUT, frontier(5))
        .unwrap();
    create_instance(&mut controller);
    controller
        .create_dataflow(INSTANCE, dataflow(0, false), None)
        .unwrap();

    // The storage dependency observes the same cap as the actual compute import.
    let (storage_tx, mut storage_rx) = mpsc::unbounded_channel();
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(move |instance| {
            let output = instance.collection_mut(OUTPUT).unwrap();
            output.storage_dependencies.insert(
                GlobalId::User(200),
                ReadHold::with_channel(GlobalId::User(200), frontier(0), storage_tx),
            );
            output.warmup_read_hold.try_downgrade(frontier(15)).unwrap();
            instance
                .collection_mut(INPUT)
                .unwrap()
                .warmup_read_hold
                .release();
            instance
                .set_read_policy(vec![
                    (INPUT, ReadPolicy::ValidFrom(Antichain::new())),
                    (OUTPUT, ReadPolicy::ValidFrom(frontier(20))),
                ])
                .unwrap();
            drain(instance);
        })
        .await;
    observe(&controller, frontier(5)).await;
    assert_eq!(
        controller.take_compaction_bound_proposals(&BTreeSet::new())[&OUTPUT],
        frontier(15)
    );
    let (_, mut changes) = storage_rx.try_recv().unwrap();
    assert_eq!(
        changes.drain().collect::<Vec<_>>(),
        vec![(0.into(), -1), (5.into(), 1)]
    );
    assert!(storage_rx.try_recv().is_err());

    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(|instance| {
            instance
                .collection_mut(OUTPUT)
                .unwrap()
                .warmup_read_hold
                .try_downgrade(frontier(30))
                .unwrap();
            drain(instance);
        })
        .await;
    assert_eq!(
        controller.take_compaction_bound_proposals(&BTreeSet::new())[&OUTPUT],
        frontier(20)
    );
    let mut reader = controller.acquire_read_hold(INSTANCE, OUTPUT).unwrap();
    reader.try_downgrade(frontier(7)).unwrap();
    for time in [10, 10, 6] {
        controller
            .apply_compaction_bound(OUTPUT, frontier(time))
            .unwrap();
    }
    observe(&controller, frontier(7)).await;
    assert_eq!(
        controller.take_compaction_bound_proposals(&BTreeSet::new())[&OUTPUT],
        frontier(7)
    );
    reader.release();
    observe(&controller, frontier(10)).await;
    controller
        .apply_compaction_bound(OUTPUT, frontier(25))
        .unwrap();
    observe(&controller, frontier(20)).await;

    let reader = controller.acquire_read_hold(INSTANCE, OUTPUT).unwrap();
    controller.drop_collections(INSTANCE, vec![OUTPUT]).unwrap();
    observe(&controller, frontier(20)).await;
    assert!(
        !controller
            .take_compaction_bound_proposals(&BTreeSet::from([OUTPUT]))
            .contains_key(&OUTPUT)
    );
    drop(reader);
    observe(&controller, Antichain::new()).await;
    controller.drop_instance(INSTANCE);
    assert!(controller.staged_compaction_bounds.is_empty());
}

#[mz_ore::test(tokio::test)]
async fn compaction_bounds_are_staged_before_creation_and_never_adopt_live_exports() {
    let mut controller = controller();
    for time in [5, 5, 3] {
        controller
            .apply_compaction_bound(INPUT, frontier(time))
            .unwrap();
        controller
            .apply_compaction_bound(OUTPUT, frontier(time))
            .unwrap();
    }
    create_instance(&mut controller);
    assert_eq!(
        controller.take_compaction_bound_proposals(&BTreeSet::new())[&INPUT],
        frontier(0)
    );
    let reader = controller.acquire_read_hold(INSTANCE, INPUT).unwrap();
    assert_eq!(reader.since(), &frontier(0));
    assert!(matches!(
        controller.create_dataflow(INSTANCE, dataflow(6, false), None),
        Err(DataflowCreationError::CompactionBoundViolation(OUTPUT))
    ));
    assert_eq!(controller.staged_compaction_bounds[&OUTPUT], frontier(5));
    assert!(controller.collection_frontiers(OUTPUT, None).is_err());
    controller
        .apply_compaction_bound(OUTPUT, frontier(6))
        .unwrap();
    controller
        .create_dataflow(INSTANCE, dataflow(6, false), None)
        .unwrap();
    assert_eq!(
        controller.take_compaction_bound_proposals(&BTreeSet::new())[&OUTPUT],
        frontier(6)
    );
    drop(reader);
    controller.drop_instance(INSTANCE);
    assert!(
        controller
            .take_compaction_bound_proposals(&BTreeSet::new())
            .is_empty()
    );
    assert!(controller.staged_compaction_bounds.is_empty());

    create_instance(&mut controller);
    assert!(matches!(
        controller.apply_compaction_bound(INPUT, frontier(8)),
        Err(CompactionBoundError::UngovernedCollection(INPUT))
    ));
    controller
        .apply_compaction_bound(OUTPUT, frontier(1))
        .unwrap();
    controller
        .create_dataflow(INSTANCE, dataflow(9, true), None)
        .unwrap();
    assert!(
        controller
            .take_compaction_bound_proposals(&BTreeSet::from([INPUT, OUTPUT]))
            .is_empty()
    );
    assert!(controller.staged_compaction_bounds.is_empty());
    assert!(matches!(
        controller.apply_compaction_bound(OUTPUT, frontier(10)),
        Err(CompactionBoundError::WriteOnlyCollection(OUTPUT))
    ));
    controller.drop_collections(INSTANCE, vec![OUTPUT]).unwrap();
    assert!(controller.staged_compaction_bounds.is_empty());
    controller.drop_instance(INSTANCE);
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn compaction_proposals_observe_permission_counts_atomically() {
    let mut controller = controller();
    controller
        .apply_compaction_bound(INPUT, frontier(0))
        .unwrap();
    create_instance(&mut controller);
    let mut reader = controller.acquire_read_hold(INSTANCE, INPUT).unwrap();
    reader.try_downgrade(frontier(1000)).unwrap();
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(|instance| {
            instance
                .collection_mut(INPUT)
                .unwrap()
                .warmup_read_hold
                .try_downgrade(frontier(3000))
                .unwrap();
            instance
                .set_read_policy(vec![(INPUT, ReadPolicy::ValidFrom(frontier(2000)))])
                .unwrap();
            drain(instance);
        })
        .await;

    for time in 1..=1500 {
        controller
            .apply_compaction_bound(INPUT, frontier(time))
            .unwrap();
        drop(reader.clone());
        assert_eq!(
            controller.take_compaction_bound_proposals(&BTreeSet::from([INPUT]))[&INPUT],
            frontier(1000)
        );
    }
    controller
        .apply_compaction_bound(INPUT, Antichain::new())
        .unwrap();
    controller
        .apply_compaction_bound(INPUT, frontier(1))
        .unwrap();
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(|instance| {
            drain(instance);
            let collection = instance.collection(INPUT).unwrap();
            assert_eq!(collection.read_frontier(), frontier(1000));
            assert_eq!(collection.shared.compaction_bound(), Some(Antichain::new()));
        })
        .await;
    reader.release();
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(drain)
        .await;
    assert_eq!(
        controller.take_compaction_bound_proposals(&BTreeSet::new())[&INPUT],
        frontier(2000)
    );
    controller.drop_instance(INSTANCE);
}

#[mz_ore::test(tokio::test)]
async fn permanent_exports_are_governed_without_saved_bounds() {
    let mut controller = controller();
    controller.catalog_read_protection_enabled = true;
    create_instance(&mut controller);
    controller
        .create_dataflow(INSTANCE, dataflow(7, false), None)
        .unwrap();
    let proposals = controller.take_compaction_bound_proposals(&BTreeSet::new());
    assert_eq!(proposals[&INPUT], frontier(0));
    assert_eq!(proposals[&OUTPUT], frontier(7));
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(|instance| {
            for (id, time) in [(INPUT, 0), (OUTPUT, 7)] {
                let collection = instance.collection_mut(id).unwrap();
                collection.warmup_read_hold.release();
                assert_eq!(collection.shared.compaction_bound(), Some(frontier(time)));
            }
            instance
                .set_read_policy(vec![
                    (INPUT, ReadPolicy::ValidFrom(frontier(30))),
                    (OUTPUT, ReadPolicy::ValidFrom(frontier(30))),
                ])
                .unwrap();
            drain(instance);
            assert_eq!(
                instance.collection(OUTPUT).unwrap().read_frontier(),
                frontier(7)
            );
        })
        .await;
    controller
        .apply_compaction_bound(OUTPUT, frontier(3))
        .unwrap();
    let reader = controller.acquire_read_hold(INSTANCE, OUTPUT).unwrap();
    controller
        .apply_compaction_bound(OUTPUT, frontier(20))
        .unwrap();
    assert_eq!(reader.since(), &frontier(7));
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(|instance| {
            drain(instance);
            let collection = instance.collection(OUTPUT).unwrap();
            assert_eq!(collection.shared.compaction_bound(), Some(frontier(20)));
            assert_eq!(collection.read_frontier(), frontier(7));
        })
        .await;
    drop(reader);
    controller.drop_instance(INSTANCE);

    create_instance(&mut controller);
    let mut transient = dataflow(7, false);
    let export = transient.index_exports.remove(&OUTPUT).unwrap();
    transient
        .index_exports
        .insert(GlobalId::Transient(1), export);
    assert!(matches!(
        controller.apply_compaction_bound(GlobalId::Transient(1), frontier(10)),
        Err(CompactionBoundError::UngovernedCollection(
            GlobalId::Transient(1)
        ))
    ));
    controller
        .create_dataflow(INSTANCE, transient, None)
        .unwrap();
    controller
        .create_dataflow(INSTANCE, dataflow(7, true), None)
        .unwrap();
    assert_eq!(
        controller
            .take_compaction_bound_proposals(&BTreeSet::from([
                INPUT,
                OUTPUT,
                GlobalId::Transient(1),
            ]))
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![INPUT]
    );
    controller.drop_instance(INSTANCE);
}

#[mz_ore::test(tokio::test)]
async fn replacement_installs_only_at_actual_readability() {
    let mut controller = controller();
    controller.catalog_read_protection_enabled = true;
    create_instance(&mut controller);
    controller
        .apply_compaction_bound(INPUT, frontier(10))
        .unwrap();
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(|instance| {
            instance
                .collection_mut(INPUT)
                .unwrap()
                .warmup_read_hold
                .release();
            instance
                .set_read_policy(vec![(INPUT, ReadPolicy::ValidFrom(frontier(10)))])
                .unwrap();
            drain(instance);
            assert_eq!(
                instance.collection(INPUT).unwrap().read_frontier(),
                frontier(10)
            );
        })
        .await;
    controller
        .apply_compaction_bound(OUTPUT, frontier(5))
        .unwrap();
    assert!(matches!(
        controller.create_dataflow(INSTANCE, dataflow(11, false), None),
        Err(DataflowCreationError::CompactionBoundViolation(OUTPUT))
    ));
    assert_eq!(controller.staged_compaction_bounds[&OUTPUT], frontier(5));
    controller
        .create_dataflow(INSTANCE, dataflow(10, false), None)
        .unwrap();
    controller
        .apply_compaction_bound(OUTPUT, frontier(5))
        .unwrap();
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(|instance| {
            drain(instance);
            let collection = instance.collection(OUTPUT).unwrap();
            assert_eq!(collection.shared.compaction_bound(), Some(frontier(10)));
            assert_eq!(collection.read_frontier(), frontier(10));
        })
        .await;
    assert_eq!(
        controller.take_compaction_bound_proposals(&BTreeSet::new())[&OUTPUT],
        frontier(10)
    );
    controller.drop_instance(INSTANCE);
}

#[mz_ore::test(tokio::test)]
async fn compaction_proposals_coalesce_and_resample_without_unchanged_redelivery() {
    let mut controller = controller();
    controller.catalog_read_protection_enabled = true;
    create_instance(&mut controller);
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(|instance| {
            instance
                .collection_mut(INPUT)
                .unwrap()
                .warmup_read_hold
                .release();
            for time in [10, 20, 30] {
                instance
                    .set_read_policy(vec![(INPUT, ReadPolicy::ValidFrom(frontier(time)))])
                    .unwrap();
            }
            drain(instance);
            assert_eq!(
                instance.collection(INPUT).unwrap().read_frontier(),
                frontier(0)
            );
        })
        .await;
    let expected = BTreeMap::from([(INPUT, frontier(30))]);
    assert_eq!(
        controller.take_compaction_bound_proposals(&BTreeSet::new()),
        expected
    );
    assert!(
        controller
            .take_compaction_bound_proposals(&BTreeSet::new())
            .is_empty()
    );
    assert_eq!(
        controller.take_compaction_bound_proposals(&BTreeSet::from([INPUT, OUTPUT])),
        expected
    );
    assert!(
        controller
            .take_compaction_bound_proposals(&BTreeSet::new())
            .is_empty()
    );

    let mut reader = controller.acquire_read_hold(INSTANCE, INPUT).unwrap();
    assert_eq!(
        controller.take_compaction_bound_proposals(&BTreeSet::new()),
        BTreeMap::from([(INPUT, frontier(0))])
    );
    reader.try_downgrade(frontier(5)).unwrap();
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(drain)
        .await;
    assert_eq!(
        controller.take_compaction_bound_proposals(&BTreeSet::new()),
        BTreeMap::from([(INPUT, frontier(5))])
    );
    drop(reader);
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(drain)
        .await;
    assert_eq!(
        controller.take_compaction_bound_proposals(&BTreeSet::new()),
        expected
    );
    assert!(
        controller
            .take_compaction_bound_proposals(&BTreeSet::new())
            .is_empty()
    );
    controller.drop_instance(INSTANCE);
    assert!(
        controller
            .take_compaction_bound_proposals(&BTreeSet::from([INPUT]))
            .is_empty()
    );
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn compaction_proposals_do_not_lose_concurrent_mutations() {
    let mut controller = controller();
    controller.catalog_read_protection_enabled = true;
    create_instance(&mut controller);
    controller
        .instance(INSTANCE)
        .unwrap()
        .call_sync(|instance| {
            instance
                .collection_mut(INPUT)
                .unwrap()
                .warmup_read_hold
                .release();
            drain(instance);
        })
        .await;
    controller.take_compaction_bound_proposals(&BTreeSet::new());

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let updater_barrier = Arc::clone(&barrier);
    controller
        .instance(INSTANCE)
        .unwrap()
        .call(move |instance| {
            for time in 1..=100 {
                updater_barrier.wait();
                instance
                    .set_read_policy(vec![(INPUT, ReadPolicy::ValidFrom(frontier(time)))])
                    .unwrap();
                drain(instance);
                updater_barrier.wait();
            }
        });
    let mut observed = Vec::new();
    for time in 1..=100 {
        barrier.wait();
        let mut proposals = controller.take_compaction_bound_proposals(&BTreeSet::new());
        barrier.wait();
        // A racing change must appear in the racing sample or remain dirty afterward.
        proposals.extend(controller.take_compaction_bound_proposals(&BTreeSet::new()));
        observed.push((time, proposals));
    }
    for (time, proposals) in observed {
        assert_eq!(proposals, BTreeMap::from([(INPUT, frontier(time))]));
    }
    assert!(
        controller
            .take_compaction_bound_proposals(&BTreeSet::new())
            .is_empty()
    );
    controller.drop_instance(INSTANCE);
}

#[derive(Debug)]
struct EmptyStorage;

#[async_trait]
impl StorageCollections for EmptyStorage {
    async fn initialize_state(
        &self,
        _txn: &mut (dyn StorageTxn + Send),
        _init_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError> {
        unimplemented!("storage is not used by these compute tests")
    }

    fn update_parameters(&self, _config_params: StorageParameters) {
        unimplemented!("storage is not used by these compute tests")
    }

    fn collection_metadata(&self, _id: GlobalId) -> Result<CollectionMetadata, CollectionMissing> {
        unimplemented!("storage is not used by these compute tests")
    }

    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)> {
        unimplemented!("storage is not used by these compute tests")
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
        unimplemented!("storage is not used by these compute tests")
    }

    fn take_read_protection_frontiers(
        &self,
        _additional_ids: &BTreeSet<GlobalId>,
    ) -> BTreeMap<GlobalId, (CollectionFrontiers, Antichain<Timestamp>)> {
        unimplemented!("storage is not used by these compute tests")
    }

    fn check_exists(&self, _id: GlobalId) -> Result<(), StorageError> {
        unimplemented!("storage is not used by these compute tests")
    }

    async fn snapshot_stats(
        &self,
        _id: GlobalId,
        _as_of: Antichain<Timestamp>,
    ) -> Result<SnapshotStats, StorageError> {
        unimplemented!("storage is not used by these compute tests")
    }

    async fn snapshot_parts_stats(
        &self,
        _id: GlobalId,
        _as_of: Antichain<Timestamp>,
    ) -> BoxFuture<'static, Result<SnapshotPartsStats, StorageError>> {
        unimplemented!("storage is not used by these compute tests")
    }

    fn snapshot(
        &self,
        _id: GlobalId,
        _as_of: Timestamp,
    ) -> BoxFuture<'static, Result<Vec<(Row, StorageDiff)>, StorageError>> {
        unimplemented!("storage is not used by these compute tests")
    }

    async fn snapshot_latest(&self, _id: GlobalId) -> Result<Vec<Row>, StorageError> {
        unimplemented!("storage is not used by these compute tests")
    }

    fn snapshot_cursor(
        &self,
        _id: GlobalId,
        _as_of: Timestamp,
    ) -> BoxFuture<'static, Result<SnapshotCursor, StorageError>> {
        unimplemented!("storage is not used by these compute tests")
    }

    fn snapshot_and_stream(
        &self,
        _id: GlobalId,
        _as_of: Timestamp,
    ) -> BoxFuture<
        'static,
        Result<BoxStream<'static, (SourceData, Timestamp, StorageDiff)>, StorageError>,
    > {
        unimplemented!("storage is not used by these compute tests")
    }

    fn create_update_builder(
        &self,
        _id: GlobalId,
    ) -> BoxFuture<
        'static,
        Result<TimestamplessUpdateBuilder<SourceData, (), StorageDiff>, StorageError>,
    > {
        unimplemented!("storage is not used by these compute tests")
    }

    async fn prepare_state(
        &self,
        _txn: &mut (dyn StorageTxn + Send),
        _ids_to_add: BTreeSet<GlobalId>,
        _ids_to_drop: BTreeSet<GlobalId>,
        _ids_to_register: BTreeMap<GlobalId, ShardId>,
        _live_collection_ids: &BTreeSet<GlobalId>,
    ) -> Result<(), StorageError> {
        unimplemented!("storage is not used by these compute tests")
    }

    async fn create_collections_for_bootstrap(
        &self,
        _storage_metadata: &StorageMetadata,
        _register_ts: Option<Timestamp>,
        _collections: Vec<(GlobalId, CollectionDescription)>,
        _migrated_storage_collections: &BTreeSet<GlobalId>,
    ) -> Result<(), StorageError> {
        unimplemented!("storage is not used by these compute tests")
    }

    async fn alter_table_desc(
        &self,
        _storage_metadata: &StorageMetadata,
        _existing_collection: GlobalId,
        _new_collection: GlobalId,
        _new_desc: RelationDesc,
        _expected_version: RelationVersion,
    ) -> Result<(), StorageError> {
        unimplemented!("storage is not used by these compute tests")
    }

    fn drop_collections_unvalidated(
        &self,
        _storage_metadata: &StorageMetadata,
        _identifiers: Vec<GlobalId>,
    ) {
        unimplemented!("storage is not used by these compute tests")
    }

    fn set_read_policies(&self, _policies: Vec<(GlobalId, ReadPolicy)>) {
        unimplemented!("storage is not used by these compute tests")
    }

    fn apply_compaction_bounds(
        &self,
        _bounds: BTreeMap<GlobalId, Antichain<Timestamp>>,
    ) -> Result<(), StorageError> {
        unimplemented!("storage is not used by these compute tests")
    }

    fn compaction_bound(&self, id: GlobalId) -> Result<Option<Antichain<Timestamp>>, StorageError> {
        Err(StorageError::IdentifierMissing(id))
    }

    fn acquire_read_holds(
        &self,
        desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold>, CollectionMissing> {
        assert!(desired_holds.is_empty());
        Ok(Vec::new())
    }

    fn determine_time_dependence(
        &self,
        _id: GlobalId,
    ) -> Result<Option<TimeDependence>, TimeDependenceError> {
        unimplemented!("storage is not used by these compute tests")
    }

    fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        unimplemented!("storage is not used by these compute tests")
    }
}
