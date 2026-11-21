// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use mz_compute_types::dataflows::{DataflowDescription, IndexDesc, IndexImport};
use mz_compute_types::sinks::ComputeSinkConnection;
use mz_compute_types::sinks::ComputeSinkDesc;
use mz_compute_types::sinks::MaterializedViewSinkConnection;
use mz_compute_types::sinks::MetricSinkConnection;
use mz_compute_types::sources::SourceInstanceArguments;
use mz_compute_types::sources::SourceInstanceDesc;
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_types::ShardId;
use mz_repr::{RelationDesc, RelationVersion, Row, SqlRelationType};
use mz_repr::{ReprRelationType, Timestamp};
use mz_storage_client::client::TimestamplessUpdateBuilder;
use mz_storage_client::controller::{CollectionDescription, StorageMetadata, StorageTxn};
use mz_storage_client::storage_collections::{CollectionFrontiers, SnapshotCursor};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::{CollectionMetadata, StorageError};
use mz_storage_types::errors::CollectionMissing;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::sources::SourceData;
use mz_storage_types::time_dependence::{TimeDependence, TimeDependenceError};
use proptest::prelude::*;

use super::*;

const SEALED: u64 = 0x5ea1ed;

fn ts_to_frontier(ts: u64) -> Antichain<Timestamp> {
    if ts == SEALED {
        Antichain::new()
    } else {
        Antichain::from_elem(ts.into())
    }
}

#[derive(Debug)]
struct StorageFrontiers(BTreeMap<GlobalId, (Antichain<Timestamp>, Antichain<Timestamp>)>);

#[async_trait]
impl StorageCollections for StorageFrontiers {
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
        let mut frontiers = Vec::with_capacity(ids.len());
        for id in ids {
            let (read, write) = self.0.get(&id).ok_or(CollectionMissing(id))?;
            frontiers.push(CollectionFrontiers {
                id,
                write_frontier: write.clone(),
                implied_capability: read.clone(),
                read_capabilities: read.clone(),
            })
        }
        Ok(frontiers)
    }

    fn active_collection_frontiers(&self) -> Vec<CollectionFrontiers> {
        unimplemented!()
    }

    fn check_exists(&self, _id: GlobalId) -> Result<(), StorageError> {
        unimplemented!()
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
        desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold>, CollectionMissing> {
        let mut holds = Vec::with_capacity(desired_holds.len());
        for id in desired_holds {
            let (read, _write) = self.0.get(&id).ok_or(CollectionMissing(id))?;
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            holds.push(ReadHold::with_channel(id, read.clone(), tx));
        }
        Ok(holds)
    }

    fn determine_time_dependence(
        &self,
        _id: GlobalId,
    ) -> Result<Option<TimeDependence>, TimeDependenceError> {
        unimplemented!()
    }

    fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        unimplemented!()
    }
}

fn dataflow(
    export_id: &str,
    input_ids: &[&str],
    storage_ids: &BTreeSet<&str>,
    metric_sink_ids: &BTreeSet<&str>,
) -> LirDataflowDescription {
    let source_imports = input_ids
        .iter()
        .filter(|s| storage_ids.contains(*s))
        .map(|s| {
            let id = s.parse().unwrap();
            let desc = SourceInstanceDesc {
                arguments: SourceInstanceArguments {
                    operators: Default::default(),
                },
                storage_metadata: Default::default(),
                typ: SqlRelationType::empty(),
            };
            (
                id,
                mz_compute_types::dataflows::SourceImport {
                    desc,
                    monotonic: Default::default(),
                    with_snapshot: true,
                    upper: Default::default(),
                },
            )
        })
        .collect();
    let index_imports = input_ids
        .iter()
        .filter(|s| !storage_ids.contains(*s))
        .map(|s| {
            let id = s.parse().unwrap();
            let import = IndexImport {
                desc: IndexDesc {
                    on_id: GlobalId::Transient(0),
                    key: Default::default(),
                },
                typ: ReprRelationType::empty(),
                monotonic: Default::default(),
                with_snapshot: true,
            };
            (id, import)
        })
        .collect();
    let index_exports = std::iter::once(export_id)
        .filter(|s| !storage_ids.contains(*s) && !metric_sink_ids.contains(*s))
        .map(|sid| {
            let id = sid.parse().unwrap();
            let desc = IndexDesc {
                on_id: GlobalId::Transient(0),
                key: Default::default(),
            };
            let typ = ReprRelationType::empty();
            (id, (desc, typ))
        })
        .collect();
    let sink_exports = std::iter::once(export_id)
        .filter(|s| storage_ids.contains(*s) || metric_sink_ids.contains(*s))
        .map(|sid| {
            let id = sid.parse().unwrap();
            let connection = if metric_sink_ids.contains(sid) {
                ComputeSinkConnection::MetricSink(MetricSinkConnection { label: sid.into() })
            } else {
                ComputeSinkConnection::MaterializedView(MaterializedViewSinkConnection {
                    value_desc: RelationDesc::empty(),
                    storage_metadata: Default::default(),
                })
            };
            let desc = ComputeSinkDesc {
                from: GlobalId::Transient(0),
                from_desc: RelationDesc::empty(),
                connection,
                with_snapshot: Default::default(),
                up_to: Default::default(),
                non_null_assertions: Default::default(),
                refresh_schedule: Default::default(),
                from_arrangement: None,
            };
            (id, desc)
        })
        .collect();

    DataflowDescription {
        source_imports,
        index_imports,
        objects_to_build: Default::default(),
        index_exports,
        sink_exports,
        as_of: None,
        until: Default::default(),
        initial_storage_as_of: Default::default(),
        refresh_schedule: Default::default(),
        debug_name: Default::default(),
        time_dependence: None,
    }
}

macro_rules! testcase {
    ($name:ident, {
        storage: { $( $storage_id:literal: ($read:expr, $write:expr), )* },
        dataflows: [ $( $export_id:literal <- $inputs:expr => $as_of:expr, )* ],
        current_time: $current_time:literal,
        $( read_policies: { $( $policy_id:literal: $policy:expr, )* }, )?
        $( read_only: $read_only:expr, )?
        $( pending_replacements: [ $( $replacement_id:literal, )* ], )?
        $( metric_sinks: [ $( $metric_sink_id:literal, )* ], )?
    }) => {
        #[mz_ore::test]
        fn $name() {
            let storage_ids = [$( $storage_id, )*].into();
            let metric_sink_ids = [$($( $metric_sink_id, )*)?].into();

            let storage_frontiers = StorageFrontiers(BTreeMap::from([
                $(
                    (
                        $storage_id.parse().unwrap(),
                        (ts_to_frontier($read), ts_to_frontier($write)),
                    ),
                )*
            ]));

            let mut dataflows = [
                $(
                    dataflow($export_id, &$inputs, &storage_ids, &metric_sink_ids),
                )*
            ];

            let read_policies = BTreeMap::from([
                $($( ($policy_id.parse().unwrap(), $policy), )*)?
            ]);

            #[allow(unused_variables)]
            let read_only = false;
            $( let read_only = $read_only; )?

            let pending_replacements = BTreeSet::from([
                $($( $replacement_id.parse().unwrap(), )*)?
            ]);

            super::run(
                &mut dataflows,
                &read_policies,
                &storage_frontiers,
                $current_time.into(),
                read_only,
                &pending_replacements,
            );

            let actual_as_ofs: Vec<_> = dataflows
                .into_iter()
                .map(|d| d.as_of.unwrap())
                .collect();
            let expected_as_ofs = [ $( ts_to_frontier($as_of), )* ];

            assert_eq!(actual_as_ofs, expected_as_ofs);
        }
    };
}

testcase!(upstream_storage_constraints, {
    storage: {
        "s1": (10, 20),
        "s2": (20, 30),
    },
    dataflows: [
        "u1" <- ["s1"]       => 10,
        "u2" <- ["s2"]       => 20,
        "u3" <- ["s1", "s2"] => 20,
        "u4" <- ["u1", "u2"] => 20,
    ],
    current_time: 0,
});

testcase!(downstream_storage_constraints, {
    storage: {
        "s1": (10, 20),
        "u3": (10, 15),
        "u4": (10, 13),
    },
    dataflows: [
        "u1" <- ["s1"] => 19,
        "u2" <- ["s1"] => 12,
        "u3" <- ["u2"] => 14,
        "u4" <- ["u2"] => 12,
    ],
    current_time: 100,
});

testcase!(warmup_constraints, {
    storage: {
        "s1": (10, 20),
        "s2": (10, 30),
        "s3": (10, 40),
        "s4": (10, 50),
    },
    dataflows: [
        "u1" <- ["s1"]       => 19,
        "u2" <- ["s2"]       => 19,
        "u3" <- ["s3"]       => 39,
        "u4" <- ["s4"]       => 39,
        "u5" <- ["u1", "u2"] => 19,
        "u6" <- ["u3", "u4"] => 39,
    ],
    current_time: 100,
});

testcase!(index_read_policy_constraints, {
    storage: {
        "s1": (10, 20),
        "u6": (10, 18),
    },
    dataflows: [
        "u1" <- ["s1"] => 15,
        "u2" <- ["s1"] => 10,
        "u3" <- ["s1"] => 13,
        "u4" <- ["s1"] => 10,
        "u5" <- []     => 95,
        "u6" <- ["s1"] => 17,
    ],
    current_time: 100,
    read_policies: {
        "u1": ReadPolicy::lag_writes_by(5.into(), 1.into()),
        "u2": ReadPolicy::lag_writes_by(15.into(), 1.into()),
        "u3": ReadPolicy::ValidFrom(Antichain::from_elem(13.into())),
        "u4": ReadPolicy::ValidFrom(Antichain::from_elem(5.into())),
        "u5": ReadPolicy::lag_writes_by(5.into(), 1.into()),
        "u6": ReadPolicy::ValidFrom(Antichain::from_elem(13.into())),
    },
});

testcase!(index_current_time_constraints, {
    storage: {
        "s1": (10, 20),
        "s2": (20, 30),
        "u4": (10, 12),
        "u5": (10, 18),
    },
    dataflows: [
        "u1" <- ["s1"] => 15,
        "u2" <- ["s2"] => 20,
        "u3" <- ["s1"] => 11,
        "u4" <- ["u3"] => 11,
        "u5" <- ["s1"] => 17,
        "u6" <- []     => 15,
    ],
    current_time: 15,
});

testcase!(sealed_storage_sink, {
    storage: {
        "s1": (10, 20),
        "u1": (10, SEALED),
    },
    dataflows: [
        "u1" <- ["s1"] => SEALED,
    ],
    current_time: 100,
});

testcase!(read_only_dropped_storage_inputs, {
    storage: {
        "s1": (10, 20),
        "s2": (SEALED, SEALED),
        "u4": (10, 20),
    },
    dataflows: [
        "u1" <- ["s1"] => 15,
        "u2" <- ["s2"] => SEALED,
        "u3" <- ["s1", "s2"] => SEALED,
        "u4" <- ["u2"] => SEALED,
    ],
    current_time: 15,
    read_only: true,
});

// A pending replacement's storage frontiers are those of its target. `u2` reads `s2`, which is
// not readable before the target's write frontier, so `u2` gets an as-of beyond it. `u3`'s
// inputs are readable there, so it gets an as-of that continues the target's output.
testcase!(pending_replacements, {
    storage: {
        "s1": (10, 30),
        "s2": (20, 30),
        "u2": (10, 15),
        "u3": (10, 15),
    },
    dataflows: [
        "u2" <- ["s2"] => 20,
        "u3" <- ["s1"] => 14,
    ],
    current_time: 100,
    pending_replacements: ["u2", "u3",],
});

// `u1` is an empty MV that never ran, so its write frontier is stuck at its read frontier and
// `u3` can't hydrate before `u1` advances. `u3` must not drag its compute input `u2` back to
// `u1`'s frontier.
testcase!(stuck_input_without_readable_data, {
    storage: {
        "s1": (10, 100),
        "u1": (10, 10),
    },
    dataflows: [
        "u1" <- ["s1"]       => 10,
        "u2" <- ["s1"]       => 99,
        "u3" <- ["u2", "u1"] => 100,
    ],
    current_time: 100,
});

// Like `stuck_input_without_readable_data`, but `u2`'s compute input `u1` is a constant index,
// which only the current-time constraint bounds.
testcase!(stuck_input_constant_compute_input, {
    storage: {
        "s1": (10, 10),
    },
    dataflows: [
        "u1" <- []           => 100,
        "u2" <- ["s1", "u1"] => 100,
    ],
    current_time: 100,
});

// `u2` is a metric sink over an MV `u1` that never ran. No constraint other than warmup bounds
// a metric sink, so it must keep a finite as-of to get installed.
testcase!(stuck_input_metric_sink, {
    storage: {
        "s1": (10, 100),
        "u1": (10, 10),
    },
    dataflows: [
        "u1" <- ["s1"] => 10,
        "u2" <- ["u1"] => 100,
    ],
    current_time: 100,
    metric_sinks: ["u2",],
});

// `u1` is an MV that ran up to 8 and then stopped. Its read hold keeps `s1` at 7, so `u3` can
// hydrate at 7 and its compute input `u2` must follow. Before the restart, `u3` held `u2` back
// to the same time.
testcase!(stuck_input_with_readable_data, {
    storage: {
        "s1": (7, 100),
        "u1": (5, 8),
    },
    dataflows: [
        "u1" <- ["s1"]       => 7,
        "u2" <- ["s1"]       => 7,
        "u3" <- ["u2", "u1"] => 7,
    ],
    current_time: 100,
});

// `u2` is an empty MV whose storage input `s2` has no readable data, so it can't hydrate. Its
// shard is readable from 10, so it must resume at 10 and hold its compute input `u1` there. This
// drag is a hard constraint, unlike the soft one `stuck_input_without_readable_data` covers.
testcase!(blocked_mv_drags_compute_input, {
    storage: {
        "s1": (0, 100),
        "s2": (10, 10),
        "u2": (10, 10),
    },
    dataflows: [
        "u1" <- ["s1"]       => 10,
        "u2" <- ["s2", "u1"] => 10,
    ],
    current_time: 100,
});

// Regression test for database-issues#9273.
testcase!(github_9273, {
    storage: {
        "s1": (10, 20),
        "u3": (14, 15),
    },
    dataflows: [
        "u1" <- ["s1"] => 14,
        "u2" <- ["u1"] => 19,
        "u3" <- ["u1"] => 14,
    ],
    current_time: 100,
    read_policies: {
        "u1": ReadPolicy::lag_writes_by(1.into(), 1.into()),
        "u2": ReadPolicy::lag_writes_by(1.into(), 1.into()),
    },
});

/// Largest timestamp used by generated instances.
const MAX_TS: u64 = 20;

/// The kind of a generated dataflow's single export.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExportKind {
    Index,
    Mv,
    MetricSink,
}

/// A dataflow in a generated instance, exporting a single collection named `id`.
#[derive(Clone, Debug)]
struct ArbDataflow {
    id: String,
    kind: ExportKind,
    inputs: Vec<String>,
    read_policy_lag: Option<u64>,
}

/// A generated as-of selection input.
#[derive(Clone, Debug)]
struct Instance {
    /// `(read, write)` frontiers of the storage collections, including MV shards.
    storage: BTreeMap<String, (u64, u64)>,
    /// Dataflows in dependency order.
    dataflows: Vec<ArbDataflow>,
    current_time: u64,
}

impl Instance {
    /// Returns the storage collections `inputs` read, directly or through indexes.
    fn transitive_storage_inputs<'a>(&'a self, inputs: &'a [String]) -> BTreeSet<&'a str> {
        let mut result = BTreeSet::new();
        let mut todo: Vec<&str> = inputs.iter().map(|s| s.as_str()).collect();
        while let Some(id) = todo.pop() {
            if self.storage.contains_key(id) {
                result.insert(id);
            } else {
                let index = self.dataflows.iter().find(|d| d.id == id).expect("known");
                todo.extend(index.inputs.iter().map(|s| s.as_str()));
            }
        }
        result
    }

    /// Returns the largest read frontier and the least write frontier of the storage collections
    /// `inputs` read, or `(0, MAX_TS)` if there are none.
    fn input_frontier_bounds(&self, inputs: &[String]) -> (u64, u64) {
        let mut max_since = 0;
        let mut min_upper = MAX_TS;
        for id in self.transitive_storage_inputs(inputs) {
            let (since, upper) = self.storage[id];
            max_since = max_since.max(since);
            min_upper = min_upper.min(upper);
        }
        (max_since, min_upper)
    }

    /// Runs as-of selection on all dataflows except `skip` and returns the selected as-ofs.
    fn run(&self, skip: Option<&str>) -> BTreeMap<String, Antichain<Timestamp>> {
        let storage_ids: BTreeSet<&str> = self.storage.keys().map(|s| s.as_str()).collect();
        let metric_sink_ids: BTreeSet<&str> = self
            .dataflows
            .iter()
            .filter(|d| d.kind == ExportKind::MetricSink)
            .map(|d| d.id.as_str())
            .collect();
        let storage_frontiers = StorageFrontiers(
            self.storage
                .iter()
                .map(|(id, (read, write))| {
                    (
                        id.parse().unwrap(),
                        (ts_to_frontier(*read), ts_to_frontier(*write)),
                    )
                })
                .collect(),
        );
        let exports: Vec<_> = self
            .dataflows
            .iter()
            .filter(|d| Some(d.id.as_str()) != skip)
            .collect();
        let mut dataflows: Vec<_> = exports
            .iter()
            .map(|d| {
                let inputs: Vec<&str> = d.inputs.iter().map(|s| s.as_str()).collect();
                dataflow(&d.id, &inputs, &storage_ids, &metric_sink_ids)
            })
            .collect();
        let read_policies = exports
            .iter()
            .filter_map(|d| {
                let lag = d.read_policy_lag?;
                let policy = ReadPolicy::lag_writes_by(lag.into(), 1.into());
                Some((d.id.parse().unwrap(), policy))
            })
            .collect();

        super::run(
            &mut dataflows,
            &read_policies,
            &storage_frontiers,
            self.current_time.into(),
            false,
            &BTreeSet::new(),
        );

        dataflows
            .into_iter()
            .map(|df| {
                let id = df.export_ids().into_element().to_string();
                (id, df.as_of.unwrap())
            })
            .collect()
    }
}

/// Generates instances that resemble states reachable before a restart.
///
/// Sources `s*` get arbitrary `since <= upper`. Dataflows `u*` are generated in dependency order
/// and read a random subset of the sources and earlier dataflows. An earlier index is a compute
/// input, an earlier MV a storage input. Nothing reads a metric sink. MV shard frontiers are derived from the frontiers of the
/// MV's transitive storage inputs, encoding what the compute controller maintains:
///
/// * The MV's read holds keep each transitive storage input's since at or below the time the MV
///   can resume from, which is `upper - 1` for a non-empty MV and `since` for an empty one.
/// * A non-empty MV's upper is at most the least upper of its transitive storage inputs, because
///   the MV can't have written times its inputs haven't.
/// * An MV whose inputs don't permit a non-empty shard is empty, with `since == upper` at or
///   beyond the largest since of its transitive storage inputs, as chosen when it was created.
///
/// These make every instance feasible: assigning each dataflow the largest since of its
/// transitive storage inputs satisfies all hard constraints.
fn arb_instance() -> impl Strategy<Value = Instance> {
    let source = (0..=MAX_TS, 0..=MAX_TS);
    let kind = proptest::sample::select(vec![
        ExportKind::Index,
        ExportKind::Mv,
        ExportKind::MetricSink,
    ]);
    let dataflow = (
        kind,
        any::<u8>(),
        0..=MAX_TS,
        0..=MAX_TS,
        any::<bool>(),
        proptest::option::of(0..5u64),
    );
    (
        proptest::collection::vec(source, 1..=3),
        proptest::collection::vec(dataflow, 1..=5),
        0..=MAX_TS,
    )
        .prop_map(|(sources, dataflows, current_time)| {
            let mut inst = Instance {
                storage: BTreeMap::new(),
                dataflows: Vec::new(),
                current_time,
            };
            let mut candidates = Vec::new();
            for (i, (a, b)) in sources.into_iter().enumerate() {
                let id = format!("s{}", i + 1);
                inst.storage.insert(id.clone(), (a.min(b), a.max(b)));
                candidates.push(id);
            }
            for (i, (kind, input_mask, a, b, empty, lag)) in dataflows.into_iter().enumerate() {
                let id = format!("u{}", i + 1);
                let inputs: Vec<String> = candidates
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| (input_mask >> j) & 1 == 1)
                    .map(|(_, c)| c.clone())
                    .collect();
                if kind == ExportKind::Mv {
                    let (max_since, min_upper) = inst.input_frontier_bounds(&inputs);
                    let frontiers = if !empty && max_since < min_upper {
                        let upper = b.clamp(max_since + 1, min_upper);
                        (a.min(upper - 1), upper)
                    } else {
                        let t = a.max(max_since);
                        (t, t)
                    };
                    inst.storage.insert(id.clone(), frontiers);
                }
                inst.dataflows.push(ArbDataflow {
                    id: id.clone(),
                    kind,
                    inputs,
                    read_policy_lag: if kind == ExportKind::Index { lag } else { None },
                });
                if kind != ExportKind::MetricSink {
                    candidates.push(id);
                }
            }
            inst
        })
}

/// As-of selection satisfies all hard constraints on feasible instances.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn proptest_as_of_selection_safety() {
    proptest!(|(inst in arb_instance())| {
        let as_ofs = inst.run(None);
        for d in &inst.dataflows {
            let as_of = &as_ofs[&d.id];
            for input in &d.inputs {
                match inst.storage.get(input) {
                    Some((since, _)) => prop_assert!(
                        PartialOrder::less_equal(&ts_to_frontier(*since), as_of),
                        "{} as-of {as_of:?} below since {since} of storage input {input}",
                        d.id,
                    ),
                    None => prop_assert!(
                        PartialOrder::less_equal(&as_ofs[input], as_of),
                        "{} as-of {as_of:?} below as-of {:?} of compute input {input}",
                        d.id,
                        as_ofs[input],
                    ),
                }
            }
            if d.kind == ExportKind::Mv {
                let (read, write) = inst.storage[&d.id];
                let max_as_of = if write <= read { read } else { write - 1 };
                prop_assert!(
                    PartialOrder::less_equal(as_of, &ts_to_frontier(max_as_of)),
                    "{} as-of {as_of:?} skips times of its shard ({read}, {write})",
                    d.id,
                );
            }
        }
    });
}

/// Every dataflow that reads a storage collection gets a non-empty as-of, so it gets installed.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn proptest_as_of_selection_liveness() {
    proptest!(|(inst in arb_instance())| {
        let as_ofs = inst.run(None);
        for d in &inst.dataflows {
            if !inst.transitive_storage_inputs(&d.inputs).is_empty() {
                prop_assert!(!as_ofs[&d.id].is_empty(), "{} has an empty as-of", d.id);
            }
        }
    });
}

/// Removing a blocked index or metric sink without a read policy and without dependents doesn't
/// change the as-ofs of the other dataflows.
///
/// A dataflow is blocked when no time is both readable and complete in all its transitive storage
/// inputs, so it can't hydrate at any as-of. Such a dataflow must not drag the as-ofs of its
/// compute inputs back to its own (CPU-336). A blocked MV can, see
/// `blocked_mv_drags_compute_input`.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn proptest_as_of_selection_no_drag_from_blocked() {
    proptest!(|(inst in arb_instance())| {
        let as_ofs = inst.run(None);
        for d in &inst.dataflows {
            let (max_since, min_upper) = inst.input_frontier_bounds(&d.inputs);
            let has_storage_inputs = !inst.transitive_storage_inputs(&d.inputs).is_empty();
            let blocked = has_storage_inputs && min_upper <= max_since;
            let has_dependents = inst.dataflows.iter().any(|o| o.inputs.contains(&d.id));
            let is_mv = d.kind == ExportKind::Mv;
            if is_mv || !blocked || d.read_policy_lag.is_some() || has_dependents {
                continue;
            }
            for (id, as_of) in inst.run(Some(&d.id)) {
                prop_assert!(
                    as_of == as_ofs[&id],
                    "removing blocked {} changed the as-of of {id} from {:?} to {as_of:?}",
                    d.id,
                    as_ofs[&id],
                );
            }
        }
    });
}
