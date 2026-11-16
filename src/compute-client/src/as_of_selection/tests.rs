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
