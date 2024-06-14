// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Support for unified compute introspection.
//!
//! Unified compute introspection is the process of collecting introspection data exported by
//! individual replicas through their logging indexes and then writing that data, tagged with the
//! respective replica ID, to "unified" storage collections. These storage collections then allow
//! querying introspection data across all replicas and regardless of the health of individual
//! replicas.
//!
//! # Lifecycle of Introspection Subscribes
//!
//! * When the controller adds a replica ([`Instance::add_replica`]) it calls
//!   [`UnifiedIntrospection::init_subscribes_for_replica`] to initialize a new set of
//!   introspection subscribes for the new replica. [`UnifiedIntrospection`] initializes tracking
//!   for the new subscribes and returns to the controller a set of [`DataflowDescription`]s to
//!   install.
//! * Whenever the controller receives a response for an introspection subscribe, it forwards the
//!   [`SubscribeBatch`] to [`UnifiedIntrospection::report_subscribe_updates`].
//!   [`UnifiedIntrospection`] sends these updates through its `introspection_tx`, to get them
//!   durably recorded.
//! * When the controller removes a replica ([`Instance::remove_replica`]) it cancels all
//!   replica-targeted subscribes. As part of doing so it calls
//!   [`UnifiedIntrospection::drop_subscribe`] for each introspection subscribe.
//!   [`UnifiedIntrospection`] removes tracking for the dropped subscribe and sends a deletion
//!   request through its `introspection_tx`, to remove all updates previously recorded for this
//!   subscribe.
//!
//! Subscribe errors are unexpected for introspection subscribes, except for
//! [`ERROR_TARGET_REPLICA_FAILED`], which is produced by [`Instance::remove_replica`].

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mz_compute_types::dataflows::{BuildDesc, DataflowDescription, IndexDesc, IndexImport};
use mz_compute_types::plan::reduce::{AccumulablePlan, KeyValPlan, ReducePlan};
use mz_compute_types::plan::{AvailableCollections, GetPlan, LirId, Plan};
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection};
use mz_expr::{
    permutation_for_arrangement, AggregateExpr, AggregateFunc, Id, MapFilterProject, MirScalarExpr,
};
use mz_ore::collections::CollectionExt;
use mz_ore::id_gen::IdGen;
use mz_ore::soft_panic_or_log;
use mz_repr::fixed_length::ToDatumIter;
use mz_repr::global_id::TransientIdGen;
use mz_repr::{Datum, GlobalId, RelationDesc, RelationType, Row, ScalarType};
use mz_storage_client::controller::IntrospectionType;
use timely::progress::Timestamp;
use tracing::info;

use crate::controller::instance::ERROR_TARGET_REPLICA_FAILED;
use crate::controller::{IntrospectionUpdates, ReplicaId};
use crate::logging::{ComputeLog, LogVariant};
use crate::protocol::response::SubscribeBatch;

/// Managment state for unified introspection.
#[derive(Debug)]
pub(super) struct UnifiedIntrospection {
    /// The tracked introspection subscribes.
    subscribes: BTreeMap<GlobalId, IntrospectionSubscribe>,
    /// Types of logging indexes and their assigned `GlobalId`s.
    log_indexes: BTreeMap<LogVariant, GlobalId>,
    /// Sender for introspection updates collected by introspection subscribes.
    #[allow(dead_code)] // TODO(#26730)
    introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    /// Generator for transient `GlobalId`s.
    id_gen: Arc<TransientIdGen>,
}

impl UnifiedIntrospection {
    pub fn new(
        log_indexes: BTreeMap<LogVariant, GlobalId>,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
        id_gen: Arc<TransientIdGen>,
    ) -> Self {
        Self {
            subscribes: Default::default(),
            log_indexes,
            introspection_tx,
            id_gen,
        }
    }

    /// Returns whether the identified subscribe is a tracked introspection subscribe.
    pub fn tracks_subscribe(&self, id: GlobalId) -> bool {
        self.subscribes.contains_key(&id)
    }

    /// Initializes introspection subscribe tracking for the given replica.
    ///
    /// Returns the set of subscribe dataflows that should be installed on the replica and whose
    /// results should be reported back via `absorb_subscribe_updates`.
    pub fn init_subscribes_for_replica<T: Timestamp>(
        &mut self,
        replica_id: ReplicaId,
    ) -> Vec<(GlobalId, DataflowDescription<Plan<T>, (), T>)> {
        vec![self.init_subscribe(
            replica_id,
            IntrospectionType::ComputeErrorCounts,
            build_subscribe_error_counts,
        )]
    }

    fn init_subscribe<T, F>(
        &mut self,
        replica_id: ReplicaId,
        introspection_type: IntrospectionType,
        build: F,
    ) -> (GlobalId, DataflowDescription<Plan<T>, (), T>)
    where
        F: Fn(SubscribeBuilder) -> DataflowDescription<Plan<T>, (), T>,
    {
        let sink_id = self.id_gen.allocate_id();
        let view_id = self.id_gen.allocate_id();
        let builder = SubscribeBuilder::new(&self.log_indexes, sink_id, view_id);
        let dataflow = build(builder);

        let subscribe = IntrospectionSubscribe {
            replica_id: replica_id.to_string(),
            introspection_type,
        };
        info!(%sink_id, ?subscribe, "initialized introspection subscribe");
        self.subscribes.insert(sink_id, subscribe);

        (sink_id, dataflow)
    }

    /// Drop an introspection subscribe.
    pub fn drop_subscribe(&mut self, id: GlobalId) {
        let Some(subscribe) = self.subscribes.remove(&id) else {
            soft_panic_or_log!("attempt to drop unknown introspection subscribe (id={id})");
            return;
        };

        let _filter = Box::new(|row: &Row| {
            let replica_id = row.unpack_first();
            replica_id == Datum::String(&subscribe.replica_id)
        });
        // TODO(#26730) send introspection deletions
        //self.introspection_tx.send((
        //    subscribe.introspection_type,
        //    WriteCommand::Delete { filter },
        //));

        info!(%id, ?subscribe, "dropped introspection subscribe");
    }

    /// Report updates for an introspection subscribe.
    pub fn report_subscribe_updates<T>(&mut self, id: GlobalId, batch: SubscribeBatch<T>) {
        let Some(subscribe) = self.subscribes.get(&id) else {
            soft_panic_or_log!("updates for unknown introspection subscribe (id={id})");
            return;
        };

        let updates = match batch.updates {
            Ok(updates) => updates,
            // Errors caused by the target replica disconnecting are expected.
            Err(error) if error == ERROR_TARGET_REPLICA_FAILED => return,
            // All other errors are unexpected.
            Err(error) => {
                soft_panic_or_log!(
                    "introspection subscribe produced an error: {error} \
                     (id={id}, subscribe={subscribe:?})",
                );
                return;
            }
        };

        // Prepend the `replica_id` to each row.
        let _updates: Vec<_> = updates
            .into_iter()
            .map(|(_time, row, diff)| {
                let datums = std::iter::once(Datum::String(&subscribe.replica_id))
                    .chain(row.to_datum_iter());
                let row = Row::pack(datums);
                (row, diff)
            })
            .collect();

        // TODO(#26730) send introspection updates
        //self.introspection_tx.send((
        //    subscribe.introspection_type,
        //    WriteCommand::Append { updates },
        //));
    }
}

/// State tracked about an active introspection subscribe.
#[derive(Debug)]
struct IntrospectionSubscribe {
    /// The ID of the targeted replica.
    ///
    /// Stored as a string to make packing into `Row`s more efficient.
    replica_id: String,
    /// The introspection type specifying the storage-managed collection to receive subscribe
    /// updates.
    #[allow(dead_code)] // TODO(#26730)
    introspection_type: IntrospectionType,
}

/// A helper for building introspection subscribe dataflows.
struct SubscribeBuilder<'a> {
    /// Mapping from log variants to the IDs of their indexes.
    index_ids: &'a BTreeMap<LogVariant, GlobalId>,
    /// The ID used for the dataflow's sink export.
    sink_id: GlobalId,
    /// The ID used for the internal view that connects to the sink.
    view_id: GlobalId,
    /// A generator for [`LirId`]s.
    lir_id_gen: IdGen,
    /// Logging indexes that have been imported through `import_log`.
    used_indexes: BTreeSet<LogVariant>,
}

impl<'a> SubscribeBuilder<'a> {
    /// Creates a new `SubscribeBuilder` using the provided IDs.
    fn new(
        index_ids: &'a BTreeMap<LogVariant, GlobalId>,
        sink_id: GlobalId,
        view_id: GlobalId,
    ) -> Self {
        Self {
            index_ids,
            sink_id,
            view_id,
            lir_id_gen: Default::default(),
            used_indexes: Default::default(),
        }
    }

    /// Allocates a new [`LirId`].
    fn allocate_lir_id(&mut self) -> LirId {
        self.lir_id_gen.allocate_id()
    }

    /// Returns the key of the given index for the given [`LogVariant`].
    fn log_key(log: LogVariant) -> Vec<MirScalarExpr> {
        log.index_by()
            .into_iter()
            .map(MirScalarExpr::Column)
            .collect()
    }

    /// Returns the relation type of the index for the given [`LogVariant`].
    fn log_typ(log: LogVariant) -> RelationType {
        log.desc().typ().clone()
    }

    /// Imports a logging index into the builder state, returns the corresponding `Get` plan.
    fn import_log<T>(&mut self, log: LogVariant, mfp: MapFilterProject) -> Plan<T> {
        self.used_indexes.insert(log);

        let index_id = self.index_ids[&log];
        let key = Self::log_key(log);
        let column_types = Self::log_typ(log).column_types;
        let arity = column_types.len();
        let (permutation, thinning) = permutation_for_arrangement(&key, arity);

        Plan::Get {
            id: Id::Global(index_id),
            keys: AvailableCollections::new_arranged(
                vec![(key.clone(), permutation, thinning)],
                Some(column_types),
            ),
            plan: GetPlan::Arrangement(key, None, mfp),
            lir_id: self.allocate_lir_id(),
        }
    }

    /// Transforms the given `plan` into a `DataflowDescription` for an introspection subscribe.
    fn dataflow_description<T: Timestamp>(
        &self,
        plan: Plan<T>,
        desc: RelationDesc,
    ) -> DataflowDescription<Plan<T>, (), T> {
        let mut index_imports = BTreeMap::new();
        for log in &self.used_indexes {
            let index_id = self.index_ids[log];
            let import = IndexImport {
                desc: IndexDesc {
                    on_id: index_id,
                    key: Self::log_key(*log),
                },
                typ: Self::log_typ(*log),
                monotonic: false,
            };
            index_imports.insert(index_id, import);
        }

        let sink_desc = ComputeSinkDesc {
            from: self.view_id,
            from_desc: desc,
            connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection {}),
            with_snapshot: true,
            up_to: Default::default(),
            non_null_assertions: Default::default(),
            refresh_schedule: None,
        };

        DataflowDescription {
            source_imports: Default::default(),
            index_imports,
            objects_to_build: vec![BuildDesc {
                id: self.view_id,
                plan,
            }],
            index_exports: Default::default(),
            sink_exports: [(self.sink_id, sink_desc)].into(),
            as_of: None,
            until: Default::default(),
            initial_storage_as_of: None,
            refresh_schedule: None,
            debug_name: format!("introspection-subscribe-{}", self.sink_id),
        }
    }
}

/// Builds a dataflow for the `ComputeErrorCounts` subscribe.
///
/// The return dataflow corresponds to this SQL query:
///
/// ```sql
/// SELECT export_id, sum(count)
/// FROM <ComputeLog::ErrorCount>
/// GROUP BY export_id
/// ```
fn build_subscribe_error_counts<T: Timestamp>(
    mut builder: SubscribeBuilder,
) -> DataflowDescription<Plan<T>, (), T> {
    let desc = RelationDesc::empty()
        .with_column("export_id", ScalarType::String.nullable(false))
        .with_column("count", ScalarType::Int64.nullable(false))
        .with_key(vec![0]);

    // Import `ComputeLog::ErrorCount`, project away the `worker_id`.
    let error_count = builder.import_log(
        LogVariant::Compute(ComputeLog::ErrorCount),
        MapFilterProject::new(3).project([0, 2]),
    );

    // Group by `export_id`, sum up `count`s.
    let group_key = [MirScalarExpr::Column(0)];
    let aggregates = [AggregateExpr {
        func: AggregateFunc::SumInt64,
        expr: MirScalarExpr::Column(1),
        distinct: false,
    }];
    let reduce = Plan::Reduce {
        input: Box::new(error_count),
        key_val_plan: KeyValPlan::new(2, &group_key, &aggregates, None),
        plan: ReducePlan::Accumulable(AccumulablePlan {
            full_aggrs: aggregates.to_vec(),
            simple_aggrs: vec![(0, 0, aggregates.into_element())],
            distinct_aggrs: Default::default(),
        }),
        input_key: None,
        mfp_after: MapFilterProject::new(2),
        lir_id: builder.allocate_lir_id(),
    };

    builder.dataflow_description(reduce, desc)
}
