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
//! * After a new replica was created, the coordinator calls `install_introspection_subscribes` to
//!   install all defined introspection subscribes on the new replica.
//! * The coordinator calls `handle_introspection_subscribe_batch` for each response it receives
//!   from an introspection subscribe, to write received updates to their corresponding
//!   storage-managed collection.
//! * Before a replica is dropped, the coordinator calls `drop_introspection_subscribes` to drop
//!   all introspection subscribes previously installed on the replica.
//! * When a replica disconnects without being dropped (e.g. because of a crash or network
//!   failure), `handle_introspection_subscribe_batch` reacts on the corresponding error responses
//!   by reinstalling the failed introspection subscribes.

use anyhow::bail;
use derivative::Derivative;
use mz_adapter_types::dyncfgs::ENABLE_INTROSPECTION_SUBSCRIBES;
use mz_cluster_client::ReplicaId;
use mz_compute_client::controller::error::ERROR_TARGET_REPLICA_FAILED;
use mz_compute_client::protocol::response::SubscribeBatch;
use mz_controller_types::ClusterId;
use mz_ore::collections::CollectionExt;
use mz_ore::soft_panic_or_log;
use mz_repr::optimize::OverrideFrom;
use mz_repr::{Datum, GlobalId, Row};
use mz_sql::catalog::SessionCatalog;
use mz_sql::plan::{Params, Plan, SubscribePlan};
use mz_sql::session::user::{MZ_SYSTEM_ROLE_ID, RoleMetadata};
use mz_storage_client::controller::{IntrospectionType, StorageWriteOp};
use tracing::{Span, info};

use crate::coord::{
    Coordinator, IntrospectionSubscribeFinish, IntrospectionSubscribeOptimizeMir,
    IntrospectionSubscribeStage, IntrospectionSubscribeTimestampOptimizeLir, Message, PlanValidity,
    StageResult, Staged,
};
use crate::optimize::Optimize;
use crate::{AdapterError, ExecuteResponse, optimize};

// State tracked about an active introspection subscribe.
#[derive(Derivative)]
#[derivative(Debug)]
pub(super) struct IntrospectionSubscribe {
    /// The ID of the targeted cluster.
    cluster_id: ClusterId,
    /// The ID of the targeted replica.
    replica_id: ReplicaId,
    /// The spec from which this subscribe was created.
    spec: &'static SubscribeSpec,
    /// A storage write to be applied the next time the introspection subscribe produces any
    /// output.
    ///
    /// This mechanism exists to delay the deletion of previous subscribe results from the target
    /// storage collection when an introspection subscribe is reinstalled. After reinstallation it
    /// can take a while for the new subscribe dataflow to produce its snapshot and keeping the old
    /// introspection data around in the meantime makes for a better UX than removing it.
    #[derivative(Debug = "ignore")]
    deferred_write: Option<StorageWriteOp>,
}

impl IntrospectionSubscribe {
    /// Returns a `StorageWriteOp` that instructs the deletion of all data previously written by
    /// this subscribe.
    fn delete_write_op(&self) -> StorageWriteOp {
        let target_replica = self.replica_id.to_string();
        let filter = Box::new(move |row: &Row| {
            let replica_id = row.unpack_first();
            replica_id == Datum::String(&target_replica)
        });
        StorageWriteOp::Delete { filter }
    }
}

impl Coordinator {
    /// Installs introspection subscribes on all existing replicas.
    ///
    /// Meant to be invoked during coordinator bootstrapping.
    pub(super) async fn bootstrap_introspection_subscribes(&mut self) {
        let mut cluster_replicas = Vec::new();
        for cluster in self.catalog.clusters() {
            for replica in cluster.replicas() {
                cluster_replicas.push((cluster.id, replica.replica_id));
            }
        }

        for (cluster_id, replica_id) in cluster_replicas {
            self.install_introspection_subscribes(cluster_id, replica_id)
                .await;
        }
    }

    /// Installs introspection subscribes on the given replica.
    pub(super) async fn install_introspection_subscribes(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) {
        let dyncfgs = self.catalog().system_config().dyncfgs();
        if !ENABLE_INTROSPECTION_SUBSCRIBES.get(dyncfgs) {
            return;
        }

        for spec in SUBSCRIBES {
            self.install_introspection_subscribe(cluster_id, replica_id, spec)
                .await;
        }
    }

    async fn install_introspection_subscribe(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        spec: &'static SubscribeSpec,
    ) {
        let (_, id) = self.allocate_transient_id();
        info!(
            %id,
            %replica_id,
            type_ = ?spec.introspection_type,
            "installing introspection subscribe",
        );

        // Sequencing is performed asynchronously, and the target replica may be dropped before it
        // completes. To ensure the subscribe does not leak in this case, we need to already add it
        // to the coordinator state here, rather than at the end of sequencing.
        let subscribe = IntrospectionSubscribe {
            cluster_id,
            replica_id,
            spec,
            deferred_write: None,
        };
        self.introspection_subscribes.insert(id, subscribe);

        self.sequence_introspection_subscribe(id, spec, cluster_id, replica_id)
            .await;
    }

    async fn sequence_introspection_subscribe(
        &mut self,
        subscribe_id: GlobalId,
        spec: &'static SubscribeSpec,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) {
        let catalog = self.catalog().for_system_session();
        let plan = spec.to_plan(&catalog).expect("valid spec");

        let role_metadata = RoleMetadata::new(MZ_SYSTEM_ROLE_ID);
        let dependencies = plan
            .from
            .depends_on()
            .iter()
            .map(|id| self.catalog().resolve_item_id(id))
            .collect();
        let validity = PlanValidity::new(
            self.catalog.transient_revision(),
            dependencies,
            Some(cluster_id),
            Some(replica_id),
            role_metadata,
        );

        let stage = IntrospectionSubscribeStage::OptimizeMir(IntrospectionSubscribeOptimizeMir {
            validity,
            plan,
            subscribe_id,
            cluster_id,
            replica_id,
        });
        self.sequence_staged((), Span::current(), stage).await;
    }

    fn sequence_introspection_subscribe_optimize_mir(
        &self,
        stage: IntrospectionSubscribeOptimizeMir,
    ) -> Result<StageResult<Box<IntrospectionSubscribeStage>>, AdapterError> {
        let IntrospectionSubscribeOptimizeMir {
            mut validity,
            plan,
            subscribe_id,
            cluster_id,
            replica_id,
        } = stage;

        let compute_instance = self.instance_snapshot(cluster_id).expect("must exist");
        let (_, view_id) = self.allocate_transient_id();

        let vars = self.catalog().system_config();
        let overrides = self.catalog.get_cluster(cluster_id).config.features();
        let optimizer_config = optimize::OptimizerConfig::from(vars).override_from(&overrides);

        let mut optimizer = optimize::subscribe::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            view_id,
            subscribe_id,
            None,
            plan.with_snapshot,
            None,
            format!("introspection-subscribe-{subscribe_id}"),
            optimizer_config,
            self.optimizer_metrics(),
        );
        let catalog = self.owned_catalog();

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize introspection subscribe (mir)",
            move || {
                span.in_scope(|| {
                    // MIR ⇒ MIR optimization (global)
                    let global_mir_plan = optimizer.catch_unwind_optimize(plan.from)?;
                    // Add introduced indexes as validity dependencies.
                    let id_bundle = global_mir_plan.id_bundle(cluster_id);
                    let item_ids = id_bundle.iter().map(|id| catalog.resolve_item_id(&id));
                    validity.extend_dependencies(item_ids);

                    let stage = IntrospectionSubscribeStage::TimestampOptimizeLir(
                        IntrospectionSubscribeTimestampOptimizeLir {
                            validity,
                            optimizer,
                            global_mir_plan,
                            cluster_id,
                            replica_id,
                        },
                    );
                    Ok(Box::new(stage))
                })
            },
        )))
    }

    fn sequence_introspection_subscribe_timestamp_optimize_lir(
        &self,
        stage: IntrospectionSubscribeTimestampOptimizeLir,
    ) -> Result<StageResult<Box<IntrospectionSubscribeStage>>, AdapterError> {
        let IntrospectionSubscribeTimestampOptimizeLir {
            validity,
            mut optimizer,
            global_mir_plan,
            cluster_id,
            replica_id,
        } = stage;

        // Timestamp selection.
        let id_bundle = global_mir_plan.id_bundle(cluster_id);
        let read_holds = self.acquire_read_holds(&id_bundle);
        let as_of = read_holds.least_valid_read();

        let global_mir_plan = global_mir_plan.resolve(as_of);

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize introspection subscribe (lir)",
            move || {
                span.in_scope(|| {
                    // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                    let global_lir_plan =
                        optimizer.catch_unwind_optimize(global_mir_plan.clone())?;

                    let stage = IntrospectionSubscribeStage::Finish(IntrospectionSubscribeFinish {
                        validity,
                        global_lir_plan,
                        read_holds,
                        cluster_id,
                        replica_id,
                    });
                    Ok(Box::new(stage))
                })
            },
        )))
    }

    async fn sequence_introspection_subscribe_finish(
        &mut self,
        stage: IntrospectionSubscribeFinish,
    ) -> Result<StageResult<Box<IntrospectionSubscribeStage>>, AdapterError> {
        let IntrospectionSubscribeFinish {
            validity: _,
            global_lir_plan,
            read_holds,
            cluster_id,
            replica_id,
        } = stage;

        let subscribe_id = global_lir_plan.sink_id();

        // The subscribe may already have been dropped, in which case we must not install a
        // dataflow for it.
        let response = if self.introspection_subscribes.contains_key(&subscribe_id) {
            let (df_desc, _df_meta) = global_lir_plan.unapply();
            self.ship_dataflow(df_desc, cluster_id, Some(replica_id))
                .await;

            Ok(StageResult::Response(
                ExecuteResponse::CreatedIntrospectionSubscribe,
            ))
        } else {
            Err(AdapterError::internal(
                "introspection",
                "introspection subscribe has already been dropped",
            ))
        };

        drop(read_holds);
        response
    }

    /// Drops the introspection subscribes installed on the given replica.
    ///
    /// Dropping an introspection subscribe entails:
    ///  * removing it from [`Coordinator::introspection_subscribes`]
    ///  * dropping its compute collection
    ///  * retracting any rows previously omitted by it from its corresponding storage-managed
    ///    collection
    pub(super) fn drop_introspection_subscribes(&mut self, replica_id: ReplicaId) {
        let to_drop: Vec<_> = self
            .introspection_subscribes
            .iter()
            .filter(|(_, s)| s.replica_id == replica_id)
            .map(|(id, _)| *id)
            .collect();

        for id in to_drop {
            self.drop_introspection_subscribe(id);
        }
    }

    fn drop_introspection_subscribe(&mut self, id: GlobalId) {
        let Some(subscribe) = self.introspection_subscribes.remove(&id) else {
            soft_panic_or_log!("attempt to remove unknown introspection subscribe (id={id})");
            return;
        };

        info!(
            %id,
            replica_id = %subscribe.replica_id,
            type_ = ?subscribe.spec.introspection_type,
            "dropping introspection subscribe",
        );

        // This can fail if the sequencing hasn't finished yet for the subscribe. In this case,
        // `sequence_introspection_subscribe_finish` will skip installing the compute collection in
        // the first place.
        let _ = self
            .controller
            .compute
            .drop_collections(subscribe.cluster_id, vec![id]);

        self.controller.storage.update_introspection_collection(
            subscribe.spec.introspection_type,
            subscribe.delete_write_op(),
        );
    }

    async fn reinstall_introspection_subscribe(&mut self, id: GlobalId) {
        let Some(mut subscribe) = self.introspection_subscribes.remove(&id) else {
            soft_panic_or_log!("attempt to reinstall unknown introspection subscribe (id={id})");
            return;
        };

        // Note that we don't simply call `drop_introspection_subscribe` here because that would
        // cause an immediate deletion of all data previously reported by the subscribe from its
        // target storage collection. We'd like to not present empty introspection data while the
        // replica reconnects, so we want to delay the `StorageWriteOp::Delete` until then.

        let IntrospectionSubscribe {
            cluster_id,
            replica_id,
            spec,
            ..
        } = subscribe;
        let old_id = id;
        let (_, new_id) = self.allocate_transient_id();

        info!(
            %old_id, %new_id, %replica_id,
            type_ = ?subscribe.spec.introspection_type,
            "reinstalling introspection subscribe",
        );

        if let Err(error) = self
            .controller
            .compute
            .drop_collections(cluster_id, vec![old_id])
        {
            soft_panic_or_log!(
                "error dropping compute collection for introspection subscribe: {error} \
                 (id={old_id}, cluster_id={cluster_id})"
            );
        }

        // Ensure that the contents of the target storage collection are cleaned when the new
        // subscribe starts reporting data.
        subscribe.deferred_write = Some(subscribe.delete_write_op());

        self.introspection_subscribes.insert(new_id, subscribe);
        self.sequence_introspection_subscribe(new_id, spec, cluster_id, replica_id)
            .await;
    }

    /// Processes a batch returned by an introspection subscribe.
    ///
    /// Depending on the contents of the batch, this either appends received updates to the
    /// corresponding storage-managed collection, or reinstalls a disconnected subscribe.
    pub(super) async fn handle_introspection_subscribe_batch(
        &mut self,
        id: GlobalId,
        batch: SubscribeBatch,
    ) {
        let Some(subscribe) = self.introspection_subscribes.get_mut(&id) else {
            soft_panic_or_log!("updates for unknown introspection subscribe (id={id})");
            return;
        };

        let updates = match batch.updates {
            Ok(updates) if updates.is_empty() => return,
            Ok(updates) => updates,
            Err(error) if error == ERROR_TARGET_REPLICA_FAILED => {
                // The target replica disconnected, reinstall the subscribe.
                self.reinstall_introspection_subscribe(id).await;
                return;
            }
            Err(error) => {
                soft_panic_or_log!(
                    "introspection subscribe produced an error: {error} \
                     (id={id}, subscribe={subscribe:?})",
                );
                return;
            }
        };

        // Prepend the `replica_id` to each row.
        let replica_id = subscribe.replica_id.to_string();
        let mut new_updates = Vec::with_capacity(updates.len());
        let mut new_row = Row::default();
        for (_time, row, diff) in updates {
            let mut packer = new_row.packer();
            packer.push(Datum::String(&replica_id));
            packer.extend_by_row(&row);
            new_updates.push((new_row.clone(), diff));
        }

        // If we have a pending deferred write, we need to apply it _before_ the append of the new
        // rows.
        if let Some(op) = subscribe.deferred_write.take() {
            self.controller
                .storage
                .update_introspection_collection(subscribe.spec.introspection_type, op);
        }

        self.controller.storage.update_introspection_collection(
            subscribe.spec.introspection_type,
            StorageWriteOp::Append {
                updates: new_updates,
            },
        );
    }
}

impl Staged for IntrospectionSubscribeStage {
    type Ctx = ();

    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            Self::OptimizeMir(stage) => &mut stage.validity,
            Self::TimestampOptimizeLir(stage) => &mut stage.validity,
            Self::Finish(stage) => &mut stage.validity,
        }
    }

    async fn stage(
        self,
        coord: &mut Coordinator,
        _ctx: &mut (),
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        match self {
            Self::OptimizeMir(stage) => coord.sequence_introspection_subscribe_optimize_mir(stage),
            Self::TimestampOptimizeLir(stage) => {
                coord.sequence_introspection_subscribe_timestamp_optimize_lir(stage)
            }
            Self::Finish(stage) => coord.sequence_introspection_subscribe_finish(stage).await,
        }
    }

    fn message(self, _ctx: (), span: Span) -> super::Message {
        Message::IntrospectionSubscribeStageReady { span, stage: self }
    }

    fn cancel_enabled(&self) -> bool {
        false
    }
}

/// The specification for an introspection subscribe.
#[derive(Debug)]
pub(super) struct SubscribeSpec {
    /// An [`IntrospectionType`] identifying the storage-managed collection to which updates
    /// received from subscribes instantiated from this spec are written.
    introspection_type: IntrospectionType,
    /// The SQL definition of the subscribe.
    sql: &'static str,
}

impl SubscribeSpec {
    fn to_plan(&self, catalog: &dyn SessionCatalog) -> Result<SubscribePlan, anyhow::Error> {
        let parsed = mz_sql::parse::parse(self.sql)?.into_element();
        let (stmt, resolved_ids) = mz_sql::names::resolve(catalog, parsed.ast)?;
        let plan = mz_sql::plan::plan(None, catalog, stmt, &Params::empty(), &resolved_ids)?;
        match plan {
            Plan::Subscribe(plan) => Ok(plan),
            _ => bail!("unexpected plan type: {plan:?}"),
        }
    }
}

const SUBSCRIBES: &[SubscribeSpec] = &[
    SubscribeSpec {
        introspection_type: IntrospectionType::ComputeErrorCounts,
        sql: "SUBSCRIBE (
            SELECT export_id, sum(count)
            FROM mz_introspection.mz_compute_error_counts_raw
            GROUP BY export_id
        )",
    },
    SubscribeSpec {
        introspection_type: IntrospectionType::ComputeHydrationTimes,
        sql: "SUBSCRIBE (
            SELECT
                export_id,
                CASE count(*) = count(time_ns)
                    WHEN true THEN max(time_ns)
                    ELSE NULL
                END AS time_ns
            FROM mz_introspection.mz_compute_hydration_times_per_worker
            WHERE export_id NOT LIKE 't%'
            GROUP BY export_id
            OPTIONS (AGGREGATE INPUT GROUP SIZE = 1)
        )",
    },
    SubscribeSpec {
        introspection_type: IntrospectionType::ComputeOperatorHydrationStatus,
        sql: "SUBSCRIBE (
            SELECT
                export_id,
                lir_id,
                bool_and(hydrated) AS hydrated
            FROM mz_introspection.mz_compute_operator_hydration_statuses_per_worker
            GROUP BY export_id, lir_id
        )",
    },
];
