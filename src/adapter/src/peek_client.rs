// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use differential_dataflow::consolidation::consolidate;
use mz_compute_client::controller::error::CollectionMissing;
use mz_compute_client::protocol::command::PeekTarget;
use mz_compute_client::protocol::response::PeekResponse;
use mz_compute_types::ComputeInstanceId;
use mz_expr::row::RowCollection;
use mz_ore::cast::CastFrom;
use mz_repr::Timestamp;
use mz_repr::global_id::TransientIdGen;
use mz_repr::{RelationDesc, Row};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_storage_types::sources::Timeline;
use mz_timestamp_oracle::TimestampOracle;
use prometheus::Histogram;
use timely::progress::Antichain;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::command::Command;
use crate::coord;
use crate::coord::Coordinator;
use crate::coord::peek::FastPathPlan;
use crate::{AdapterError, Client, CollectionIdBundle, ReadHolds};

/// Storage collections trait alias we need to consult for since/frontiers.
pub type StorageCollectionsHandle = Arc<
    dyn mz_storage_client::storage_collections::StorageCollections<Timestamp = Timestamp>
        + Send
        + Sync,
>;

/// Clients needed for peek sequencing in the Adapter Frontend.
#[derive(Debug)]
pub struct PeekClient {
    coordinator_client: Client,
    /// Channels to talk to each compute Instance task directly. Lazily populated.
    compute_instances:
        BTreeMap<ComputeInstanceId, mz_compute_client::controller::instance::Client<Timestamp>>,
    /// Handle to storage collections for reading frontiers and policies.
    pub storage_collections: StorageCollectionsHandle,
    /// A generator for transient `GlobalId`s, shared with Coordinator.
    pub transient_id_gen: Arc<TransientIdGen>,
    pub optimizer_metrics: OptimizerMetrics,
    /// Per-timeline oracles from the coordinator. Lazily populated.
    oracles: BTreeMap<Timeline, Arc<dyn TimestampOracle<Timestamp> + Send + Sync>>,
    // TODO: This is initialized only at session startup. We'll be able to properly check
    // the actual feature flag value (without a Coordinator call) once we'll always have a catalog
    // snapshot at hand.
    pub enable_frontend_peek_sequencing: bool,
}

impl PeekClient {
    /// Creates a PeekClient. Leaves `enable_frontend_peek_sequencing` false! This should be filled
    /// in later.
    pub fn new(
        coordinator_client: Client,
        storage_collections: StorageCollectionsHandle,
        transient_id_gen: Arc<TransientIdGen>,
        optimizer_metrics: OptimizerMetrics,
    ) -> Self {
        Self {
            coordinator_client,
            compute_instances: Default::default(), // lazily populated
            storage_collections,
            transient_id_gen,
            optimizer_metrics,
            oracles: Default::default(),            // lazily populated
            enable_frontend_peek_sequencing: false, // should be filled in later!
        }
    }

    pub async fn ensure_compute_instance_client(
        &mut self,
        compute_instance: ComputeInstanceId,
    ) -> Result<&mut mz_compute_client::controller::instance::Client<Timestamp>, AdapterError> {
        if !self.compute_instances.contains_key(&compute_instance) {
            let client = self
                .send_to_coordinator(|tx| Command::GetComputeInstanceClient {
                    instance_id: compute_instance,
                    tx,
                })
                .await?;
            self.compute_instances.insert(compute_instance, client);
        }
        Ok(self
            .compute_instances
            .get_mut(&compute_instance)
            .expect("ensured above"))
    }

    pub async fn ensure_oracle(
        &mut self,
        timeline: Timeline,
    ) -> Result<&mut Arc<dyn TimestampOracle<Timestamp> + Send + Sync>, AdapterError> {
        if !self.oracles.contains_key(&timeline) {
            let oracle = self
                .send_to_coordinator(|tx| Command::GetOracle {
                    timeline: timeline.clone(),
                    tx,
                })
                .await?;
            self.oracles.insert(timeline.clone(), oracle);
        }
        Ok(self.oracles.get_mut(&timeline).expect("ensured above"))
    }

    async fn send_to_coordinator<T, F>(&self, f: F) -> T
    where
        F: FnOnce(oneshot::Sender<T>) -> Command,
    {
        let (tx, rx) = oneshot::channel();
        self.coordinator_client.send(f(tx));
        rx.await.expect("sender dropped")
    }

    /// Acquire read holds on the given compute/storage collections, and
    /// determine the smallest common valid write frontier among the specified collections.
    ///
    /// Similar to `Coordinator::acquire_read_holds` and `TimestampProvider::least_valid_write`
    /// combined.
    ///
    /// Note: Unlike the Coordinator/StorageController's `least_valid_write` that treats sinks
    /// specially when fetching storage frontiers (see `mz_storage_controller::collections_frontiers`),
    /// we intentionally do not special‑case sinks here because peeks never read from sinks.
    /// Therefore, using `StorageCollections::collections_frontiers` is sufficient.
    ///
    /// Note: self is taken &mut because of the lazy fetching in `get_compute_instance_client`.
    pub async fn acquire_read_holds_and_collection_write_frontiers(
        &mut self,
        id_bundle: &CollectionIdBundle,
    ) -> Result<(ReadHolds<Timestamp>, Antichain<Timestamp>), CollectionMissing> {
        let mut read_holds = ReadHolds::new();
        let mut upper = Antichain::new();

        if !id_bundle.storage_ids.is_empty() {
            let desired_storage: Vec<_> = id_bundle.storage_ids.iter().copied().collect();
            let storage_read_holds = self
                .storage_collections
                .acquire_read_holds(desired_storage)
                .expect("missing storage collections");
            read_holds.storage_holds = storage_read_holds
                .into_iter()
                .map(|hold| (hold.id(), hold))
                .collect();

            let storage_ids: Vec<_> = id_bundle.storage_ids.iter().copied().collect();
            for f in self
                .storage_collections
                .collections_frontiers(storage_ids)
                .expect("missing collections")
            {
                upper.extend(f.write_frontier);
            }
        }

        for (&instance_id, collection_ids) in &id_bundle.compute_ids {
            let client = self
                .ensure_compute_instance_client(instance_id)
                .await
                .expect("missing compute instance client");

            for (id, read_hold, write_frontier) in client
                .acquire_read_holds_and_collection_write_frontiers(
                    collection_ids.iter().copied().collect(),
                )
                .await?
            {
                let prev = read_holds
                    .compute_holds
                    .insert((instance_id, id), read_hold);
                assert!(
                    prev.is_none(),
                    "duplicate compute ID in id_bundle {id_bundle:?}"
                );

                upper.extend(write_frontier);
            }
        }

        Ok((read_holds, upper))
    }

    /// Implement a fast-path peek plan.
    /// This is similar to `Coordinator::implement_peek_plan`, but only for fast path peeks.
    ///
    /// Supported variants:
    /// - FastPathPlan::Constant
    /// - FastPathPlan::PeekExisting (PeekTarget::Index only)
    ///
    /// This fn assumes that the caller has already acquired read holds for the peek at the
    /// appropriate timestamp. Before this fn returns, it passes on the responsibility of holding
    /// back the frontiers to Compute, so then the caller can forget its own read holds.
    ///
    /// Note: FastPathPlan::PeekPersist is not yet supported here; we may add this later.
    ///
    /// Note: self is taken &mut because of the lazy fetching in `get_compute_instance_client`.
    ///
    /// TODO: add statement logging
    /// TODO: cancellation (see pending_peeks/client_pending_peeks wiring in the old sequencing)
    pub async fn implement_fast_path_peek_plan(
        &mut self,
        fast_path: FastPathPlan,
        timestamp: Timestamp,
        finishing: mz_expr::RowSetFinishing,
        compute_instance: ComputeInstanceId,
        target_replica: Option<mz_cluster_client::ReplicaId>,
        intermediate_result_type: mz_repr::SqlRelationType,
        max_result_size: u64,
        max_returned_query_size: Option<u64>,
        row_set_finishing_seconds: Histogram,
        _read_holds: Option<ReadHolds<Timestamp>>,
    ) -> Result<crate::ExecuteResponse, AdapterError> {
        match fast_path {
            // If the dataflow optimizes to a constant expression, we can immediately return the result.
            FastPathPlan::Constant(rows_res, _) => {
                let mut rows = match rows_res {
                    Ok(rows) => rows,
                    Err(e) => return Err(e.into()),
                };
                consolidate(&mut rows);

                let mut results = Vec::new();
                for (row, count) in rows {
                    if count.is_negative() {
                        return Err(AdapterError::Unstructured(anyhow::anyhow!(
                            "Negative multiplicity in constant result: {}",
                            count
                        )));
                    }
                    if count.is_positive() {
                        let count = usize::cast_from(
                            u64::try_from(count.into_inner())
                                .expect("known to be positive from check above"),
                        );
                        results.push((
                            row,
                            std::num::NonZeroUsize::new(count)
                                .expect("known to be non-zero from check above"),
                        ));
                    }
                }
                let row_collection = RowCollection::new(results, &finishing.order_by);
                match finishing.finish(
                    row_collection,
                    max_result_size,
                    max_returned_query_size,
                    &row_set_finishing_seconds,
                ) {
                    Ok((rows, _bytes)) => Ok(Coordinator::send_immediate_rows(rows)),
                    // TODO: make this a structured error. (also in the old sequencing)
                    Err(e) => Err(AdapterError::ResultSize(e)),
                }
            }
            FastPathPlan::PeekExisting(_coll_id, idx_id, literal_constraints, mfp) => {
                let (rows_tx, rows_rx) = oneshot::channel();
                let uuid = Uuid::new_v4();

                // At this stage we don't know column names for the result because we
                // only know the peek's result type as a bare SqlRelationType.
                let cols = (0..intermediate_result_type.arity()).map(|i| format!("peek_{i}"));
                let result_desc = RelationDesc::new(intermediate_result_type.clone(), cols);

                // Issue peek to the instance
                let client = self
                    .ensure_compute_instance_client(compute_instance)
                    .await
                    .expect("missing compute instance client");
                let peek_target = PeekTarget::Index { id: idx_id };
                let literal_vec: Option<Vec<Row>> = literal_constraints;
                let map_filter_project = mfp;
                let finishing_for_instance = finishing.clone();
                client
                    .peek_call_sync(
                        peek_target,
                        literal_vec,
                        uuid,
                        timestamp,
                        result_desc,
                        finishing_for_instance,
                        map_filter_project,
                        // We let `Instance::peek` acquire read holds instead of passing them in.
                        // This is different from the old peek sequencing: That code is able to
                        // acquire read holds in `ComputeController::peek` at no significant cost,
                        // because that is a thick client to the controller. Here, we only have a
                        // thin client, which would need to do a roundtrip to the controller task
                        // for acquiring read holds, so it's better to let the instance do it in the
                        // same call that kicks off the peek itself. (Another difference is that
                        // the old peek sequencing's `ComputeController::peek` does not wait for
                        // `Instance::peek` to finish before returning, which means that if it were
                        // to let `Instance::peek` acquire the read holds, then we might acquire
                        // read holds too late, leading to panic if the Coordinator drops its own
                        // read holds in the meantime, e.g., due to cancelling the peek.)
                        None,
                        target_replica,
                        rows_tx,
                    )
                    .await;

                // TODO: call `create_peek_response_stream` instead. For that, we'll need to pass in
                // a PersistClient from afar.
                let peek_response_stream = async_stream::stream!({
                    match rows_rx.await {
                        Ok(PeekResponse::Rows(rows)) => {
                            match finishing.finish(
                                rows,
                                max_result_size,
                                max_returned_query_size,
                                &row_set_finishing_seconds,
                            ) {
                                Ok((rows, _size_bytes)) => {
                                    yield coord::peek::PeekResponseUnary::Rows(Box::new(rows))
                                }
                                Err(e) => yield coord::peek::PeekResponseUnary::Error(e),
                            }
                        }
                        Ok(PeekResponse::Stashed(_response)) => {
                            // TODO: support this (through `create_peek_response_stream`)
                            yield coord::peek::PeekResponseUnary::Error("stashed peek responses not yet supported in frontend peek sequencing".into());
                        }
                        Ok(PeekResponse::Error(err)) => {
                            yield coord::peek::PeekResponseUnary::Error(err);
                        }
                        Ok(PeekResponse::Canceled) => {
                            yield coord::peek::PeekResponseUnary::Canceled;
                        }
                        Err(e) => {
                            yield coord::peek::PeekResponseUnary::Error(e.to_string());
                        }
                    }
                });

                Ok(crate::ExecuteResponse::SendingRowsStreaming {
                    rows: Box::pin(peek_response_stream),
                    instance_id: compute_instance,
                    strategy: crate::statement_logging::StatementExecutionStrategy::FastPath,
                })
            }
            FastPathPlan::PeekPersist(..) => {
                // TODO: Implement this. (We currently bail out in `try_frontend_peek_inner`,
                // similarly to slow-path peeks.)
                // Note that `Instance::peek` has the following comment:
                // "For persist peeks, the controller should provide a storage read hold.
                // We don't support acquiring it here."
                // Can we do this from here?
                // (Note that if we want to bail out for this case, we need to bail out earlier!)
                unimplemented!("PeekPersist not yet supported in frontend peek sequencing")
            }
        }
    }
}
