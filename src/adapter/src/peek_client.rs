// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Thin client for peek sequencing from the Adapter Frontend. This intentionally carries
//! minimal state: just the handles necessary to talk directly to compute
//! instances and to the storage collections view.

use std::collections::BTreeMap;
use std::sync::Arc;

use differential_dataflow::consolidation::consolidate;
use mz_compute_client::controller::error::{CollectionMissing, InstanceMissing};
use mz_compute_client::protocol::command::PeekTarget;
use mz_compute_client::protocol::response::PeekResponse;
use mz_compute_types::ComputeInstanceId;
use mz_expr::row::RowCollection;
use mz_ore::cast::CastFrom;
use mz_repr::{GlobalId, Timestamp};
use mz_repr::global_id::TransientIdGen;
use mz_repr::{RelationDesc, Row};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_storage_types::sources::Timeline;
use mz_timestamp_oracle::TimestampOracle;
use timely::progress::Antichain;
use tokio::sync::oneshot;
use uuid::Uuid;
use mz_storage_types::read_holds::ReadHold;
use crate::{CollectionIdBundle, ReadHolds};
use crate::coord::peek::FastPathPlan;
use crate::optimize::dataflows::ComputeInstanceSnapshot;

/// Storage collections trait alias we need to consult for since/frontiers.
pub type StorageCollectionsHandle = Arc<
    dyn mz_storage_client::storage_collections::StorageCollections<Timestamp = Timestamp>
        + Send
        + Sync,
>;

/// A thin client to the compute and storage controllers for sequencing fast-path peeks from the
/// session task.
#[derive(Debug)]
pub struct PeekClient {
    /// Channels to talk to each compute Instance task directly.
    /// ////////////// todo: we'll need to update this somehow when instances come and go
    /// ////// todo: do we want to make this generic, like instance::Client?
    pub compute_instances:
        BTreeMap<ComputeInstanceId, mz_compute_client::controller::instance::Client<Timestamp>>,
    /// Handle to storage collections for reading frontiers and policies.
    pub storage_collections: StorageCollectionsHandle,
    /// A generator for transient [`GlobalId`]s, shared with Coordinator.
    pub transient_id_gen: Arc<TransientIdGen>,
    pub optimizer_metrics: OptimizerMetrics,
    ///// todo: generic timestamp?
    pub oracles: BTreeMap<Timeline, Arc<dyn TimestampOracle<Timestamp> + Send + Sync>>,
    ////////// todo: This is initialized only at session startup. We'll be able to properly check
    // the actual feature flag value (without a Coordinator call) once we'll always have a catalog
    // snapshot at hand.
    pub enable_frontend_peek_sequencing: bool,
}

impl PeekClient {
    ///////// todo: This is a temporary thing.
    // We should refactor stuff to make a snapshot optional, and rather than panicking in the
    // Controller when we try to do something with a non-existent collection, return a graceful
    // error, which we can handle in the peek sequencing code.
    pub async fn snapshot(
        &self,
        compute_instance: ComputeInstanceId,
    ) -> Result<ComputeInstanceSnapshot, InstanceMissing> {
        self.compute_instances
            .get(&compute_instance)
            .expect("/////// todo: return proper error")
            .call_sync(move |i| {
                Ok(ComputeInstanceSnapshot::new_from_parts(
                    compute_instance,
                    i.snapshot(),
                ))
            })
            .await
    }

    /// Acquire read holds on the required compute/storage collections.
    /// Similar to Coordinator::acquire_read_holds.
    pub async fn acquire_read_holds(
        &self,
        id_bundle: &CollectionIdBundle,
    ) -> crate::ReadHolds<Timestamp> {
        let mut read_holds = crate::ReadHolds::new();

        // Acquire storage read holds via StorageCollections.
        let desired_storage: Vec<_> = id_bundle.storage_ids.iter().copied().collect();
        let storage_read_holds = self
            .storage_collections
            .acquire_read_holds(desired_storage)
            .expect("missing storage collections");
        read_holds.storage_holds = storage_read_holds
            .into_iter()
            .map(|hold| (hold.id(), hold))
            .collect();

        // Acquire compute read holds by calling into the appropriate instance tasks.
        for (&instance_id, collection_ids) in &id_bundle.compute_ids {
            let client = self
                .compute_instances
                .get(&instance_id)
                .expect("missing compute instance client");
            for &id in collection_ids {
                let hold = client
                    .acquire_read_hold(id)
                    .await
                    .expect("missing compute collection");
                let prev = read_holds.compute_holds.insert((instance_id, id), hold);
                assert!(
                    prev.is_none(),
                    "duplicate compute ID in id_bundle {id_bundle:?}"
                );
            }
        }

        read_holds
    }

    /// Determine the smallest common valid write frontier among the specified collections.
    ///
    /// Note: Unlike the old Coordinator/StorageController `least_valid_write` that treated sinks
    /// specially when fetching storage frontiers, we intentionally do not special‑case sinks here
    /// because peeks never read from sinks. Therefore, using
    /// `StorageCollections::collections_frontiers` is sufficient. //////// todo: verify
    pub async fn least_valid_write(
        &self,
        id_bundle: &CollectionIdBundle,
    ) -> Antichain<Timestamp> {
        let mut upper = Antichain::new();
        if !id_bundle.storage_ids.is_empty() {
            let storage_ids: Vec<_> = id_bundle.storage_ids.iter().copied().collect();
            for f in self
                .storage_collections
                .collections_frontiers(storage_ids)
                .expect("missing collections")
            {
                upper.extend(f.write_frontier);
            }
        }
        for (instance_id, ids) in &id_bundle.compute_ids {
            let client = self
                .compute_instances
                .get(instance_id)
                .expect("PeekClient is missing a compute instance client");
            for id in ids {
                let wf = client.collection_write_frontier(*id).await;
                upper.extend(wf);
            }
        }
        upper
    }

    pub async fn acquire_read_holds_and_collection_write_frontiers(&self, id_bundle: &CollectionIdBundle) -> Result<(ReadHolds<Timestamp>, Antichain<Timestamp>), CollectionMissing> {
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
                .compute_instances
                .get(&instance_id)
                .expect("missing compute instance client");

            for (id, read_hold, write_frontier) in client.acquire_read_holds_and_collection_write_frontiers(collection_ids.iter().copied().collect()).await? {
                let prev = read_holds.compute_holds.insert((instance_id, id), read_hold);
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
    ///
    /// Supported variants:
    /// - FastPathPlan::Constant
    /// - FastPathPlan::PeekExisting (PeekTarget::Index only)
    ///
    /// Note: FastPathPlan::PeekPersist is not yet supported here; we may add this later.
    /// ////////// todo: add statement logging, pending_peeks/client_pending_peeks wiring, and metrics.row_set_finishing_seconds.
    pub async fn implement_fast_path_peek_plan(
        &self,
        fast_path: FastPathPlan,
        timestamp: Timestamp,
        finishing: mz_expr::RowSetFinishing,
        compute_instance: ComputeInstanceId,
        target_replica: Option<mz_cluster_client::ReplicaId>,
        intermediate_result_type: mz_repr::SqlRelationType,
        max_result_size: u64,
        max_returned_query_size: Option<u64>,
    ) -> Result<crate::ExecuteResponse, crate::AdapterError> {
        //////// todo: don't we need other things from PlannedPeek?

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
                        /////// todo: check error type (might need to revise also in the old code)
                        return Err(crate::AdapterError::Unstructured(anyhow::anyhow!(
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
                // /////////// todo: wire up metrics.row_set_finishing_seconds
                let histogram = prometheus::Histogram::with_opts(
                    prometheus::HistogramOpts::new(
                        "new_fast_path_peek_finish",
                        "temporary histogram for fast path finishing",
                    )
                    .buckets(vec![0.001, 0.01, 0.1, 1.0, 10.0]),
                )
                .unwrap();
                match finishing.finish(
                    row_collection,
                    max_result_size,
                    max_returned_query_size,
                    &histogram,
                ) {
                    ////////// todo: put send_immediate_rows somewhere where we can call it from here
                    Ok((rows, _bytes)) => Ok(crate::ExecuteResponse::SendingRowsImmediate {
                        rows: Box::new(rows),
                    }),
                    Err(e) => Err(crate::AdapterError::ResultSize(e)),
                }
            }
            FastPathPlan::PeekExisting(_coll_id, idx_id, literal_constraints, mfp) => {
                let (rows_tx, rows_rx) = oneshot::channel();
                let uuid = Uuid::new_v4();
                // ////// todo: manage pending peeks for cancellation/cleanup

                // ///////// todo: what are these names used for? (also in the original code)
                let cols = (0..intermediate_result_type.arity()).map(|i| format!("peek_{i}"));
                let result_desc = RelationDesc::new(intermediate_result_type.clone(), cols);

                // Issue peek to the instance
                let client = self
                    .compute_instances
                    .get(&compute_instance)
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
                        None, // let the instance acquire read holds
                        target_replica,
                        rows_tx,
                    )
                    .await;

                //////// todo: this is a dummy histogram; need to wire up metrics
                let duration_histogram = prometheus::Histogram::with_opts(
                    prometheus::HistogramOpts::new(
                        "new_fast_path_peek_finish",
                        "temporary histogram for fast path finishing",
                    )
                    .buckets(vec![0.001, 0.01, 0.1, 1.0, 10.0]),
                )
                .unwrap();

                let peek_response_stream = async_stream::stream!({
                    match rows_rx.await {
                        Ok(PeekResponse::Rows(rows)) => {
                            match finishing.finish(
                                rows,
                                max_result_size,
                                max_returned_query_size,
                                &duration_histogram,
                            ) {
                                Ok((rows, _size_bytes)) => {
                                    yield crate::coord::peek::PeekResponseUnary::Rows(Box::new(
                                        rows,
                                    ))
                                }
                                Err(e) => yield crate::coord::peek::PeekResponseUnary::Error(e),
                            }
                        }
                        Ok(PeekResponse::Stashed(_response)) => {
                            // /////// todo: support stashed peek responses; put create_peek_response_stream somewhere where we can call it from here
                            panic!("stashed peek responses not yet supported in new fast-path");
                        }
                        ////////// todo: check error cases
                        Ok(PeekResponse::Error(err)) => {
                            yield crate::coord::peek::PeekResponseUnary::Error(err.to_string());
                        }
                        Ok(PeekResponse::Canceled) => {
                            yield crate::coord::peek::PeekResponseUnary::Error(
                                "peek canceled".to_string(),
                            );
                        }
                        Err(e) => {
                            yield crate::coord::peek::PeekResponseUnary::Error(e.to_string());
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
                ////////// todo: do we want to support PeekPersist here? I guess why not?
                panic!()
            }
        }
    }
}
