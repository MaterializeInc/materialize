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
use mz_compute_client::controller::error::{CollectionLookupError, InstanceMissing};
use mz_compute_client::protocol::command::PeekTarget;
use mz_compute_types::ComputeInstanceId;
use mz_expr::row::RowCollection;
use mz_ore::cast::CastFrom;
use mz_persist_client::PersistClient;
use mz_repr::RelationDesc;
use mz_repr::Timestamp;
use mz_repr::global_id::TransientIdGen;
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_storage_types::sources::Timeline;
use mz_timestamp_oracle::TimestampOracle;
use prometheus::Histogram;
use timely::progress::Antichain;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::command::{CatalogSnapshot, Command};
use crate::coord::Coordinator;
use crate::coord::peek::FastPathPlan;
use crate::{AdapterError, Client, CollectionIdBundle, ReadHolds, statement_logging};

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
    /// Note that these are never cleaned up. In theory, this could lead to a very slow memory leak
    /// if a long-running user session keeps peeking on clusters that are being created and dropped
    /// in a hot loop. Hopefully this won't occur any time soon.
    compute_instances:
        BTreeMap<ComputeInstanceId, mz_compute_client::controller::instance::Client<Timestamp>>,
    /// Handle to storage collections for reading frontiers and policies.
    pub storage_collections: StorageCollectionsHandle,
    /// A generator for transient `GlobalId`s, shared with Coordinator.
    pub transient_id_gen: Arc<TransientIdGen>,
    pub optimizer_metrics: OptimizerMetrics,
    /// Per-timeline oracles from the coordinator. Lazily populated.
    oracles: BTreeMap<Timeline, Arc<dyn TimestampOracle<Timestamp> + Send + Sync>>,
    persist_client: PersistClient,
}

impl PeekClient {
    /// Creates a PeekClient.
    pub fn new(
        coordinator_client: Client,
        storage_collections: StorageCollectionsHandle,
        transient_id_gen: Arc<TransientIdGen>,
        optimizer_metrics: OptimizerMetrics,
        persist_client: PersistClient,
    ) -> Self {
        Self {
            coordinator_client,
            compute_instances: Default::default(), // lazily populated
            storage_collections,
            transient_id_gen,
            optimizer_metrics,
            oracles: Default::default(), // lazily populated
            persist_client,
        }
    }

    pub async fn ensure_compute_instance_client(
        &mut self,
        compute_instance: ComputeInstanceId,
    ) -> Result<&mut mz_compute_client::controller::instance::Client<Timestamp>, InstanceMissing>
    {
        if !self.compute_instances.contains_key(&compute_instance) {
            let client = self
                .call_coordinator(|tx| Command::GetComputeInstanceClient {
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
                .call_coordinator(|tx| Command::GetOracle {
                    timeline: timeline.clone(),
                    tx,
                })
                .await?;
            self.oracles.insert(timeline.clone(), oracle);
        }
        Ok(self.oracles.get_mut(&timeline).expect("ensured above"))
    }

    /// Fetch a snapshot of the catalog for use in frontend peek sequencing.
    /// Records the time taken in the adapter metrics, labeled by `context`.
    pub async fn catalog_snapshot(&self, context: &str) -> Arc<Catalog> {
        let start = std::time::Instant::now();
        let CatalogSnapshot { catalog } = self
            .call_coordinator(|tx| Command::CatalogSnapshot { tx })
            .await;
        self.coordinator_client
            .metrics()
            .catalog_snapshot_seconds
            .with_label_values(&[context])
            .observe(start.elapsed().as_secs_f64());
        catalog
    }

    async fn call_coordinator<T, F>(&self, f: F) -> T
    where
        F: FnOnce(oneshot::Sender<T>) -> Command,
    {
        let (tx, rx) = oneshot::channel();
        self.coordinator_client.send(f(tx));
        rx.await
            .expect("if the coordinator is still alive, it shouldn't have dropped our call")
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
    pub async fn acquire_read_holds_and_least_valid_write(
        &mut self,
        id_bundle: &CollectionIdBundle,
    ) -> Result<(ReadHolds<Timestamp>, Antichain<Timestamp>), CollectionLookupError> {
        let mut read_holds = ReadHolds::new();
        let mut upper = Antichain::new();

        if !id_bundle.storage_ids.is_empty() {
            let desired_storage: Vec<_> = id_bundle.storage_ids.iter().copied().collect();
            let storage_read_holds = self
                .storage_collections
                .acquire_read_holds(desired_storage)?;
            read_holds.storage_holds = storage_read_holds
                .into_iter()
                .map(|hold| (hold.id(), hold))
                .collect();

            let storage_ids: Vec<_> = id_bundle.storage_ids.iter().copied().collect();
            for f in self
                .storage_collections
                .collections_frontiers(storage_ids)?
            {
                upper.extend(f.write_frontier);
            }
        }

        for (&instance_id, collection_ids) in &id_bundle.compute_ids {
            let client = self.ensure_compute_instance_client(instance_id).await?;

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
    /// Note: self is taken &mut because of the lazy fetching in `get_compute_instance_client`.
    ///
    /// Note: `input_read_holds` has holds for all inputs. For fast-path peeks, this includes the
    /// peek target. For slow-path peeks (to be implemented later), we'll need to additionally call
    /// into the Controller to acquire a hold on the peek target after we create the dataflow.
    ///
    /// TODO(peek-seq): add statement logging
    /// TODO(peek-seq): cancellation (see pending_peeks/client_pending_peeks wiring in the old
    /// sequencing)
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
        input_read_holds: ReadHolds<Timestamp>,
        peek_stash_read_batch_size_bytes: usize,
        peek_stash_read_memory_budget_bytes: usize,
    ) -> Result<crate::ExecuteResponse, AdapterError> {
        // If the dataflow optimizes to a constant expression, we can immediately return the result.
        if let FastPathPlan::Constant(rows_res, _) = fast_path {
            let mut rows = match rows_res {
                Ok(rows) => rows,
                Err(e) => return Err(e.into()),
            };
            consolidate(&mut rows);

            let mut results = Vec::new();
            for (row, count) in rows {
                let count = match u64::try_from(count.into_inner()) {
                    Ok(u) => usize::cast_from(u),
                    Err(_) => {
                        return Err(AdapterError::Unstructured(anyhow::anyhow!(
                            "Negative multiplicity in constant result: {}",
                            count
                        )));
                    }
                };
                match std::num::NonZeroUsize::new(count) {
                    Some(nzu) => {
                        results.push((row, nzu));
                    }
                    None => {
                        // No need to retain 0 diffs.
                    }
                };
            }
            let row_collection = RowCollection::new(results, &finishing.order_by);
            return match finishing.finish(
                row_collection,
                max_result_size,
                max_returned_query_size,
                &row_set_finishing_seconds,
            ) {
                Ok((rows, _bytes)) => Ok(Coordinator::send_immediate_rows(rows)),
                // TODO(peek-seq): make this a structured error. (also in the old sequencing)
                Err(e) => Err(AdapterError::ResultSize(e)),
            };
        }

        let (peek_target, target_read_hold, literal_constraints, mfp, strategy) = match fast_path {
            FastPathPlan::PeekExisting(_coll_id, idx_id, literal_constraints, mfp) => {
                let peek_target = PeekTarget::Index { id: idx_id };
                let target_read_hold = input_read_holds
                    .compute_holds
                    .get(&(compute_instance, idx_id))
                    .expect("missing compute read hold on PeekExisting peek target")
                    .clone();
                let strategy = statement_logging::StatementExecutionStrategy::FastPath;
                (
                    peek_target,
                    target_read_hold,
                    literal_constraints,
                    mfp,
                    strategy,
                )
            }
            FastPathPlan::PeekPersist(coll_id, literal_constraint, mfp) => {
                let literal_constraints = literal_constraint.map(|r| vec![r]);
                let metadata = self
                    .storage_collections
                    .collection_metadata(coll_id)
                    .map_err(AdapterError::concurrent_dependency_drop_from_collection_missing)?
                    .clone();
                let peek_target = PeekTarget::Persist {
                    id: coll_id,
                    metadata,
                };
                let target_read_hold = input_read_holds
                    .storage_holds
                    .get(&coll_id)
                    .expect("missing storage read hold on PeekPersist peek target")
                    .clone();
                let strategy = statement_logging::StatementExecutionStrategy::PersistFastPath;
                (
                    peek_target,
                    target_read_hold,
                    literal_constraints,
                    mfp,
                    strategy,
                )
            }
            _ => {
                // FastPathPlan::Constant handled above.
                unreachable!()
            }
        };

        let (rows_tx, rows_rx) = oneshot::channel();
        let uuid = Uuid::new_v4();

        // At this stage we don't know column names for the result because we
        // only know the peek's result type as a bare SqlRelationType.
        let cols = (0..intermediate_result_type.arity()).map(|i| format!("peek_{i}"));
        let result_desc = RelationDesc::new(intermediate_result_type.clone(), cols);

        // Issue the peek to the instance
        let client = self
            .ensure_compute_instance_client(compute_instance)
            .await
            .map_err(AdapterError::concurrent_dependency_drop_from_instance_missing)?;
        let finishing_for_instance = finishing.clone();
        client
            .peek(
                peek_target,
                literal_constraints,
                uuid,
                timestamp,
                result_desc,
                finishing_for_instance,
                mfp,
                target_read_hold,
                target_replica,
                rows_tx,
            )
            .await
            .map_err(AdapterError::concurrent_dependency_drop_from_peek_error)?;

        let peek_response_stream = Coordinator::create_peek_response_stream(
            rows_rx,
            finishing,
            max_result_size,
            max_returned_query_size,
            row_set_finishing_seconds,
            self.persist_client.clone(),
            peek_stash_read_batch_size_bytes,
            peek_stash_read_memory_budget_bytes,
        );
        Ok(crate::ExecuteResponse::SendingRowsStreaming {
            rows: Box::pin(peek_response_stream),
            instance_id: compute_instance,
            strategy,
        })
    }
}
