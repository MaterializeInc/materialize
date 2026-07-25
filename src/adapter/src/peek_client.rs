// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

use differential_dataflow::consolidation::consolidate;
use mz_compute_client::controller::error::{CollectionMissing, InstanceMissing};
use mz_compute_client::controller::instance_client::InstanceClient;
use mz_compute_client::controller::instance_client::{AcquireReadHoldsError, InstanceShutDown};
use mz_compute_client::protocol::command::PeekTarget;
use mz_compute_types::ComputeInstanceId;
use mz_expr::row::RowCollection;
use mz_ore::cast::CastFrom;
use mz_persist_client::PersistClient;
use mz_repr::GlobalId;
use mz_repr::Timestamp;
use mz_repr::global_id::TransientIdGen;
use mz_repr::{RelationDesc, Row};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::Params;
use mz_storage_types::sources::Timeline;
use mz_timestamp_oracle::TimestampOracle;
use prometheus::Histogram;
use qcell::QCell;
use thiserror::Error;
use timely::progress::Antichain;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::command::{CatalogSnapshot, Command};
use crate::coord::peek::FastPathPlan;
use crate::coord::{Coordinator, ExecuteContextGuard};
use crate::peek_registry::{FrontendPeekRegistry, PeekRegistrationGuard, PendingPeekEntry};
use crate::session::{LifecycleTimestamps, Session};
use crate::statement_logging::{
    FrontendStatementLoggingEvent, PreparedStatementEvent, PreparedStatementLoggingInfo,
    StatementLoggingFrontend, StatementLoggingId, WatchSetCreation,
};
use crate::{AdapterError, Client, CollectionIdBundle, ReadHolds, statement_logging};

/// Storage collections trait alias we need to consult for since/frontiers.
pub type StorageCollectionsHandle =
    Arc<dyn mz_storage_client::storage_collections::StorageCollections + Send + Sync>;

/// Clients needed for peek sequencing in the Adapter Frontend.
#[derive(Debug)]
pub struct PeekClient {
    coordinator_client: Client,
    /// Cache of the latest catalog snapshot. Serves
    /// [`PeekClient::catalog_snapshot`] without a Coordinator round-trip
    /// while the catalog's transient revision is unchanged.
    ///
    /// Holds a `Weak` so that an idle session does not keep a superseded
    /// catalog version alive.
    catalog_cache: Weak<Catalog>,
    /// Channels to talk to each compute Instance task directly. Lazily populated.
    /// Note that these are never cleaned up. In theory, this could lead to a very slow memory leak
    /// if a long-running user session keeps peeking on clusters that are being created and dropped
    /// in a hot loop. Hopefully this won't occur any time soon.
    compute_instances: BTreeMap<ComputeInstanceId, InstanceClient>,
    /// Handle to storage collections for reading frontiers and policies.
    pub storage_collections: StorageCollectionsHandle,
    /// A generator for transient `GlobalId`s, shared with Coordinator.
    pub transient_id_gen: Arc<TransientIdGen>,
    pub optimizer_metrics: OptimizerMetrics,
    /// Per-timeline oracles from the coordinator. Lazily populated.
    oracles: BTreeMap<Timeline, Arc<dyn TimestampOracle<Timestamp> + Send + Sync>>,
    persist_client: PersistClient,
    /// Statement logging state for frontend peek sequencing.
    pub statement_logging_frontend: StatementLoggingFrontend,
    /// Registry of in-flight frontend peeks, shared with the coordinator. Peeks
    /// register/unregister here directly rather than through a coordinator
    /// round-trip.
    peek_registry: Arc<FrontendPeekRegistry>,
}

impl PeekClient {
    /// Creates a PeekClient.
    ///
    /// `catalog` seeds the catalog snapshot cache, so that the session's
    /// first statements don't need a `Command::CatalogSnapshot` round-trip.
    pub fn new(
        coordinator_client: Client,
        catalog: &Arc<Catalog>,
        storage_collections: StorageCollectionsHandle,
        transient_id_gen: Arc<TransientIdGen>,
        optimizer_metrics: OptimizerMetrics,
        persist_client: PersistClient,
        statement_logging_frontend: StatementLoggingFrontend,
        peek_registry: Arc<FrontendPeekRegistry>,
    ) -> Self {
        Self {
            coordinator_client,
            catalog_cache: Arc::downgrade(catalog),
            compute_instances: Default::default(), // lazily populated
            storage_collections,
            transient_id_gen,
            optimizer_metrics,
            statement_logging_frontend,
            oracles: Default::default(), // lazily populated
            persist_client,
            peek_registry,
        }
    }

    pub async fn ensure_compute_instance_client(
        &mut self,
        compute_instance: ComputeInstanceId,
    ) -> Result<InstanceClient, InstanceMissing> {
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
            .get(&compute_instance)
            .expect("ensured above")
            .clone())
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

    /// Fetch a snapshot of the catalog.
    ///
    /// Serves from the session-side cache when the catalog's transient
    /// revision is unchanged since the cached snapshot was taken (see
    /// [`Catalog::transient_revision_is_current`]). An unchanged revision
    /// means the cached snapshot is identical to what a fresh fetch would
    /// return. Otherwise falls back to a `Command::CatalogSnapshot`
    /// round-trip and re-populates the cache.
    ///
    /// Cache misses record the round-trip time in the adapter metrics,
    /// labeled by `context`. Hits and misses are counted in
    /// `catalog_snapshot_cache`.
    pub async fn catalog_snapshot(&mut self, context: &str) -> Arc<Catalog> {
        // NOTE: The upgrade can fail even when the revision is unchanged: any
        // in-place mutation of the Coordinator's catalog (including
        // revision-preserving ones) moves it to a new allocation, and the
        // cached allocation is freed once its last user drops. We then fall
        // through to a refetch.
        let cached = self
            .catalog_cache
            .upgrade()
            .filter(|catalog| catalog.transient_revision_is_current());
        if let Some(catalog) = cached {
            self.coordinator_client
                .metrics()
                .catalog_snapshot_cache
                .with_label_values(&[context, "hit"])
                .inc();
            return catalog;
        }

        // The cache is empty, stale, or its allocation is gone: do the
        // round-trip.
        let start = std::time::Instant::now();
        let CatalogSnapshot { catalog } = self
            .call_coordinator(|tx| Command::CatalogSnapshot { tx })
            .await;
        let metrics = self.coordinator_client.metrics();
        metrics
            .catalog_snapshot_seconds
            .with_label_values(&[context])
            .observe(start.elapsed().as_secs_f64());
        metrics
            .catalog_snapshot_cache
            .with_label_values(&[context, "miss"])
            .inc();
        self.catalog_cache = Arc::downgrade(&catalog);
        catalog
    }

    pub(crate) async fn call_coordinator<T, F>(&self, f: F) -> T
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
    ) -> Result<(ReadHolds, Antichain<Timestamp>), CollectionLookupError> {
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
    /// `logging_guard` owns end-of-execution logging for this statement. For a
    /// constant peek it stays armed and the caller logs the end from the
    /// returned result. For a `PeekExisting`/`PeekPersist` peek the guard is
    /// defused here once the peek is registered in the shared registry, since
    /// end-of-execution logging is not owned by the frontend from that point.
    /// That holds even when the subsequent `client.peek()` fails to issue.
    pub(crate) async fn implement_fast_path_peek_plan(
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
        input_read_holds: ReadHolds,
        peek_stash_read_batch_size_bytes: usize,
        peek_stash_read_memory_budget_bytes: usize,
        conn_id: mz_adapter_types::connection::ConnectionId,
        // NOTE: Unused in the prototype. The coordinator used this for DROP
        // CLUSTER teardown of registered peeks, which the shared registry does
        // not yet implement.
        _depends_on: std::collections::BTreeSet<mz_repr::GlobalId>,
        watch_set: Option<WatchSetCreation>,
        logging_guard: &mut StatementLoggingGuard,
    ) -> Result<crate::ExecuteResponse, AdapterError> {
        // If the dataflow optimizes to a constant expression, we can immediately return the result.
        if let FastPathPlan::Constant(rows_res, _) = fast_path {
            // For constant queries with statement logging, immediately log that
            // dependencies are "ready" (trivially, because there are none).
            if let Some(ref ws) = watch_set {
                self.log_lifecycle_event(
                    ws.logging_id,
                    statement_logging::StatementLifecycleEvent::StorageDependenciesFinished,
                );
                self.log_lifecycle_event(
                    ws.logging_id,
                    statement_logging::StatementLifecycleEvent::ComputeDependenciesFinished,
                );
            }

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
            FastPathPlan::Constant(..) => {
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

        let client = self
            .ensure_compute_instance_client(compute_instance)
            .await
            .map_err(AdapterError::concurrent_dependency_drop_from_instance_missing)?;

        // Register the peek in the shared registry, off the coordinator task.
        // This must happen strictly before the peek is issued so that a
        // concurrent cancellation observes the entry.
        //
        // Warning: If we fail to actually issue the peek after this point, then we need to
        // remove the registration to avoid an orphaned entry.
        self.peek_registry.register(
            uuid,
            PendingPeekEntry {
                conn_id: conn_id.clone(),
                cluster_id: compute_instance,
            },
        );

        // The peek's rows stream directly to the session and its registration is
        // cleaned up off the coordinator task. Statement-logging end is not
        // owned by the coordinator here, so we defuse the guard to avoid
        // double-logging the end. For non-sampled statements this is a no-op.
        logging_guard.defuse();

        // Test-only synchronization point: parks a peek between registration
        // and issue, so a test can land a concurrent DROP CLUSTER in this
        // window. Used by
        // workflow_test_drop_cluster_during_registered_peeks_fast_path.
        fail::fail_point!("peek_after_register_before_issue");

        let finishing_for_instance = finishing.clone();
        let peek_result = client
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
                // Frontend-sequenced peek: rows stream directly to the session
                // and the registry entry is cleaned up off the coordinator task,
                // so no `PeekNotification` is wanted.
                false,
            )
            .await;

        if let Err(err) = peek_result {
            let err = AdapterError::concurrent_dependency_drop_from_instance_peek_error(
                err,
                compute_instance,
            );
            // The peek failed to issue, so no peek response will ever arrive and
            // no response stream will be created to clean up the entry. Remove
            // the registration here to avoid an orphan. Removal is idempotent,
            // so a concurrent teardown that already removed it makes this a
            // no-op.
            let _ = self.peek_registry.remove(uuid);
            return Err(err);
        }

        // Hand the registration to the response stream: it is removed when the
        // stream completes or is dropped, keeping the registry bounded without
        // any coordinator involvement.
        let registration_guard = PeekRegistrationGuard::new(Arc::clone(&self.peek_registry), uuid);
        let peek_response_stream = Coordinator::create_peek_response_stream(
            rows_rx,
            finishing,
            max_result_size,
            max_returned_query_size,
            row_set_finishing_seconds,
            self.persist_client.clone(),
            peek_stash_read_batch_size_bytes,
            peek_stash_read_memory_budget_bytes,
            Some(registration_guard),
        );

        Ok(crate::ExecuteResponse::SendingRowsStreaming {
            rows: Box::pin(peek_response_stream),
            instance_id: compute_instance,
            strategy,
        })
    }

    /// Set up statement logging for a frontend-sequenced operation.
    ///
    /// If `outer_ctx_extra` is `None`, begins a new statement execution log
    /// entry. If `outer_ctx_extra` is `Some` (e.g. EXECUTE/FETCH), reuses and
    /// retires the existing logging context.
    ///
    /// Returns a [`StatementLoggingGuard`]. Callers must either
    /// [`retire`](StatementLoggingGuard::retire) the guard on the execution's
    /// terminal outcome, or [`defuse`](StatementLoggingGuard::defuse) it at
    /// the point where end-of-execution logging is handed off to another
    /// component. Dropping the guard without retiring it emits an `Aborted`
    /// end-execution event.
    pub(crate) fn begin_statement_logging(
        &self,
        session: &mut Session,
        params: &Params,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
        catalog: &Catalog,
        lifecycle_timestamps: Option<LifecycleTimestamps>,
        outer_ctx_extra: &mut Option<ExecuteContextGuard>,
    ) -> StatementLoggingGuard {
        let id = if outer_ctx_extra.is_none() {
            // This is a new statement, so begin statement logging.
            let result = self.statement_logging_frontend.begin_statement_execution(
                session,
                params,
                logging,
                catalog.system_config(),
                lifecycle_timestamps,
            );

            if let Some((logging_id, began_execution, mseh_update, prepared_statement)) = result {
                self.log_began_execution(began_execution, mseh_update, prepared_statement);
                Some(logging_id)
            } else {
                None
            }
        } else {
            // We're executing in the context of another statement (e.g. FETCH),
            // so take ownership of the outer context and inherit its logging id
            // (if any). The end of execution will be logged by the caller.
            outer_ctx_extra
                .take()
                .and_then(|guard| guard.defuse().retire())
        };

        StatementLoggingGuard {
            id,
            coordinator_client: self.coordinator_client.clone(),
            now: self.statement_logging_frontend.now.clone(),
        }
    }

    /// Log the beginning of statement execution.
    pub(crate) fn log_began_execution(
        &self,
        record: statement_logging::StatementBeganExecutionRecord,
        mseh_update: Row,
        prepared_statement: Option<PreparedStatementEvent>,
    ) {
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::BeganExecution {
                    record,
                    mseh_update,
                    prepared_statement,
                },
            ));
    }

    /// Log cluster selection for a statement.
    pub(crate) fn log_set_cluster(
        &self,
        id: StatementLoggingId,
        cluster_id: mz_controller_types::ClusterId,
        cluster_name: String,
    ) {
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::SetCluster {
                    id,
                    cluster_id,
                    cluster_name,
                },
            ));
    }

    /// Log timestamp determination for a statement.
    pub(crate) fn log_set_timestamp(&self, id: StatementLoggingId, timestamp: mz_repr::Timestamp) {
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::SetTimestamp { id, timestamp },
            ));
    }

    /// Log transient index ID for a statement.
    pub(crate) fn log_set_transient_index_id(
        &self,
        id: StatementLoggingId,
        transient_index_id: mz_repr::GlobalId,
    ) {
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::SetTransientIndex {
                    id,
                    transient_index_id,
                },
            ));
    }

    /// Log a statement lifecycle event.
    pub(crate) fn log_lifecycle_event(
        &self,
        id: StatementLoggingId,
        event: statement_logging::StatementLifecycleEvent,
    ) {
        let when = (self.statement_logging_frontend.now)();
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::Lifecycle { id, event, when },
            ));
    }

    /// Emit a `FrontendStatementLoggingEvent::EndedExecution` for the given
    /// logging id. Used by the few callers that hold a bare
    /// [`StatementLoggingId`] rather than a [`StatementLoggingGuard`], e.g.
    /// error paths of the EXECUTE unrolling where the id is not wrapped in a
    /// guard.
    pub(crate) fn log_ended_execution(
        &self,
        id: StatementLoggingId,
        reason: statement_logging::StatementEndedExecutionReason,
    ) {
        let ended_at = (self.statement_logging_frontend.now)();
        let record = statement_logging::StatementEndedExecutionRecord {
            id: id.0,
            reason,
            ended_at,
        };
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::EndedExecution(record),
            ));
    }
}

/// RAII guard owning a frontend statement-logging lifecycle.
///
/// Created by [`PeekClient::begin_statement_logging`]. Unless logging
/// responsibility is handed off via [`defuse`](StatementLoggingGuard::defuse),
/// the guard ensures that every statement for which `BeganExecution` was logged
/// also receives a corresponding `EndedExecution`, even on early-return, panic,
/// or mid-flight drop of the enclosing future: if the guard is dropped without
/// being defused, it emits `StatementEndedExecutionReason::Aborted`.
///
/// When the guard is `defuse`d, some other component (e.g. the coordinator, for
/// streaming peek / subscribe responses) takes over and logs `EndedExecution`
/// itself.
///
/// For non-sampled statements the guard still exists but carries no id, and
/// retirement / drop are no-ops.
#[must_use = "StatementLoggingGuard must be explicitly retired or handed off; \
              otherwise `Drop` will log the statement as Aborted"]
pub(crate) struct StatementLoggingGuard {
    /// `None` if the statement was not sampled for logging.
    id: Option<StatementLoggingId>,
    coordinator_client: Client,
    now: mz_ore::now::NowFn,
}

impl StatementLoggingGuard {
    /// Returns the logging id, if this statement is being logged.
    pub(crate) fn id(&self) -> Option<StatementLoggingId> {
        self.id
    }

    /// Retires the guard with an explicit end-execution reason.
    /// A no-op if the guard was defused or the statement is not sampled.
    pub(crate) fn retire(mut self, reason: statement_logging::StatementEndedExecutionReason) {
        self.emit(reason);
    }

    /// Hands off logging responsibility without emitting an end-execution
    /// event. Call this at the point where another component takes over
    /// end-of-execution logging. Afterwards the guard is inert.
    pub(crate) fn defuse(&mut self) {
        self.id = None;
    }

    fn emit(&mut self, reason: statement_logging::StatementEndedExecutionReason) {
        let Some(id) = self.id.take() else {
            return;
        };
        let ended_at = (self.now)();
        let record = statement_logging::StatementEndedExecutionRecord {
            id: id.0,
            reason,
            ended_at,
        };
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::EndedExecution(record),
            ));
    }
}

impl Drop for StatementLoggingGuard {
    fn drop(&mut self) {
        // `emit` is a no-op if the guard was already retired or defused (i.e.
        // `id` is `None`).
        self.emit(statement_logging::StatementEndedExecutionReason::Aborted);
    }
}

/// Errors arising during collection lookup in peek client operations.
#[derive(Error, Debug)]
pub enum CollectionLookupError {
    /// The specified compute instance does not exist.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// The specified compute instance has shut down.
    #[error("the instance has shut down")]
    InstanceShutDown,
    /// The compute collection does not exist.
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
}

impl From<InstanceMissing> for CollectionLookupError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<InstanceShutDown> for CollectionLookupError {
    fn from(_error: InstanceShutDown) -> Self {
        Self::InstanceShutDown
    }
}

impl From<CollectionMissing> for CollectionLookupError {
    fn from(error: CollectionMissing) -> Self {
        Self::CollectionMissing(error.0)
    }
}

impl From<AcquireReadHoldsError> for CollectionLookupError {
    fn from(error: AcquireReadHoldsError) -> Self {
        match error {
            AcquireReadHoldsError::CollectionMissing(id) => Self::CollectionMissing(id),
            AcquireReadHoldsError::InstanceShutDown => Self::InstanceShutDown,
        }
    }
}
