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
use mz_ore::soft_panic_or_log;
use mz_persist_client::PersistClient;
use mz_repr::GlobalId;
use mz_repr::Timestamp;
use mz_repr::global_id::TransientIdGen;
use mz_repr::{RelationDesc, Row};
use mz_sql::ast::{Raw, Statement};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::Params;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast::{CopyRelation, CopyStatement, SubscribeStatement};
use mz_storage_types::sources::Timeline;
use mz_timestamp_oracle::TimestampOracle;
use prometheus::Histogram;
use qcell::QCell;
use thiserror::Error;
use timely::progress::Antichain;
use tokio::sync::{Semaphore, oneshot};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::command::{CatalogSnapshot, Command, ExecuteResponse};
use crate::coord::peek::FastPathPlan;
use crate::coord::{Coordinator, ExecuteContextExtra, ExecuteContextGuard};
use crate::session::{LifecycleTimestamps, Session};
use crate::statement_logging::{
    FrontendStatementLoggingEvent, PreparedStatementEvent, PreparedStatementLoggingInfo,
    StatementLoggingFrontend, StatementLoggingId, WatchSetCreation,
};
use crate::{AdapterError, Client, CollectionIdBundle, ReadHolds, metrics, statement_logging};

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
    /// Semaphore for limiting concurrent OCC (optimistic concurrency control) write operations.
    pub occ_write_semaphore: Arc<Semaphore>,
    /// Whether frontend OCC read-then-write is enabled (determined once at process startup).
    pub frontend_read_then_write_enabled: bool,
    /// Whether the coordinator is in read-only mode. Mutations must be rejected.
    pub read_only: bool,
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
        occ_write_semaphore: Arc<Semaphore>,
        frontend_read_then_write_enabled: bool,
        read_only: bool,
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
            occ_write_semaphore,
            frontend_read_then_write_enabled,
            read_only,
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

    /// Returns a clone of the coordinator client, for use in cleanup guards
    /// that need to send fire-and-forget commands.
    pub(crate) fn coordinator_client(&self) -> &crate::Client {
        &self.coordinator_client
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
    /// For a constant peek the logging slot stays armed and the caller logs the
    /// end from the returned result. For a `PeekExisting`/`PeekPersist` peek,
    /// successful registration with the coordinator hands ownership of the end
    /// to the coordinator and the slot is defused here. That holds even when the
    /// subsequent `client.peek()` fails to issue.
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
        depends_on: std::collections::BTreeSet<mz_repr::GlobalId>,
        watch_set: Option<WatchSetCreation>,
        logging: &mut ExecutionLogging,
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

        // Register coordinator tracking of this peek. This has to complete before issuing the peek.
        //
        // Warning: If we fail to actually issue the peek after this point, then we need to
        // unregister it to avoid an orphaned registration.
        self.call_coordinator(|tx| Command::RegisterFrontendPeek {
            uuid,
            conn_id: conn_id.clone(),
            cluster_id: compute_instance,
            depends_on,
            is_fast_path: true,
            watch_set,
            tx,
        })
        .await?;

        // The peek is registered: the coordinator's `pending_peeks` entry now
        // owns end-of-execution logging. It logs the end on peek completion,
        // cancellation, concurrent teardown (e.g. a DROP CLUSTER), or the
        // unregistration below. We defuse the guard so the frontend doesn't
        // also log the end.
        logging.defuse();

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
            )
            .await;

        if let Err(err) = peek_result {
            let err = AdapterError::concurrent_dependency_drop_from_instance_peek_error(
                err,
                compute_instance,
            );
            // The peek failed to issue, so no peek response will ever arrive.
            // The coordinator owns end-of-execution logging (see above), so we
            // ask it to unregister the peek and retire it with this error. If
            // a concurrent teardown already retired the peek, the end is
            // already logged and the unregistration is a no-op.
            self.call_coordinator(|tx| Command::UnregisterFrontendPeek {
                uuid,
                reason: statement_logging::StatementEndedExecutionReason::Errored {
                    error: err.to_string(),
                },
                tx,
            })
            .await;
            return Err(err);
        }

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

    /// Begins a new statement execution log entry, sampling permitting.
    ///
    /// Only [`ExecutionLogging::take_over`] may call this: an entry that exists
    /// without the session task owning its end would stay unfinished forever.
    fn begin_statement_logging(
        &self,
        session: &mut Session,
        params: &Params,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
        catalog: &Catalog,
        lifecycle_timestamps: Option<LifecycleTimestamps>,
    ) -> StatementLoggingGuard {
        let result = self.statement_logging_frontend.begin_statement_execution(
            session,
            params,
            logging,
            catalog.system_config(),
            lifecycle_timestamps,
        );

        let id = result.map(
            |(logging_id, began_execution, mseh_update, prepared_statement)| {
                self.log_began_execution(began_execution, mseh_update, prepared_statement);
                logging_id
            },
        );

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
}

/// RAII guard owning a frontend statement-logging lifecycle.
///
/// Unless logging responsibility is handed off via
/// [`defuse`](StatementLoggingGuard::defuse), the guard ensures that every
/// statement for which `BeganExecution` was logged also receives a
/// corresponding `EndedExecution`, even on early-return, panic, or mid-flight
/// drop of the enclosing future: if the guard is dropped without being defused,
/// it emits `StatementEndedExecutionReason::Aborted`.
///
/// When the guard is `defuse`d, some other component (e.g. the coordinator, for
/// streaming peek / subscribe responses) takes over and logs `EndedExecution`
/// itself.
///
/// For non-sampled statements the guard still exists but carries no id, and
/// retirement / drop are no-ops.
#[must_use = "StatementLoggingGuard must be explicitly retired or handed off; \
              otherwise `Drop` will log the statement as Aborted"]
struct StatementLoggingGuard {
    /// `None` if the statement was not sampled for logging.
    id: Option<StatementLoggingId>,
    coordinator_client: Client,
    now: mz_ore::now::NowFn,
}

impl StatementLoggingGuard {
    /// Arms a guard for the obligation the coordinator armed for `outer`, the
    /// statement whose execution the one we are about to run serves.
    fn adopt(outer: ExecuteContextGuard, peek_client: &PeekClient) -> Self {
        Self {
            id: outer.defuse().retire(),
            coordinator_client: peek_client.coordinator_client.clone(),
            now: peek_client.statement_logging_frontend.now.clone(),
        }
    }

    /// Returns the logging id, if this statement is being logged.
    fn id(&self) -> Option<StatementLoggingId> {
        self.id
    }

    /// Retires the guard with an explicit end-execution reason.
    /// A no-op if the guard was defused or the statement is not sampled.
    fn retire(mut self, reason: statement_logging::StatementEndedExecutionReason) {
        self.emit(reason);
    }

    /// Turns the obligation back into its transferable form, disarming this
    /// guard.
    fn release(mut self) -> ExecuteContextExtra {
        ExecuteContextExtra::new(self.id.take())
    }

    /// Hands off logging responsibility without emitting an end-execution
    /// event. Call this at the point where another component takes over
    /// end-of-execution logging. Afterwards the guard is inert.
    fn defuse(&mut self) {
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
        // A guard can outlive the coordinator during shutdown. Failing to send
        // costs us one end event, panicking in `Drop` would cost the whole
        // connection.
        let _ = self
            .coordinator_client
            .try_send(Command::FrontendStatementLogging(
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

/// The session task's slot for the end-of-execution obligation of the statement
/// it is running. One slot is held for the whole `SessionClient::execute` call
/// and there is exactly one retirement site.
///
/// An empty slot means no log entry exists for this execution, so a fallback to
/// the coordinator lets it begin its own. An occupied slot means the session
/// task owes an end event: [`Self::retire`] pays it, [`Self::release`] transfers
/// it to the coordinator. [`Self::id`] returning `None` covers both "not
/// sampled" and "the end is logged elsewhere", which want identical treatment
/// everywhere.
pub(crate) struct ExecutionLogging {
    guard: Option<StatementLoggingGuard>,
    /// Whether the coordinator must not run this statement, because the session
    /// task has already counted it in the metrics `Coordinator::handle_execute`
    /// maintains. Handing it over afterwards would count it twice.
    coordinator_must_not_run: bool,
}

/// Which statement the session task is taking the log entry over for.
pub(crate) enum TakeOver {
    /// The statement that will run here, so the coordinator must not run it.
    StatementToRun,
    /// A SQL `EXECUTE` that unrolls into an inner statement, for a session task
    /// that will go on to run that inner statement.
    ///
    /// The entry stays armed, so a failure to unroll is recorded against the
    /// `EXECUTE`. Only the `EXECUTE` itself is counted here. The inner
    /// statement is a statement in its own right and is counted wherever it
    /// ends up running, exactly as the coordinator does when it re-dispatches a
    /// `Plan::Execute`, which is why the slot stays releasable.
    UnrolledExecute,
}

impl ExecutionLogging {
    /// Adopts the end-of-execution obligation of an outer statement (a FETCH or
    /// an EXECUTE running its inner statement), or starts out empty when there
    /// is no outer statement.
    pub(crate) fn adopt(outer: Option<ExecuteContextGuard>, peek_client: &PeekClient) -> Self {
        Self {
            guard: outer.map(|outer| StatementLoggingGuard::adopt(outer, peek_client)),
            coordinator_must_not_run: false,
        }
    }

    /// Returns the logging id, if an end event is owed and the statement is
    /// being logged.
    pub(crate) fn id(&self) -> Option<StatementLoggingId> {
        self.guard.as_ref().and_then(|guard| guard.id())
    }

    /// Records that the session task, not the coordinator, is executing `stmt`.
    /// Bumps the counters `Coordinator::handle_execute` would have bumped and
    /// makes sure a log entry exists, inheriting the adopted one if there is
    /// one. `stmt` is `None` for an empty portal, which is logged but not
    /// counted.
    ///
    /// For [`TakeOver::StatementToRun`], every exit after this call must produce
    /// an outcome for the statement: the coordinator will not see it.
    pub(crate) fn take_over(
        &mut self,
        peek_client: &PeekClient,
        session: &mut Session,
        stmt: Option<&Statement<Raw>>,
        params: &Params,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
        catalog: &Catalog,
        lifecycle_timestamps: Option<LifecycleTimestamps>,
        taking_over: TakeOver,
    ) -> Option<StatementLoggingId> {
        self.begin_or_inherit(
            peek_client,
            session,
            params,
            logging,
            catalog,
            lifecycle_timestamps,
        );
        count_statement(session, stmt);
        if matches!(taking_over, TakeOver::StatementToRun) {
            self.coordinator_must_not_run = true;
        }
        self.id()
    }

    /// Makes sure a log entry exists for this execution, keeping an adopted one
    /// if there is one.
    fn begin_or_inherit(
        &mut self,
        peek_client: &PeekClient,
        session: &mut Session,
        params: &Params,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
        catalog: &Catalog,
        lifecycle_timestamps: Option<LifecycleTimestamps>,
    ) {
        if self.guard.is_none() {
            self.guard = Some(peek_client.begin_statement_logging(
                session,
                params,
                logging,
                catalog,
                lifecycle_timestamps,
            ));
        }
    }

    /// Hands the obligation to the coordinator, which retires it once the
    /// statement it dispatches finishes. `None` tells the coordinator to begin
    /// its own entry, `Some` with no id inside tells it that an entry already
    /// exists or that sampling declined.
    #[must_use]
    pub(crate) fn release(&mut self) -> Option<ExecuteContextExtra> {
        if self.coordinator_must_not_run {
            soft_panic_or_log!(
                "statement handed to the coordinator after the session task took it over: \
                 its per-statement metrics are counted twice"
            );
        }
        self.guard.take().map(|guard| guard.release())
    }

    /// Emits the end event for `result`, if we still owe one.
    pub(crate) fn retire(self, result: &Result<ExecuteResponse, AdapterError>) {
        let Some(guard) = self.guard else {
            return;
        };
        // A defused or released slot owes no end event. Bail before mapping
        // `result` to an end reason, which soft-panics for the responses whose
        // end is logged elsewhere.
        if guard.id().is_none() {
            return;
        }
        guard.retire(end_reason(result));
    }

    /// Leaves the slot inert, for dispatch sites that hand the end of execution
    /// to the coordinator or the protocol layer (registered peeks, subscribes).
    pub(crate) fn defuse(&mut self) {
        if let Some(guard) = self.guard.as_mut() {
            guard.defuse();
        }
    }
}

/// Maps an execution outcome to the reason to record for it.
///
/// The responses whose end is logged elsewhere are filtered out first: their
/// `StatementEndedExecutionReason` conversion panics, and this runs for every
/// statement the session task executes, so that panic would take down the
/// connection.
fn end_reason(
    result: &Result<ExecuteResponse, AdapterError>,
) -> statement_logging::StatementEndedExecutionReason {
    if let Ok(response) = result {
        if terminates_elsewhere(response) {
            soft_panic_or_log!(
                "frontend-sequenced statement still owed an end event while returning {:?}",
                crate::command::ExecuteResponseKind::from(response)
            );
            return statement_logging::StatementEndedExecutionReason::Aborted;
        }
    }
    result.into()
}

/// Bumps the per-statement counters `Coordinator::handle_execute` maintains for
/// the statement the session task runs instead. `stmt` is `None` for an empty
/// portal, which is logged but not counted.
fn count_statement(session: &Session, stmt: Option<&Statement<Raw>>) {
    let Some(stmt) = stmt else {
        return;
    };
    let session_type = metrics::session_type_label_value(session.user());
    session
        .metrics()
        .query_total(&[session_type, metrics::statement_type_label_value(stmt)])
        .inc();
    if let Statement::Subscribe(SubscribeStatement { output, .. })
    | Statement::Copy(CopyStatement {
        relation: CopyRelation::Subscribe(SubscribeStatement { output, .. }),
        ..
    }) = stmt
    {
        session
            .metrics()
            .subscribe_outputs(&[session_type, metrics::subscribe_output_label_value(output)])
            .inc();
    }
}

/// Whether someone else logs the end of execution for `response`: the
/// coordinator for a registered peek, the protocol layer for a subscribe, a
/// FETCH or a COPY FROM. The dispatch sites that produce these defuse the slot,
/// so an armed slot alongside one of them means a dispatch site did not.
fn terminates_elsewhere(response: &ExecuteResponse) -> bool {
    match response {
        ExecuteResponse::SendingRowsStreaming { .. }
        | ExecuteResponse::Subscribing { .. }
        | ExecuteResponse::Fetch { .. }
        | ExecuteResponse::CopyFrom { .. } => true,
        // COPY TO STDOUT of an immediate result terminates here. Anything else
        // it can wrap does not.
        ExecuteResponse::CopyTo { resp, .. } => {
            !matches!(**resp, ExecuteResponse::SendingRowsImmediate { .. })
        }
        _ => false,
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
