// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic for processing [`Coordinator`] messages. The [`Coordinator`] receives
//! messages from various sources (ex: controller, clients, background tasks, etc).

use std::collections::{BTreeMap, BTreeSet, btree_map};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;
use maplit::btreemap;
use mz_audit_log::VersionedStorageUsage;
use mz_catalog::memory::objects::ClusterReplicaProcessStatus;
use mz_controller::ControllerResponse;
use mz_controller::clusters::{ClusterEvent, ClusterStatus};
use mz_ore::cast::CastFrom;
use mz_ore::instrument;
use mz_ore::now::EpochMillis;
use mz_ore::option::OptionExt;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{soft_assert_or_log, soft_panic_or_log, task};
use mz_persist_client::usage::ShardsUsageReferenced;
use mz_repr::{Datum, Diff, Row};
use mz_sql::ast::Statement;
use mz_sql::names::ResolvedIds;
use mz_sql::pure::PurifiedStatement;
use mz_storage_client::controller::IntrospectionType;
use mz_storage_types::StorageDiff;
use opentelemetry::trace::TraceContextExt;
use rand::{Rng, SeedableRng, rngs};
use serde_json::json;
use tracing::{Instrument, Level, event, info_span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::active_compute_sink::{ActiveComputeSink, ActiveComputeSinkRetireReason};
use crate::catalog::BuiltinTableUpdate;
use crate::command::Command;
use crate::coord::{
    AlterConnectionValidationReady, ArrangementSizeRecord, ClusterReplicaStatuses, Coordinator,
    CreateConnectionValidationReady, Message, PurifiedStatementReady, WatchSetResponse,
};
use crate::telemetry::{EventDetails, SegmentClientExt};
use crate::{AdapterNotice, TimestampContext};

/// How long an introspection subscribe must have been delivering data before
/// the arrangement sizes snapshot trusts its replica's rows.
///
/// See `Coordinator::fresh_introspection_replicas` for why a margin is needed.
/// 10s comfortably covers the collection manager's ~1s write batching plus the
/// oracle read timestamp trailing the wall clock.
const ARRANGEMENT_SIZES_FRESHNESS_MARGIN: Duration = Duration::from_secs(10);

impl Coordinator {
    /// BOXED FUTURE: As of Nov 2023 the returned Future from this function was 74KB. This would
    /// get stored on the stack which is bad for runtime performance, and blow up our stack usage.
    /// Because of that we purposefully move Futures of inner function calls onto the heap
    /// (i.e. Box it).
    #[instrument]
    pub(crate) async fn handle_message(&mut self, msg: Message) -> () {
        match msg {
            Message::Command(otel_ctx, cmd) => {
                // TODO: We need a Span that is not none for the otel_ctx to attach the parent
                // relationship to. If we swap the otel_ctx in `Command::Message` for a Span, we
                // can downgrade this to a debug_span.
                let span = tracing::info_span!("message_command").or_current();
                span.in_scope(|| otel_ctx.attach_as_parent());
                self.message_command(cmd).instrument(span).await
            }
            Message::ControllerReady { controller: _ } => {
                let Coordinator {
                    controller,
                    catalog,
                    ..
                } = self;
                let storage_metadata = catalog.state().storage_metadata();
                if let Some(m) = controller
                    .process(storage_metadata)
                    .expect("`process` never returns an error")
                {
                    self.message_controller(m).boxed_local().await
                }
            }
            Message::PurifiedStatementReady(ready) => {
                self.message_purified_statement_ready(ready)
                    .boxed_local()
                    .await
            }
            Message::CreateConnectionValidationReady(ready) => {
                self.message_create_connection_validation_ready(ready)
                    .boxed_local()
                    .await
            }
            Message::AlterConnectionValidationReady(ready) => {
                self.message_alter_connection_validation_ready(ready)
                    .boxed_local()
                    .await
            }
            Message::TryDeferred {
                conn_id,
                acquired_lock,
            } => self.try_deferred(conn_id, acquired_lock).await,
            Message::GroupCommitInitiate(span, permit) => {
                // Add an OpenTelemetry link to our current span.
                tracing::Span::current().add_link(span.context().span().span_context().clone());
                span.in_scope(|| self.stage_group_commit(permit));
            }
            Message::GroupCommitApplied {
                responses,
                statement_logging_ids,
                write_ts,
            } => {
                // Record statement timestamps before retiring, since retiring ends the statement
                // execution and drops its logging record.
                for id in statement_logging_ids {
                    self.set_statement_execution_timestamp(id, write_ts);
                }
                for response in responses {
                    let (mut ctx, result) = response.finalize();
                    ctx.session_mut().apply_write(write_ts);
                    ctx.retire(result);
                }
                // The committer applied `write_ts` to the oracle, so the read ts is at least
                // that and we can downgrade the local read holds without an oracle round trip.
                self.downgrade_local_read_holds(write_ts);
                self.advance_custom_timelines().boxed_local().await;
            }
            Message::AdvanceTimelines => {
                // Only sent by the periodic tick in read-only mode, where group commits (which
                // otherwise drive the local timeline) don't run. Fetch the oracle's read ts here,
                // it is the freshest timestamp the read holds may downgrade to.
                let read_ts = self.get_local_read_ts().await;
                self.downgrade_local_read_holds(read_ts);
                self.advance_custom_timelines().boxed_local().await;
            }
            Message::ClusterEvent(event) => self.message_cluster_event(event).boxed_local().await,
            Message::CancelPendingPeeks { conn_id } => {
                self.cancel_pending_peeks(&conn_id);
            }
            Message::LinearizeReads => {
                self.message_linearize_reads().boxed_local().await;
            }
            Message::StagedBatches {
                conn_id,
                table_id,
                batches,
            } => {
                self.commit_staged_batches(conn_id, table_id, batches);
            }
            Message::StorageUsageSchedule => {
                self.schedule_storage_usage_collection().boxed_local().await;
            }
            Message::StorageUsageFetch => {
                self.storage_usage_fetch().boxed_local().await;
            }
            Message::StorageUsageUpdate(sizes) => {
                self.storage_usage_update(sizes).boxed_local().await;
            }
            Message::StorageUsagePrune(expired) => {
                self.storage_usage_prune(expired).boxed_local().await;
            }
            Message::ArrangementSizesSchedule => {
                self.schedule_arrangement_sizes_collection()
                    .boxed_local()
                    .await;
            }
            Message::ArrangementSizesSnapshot => {
                self.arrangement_sizes_snapshot().boxed_local().await;
            }
            Message::ArrangementSizesWrite(records) => {
                self.arrangement_sizes_write(records).boxed_local().await;
            }
            Message::ArrangementSizesPrune(expired) => {
                self.arrangement_sizes_prune(expired).boxed_local().await;
            }
            Message::RetireExecute {
                otel_ctx,
                data,
                reason,
            } => {
                otel_ctx.attach_as_parent();
                self.retire_execution(reason, data);
            }
            Message::ExecuteSingleStatementTransaction {
                ctx,
                otel_ctx,
                stmt,
                params,
            } => {
                otel_ctx.attach_as_parent();
                self.sequence_execute_single_statement_transaction(ctx, stmt, params)
                    .boxed_local()
                    .await;
            }
            Message::PeekStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::CreateIndexStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::CreateViewStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::CreateMaterializedViewStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::SubscribeStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::IntrospectionSubscribeStageReady { span, stage } => {
                self.sequence_staged((), span, stage).boxed_local().await;
            }
            Message::ExplainTimestampStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::SecretStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::ClusterStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::DrainStatementLog => {
                self.drain_statement_log();
            }
            Message::PrivateLinkVpcEndpointEvents(events) => {
                if !self.controller.read_only() {
                    self.controller.storage.append_introspection_updates(
                        IntrospectionType::PrivatelinkConnectionStatusHistory,
                        events
                            .into_iter()
                            .map(|e| (mz_repr::Row::from(e), Diff::ONE))
                            .collect(),
                    );
                }
            }
            Message::CheckSchedulingPolicies => {
                self.check_scheduling_policies().boxed_local().await;
            }
            Message::SchedulingDecisions(decisions) => {
                self.handle_scheduling_decisions(decisions)
                    .boxed_local()
                    .await;
            }
            Message::ClusterControllerRequest(request) => {
                self.handle_cluster_controller_request(request)
                    .boxed_local()
                    .await;
            }
            Message::DeferredStatementReady => {
                self.handle_deferred_statement().boxed_local().await;
            }
        }
    }

    #[mz_ore::instrument(level = "debug")]
    pub async fn storage_usage_fetch(&self) {
        // In read-only mode (e.g. a standby coordinator during a zero-downtime
        // deployment) we cannot durably write the per-batch allocator bump or
        // append to `mz_storage_usage_by_shard`, and we also don't want to do
        // the slow shard scan on a process that isn't going to record the
        // results. Skip the whole cycle and reschedule so we resume
        // automatically once the coordinator transitions out of read-only.
        if self.controller.read_only() {
            tracing::info!("skipping storage usage collection in read-only mode");
            if let Err(e) = self.internal_cmd_tx.send(Message::StorageUsageSchedule) {
                warn!("internal_cmd_rx dropped before we could send: {:?}", e);
            }
            return;
        }

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let client = self.storage_usage_client.clone();

        // Record the currently live shards.
        let live_shards: BTreeSet<_> = self
            .controller
            .storage
            .active_collection_metadatas()
            .into_iter()
            .map(|(_id, m)| m.data_shard)
            .collect();

        let collection_metric = self.metrics.storage_usage_collection_time_seconds.clone();

        // Spawn an asynchronous task to compute the storage usage, which
        // requires a slow scan of the underlying storage engine.
        task::spawn(|| "storage_usage_fetch", async move {
            let collection_metric_timer = collection_metric.start_timer();
            let shard_sizes = client.shards_usage_referenced(live_shards).await;
            collection_metric_timer.observe_duration();

            // It is not an error for shard sizes to become ready after
            // `internal_cmd_rx` is dropped.
            if let Err(e) = internal_cmd_tx.send(Message::StorageUsageUpdate(shard_sizes)) {
                warn!("internal_cmd_rx dropped before we could send: {:?}", e);
            }
        });
    }

    #[mz_ore::instrument(level = "debug")]
    async fn storage_usage_update(&mut self, shards_usage: ShardsUsageReferenced) {
        // Similar to audit events, use the oracle ts so this is guaranteed to
        // increase. This is intentionally the timestamp of when collection
        // finished, not when it started, so that we don't write data with a
        // timestamp in the past.
        //
        // `storage_usage_fetch` skips this path in read-only mode, so we can
        // unconditionally bump the oracle write ts here.
        let write_ts = self.get_catalog_write_ts().await;
        let collection_timestamp: EpochMillis = write_ts.into();

        // All rows in this collection cycle share `batch_id` so consumers can
        // identify rows that were collected together. We use one durable
        // allocator bump per cycle (rather than per shard) so the id is
        // monotonic across coordinator restarts while still keeping the
        // coord-blocking cost proportional to one round-trip, not N.
        let batch_id = match self.catalog().allocate_storage_usage_id(write_ts).await {
            Ok(id) => id,
            Err(err) => {
                tracing::warn!("failed to allocate storage usage batch id: {:?}", err);
                return;
            }
        };

        let updates: Vec<_> = shards_usage
            .by_shard
            .into_iter()
            .map(|(shard_id, shard_usage)| {
                let event = VersionedStorageUsage::new(
                    batch_id,
                    Some(shard_id.to_string()),
                    shard_usage.size_bytes(),
                    collection_timestamp,
                );
                self.catalog().pack_storage_usage_update(event, Diff::ONE)
            })
            .collect();

        let table_updates = self.builtin_table_update().execute(updates);

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let task_span = info_span!(parent: None, "coord::storage_usage_update::table_updates");
        OpenTelemetryContext::obtain().attach_as_parent_to(&task_span);
        task::spawn(|| "storage_usage_update_table_updates", async move {
            table_updates.instrument(task_span).await;
            // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
            if let Err(e) = internal_cmd_tx.send(Message::StorageUsageSchedule) {
                warn!("internal_cmd_rx dropped before we could send: {e:?}");
            }
        });
    }

    #[mz_ore::instrument(level = "debug")]
    async fn storage_usage_prune(&mut self, expired: Vec<BuiltinTableUpdate>) {
        let fut = self.builtin_table_update().execute(expired);
        task::spawn(|| "storage_usage_pruning_apply", async move {
            fut.await;
        });
    }

    pub async fn schedule_storage_usage_collection(&self) {
        // Instead of using an `tokio::timer::Interval`, we calculate the time until the next
        // usage collection and wait for that amount of time. This is so we can keep the intervals
        // consistent even across restarts. If collection takes too long, it is possible that
        // we miss an interval.

        // 1) Deterministically pick some offset within the collection interval to prevent
        // thundering herds across environments.
        const SEED_LEN: usize = 32;
        let mut seed = [0; SEED_LEN];
        for (i, byte) in self
            .catalog()
            .state()
            .config()
            .environment_id
            .organization_id()
            .as_bytes()
            .into_iter()
            .take(SEED_LEN)
            .enumerate()
        {
            seed[i] = *byte;
        }
        let storage_usage_collection_interval_ms: EpochMillis =
            EpochMillis::try_from(self.storage_usage_collection_interval.as_millis())
                .expect("storage usage collection interval must fit into u64");
        let offset =
            rngs::SmallRng::from_seed(seed).random_range(0..storage_usage_collection_interval_ms);
        let now_ts: EpochMillis = self.peek_local_write_ts().await.into();

        // 2) Determine the amount of ms between now and the next collection time.
        let previous_collection_ts =
            (now_ts - (now_ts % storage_usage_collection_interval_ms)) + offset;
        let next_collection_ts = if previous_collection_ts > now_ts {
            previous_collection_ts
        } else {
            previous_collection_ts + storage_usage_collection_interval_ms
        };
        let next_collection_interval = Duration::from_millis(next_collection_ts - now_ts);

        // 3) Sleep for that amount of time, then initiate another storage usage collection.
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        task::spawn(|| "storage_usage_collection", async move {
            tokio::time::sleep(next_collection_interval).await;
            if internal_cmd_tx.send(Message::StorageUsageFetch).is_err() {
                // If sending fails, the main thread has shutdown.
            }
        });
    }

    /// Schedules the next per-object arrangement sizes snapshot.
    ///
    /// Aligns each fire to an `organization_id`-seeded offset within the
    /// interval so collections stay consistent across restarts and don't
    /// synchronize across environments. Sleeps are capped at `MAX_SLEEP`,
    /// so dyncfg changes (interval edits or the `0s` disable sentinel) take
    /// effect within one cap rather than after the full interval.
    pub async fn schedule_arrangement_sizes_collection(&self) {
        const MAX_SLEEP: Duration = Duration::from_secs(60);

        let interval_duration =
            mz_adapter_types::dyncfgs::ARRANGEMENT_SIZE_HISTORY_COLLECTION_INTERVAL
                .get(self.catalog().system_config().dyncfgs());

        // `0s` disables collection. Keep polling so re-enabling takes effect
        // within `MAX_SLEEP` rather than requiring an envd restart.
        if interval_duration.is_zero() {
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "arrangement_sizes_collection_disabled", async move {
                tokio::time::sleep(MAX_SLEEP).await;
                let _ = internal_cmd_tx.send(Message::ArrangementSizesSchedule);
            });
            return;
        }

        const SEED_LEN: usize = 32;
        let mut seed = [0; SEED_LEN];
        for (i, byte) in self
            .catalog()
            .state()
            .config()
            .environment_id
            .organization_id()
            .as_bytes()
            .into_iter()
            .take(SEED_LEN)
            .enumerate()
        {
            seed[i] = *byte;
        }
        let interval_ms: EpochMillis = EpochMillis::try_from(interval_duration.as_millis())
            .expect("arrangement_size_history_collection_interval must fit into u64");
        // `rand::random_range` panics on an empty range.
        let interval_ms = interval_ms.max(1);
        let offset = rngs::SmallRng::from_seed(seed).random_range(0..interval_ms);
        let now_ts: EpochMillis = self.peek_local_write_ts().await.into();

        let previous_collection_ts = (now_ts - (now_ts % interval_ms)) + offset;
        let next_collection_ts = if previous_collection_ts > now_ts {
            previous_collection_ts
        } else {
            previous_collection_ts + interval_ms
        };
        let sleep_for = Duration::from_millis(next_collection_ts - now_ts);

        // Within one cap of the next fire we sleep the remainder and snapshot;
        // further out we sleep the cap and re-enter so a dyncfg change is
        // picked up before committing to a long sleep.
        let (capped_sleep, fire_snapshot) = if sleep_for <= MAX_SLEEP {
            (sleep_for, true)
        } else {
            (MAX_SLEEP, false)
        };

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        task::spawn(|| "arrangement_sizes_collection", async move {
            tokio::time::sleep(capped_sleep).await;
            let msg = if fire_snapshot {
                Message::ArrangementSizesSnapshot
            } else {
                Message::ArrangementSizesSchedule
            };
            // Send is best-effort: if the coordinator is shutting down, drop.
            let _ = internal_cmd_tx.send(msg);
        });
    }

    /// Kicks off a snapshot of `mz_object_arrangement_sizes` for appending to
    /// `mz_object_arrangement_size_history`.
    ///
    /// The persist reads and row preparation are too slow for the coordinator
    /// main loop, so they run on a spawned task. The prepared records come
    /// back as [`Message::ArrangementSizesWrite`] and are appended by
    /// [`Coordinator::arrangement_sizes_write`], which also reschedules the
    /// next collection. An empty or failed snapshot reschedules directly.
    ///
    /// Rows from replicas without fresh introspection data are excluded, so
    /// sizes predating an environmentd or replica restart are not recorded.
    /// See [`Coordinator::fresh_introspection_replicas`].
    #[mz_ore::instrument(level = "debug")]
    async fn arrangement_sizes_snapshot(&self) {
        // Builtin collections are not writable in read-only mode. Skip the
        // cycle but keep rescheduling, mirroring `storage_usage_fetch`, so
        // collection stays alive regardless of how the coordinator leaves
        // read-only mode. The transition is one-way, so
        // `arrangement_sizes_write` needs no check of its own.
        if self.controller.read_only() {
            self.schedule_arrangement_sizes_collection().await;
            return;
        }

        let fresh_size_replicas = self.fresh_introspection_replicas(
            IntrospectionType::ComputeObjectArrangementSizes,
            ARRANGEMENT_SIZES_FRESHNESS_MARGIN,
        );
        let fresh_hydration_replicas = self.fresh_introspection_replicas(
            IntrospectionType::ComputeHydrationTimes,
            ARRANGEMENT_SIZES_FRESHNESS_MARGIN,
        );
        if fresh_size_replicas.is_empty() {
            // No replica has reported sizes in this process yet, so the live
            // collection contains only stale rows (or none). Skip the cycle.
            self.schedule_arrangement_sizes_collection().await;
            return;
        }

        let live_item_id = self.catalog().resolve_builtin_storage_collection(
            &mz_catalog::builtin::MZ_OBJECT_ARRANGEMENT_SIZES_UNIFIED,
        );
        let live_global_id = self.catalog.get_entry(&live_item_id).latest_global_id();
        let hydration_item_id = self
            .catalog()
            .resolve_builtin_storage_collection(&mz_catalog::builtin::MZ_COMPUTE_HYDRATION_TIMES);
        let hydration_global_id = self
            .catalog
            .get_entry(&hydration_item_id)
            .latest_global_id();

        let oracle = self.get_local_timestamp_oracle();
        let storage_collections = Arc::clone(&self.controller.storage_collections);
        let collection_metric = self
            .metrics
            .arrangement_sizes_collection_time_seconds
            .clone();
        let internal_cmd_tx = self.internal_cmd_tx.clone();

        task::spawn(|| "arrangement_sizes_snapshot", async move {
            let collection_metric_timer = collection_metric.start_timer();

            // No read hold is taken, so the reads rely on both collections
            // being retained-metrics objects, whose since lags the upper by
            // `metrics_retention` rather than tracking it closely. If the
            // since still overtakes `read_ts`, the snapshot fails, and the
            // cycle is skipped and retried at the next interval.
            let read_ts = oracle.read_ts().await;
            let live_snapshot = match storage_collections.snapshot(live_global_id, read_ts).await {
                Ok(s) => s,
                Err(e) => {
                    // Unreachable short of a read-policy bug or catalog
                    // corruption, so be loud, but degrade to a skipped cycle
                    // in production.
                    soft_panic_or_log!("arrangement sizes snapshot failed: {e:?}");
                    let _ = internal_cmd_tx.send(Message::ArrangementSizesSchedule);
                    return;
                }
            };
            let hydration_snapshot = match storage_collections
                .snapshot(hydration_global_id, read_ts)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    soft_panic_or_log!("arrangement sizes hydration snapshot failed: {e:?}");
                    let _ = internal_cmd_tx.send(Message::ArrangementSizesSchedule);
                    return;
                }
            };

            let records = arrangement_sizes_records(
                live_snapshot,
                hydration_snapshot,
                &fresh_size_replicas,
                &fresh_hydration_replicas,
            );
            collection_metric_timer.observe_duration();

            let msg = if records.is_empty() {
                Message::ArrangementSizesSchedule
            } else {
                Message::ArrangementSizesWrite(records)
            };
            // It is not an error for this task to outlive `internal_cmd_rx`.
            let _ = internal_cmd_tx.send(msg);
        });
    }

    /// Stamps prepared snapshot records with a shared `collection_timestamp`
    /// and appends them to `mz_object_arrangement_size_history`. Reschedules
    /// the next collection once the append completes.
    #[mz_ore::instrument(level = "debug")]
    async fn arrangement_sizes_write(&mut self, records: Vec<ArrangementSizeRecord>) {
        // Freshness may have been invalidated while the snapshot task ran,
        // e.g. by a cluster event reporting a replica offline. Revalidate so
        // records prepared from a now-untrusted replica's data are dropped.
        let fresh_size_replicas = self.fresh_introspection_replicas(
            IntrospectionType::ComputeObjectArrangementSizes,
            ARRANGEMENT_SIZES_FRESHNESS_MARGIN,
        );
        let records: Vec<_> = records
            .into_iter()
            .filter(|record| fresh_size_replicas.contains(&record.replica_id))
            .collect();
        if records.is_empty() {
            self.schedule_arrangement_sizes_collection().await;
            return;
        }

        // `collection_ts` is stamped after the snapshot so it's always >= the
        // state the rows describe, and monotone across restarts. The snapshot
        // read and this stamp aren't atomic, but the resulting skew is bounded
        // by snapshot latency and negligible at this cadence.
        let collection_ts: EpochMillis = self.get_local_write_ts().await.timestamp.into();
        let collection_datum = Datum::TimestampTz(
            mz_ore::now::to_datetime(collection_ts)
                .try_into()
                .expect("collection_timestamp must fit into TimestampTz"),
        );

        let history_item_id = self
            .catalog()
            .resolve_builtin_table(&mz_catalog::builtin::MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY);

        let updates: Vec<_> = records
            .into_iter()
            .map(|record| {
                let row = Row::pack_slice(&[
                    Datum::String(&record.replica_id),
                    Datum::String(&record.object_id),
                    Datum::Int64(record.size),
                    collection_datum,
                    Datum::from(record.hydration_complete),
                ]);
                BuiltinTableUpdate::row(history_item_id, row, Diff::ONE)
            })
            .collect();

        let row_count = updates.len();
        self.metrics
            .arrangement_sizes_rows_written
            .inc_by(u64::cast_from(row_count));

        // TODO(arrangement-sizes): when the writeable-catalog-server plumbing
        // in https://github.com/MaterializeInc/materialize/pull/35436 lands,
        // append directly on `mz_catalog_server` instead of going through
        // the environmentd builtin-table-update path.
        let fut = self.builtin_table_update().execute(updates);
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let task_span = info_span!(parent: None, "coord::arrangement_sizes_write::table_updates");
        OpenTelemetryContext::obtain().attach_as_parent_to(&task_span);
        task::spawn(|| "arrangement_sizes_write_table_updates", async move {
            fut.instrument(task_span).await;
            if let Err(e) = internal_cmd_tx.send(Message::ArrangementSizesSchedule) {
                warn!("internal_cmd_rx dropped before we could send: {e:?}");
            }
        });

        tracing::debug!(
            "appended {row_count} rows to mz_object_arrangement_size_history at ts {collection_ts}"
        );
    }

    #[mz_ore::instrument(level = "debug")]
    async fn arrangement_sizes_prune(&mut self, expired: Vec<BuiltinTableUpdate>) {
        let fut = self.builtin_table_update().execute(expired);
        task::spawn(|| "arrangement_sizes_pruning_apply", async move {
            fut.await;
        });
    }

    #[mz_ore::instrument(level = "debug")]
    async fn message_command(&mut self, cmd: Command) {
        self.handle_command(cmd).await;
    }

    #[mz_ore::instrument(level = "debug")]
    async fn message_controller(&mut self, message: ControllerResponse) {
        event!(Level::TRACE, message = format!("{:?}", message));
        match message {
            ControllerResponse::PeekNotification(uuid, response, otel_ctx) => {
                self.handle_peek_notification(uuid, response, otel_ctx);
            }
            ControllerResponse::SubscribeResponse(sink_id, response) => {
                if let Some(ActiveComputeSink::Subscribe(active_subscribe)) =
                    self.active_compute_sinks.get_mut(&sink_id)
                {
                    let finished = active_subscribe.process_response(response);
                    if finished {
                        let retire_notify = self
                            .retire_compute_sinks(btreemap! {
                                sink_id => ActiveComputeSinkRetireReason::Finished,
                            })
                            .await;
                        // `retire_compute_sinks` waits before sending the terminal
                        // SUBSCRIBE response. There is no separate statement response here.
                        drop(retire_notify);
                    }

                    soft_assert_or_log!(
                        !self.introspection_subscribes.contains_key(&sink_id),
                        "`sink_id` {sink_id} unexpectedly found in both `active_subscribes` \
                         and `introspection_subscribes`",
                    );
                } else if self.introspection_subscribes.contains_key(&sink_id) {
                    self.handle_introspection_subscribe_batch(sink_id, response)
                        .await;
                } else {
                    // Cancellation may cause us to receive responses for subscribes no longer
                    // tracked, so we quietly ignore them.
                }
            }
            ControllerResponse::CopyToResponse(sink_id, response) => {
                match self.drop_compute_sink(sink_id).await {
                    Some((ActiveComputeSink::CopyTo(active_copy_to), _write_notify)) => {
                        active_copy_to.retire_with_response(response);
                    }
                    _ => {
                        // Cancellation may cause us to receive responses for subscribes no longer
                        // tracked, so we quietly ignore them.
                    }
                }
            }
            ControllerResponse::WatchSetFinished(ws_ids) => {
                let now = self.now();
                for ws_id in ws_ids {
                    let Some((conn_id, rsp)) = self.installed_watch_sets.remove(&ws_id) else {
                        continue;
                    };
                    self.connection_watch_sets
                        .get_mut(&conn_id)
                        .expect("corrupted coordinator state: unknown connection id")
                        .remove(&ws_id);
                    if self.connection_watch_sets[&conn_id].is_empty() {
                        self.connection_watch_sets.remove(&conn_id);
                    }

                    match rsp {
                        WatchSetResponse::StatementDependenciesReady(id, ev) => {
                            self.record_statement_lifecycle_event(&id, &ev, now);
                        }
                        WatchSetResponse::AlterSinkReady(ctx) => {
                            self.sequence_alter_sink_finish(ctx).await;
                        }
                        WatchSetResponse::AlterMaterializedViewReady(ctx) => {
                            self.sequence_alter_materialized_view_apply_replacement_finish(ctx)
                                .await;
                        }
                    }
                }
            }
        }
    }

    #[mz_ore::instrument(level = "debug")]
    async fn message_purified_statement_ready(
        &mut self,
        PurifiedStatementReady {
            ctx,
            result,
            params,
            mut plan_validity,
            original_stmt,
            otel_ctx,
        }: PurifiedStatementReady,
    ) {
        otel_ctx.attach_as_parent();

        // Ensure that all dependencies still exist after purification, as a
        // `DROP CONNECTION` or other `DROP` may have sneaked in. If any have gone missing, we
        // repurify the original statement. This will either produce a nice
        // "unknown connector" error, or pick up a new connector that has
        // replaced the dropped connector.
        //
        // n.b. an `ALTER CONNECTION` occurring during purification is OK
        // because we always look up/populate a connection's state after
        // committing to the catalog, so are guaranteed to see the connection's
        // most recent version.
        if plan_validity.check(self.catalog()).is_err() {
            self.handle_execute_inner(original_stmt, params, ctx).await;
            return;
        }

        let purified_statement = match result {
            Ok(ok) => ok,
            Err(e) => return ctx.retire(Err(e)),
        };

        let plan = match purified_statement {
            PurifiedStatement::PurifiedCreateSource {
                create_progress_subsource_stmt,
                create_source_stmt,
                subsources,
                available_source_references,
            } => self
                .plan_purified_create_source(
                    &ctx,
                    params,
                    create_progress_subsource_stmt,
                    create_source_stmt,
                    subsources,
                    available_source_references,
                )
                .await
                .map(|(plan, resolved_ids)| (plan, resolved_ids, ResolvedIds::empty())),
            PurifiedStatement::PurifiedAlterSourceAddSubsources {
                source_name,
                options,
                subsources,
            } => self
                .plan_purified_alter_source_add_subsource(
                    ctx.session(),
                    params,
                    source_name,
                    options,
                    subsources,
                )
                .await
                .map(|(plan, resolved_ids)| (plan, resolved_ids, ResolvedIds::empty())),
            PurifiedStatement::PurifiedAlterSourceRefreshReferences {
                source_name,
                available_source_references,
            } => self
                .plan_purified_alter_source_refresh_references(
                    ctx.session(),
                    params,
                    source_name,
                    available_source_references,
                )
                .map(|(plan, resolved_ids)| (plan, resolved_ids, ResolvedIds::empty())),
            o @ (PurifiedStatement::PurifiedAlterSource { .. }
            | PurifiedStatement::PurifiedCreateSink(..)
            | PurifiedStatement::PurifiedCreateTableFromSource { .. }) => {
                // Unify these into a `Statement`.
                let stmt = match o {
                    PurifiedStatement::PurifiedAlterSource { alter_source_stmt } => {
                        Statement::AlterSource(alter_source_stmt)
                    }
                    PurifiedStatement::PurifiedCreateTableFromSource { stmt } => {
                        Statement::CreateTableFromSource(stmt)
                    }
                    PurifiedStatement::PurifiedCreateSink(stmt) => Statement::CreateSink(stmt),
                    PurifiedStatement::PurifiedCreateSource { .. }
                    | PurifiedStatement::PurifiedAlterSourceAddSubsources { .. }
                    | PurifiedStatement::PurifiedAlterSourceRefreshReferences { .. } => {
                        unreachable!("not part of exterior match stmt")
                    }
                };

                // Determine all dependencies, not just those in the statement
                // itself.
                let catalog = self.catalog().for_session(ctx.session());
                let resolved_ids = mz_sql::names::visit_dependencies(&catalog, &stmt);
                self.plan_statement(ctx.session(), stmt, &params, &resolved_ids)
                    .map(|(plan, sql_impl_ids)| (plan, resolved_ids, sql_impl_ids))
            }
        };

        match plan {
            Ok((plan, resolved_ids, sql_impl_ids)) => {
                self.sequence_plan(ctx, plan, resolved_ids, sql_impl_ids)
                    .await
            }
            Err(e) => ctx.retire(Err(e)),
        }
    }

    #[mz_ore::instrument(level = "debug")]
    async fn message_create_connection_validation_ready(
        &mut self,
        CreateConnectionValidationReady {
            mut ctx,
            result,
            connection_id,
            connection_gid,
            mut plan_validity,
            otel_ctx,
            resolved_ids,
        }: CreateConnectionValidationReady,
    ) {
        otel_ctx.attach_as_parent();

        // Ensure that all dependencies still exist after validation, as a
        // `DROP SECRET` may have sneaked in.
        //
        // WARNING: If we support `ALTER SECRET`, we'll need to also check
        // for connectors that were altered while we were purifying.
        if let Err(e) = plan_validity.check(self.catalog()) {
            if self.secrets_controller.delete(connection_id).await.is_ok() {
                self.caching_secrets_reader.invalidate(connection_id);
            }
            return ctx.retire(Err(e));
        }

        let plan = match result {
            Ok(ok) => ok,
            Err(e) => {
                if self.secrets_controller.delete(connection_id).await.is_ok() {
                    self.caching_secrets_reader.invalidate(connection_id);
                }
                return ctx.retire(Err(e));
            }
        };

        let result = self
            .sequence_create_connection_stage_finish(
                &mut ctx,
                connection_id,
                connection_gid,
                plan,
                resolved_ids,
            )
            .await;
        ctx.retire(result);
    }

    #[mz_ore::instrument(level = "debug")]
    async fn message_alter_connection_validation_ready(
        &mut self,
        AlterConnectionValidationReady {
            mut ctx,
            result,
            connection_id,
            connection_gid: _,
            mut plan_validity,
            otel_ctx,
            resolved_ids: _,
        }: AlterConnectionValidationReady,
    ) {
        otel_ctx.attach_as_parent();

        // Ensure that all dependencies still exist after validation, as a
        // `DROP SECRET` may have sneaked in.
        //
        // WARNING: If we support `ALTER SECRET`, we'll need to also check
        // for connectors that were altered while we were purifying.
        if let Err(e) = plan_validity.check(self.catalog()) {
            return ctx.retire(Err(e));
        }

        let conn = match result {
            Ok(ok) => ok,
            Err(e) => {
                return ctx.retire(Err(e));
            }
        };

        let result = self
            .sequence_alter_connection_stage_finish(ctx.session_mut(), connection_id, conn)
            .await;
        ctx.retire(result);
    }

    #[mz_ore::instrument(level = "debug")]
    async fn message_cluster_event(&mut self, event: ClusterEvent) {
        event!(Level::TRACE, event = format!("{:?}", event));

        if let Some(segment_client) = &self.segment_client {
            let env_id = &self.catalog().config().environment_id;
            let mut properties = json!({
                "cluster_id": event.cluster_id.to_string(),
                "replica_id": event.replica_id.to_string(),
                "process_id": event.process_id,
                "status": event.status.as_kebab_case_str(),
            });
            match event.status {
                ClusterStatus::Online => (),
                ClusterStatus::Offline(reason) => {
                    let properties = match &mut properties {
                        serde_json::Value::Object(map) => map,
                        _ => unreachable!(),
                    };
                    properties.insert(
                        "reason".into(),
                        json!(reason.display_or("unknown").to_string()),
                    );
                }
            };
            segment_client.environment_track(
                env_id,
                "Cluster Changed Status",
                properties,
                EventDetails {
                    timestamp: Some(event.time),
                    ..Default::default()
                },
            );
        }

        // It is possible that we receive a status update for a replica that has
        // already been dropped from the catalog. Just ignore these events.
        let Some(replica_statuses) = self
            .cluster_replica_statuses
            .try_get_cluster_replica_statuses(event.cluster_id, event.replica_id)
        else {
            return;
        };

        let old_process_status = &replica_statuses[&event.process_id];
        let status_changed = event.status != old_process_status.status;
        let restart_count_changed = event.restart_count != old_process_status.restart_count;

        // We mirror the restart count in memory even when only it changes (and the
        // status stays the same), so the 0dt caught-up check can detect replica
        // restarts it would otherwise miss by only sampling the status. The status
        // history and the status-changed notice are keyed on the status itself, so
        // we only touch those when the status actually changes.
        //
        // NOTE: The 0dt stability gate detects flaps by watching a process's
        // status-change `time` advance between checks. That only works because we
        // freeze `time` on no-op events, i.e. we return early here instead of
        // rewriting the record when neither the status nor the restart count
        // changed.
        if !status_changed && !restart_count_changed {
            return;
        }

        if status_changed && !self.controller.read_only() {
            let offline_reason = match event.status {
                ClusterStatus::Online => None,
                ClusterStatus::Offline(None) => None,
                ClusterStatus::Offline(Some(reason)) => Some(reason.to_string()),
            };
            let row = Row::pack_slice(&[
                Datum::String(&event.replica_id.to_string()),
                Datum::UInt64(event.process_id),
                Datum::String(event.status.as_kebab_case_str()),
                Datum::from(offline_reason.as_deref()),
                Datum::TimestampTz(event.time.try_into().expect("must fit")),
            ]);
            self.controller.storage.append_introspection_updates(
                IntrospectionType::ReplicaStatusHistory,
                vec![(row, Diff::ONE)],
            );
        }

        // Capture the rolled-up replica status before the update so we can tell
        // whether the user-visible status changed. Only needed for the notice.
        let old_replica_status = status_changed
            .then(|| ClusterReplicaStatuses::cluster_replica_status(replica_statuses));

        let new_process_status = ClusterReplicaProcessStatus {
            status: event.status,
            restart_count: event.restart_count,
            time: event.time,
        };
        self.cluster_replica_statuses.ensure_cluster_status(
            event.cluster_id,
            event.replica_id,
            event.process_id,
            new_process_status,
        );

        // The replica's introspection subscribes may keep serving data written
        // for its previous incarnation until their failure responses are
        // processed. Invalidate freshness eagerly so consumers like the
        // arrangement sizes history don't record that data as current.
        if !matches!(event.status, ClusterStatus::Online) || restart_count_changed {
            self.invalidate_introspection_freshness(event.replica_id);
        }

        if let Some(old_replica_status) = old_replica_status {
            let cluster = self.catalog().get_cluster(event.cluster_id);
            let replica = cluster.replica(event.replica_id).expect("Replica exists");
            let new_replica_status = self
                .cluster_replica_statuses
                .get_cluster_replica_status(event.cluster_id, event.replica_id);

            if old_replica_status != new_replica_status {
                let notifier = self.broadcast_notice_tx();
                let notice = AdapterNotice::ClusterReplicaStatusChanged {
                    cluster: cluster.name.clone(),
                    replica: replica.name.clone(),
                    status: new_replica_status,
                    time: event.time,
                };
                notifier(notice);
            }
        }
    }

    #[mz_ore::instrument(level = "debug")]
    /// Linearizes sending the results of a read transaction by,
    ///   1. Holding back any results that were executed at some point in the future, until the
    ///   containing timeline has advanced to that point in the future.
    ///   2. Confirming that we are still the current leader before sending results to the client.
    async fn message_linearize_reads(&mut self) {
        let mut shortest_wait = Duration::MAX;
        let mut ready_txns = Vec::new();

        // Cache for `TimestampOracle::read_ts` calls. These are somewhat
        // expensive so we cache the value. This is correct since all we're
        // risking is being too conservative. We will not accidentally "release"
        // a result too early.
        let mut cached_oracle_ts = BTreeMap::new();

        for (conn_id, mut read_txn) in std::mem::take(&mut self.pending_linearize_read_txns) {
            if let TimestampContext::TimelineTimestamp {
                timeline,
                chosen_ts,
                oracle_ts,
            } = read_txn.timestamp_context()
            {
                let oracle_ts = match oracle_ts {
                    Some(oracle_ts) => oracle_ts,
                    None => {
                        // There was no oracle timestamp, so no need to delay.
                        ready_txns.push(read_txn);
                        continue;
                    }
                };

                if chosen_ts <= oracle_ts {
                    // Chosen ts was already <= the oracle ts, so we're good
                    // to go!
                    ready_txns.push(read_txn);
                    continue;
                }

                // See what the oracle timestamp is now and delay when needed.
                let current_oracle_ts = cached_oracle_ts.entry(timeline.clone());
                let current_oracle_ts = match current_oracle_ts {
                    btree_map::Entry::Vacant(entry) => {
                        let timestamp_oracle = self.get_timestamp_oracle(timeline);
                        let read_ts = timestamp_oracle.read_ts().await;
                        entry.insert(read_ts.clone());
                        read_ts
                    }
                    btree_map::Entry::Occupied(entry) => entry.get().clone(),
                };

                if *chosen_ts <= current_oracle_ts {
                    ready_txns.push(read_txn);
                } else {
                    let wait =
                        Duration::from_millis(chosen_ts.saturating_sub(current_oracle_ts).into());
                    if wait < shortest_wait {
                        shortest_wait = wait;
                    }
                    read_txn.num_requeues += 1;
                    self.pending_linearize_read_txns.insert(conn_id, read_txn);
                }
            } else {
                ready_txns.push(read_txn);
            }
        }

        if !ready_txns.is_empty() {
            // Sniff out one ctx, this is where tracing breaks down because we
            // process all outstanding txns as a batch here.
            let otel_ctx = ready_txns.first().expect("known to exist").otel_ctx.clone();
            let span = tracing::debug_span!("message_linearize_reads");
            otel_ctx.attach_as_parent_to(&span);

            let now = Instant::now();
            for ready_txn in ready_txns {
                let span = tracing::debug_span!("retire_read_results");
                ready_txn.otel_ctx.attach_as_parent_to(&span);
                let _entered = span.enter();
                self.metrics
                    .linearize_message_seconds
                    .with_label_values(&[
                        ready_txn.txn.label(),
                        if ready_txn.num_requeues == 0 {
                            "true"
                        } else {
                            "false"
                        },
                    ])
                    .observe((now - ready_txn.created).as_secs_f64());
                if let Some((ctx, result)) = ready_txn.txn.finish() {
                    ctx.retire(result);
                }
            }
        }

        if !self.pending_linearize_read_txns.is_empty() {
            // Cap wait time to 1s, then signal a re-check. `serve` awaits this
            // below group commit; see its linearize branch for why.
            let remaining_ms = std::cmp::min(shortest_wait, Duration::from_millis(1_000));
            let linearize_reads_notify = Arc::clone(&self.linearize_reads_notify);
            task::spawn(|| "deferred_read_txns", async move {
                tokio::time::sleep(remaining_ms).await;
                linearize_reads_notify.notify_one();
            });
        }
    }
}

/// Builds history records from snapshots of `mz_object_arrangement_sizes` and
/// `mz_compute_hydration_times`.
///
/// Each `(replica_id, object_id)` pair is recorded with a
/// `hydration_complete` flag: `true` once the pair's initial hydration on that
/// replica is finished (`time_ns IS NOT NULL`), `false` while still building.
/// Consumers that want only stable sizes should filter
/// `WHERE hydration_complete`.
///
/// Rows from replicas outside `fresh_size_replicas` are dropped, and the
/// hydration flag is only trusted for replicas in `fresh_hydration_replicas`.
/// Rows for other replicas may predate an environmentd or replica restart.
///
/// Rows with a size of 0 (arrangements below the live collection's 5 MiB
/// quantization threshold) are not recorded.
fn arrangement_sizes_records(
    mut live_snapshot: Vec<(Row, StorageDiff)>,
    mut hydration_snapshot: Vec<(Row, StorageDiff)>,
    fresh_size_replicas: &BTreeSet<String>,
    fresh_hydration_replicas: &BTreeSet<String>,
) -> Vec<ArrangementSizeRecord> {
    differential_dataflow::consolidation::consolidate(&mut live_snapshot);
    differential_dataflow::consolidation::consolidate(&mut hydration_snapshot);

    let mut datum_vec = mz_repr::DatumVec::new();

    // Column positions in `mz_compute_hydration_times`.
    const HYDRATION_COL_REPLICA_ID: usize = 0;
    const HYDRATION_COL_OBJECT_ID: usize = 1;
    const HYDRATION_COL_TIME_NS: usize = 2;
    const HYDRATION_COL_COUNT: usize = 3;

    let mut hydrated: BTreeSet<(String, String)> = BTreeSet::new();
    for (row, diff) in &hydration_snapshot {
        if *diff != 1 {
            continue;
        }
        let datums = datum_vec.borrow_with(row);
        if datums.len() < HYDRATION_COL_COUNT {
            continue;
        }
        if datums[HYDRATION_COL_TIME_NS].is_null() {
            continue;
        }
        let replica_id = datums[HYDRATION_COL_REPLICA_ID].unwrap_str();
        if !fresh_hydration_replicas.contains(replica_id) {
            continue;
        }
        hydrated.insert((
            replica_id.to_string(),
            datums[HYDRATION_COL_OBJECT_ID].unwrap_str().to_string(),
        ));
    }

    // Column positions in `mz_object_arrangement_sizes`.
    const LIVE_COL_REPLICA_ID: usize = 0;
    const LIVE_COL_OBJECT_ID: usize = 1;
    const LIVE_COL_SIZE: usize = 2;
    const LIVE_COL_COUNT: usize = 3;

    let mut skipped_malformed: u64 = 0;
    let mut skipped_null_size: u64 = 0;
    let mut skipped_zero_size: u64 = 0;
    let mut skipped_stale_replica: u64 = 0;
    let mut records = Vec::with_capacity(live_snapshot.len());
    for (row, diff) in &live_snapshot {
        if *diff != 1 {
            continue;
        }
        let datums = datum_vec.borrow_with(row);
        // Surface schema drift via a warn log below rather than silently
        // skipping entire snapshots.
        if datums.len() != LIVE_COL_COUNT {
            skipped_malformed += 1;
            continue;
        }
        let replica_id = datums[LIVE_COL_REPLICA_ID].unwrap_str();
        if !fresh_size_replicas.contains(replica_id) {
            skipped_stale_replica += 1;
            continue;
        }
        let object_id = datums[LIVE_COL_OBJECT_ID].unwrap_str();
        let size_datum = datums[LIVE_COL_SIZE];
        // The history table's `size` is non-null; fabricating zero would
        // be misleading, so drop.
        if size_datum.is_null() {
            skipped_null_size += 1;
            continue;
        }
        // A quantized size of 0 means "below 5 MiB". The live collection
        // keeps such rows so small objects stay visible, but recording them
        // every cycle would bloat the history with rows carrying no signal.
        if size_datum.unwrap_int64() == 0 {
            skipped_zero_size += 1;
            continue;
        }
        let hydration_complete =
            hydrated.contains(&(replica_id.to_string(), object_id.to_string()));
        records.push(ArrangementSizeRecord {
            replica_id: replica_id.to_string(),
            object_id: object_id.to_string(),
            size: size_datum.unwrap_int64(),
            hydration_complete,
        });
    }
    if skipped_malformed > 0 {
        warn!(
            "mz_object_arrangement_sizes schema drift: skipped {skipped_malformed} rows \
             with unexpected arity"
        );
    }
    if skipped_null_size > 0 {
        tracing::debug!("skipped {skipped_null_size} live rows with null size");
    }
    if skipped_zero_size > 0 {
        tracing::debug!("skipped {skipped_zero_size} live rows with zero size");
    }
    if skipped_stale_replica > 0 {
        tracing::debug!(
            "skipped {skipped_stale_replica} live rows from replicas without fresh \
             introspection data"
        );
    }
    records
}

#[cfg(test)]
mod arrangement_sizes_records_tests {
    use std::collections::BTreeSet;

    use mz_repr::{Datum, Row};

    use super::arrangement_sizes_records;

    fn live_row(replica_id: &str, object_id: &str, size: Option<i64>) -> Row {
        Row::pack_slice(&[
            Datum::String(replica_id),
            Datum::String(object_id),
            size.map_or(Datum::Null, Datum::Int64),
        ])
    }

    fn hydration_row(replica_id: &str, object_id: &str, hydrated: bool) -> Row {
        Row::pack_slice(&[
            Datum::String(replica_id),
            Datum::String(object_id),
            if hydrated {
                Datum::UInt64(1)
            } else {
                Datum::Null
            },
        ])
    }

    fn replicas(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|id| id.to_string()).collect()
    }

    #[mz_ore::test]
    fn hydration_flag_per_pair() {
        let live = vec![
            (live_row("u1", "u100", Some(10)), 1),
            (live_row("u1", "u200", Some(20)), 1),
        ];
        let hydration = vec![
            (hydration_row("u1", "u100", true), 1),
            (hydration_row("u1", "u200", false), 1),
        ];
        let fresh = replicas(&["u1"]);
        let records = arrangement_sizes_records(live, hydration, &fresh, &fresh);
        assert_eq!(records.len(), 2);
        assert!(
            records
                .iter()
                .any(|r| r.object_id == "u100" && r.hydration_complete)
        );
        assert!(
            records
                .iter()
                .any(|r| r.object_id == "u200" && !r.hydration_complete)
        );
    }

    #[mz_ore::test]
    fn skips_malformed_null_and_retracted() {
        let live = vec![
            // Wrong arity.
            (Row::pack_slice(&[Datum::String("u1")]), 1),
            // Null size.
            (live_row("u1", "u100", None), 1),
            // Retracted by consolidation.
            (live_row("u1", "u200", Some(20)), 1),
            (live_row("u1", "u200", Some(20)), -1),
            (live_row("u1", "u300", Some(30)), 1),
        ];
        let fresh = replicas(&["u1"]);
        let records = arrangement_sizes_records(live, Vec::new(), &fresh, &fresh);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].object_id, "u300");
        assert_eq!(records[0].size, 30);
        assert!(!records[0].hydration_complete);
    }

    #[mz_ore::test]
    fn skips_zero_size_rows() {
        // Size 0 means "below the live collection's quantization threshold".
        // Such objects stay visible live but are not recorded in the history.
        let live = vec![
            (live_row("u1", "u100", Some(0)), 1),
            (live_row("u1", "u200", Some(10485760)), 1),
        ];
        let fresh = replicas(&["u1"]);
        let records = arrangement_sizes_records(live, Vec::new(), &fresh, &fresh);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].object_id, "u200");
    }

    #[mz_ore::test]
    fn skips_rows_from_stale_replicas() {
        // u1 has fresh introspection data, u2's rows predate a restart.
        let live = vec![
            (live_row("u1", "u100", Some(10)), 1),
            (live_row("u2", "u100", Some(99)), 1),
        ];
        let hydration = vec![
            (hydration_row("u1", "u100", true), 1),
            (hydration_row("u2", "u100", true), 1),
        ];
        let fresh = replicas(&["u1"]);
        let records = arrangement_sizes_records(live, hydration, &fresh, &fresh);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].replica_id, "u1");
        assert!(records[0].hydration_complete);
    }

    #[mz_ore::test]
    fn stale_hydration_data_is_not_trusted() {
        // u1's sizes subscribe is fresh but its hydration subscribe is not,
        // so its stale "hydrated" row must not mark the record complete.
        let live = vec![(live_row("u1", "u100", Some(10)), 1)];
        let hydration = vec![(hydration_row("u1", "u100", true), 1)];
        let records =
            arrangement_sizes_records(live, hydration, &replicas(&["u1"]), &replicas(&[]));
        assert_eq!(records.len(), 1);
        assert!(!records[0].hydration_complete);
    }
}
