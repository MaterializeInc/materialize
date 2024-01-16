// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::sync::Arc;

use mz_controller_types::ClusterId;
use mz_expr::CollectionPlan;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::CatalogCluster;
// Import `plan` module, but only import select elements to avoid merge conflicts on use statements.
use mz_catalog::memory::objects::CatalogItem;
use mz_sql::plan;
use mz_sql::plan::QueryWhen;
use mz_transform::{EmptyStatisticsOracle, StatisticsOracle};
use tracing::Instrument;
use tracing::{event, warn, Level};

use crate::command::ExecuteResponse;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::peek::{self, PeekDataflowPlan, PlannedPeek};
use crate::coord::sequencer::inner::{check_log_reads, return_if_err};
use crate::coord::timeline::TimelineContext;
use crate::coord::timestamp_selection::{
    TimestampContext, TimestampDetermination, TimestampProvider,
};
use crate::coord::{
    Coordinator, ExecuteContext, Message, PeekStage, PeekStageFinish, PeekStageOptimize,
    PeekStageRealTimeRecency, PeekStageTimestamp, PeekStageValidate, PlanValidity,
    RealTimeRecencyContext, TargetCluster,
};
use crate::error::AdapterError;
use crate::notice::AdapterNotice;
use crate::optimize::{self, Optimize, OptimizerConfig};
use crate::session::{Session, TransactionOps, TransactionStatus};
use crate::statement_logging::StatementLifecycleEvent;

impl Coordinator {
    /// Sequence a peek, determining a timestamp and the most efficient dataflow interaction.
    ///
    /// Peeks are sequenced by assigning a timestamp for evaluation, and then determining and
    /// deploying the most efficient evaluation plan. The peek could evaluate to a constant,
    /// be a simple read out of an existing arrangement, or required a new dataflow to build
    /// the results to return.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_peek(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::SelectPlan,
        target_cluster: TargetCluster,
    ) {
        event!(Level::TRACE, plan = format!("{:?}", plan));

        self.sequence_peek_stage(
            ctx,
            OpenTelemetryContext::obtain(),
            PeekStage::Validate(PeekStageValidate {
                plan,
                target_cluster,
            }),
        )
        .await;
    }

    /// Processes as many `peek` stages as possible.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_peek_stage(
        &mut self,
        mut ctx: ExecuteContext,
        root_otel_ctx: OpenTelemetryContext,
        mut stage: PeekStage,
    ) {
        // Process the current stage and allow for processing the next.
        loop {
            event!(Level::TRACE, stage = format!("{:?}", stage));

            // Always verify peek validity. This is cheap, and prevents programming errors
            // if we move any stages off thread.
            if let Some(validity) = stage.validity() {
                if let Err(err) = validity.check(self.catalog()) {
                    ctx.retire(Err(err));
                    return;
                }
            }

            (ctx, stage) = match stage {
                PeekStage::Validate(stage) => {
                    let next =
                        return_if_err!(self.peek_stage_validate(ctx.session_mut(), stage), ctx);

                    (ctx, PeekStage::Timestamp(next))
                }
                PeekStage::Timestamp(stage) => {
                    self.peek_stage_timestamp(ctx, root_otel_ctx.clone(), stage)
                        .await;
                    return;
                }
                PeekStage::Optimize(stage) => {
                    self.peek_stage_optimize(ctx, root_otel_ctx.clone(), stage)
                        .await;
                    return;
                }
                PeekStage::RealTimeRecency(stage) => {
                    match self.peek_stage_real_time_recency(ctx, root_otel_ctx.clone(), stage) {
                        Some((ctx, next)) => (ctx, PeekStage::Finish(next)),
                        None => return,
                    }
                }
                PeekStage::Finish(stage) => {
                    let res = self.peek_stage_finish(&mut ctx, stage).await;
                    ctx.retire(res);
                    return;
                }
            }
        }
    }

    /// Do some simple validation. We must defer most of it until after any off-thread work.
    #[tracing::instrument(level = "debug", skip_all)]
    fn peek_stage_validate(
        &mut self,
        session: &Session,
        PeekStageValidate {
            plan,
            target_cluster,
        }: PeekStageValidate,
    ) -> Result<PeekStageTimestamp, AdapterError> {
        let plan::SelectPlan {
            source,
            when,
            finishing,
            copy_to,
        } = plan;

        // Collect optimizer parameters.
        let catalog = self.owned_catalog();
        let cluster = catalog.resolve_target_cluster(target_cluster, session)?;
        let compute_instance = self
            .instance_snapshot(cluster.id())
            .expect("compute instance does not exist");
        let view_id = self.allocate_transient_id()?;
        let index_id = self.allocate_transient_id()?;
        let optimizer_config = OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this SELECT.
        let optimizer = optimize::peek::Optimizer::new(
            Arc::clone(&catalog),
            compute_instance,
            finishing.clone(),
            view_id,
            index_id,
            optimizer_config,
        );

        let target_replica_name = session.vars().cluster_replica();
        let mut target_replica = target_replica_name
            .map(|name| {
                cluster
                    .replica_id(name)
                    .ok_or(AdapterError::UnknownClusterReplica {
                        cluster_name: cluster.name.clone(),
                        replica_name: name.to_string(),
                    })
            })
            .transpose()?;

        if cluster.replicas().next().is_none() {
            return Err(AdapterError::NoClusterReplicasAvailable(
                cluster.name.clone(),
            ));
        }

        let source_ids = source.depends_on();
        let mut timeline_context = self.validate_timeline_context(source_ids.clone())?;
        if matches!(timeline_context, TimelineContext::TimestampIndependent)
            && source.contains_temporal()
        {
            // If the source IDs are timestamp independent but the query contains temporal functions,
            // then the timeline context needs to be upgraded to timestamp dependent. This is
            // required because `source_ids` doesn't contain functions.
            timeline_context = TimelineContext::TimestampDependent;
        }
        let in_immediate_multi_stmt_txn = session.transaction().is_in_multi_statement_transaction()
            && when == QueryWhen::Immediately;

        let notices = check_log_reads(
            &catalog,
            cluster,
            &source_ids,
            &mut target_replica,
            session.vars(),
        )?;
        session.add_notices(notices);

        let validity = PlanValidity {
            transient_revision: catalog.transient_revision(),
            dependency_ids: source_ids.clone(),
            cluster_id: Some(cluster.id()),
            replica_id: target_replica,
            role_metadata: session.role_metadata().clone(),
        };

        Ok(PeekStageTimestamp {
            validity,
            source,
            copy_to,
            source_ids,
            when,
            target_replica,
            timeline_context,
            in_immediate_multi_stmt_txn,
            optimizer,
        })
    }

    /// Determine a linearized read timestamp (from a `TimestampOracle`), if
    /// needed.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn peek_stage_timestamp(
        &mut self,
        ctx: ExecuteContext,
        root_otel_ctx: OpenTelemetryContext,
        PeekStageTimestamp {
            validity,
            source,
            copy_to,
            source_ids,
            when,
            target_replica,
            timeline_context,
            in_immediate_multi_stmt_txn,
            optimizer,
        }: PeekStageTimestamp,
    ) {
        let isolation_level = ctx.session.vars().transaction_isolation().clone();
        let linearized_timeline =
            Coordinator::get_linearized_timeline(&isolation_level, &when, &timeline_context);

        let internal_cmd_tx = self.internal_cmd_tx.clone();

        let build_optimize_stage = move |oracle_read_ts: Option<Timestamp>| -> PeekStageOptimize {
            PeekStageOptimize {
                validity,
                source,
                copy_to,
                source_ids,
                when,
                target_replica,
                timeline_context,
                oracle_read_ts,
                in_immediate_multi_stmt_txn,
                optimizer,
            }
        };

        match linearized_timeline {
            Some(timeline) => {
                let shared_oracle = self.get_shared_timestamp_oracle(&timeline);

                if let Some(shared_oracle) = shared_oracle {
                    // We can do it in an async task, because we can ship off
                    // the timetamp oracle.

                    let span = tracing::debug_span!("linearized timestamp task");
                    mz_ore::task::spawn(|| "linearized timestamp task", async move {
                        let oracle_read_ts = shared_oracle.read_ts().instrument(span).await;
                        let stage = build_optimize_stage(Some(oracle_read_ts));

                        let stage = PeekStage::Optimize(stage);
                        // Ignore errors if the coordinator has shut down.
                        let _ = internal_cmd_tx.send(Message::PeekStageReady {
                            ctx,
                            otel_ctx: root_otel_ctx,
                            stage,
                        });
                    });
                } else {
                    // Timestamp oracle can't be shipped to an async task, we
                    // have to do it here.
                    let oracle = self.get_timestamp_oracle(&timeline);
                    let oracle_read_ts = oracle.read_ts().await;
                    let stage = build_optimize_stage(Some(oracle_read_ts));

                    let stage = PeekStage::Optimize(stage);
                    // Ignore errors if the coordinator has shut down.
                    let _ = internal_cmd_tx.send(Message::PeekStageReady {
                        ctx,
                        otel_ctx: root_otel_ctx,
                        stage,
                    });
                }
            }
            None => {
                let stage = build_optimize_stage(None);
                let stage = PeekStage::Optimize(stage);
                // Ignore errors if the coordinator has shut down.
                let _ = internal_cmd_tx.send(Message::PeekStageReady {
                    ctx,
                    otel_ctx: root_otel_ctx,
                    stage,
                });
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn peek_stage_optimize(
        &mut self,
        ctx: ExecuteContext,
        root_otel_ctx: OpenTelemetryContext,
        mut stage: PeekStageOptimize,
    ) {
        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let internal_cmd_tx = self.internal_cmd_tx.clone();

        // TODO: Is there a way to avoid making two dataflow_builders (the second is in
        // optimize_peek)?
        let id_bundle = self
            .dataflow_builder(stage.optimizer.cluster_id())
            .sufficient_collections(&stage.source_ids);
        // Although we have added `sources.depends_on()` to the validity already, also add the
        // sufficient collections for safety.
        stage.validity.dependency_ids.extend(id_bundle.iter());

        let stats = {
            match self
                .determine_timestamp(
                    ctx.session(),
                    &id_bundle,
                    &stage.when,
                    stage.optimizer.cluster_id(),
                    &stage.timeline_context,
                    stage.oracle_read_ts.clone(),
                    None,
                )
                .await
            {
                Err(_) => Box::new(EmptyStatisticsOracle),
                Ok(query_as_of) => self
                    .statistics_oracle(
                        ctx.session(),
                        &stage.source_ids,
                        query_as_of.timestamp_context.antichain(),
                        true,
                    )
                    .await
                    .unwrap_or_else(|_| Box::new(EmptyStatisticsOracle)),
            }
        };

        let span = tracing::debug_span!("optimize peek task");

        mz_ore::task::spawn_blocking(
            || "optimize peek",
            move || {
                let _guard = span.enter();

                match Self::optimize_peek(ctx.session(), stats, id_bundle, stage) {
                    Ok(stage) => {
                        let stage = PeekStage::RealTimeRecency(stage);
                        // Ignore errors if the coordinator has shut down.
                        let _ = internal_cmd_tx.send(Message::PeekStageReady {
                            ctx,
                            otel_ctx: root_otel_ctx,
                            stage,
                        });
                    }
                    Err(err) => ctx.retire(Err(err)),
                }
            },
        );
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn optimize_peek(
        session: &Session,
        stats: Box<dyn StatisticsOracle>,
        id_bundle: CollectionIdBundle,
        PeekStageOptimize {
            validity,
            source,
            copy_to,
            source_ids,
            when,
            target_replica,
            timeline_context,
            oracle_read_ts,
            in_immediate_multi_stmt_txn,
            mut optimizer,
        }: PeekStageOptimize,
    ) -> Result<PeekStageRealTimeRecency, AdapterError> {
        let local_mir_plan = optimizer.catch_unwind_optimize(source)?;
        let local_mir_plan = local_mir_plan.resolve(session, stats);
        let global_mir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;

        Ok(PeekStageRealTimeRecency {
            validity,
            copy_to,
            source_ids,
            id_bundle,
            when,
            target_replica,
            timeline_context,
            oracle_read_ts,
            in_immediate_multi_stmt_txn,
            optimizer,
            global_mir_plan,
        })
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn peek_stage_real_time_recency(
        &mut self,
        ctx: ExecuteContext,
        root_otel_ctx: OpenTelemetryContext,
        PeekStageRealTimeRecency {
            validity,
            copy_to,
            source_ids,
            id_bundle,
            when,
            target_replica,
            timeline_context,
            oracle_read_ts,
            in_immediate_multi_stmt_txn,
            optimizer,
            global_mir_plan,
        }: PeekStageRealTimeRecency,
    ) -> Option<(ExecuteContext, PeekStageFinish)> {
        match self.recent_timestamp(ctx.session(), source_ids.iter().cloned()) {
            Some(fut) => {
                let internal_cmd_tx = self.internal_cmd_tx.clone();
                let conn_id = ctx.session().conn_id().clone();
                self.pending_real_time_recency_timestamp.insert(
                    conn_id.clone(),
                    RealTimeRecencyContext::Peek {
                        ctx,
                        root_otel_ctx,
                        copy_to,
                        when,
                        target_replica,
                        timeline_context,
                        oracle_read_ts: oracle_read_ts.clone(),
                        source_ids,
                        in_immediate_multi_stmt_txn,
                        optimizer,
                        global_mir_plan,
                    },
                );
                task::spawn(|| "real_time_recency_peek", async move {
                    let real_time_recency_ts = fut.await;
                    // It is not an error for these results to be ready after `internal_cmd_rx` has been dropped.
                    let result = internal_cmd_tx.send(Message::RealTimeRecencyTimestamp {
                        conn_id: conn_id.clone(),
                        real_time_recency_ts,
                        validity,
                    });
                    if let Err(e) = result {
                        warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                    }
                });
                None
            }
            None => Some((
                ctx,
                PeekStageFinish {
                    validity,
                    copy_to,
                    id_bundle: Some(id_bundle),
                    when,
                    target_replica,
                    timeline_context,
                    oracle_read_ts,
                    source_ids,
                    real_time_recency_ts: None,
                    optimizer,
                    global_mir_plan,
                },
            )),
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn peek_stage_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        PeekStageFinish {
            validity: _,
            copy_to,
            id_bundle,
            when,
            target_replica,
            timeline_context,
            oracle_read_ts,
            source_ids,
            real_time_recency_ts,
            mut optimizer,
            global_mir_plan,
        }: PeekStageFinish,
    ) -> Result<ExecuteResponse, AdapterError> {
        let id_bundle = id_bundle.unwrap_or_else(|| {
            self.index_oracle(optimizer.cluster_id())
                .sufficient_collections(&source_ids)
        });

        let session = ctx.session_mut();
        let conn_id = session.conn_id().clone();
        let determination = self
            .sequence_peek_timestamp(
                session,
                &when,
                optimizer.cluster_id(),
                timeline_context,
                oracle_read_ts,
                &id_bundle,
                &source_ids,
                real_time_recency_ts,
            )
            .await?;

        let timestamp_context = determination.clone().timestamp_context;
        let ts = timestamp_context.clone().timestamp_or_default();

        let global_mir_plan = global_mir_plan.resolve(timestamp_context, session);
        let global_lir_plan = optimizer.catch_unwind_optimize(global_mir_plan)?;

        let source_arity = global_lir_plan.typ().arity();
        let (plan, df_meta) = global_lir_plan.unapply();

        self.emit_optimizer_notices(&*session, &df_meta.optimizer_notices);

        let planned_peek = PlannedPeek {
            plan,
            determination: determination.clone(),
            conn_id,
            source_arity,
            source_ids,
        };

        if let Some(transient_index_id) = match &planned_peek.plan {
            peek::PeekPlan::FastPath(_) => None,
            peek::PeekPlan::SlowPath(PeekDataflowPlan { id, .. }) => Some(id),
        } {
            if let Some(statement_logging_id) = ctx.extra.contents() {
                self.set_transient_index_id(statement_logging_id, *transient_index_id);
            }
        }

        if let Some(uuid) = ctx.extra().contents() {
            let mut transitive_storage_deps = BTreeSet::new();
            let mut transitive_compute_deps = BTreeSet::new();
            for id in id_bundle
                .iter()
                .flat_map(|id| self.catalog.state().transitive_uses(id))
            {
                match self.catalog.state().get_entry(&id).item() {
                    CatalogItem::Table(_) | CatalogItem::Source(_) => {
                        transitive_storage_deps.insert(id);
                    }
                    CatalogItem::MaterializedView(_) | CatalogItem::Index(_) => {
                        transitive_compute_deps.insert(id);
                    }
                    _ => {}
                }
            }
            self.controller.install_watch_set(
                transitive_storage_deps,
                ts,
                Box::new((uuid, StatementLifecycleEvent::StorageDependenciesFinished)),
            );
            self.controller.install_watch_set(
                transitive_compute_deps,
                ts,
                Box::new((uuid, StatementLifecycleEvent::ComputeDependenciesFinished)),
            );
        }
        let max_query_result_size = std::cmp::min(
            ctx.session().vars().max_query_result_size(),
            self.catalog().system_config().max_result_size(),
        );
        // Implement the peek, and capture the response.
        let resp = self
            .implement_peek_plan(
                ctx.extra_mut(),
                planned_peek,
                optimizer.finishing().clone(),
                optimizer.cluster_id(),
                target_replica,
                max_query_result_size,
            )
            .await?;

        if ctx.session().vars().emit_timestamp_notice() {
            let explanation = self.explain_timestamp(
                ctx.session(),
                optimizer.cluster_id(),
                &id_bundle,
                determination,
            );
            ctx.session()
                .add_notice(AdapterNotice::QueryTimestamp { explanation });
        }

        match copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }

    /// Determines the query timestamp and acquires read holds on dependent sources
    /// if necessary.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) async fn sequence_peek_timestamp(
        &mut self,
        session: &mut Session,
        when: &QueryWhen,
        cluster_id: ClusterId,
        timeline_context: TimelineContext,
        oracle_read_ts: Option<Timestamp>,
        source_bundle: &CollectionIdBundle,
        source_ids: &BTreeSet<GlobalId>,
        real_time_recency_ts: Option<Timestamp>,
    ) -> Result<TimestampDetermination<Timestamp>, AdapterError> {
        let in_immediate_multi_stmt_txn = session.transaction().in_immediate_multi_stmt_txn(when);
        let timedomain_bundle;

        // Fetch or generate a timestamp for this query and what the read holds would be if we need to set
        // them.
        let (determination, potential_read_holds) =
            match session.get_transaction_timestamp_determination() {
                // Use the transaction's timestamp if it exists and this isn't an AS OF query.
                Some(
                    determination @ TimestampDetermination {
                        timestamp_context: TimestampContext::TimelineTimestamp { .. },
                        ..
                    },
                ) if in_immediate_multi_stmt_txn => (determination, None),
                _ => {
                    let determine_bundle = if in_immediate_multi_stmt_txn {
                        // In a transaction, determine a timestamp that will be valid for anything in
                        // any schema referenced by the first query.
                        timedomain_bundle = self.timedomain_for(
                            source_ids,
                            &timeline_context,
                            session.conn_id(),
                            cluster_id,
                        )?;
                        &timedomain_bundle
                    } else {
                        // If not in a transaction, use the source.
                        source_bundle
                    };
                    let determination = self
                        .determine_timestamp(
                            session,
                            determine_bundle,
                            when,
                            cluster_id,
                            &timeline_context,
                            oracle_read_ts,
                            real_time_recency_ts,
                        )
                        .await?;
                    // We only need read holds if the read depends on a timestamp. We don't set the
                    // read holds here because it makes the code a bit more clear to handle the two
                    // cases for "is this the first statement in a transaction?" in an if/else block
                    // below.
                    let read_holds = determination
                        .timestamp_context
                        .timestamp()
                        .map(|timestamp| (timestamp.clone(), determine_bundle));
                    (determination, read_holds)
                }
            };

        // If we're in a multi-statement transaction and the query does not use `AS OF`,
        // acquire read holds on any sources in the current time-domain if they have not
        // already been acquired. If the query does use `AS OF`, it is not necessary to
        // acquire read holds.
        if in_immediate_multi_stmt_txn {
            // Either set the valid read ids for this transaction (if it's the first statement in a
            // transaction) otherwise verify the ids referenced in this query are in the timedomain.
            if let Some(txn_reads) = self.txn_reads.get(session.conn_id()) {
                // Find referenced ids not in the read hold. A reference could be caused by a
                // user specifying an object in a different schema than the first query. An
                // index could be caused by a CREATE INDEX after the transaction started.
                let allowed_id_bundle = txn_reads.id_bundle();
                let outside = source_bundle.difference(&allowed_id_bundle);
                // Queries without a timestamp and timeline can belong to any existing timedomain.
                if determination.timestamp_context.contains_timestamp() && !outside.is_empty() {
                    let valid_names =
                        self.resolve_collection_id_bundle_names(session, &allowed_id_bundle);
                    let invalid_names = self.resolve_collection_id_bundle_names(session, &outside);
                    return Err(AdapterError::RelationOutsideTimeDomain {
                        relations: invalid_names,
                        names: valid_names,
                    });
                }
            } else {
                if let Some((timestamp, bundle)) = potential_read_holds {
                    let read_holds = self.acquire_read_holds(timestamp, bundle);
                    self.txn_reads.insert(session.conn_id().clone(), read_holds);
                }
            }
        }

        // TODO: Checking for only `InTransaction` and not `Implied` (also `Started`?) seems
        // arbitrary and we don't recall why we did it (possibly an error!). Change this to always
        // set the transaction ops. Decide and document what our policy should be on AS OF queries.
        // Maybe they shouldn't be allowed in transactions at all because it's hard to explain
        // what's going on there. This should probably get a small design document.

        // We only track the peeks in the session if the query doesn't use AS
        // OF or we're inside an explicit transaction. The latter case is
        // necessary to support PG's `BEGIN` semantics, whose behavior can
        // depend on whether or not reads have occurred in the txn.
        let mut transaction_determination = determination.clone();
        if when.is_transactional() {
            session.add_transaction_ops(TransactionOps::Peeks {
                determination: transaction_determination,
                cluster_id,
            })?;
        } else if matches!(session.transaction(), &TransactionStatus::InTransaction(_)) {
            // If the query uses AS OF, then ignore the timestamp.
            transaction_determination.timestamp_context = TimestampContext::NoTimestamp;
            session.add_transaction_ops(TransactionOps::Peeks {
                determination: transaction_determination,
                cluster_id,
            })?;
        };

        Ok(determination)
    }
}
