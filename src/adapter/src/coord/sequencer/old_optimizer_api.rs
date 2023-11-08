// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains alternative implementations of `sequence_~` methods
//! that optimize plans using the new optimizer API. The `sequence_plan` method
//! in the parent module will delegate to these methods only if the
//! `enable_unified_optimizer_api` feature flag is off. Once we have gained
//! enough confidence that the new methods behave equally well as the old ones,
//! we will deprecate the old methods and move the implementations here to the
//! `inner` module.

#![allow(deprecated)]

use std::collections::{BTreeMap, BTreeSet};

use mz_cluster_client::ReplicaId;
use mz_compute_types::dataflows::{DataflowDesc, DataflowDescription, IndexDesc};
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection};
use mz_controller_types::ClusterId;
use mz_expr::{
    permutation_for_arrangement, CollectionPlan, MirRelationExpr, MirScalarExpr,
    OptimizedMirRelationExpr, RowSetFinishing,
};
use mz_ore::task;
use mz_repr::explain::{TransientItem, UsedIndexes};
use mz_repr::{GlobalId, RelationDesc, RelationType, Timestamp};
use mz_sql::catalog::CatalogCluster;
use mz_sql::plan::{self, CopyFormat, QueryWhen, SubscribeFrom};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::{EmptyStatisticsOracle, Optimizer, StatisticsOracle};
use timely::progress::Antichain;
use tokio::sync::mpsc;
use tracing::instrument::WithSubscriber;
use tracing::{event, warn, Level};

use crate::catalog::CatalogState;
use crate::coord::dataflows::{
    prep_relation_expr, prep_scalar_expr, ComputeInstanceSnapshot, DataflowBuilder, EvalTime,
    ExprPrepStyle,
};
use crate::coord::peek::{self, FastPathPlan, PlannedPeek};
use crate::coord::sequencer::inner::{catch_unwind, check_log_reads, return_if_err};
use crate::coord::timestamp_selection::TimestampDetermination;
use crate::coord::{Coordinator, Message, PlanValidity, RealTimeRecencyContext, TargetCluster};
use crate::optimize::OptimizerConfig;
use crate::session::{Session, TransactionOps, TransactionStatus};
use crate::subscribe::ActiveSubscribe;
use crate::util::{ComputeSinkId, ResultExt};
use crate::{
    AdapterError, AdapterNotice, CollectionIdBundle, ExecuteContext, ExecuteResponse,
    TimelineContext, TimestampContext,
};

impl Coordinator {
    // Subscribe
    // ---------

    /// This should mirror the operational semantics of
    /// `Coordinator::sequence_subscribe`.
    #[deprecated = "This is being replaced by sequence_subscribe (see #20569)."]
    pub(super) async fn sequence_subscribe_deprecated(
        &mut self,
        ctx: &mut ExecuteContext,
        plan: plan::SubscribePlan,
        target_cluster: TargetCluster,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::SubscribePlan {
            from,
            with_snapshot,
            when,
            copy_to,
            emit_progress,
            up_to,
            output,
        } = plan;

        let cluster = self
            .catalog()
            .resolve_target_cluster(target_cluster, ctx.session())?;
        let cluster_id = cluster.id;

        let target_replica_name = ctx.session().vars().cluster_replica();
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

        // SUBSCRIBE AS OF, similar to peeks, doesn't need to worry about transaction
        // timestamp semantics.
        if when == QueryWhen::Immediately {
            // If this isn't a SUBSCRIBE AS OF, the SUBSCRIBE can be in a transaction if it's the
            // only operation.
            ctx.session_mut()
                .add_transaction_ops(TransactionOps::Subscribe)?;
        }

        // Determine the frontier of updates to subscribe *from*.
        // Updates greater or equal to this frontier will be produced.
        let depends_on = from.depends_on();
        let notices = check_log_reads(
            self.catalog(),
            cluster,
            &depends_on,
            &mut target_replica,
            ctx.session().vars(),
        )?;
        ctx.session_mut().add_notices(notices);

        let id_bundle = self
            .index_oracle(cluster_id)
            .sufficient_collections(&depends_on);
        let mut timeline = self.validate_timeline_context(depends_on.clone())?;
        if matches!(timeline, TimelineContext::TimestampIndependent) && from.contains_temporal() {
            // If the from IDs are timestamp independent but the query contains temporal functions
            // then the timeline context needs to be upgraded to timestamp dependent.
            timeline = TimelineContext::TimestampDependent;
        }
        let as_of = self
            .determine_timestamp(
                ctx.session(),
                &id_bundle,
                &when,
                cluster_id,
                &timeline,
                None,
            )
            .await?
            .timestamp_context
            .timestamp_or_default();
        if let Some(id) = ctx.extra().contents() {
            self.set_statement_execution_timestamp(id, as_of);
        }

        let up_to = up_to
            .map(|expr| Coordinator::evaluate_when(self.catalog().state(), expr, ctx.session_mut()))
            .transpose()?;
        if let Some(up_to) = up_to {
            if as_of == up_to {
                ctx.session_mut()
                    .add_notice(AdapterNotice::EqualSubscribeBounds { bound: up_to });
            } else if as_of > up_to {
                return Err(AdapterError::AbsurdSubscribeBounds { as_of, up_to });
            }
        }
        let up_to = up_to.map(Antichain::from_elem).unwrap_or_default();

        let (mut dataflow, dataflow_metainfo) = match from {
            SubscribeFrom::Id(from_id) => {
                let from = self.catalog().get_entry(&from_id);
                let from_desc = from
                    .desc(
                        &self
                            .catalog()
                            .resolve_full_name(from.name(), Some(ctx.session().conn_id())),
                    )
                    .expect("subscribes can only be run on items with descs")
                    .into_owned();
                let sink_id = self.allocate_transient_id()?;
                let sink_desc = ComputeSinkDesc {
                    from: from_id,
                    from_desc,
                    connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
                    with_snapshot,
                    up_to,
                    // No `FORCE NOT NULL` for subscribes
                    non_null_assertions: vec![],
                };
                let sink_name = format!("subscribe-{}", sink_id);
                self.dataflow_builder(cluster_id)
                    .build_sink_dataflow(sink_name, sink_id, sink_desc)?
            }
            SubscribeFrom::Query { expr, desc } => {
                let id = self.allocate_transient_id()?;
                let expr = self.view_optimizer.optimize(expr)?;
                let desc = RelationDesc::new(expr.typ(), desc.iter_names());
                let sink_desc = ComputeSinkDesc {
                    from: id,
                    from_desc: desc,
                    connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
                    with_snapshot,
                    up_to,
                    // No `FORCE NOT NULL` for subscribes
                    non_null_assertions: vec![],
                };
                let mut dataflow = DataflowDesc::new(format!("subscribe-{}", id));
                let mut dataflow_builder = self.dataflow_builder(cluster_id);
                dataflow_builder.import_view_into_dataflow(&id, &expr, &mut dataflow)?;
                let dataflow_metainfo =
                    dataflow_builder.build_sink_dataflow_into(&mut dataflow, id, sink_desc)?;
                (dataflow, dataflow_metainfo)
            }
        };

        self.emit_optimizer_notices(ctx.session(), &dataflow_metainfo.optimizer_notices);

        dataflow.set_as_of(Antichain::from_elem(as_of));

        let (&sink_id, sink_desc) = dataflow
            .sink_exports
            .iter()
            .next()
            .expect("subscribes have a single sink export");
        let (tx, rx) = mpsc::unbounded_channel();
        let active_subscribe = ActiveSubscribe {
            user: ctx.session().user().clone(),
            conn_id: ctx.session().conn_id().clone(),
            channel: tx,
            emit_progress,
            as_of,
            arity: sink_desc.from_desc.arity(),
            cluster_id,
            depends_on: depends_on.into_iter().collect(),
            start_time: self.now(),
            dropping: false,
            output,
        };
        active_subscribe.initialize();
        self.add_active_subscribe(sink_id, active_subscribe).await;

        // Allow while the introduction of the new optimizer API in
        // #20569 is in progress.
        #[allow(deprecated)]
        match self.ship_dataflow(dataflow, cluster_id).await {
            Ok(_) => {}
            Err(e) => {
                self.remove_active_subscribe(sink_id).await;
                return Err(e);
            }
        };
        if let Some(target) = target_replica {
            self.controller
                .compute
                .set_subscribe_target_replica(cluster_id, sink_id, target)
                .unwrap_or_terminate("cannot fail to set subscribe target replica");
        }

        self.active_conns
            .get_mut(ctx.session().conn_id())
            .expect("must exist for active sessions")
            .drop_sinks
            .push(ComputeSinkId {
                cluster_id,
                global_id: sink_id,
            });

        let resp = ExecuteResponse::Subscribing {
            rx,
            ctx_extra: std::mem::take(ctx.extra_mut()),
        };
        match copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }

    // Peek
    // ----

    /// Processes as many peek stages as possible.
    #[deprecated = "This is being replaced by sequence_peek_stage (see #20569)."]
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_peek_stage_deprecated(
        &mut self,
        mut ctx: ExecuteContext,
        mut stage: PeekStageDeprecated,
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
                PeekStageDeprecated::Validate(stage) => {
                    let next = return_if_err!(
                        self.peek_stage_validate_deprecated(ctx.session_mut(), stage),
                        ctx
                    );
                    (ctx, PeekStageDeprecated::Optimize(next))
                }
                PeekStageDeprecated::Optimize(stage) => {
                    self.peek_stage_optimize_deprecated(ctx, stage).await;
                    return;
                }
                PeekStageDeprecated::Timestamp(stage) => {
                    match self.peek_stage_timestamp_deprecated(ctx, stage) {
                        Some((ctx, next)) => (ctx, PeekStageDeprecated::Finish(next)),
                        None => return,
                    }
                }
                PeekStageDeprecated::Finish(stage) => {
                    let res = self.peek_stage_finish_deprecated(&mut ctx, stage).await;
                    ctx.retire(res);
                    return;
                }
            }
        }
    }

    // Do some simple validation. We must defer most of it until after any off-thread work.
    #[deprecated = "This is being replaced by peek_stage_validate (see #20569)."]
    fn peek_stage_validate_deprecated(
        &mut self,
        session: &Session,
        PeekStageValidateDeprecated {
            plan,
            target_cluster,
        }: PeekStageValidateDeprecated,
    ) -> Result<PeekStageOptimizeDeprecated, AdapterError> {
        let plan::SelectPlan {
            source,
            when,
            finishing,
            copy_to,
        } = plan;

        // Two transient allocations. We could reclaim these if we don't use them, potentially.
        // TODO: reclaim transient identifiers in fast path cases.
        let view_id = self.allocate_transient_id()?;
        let index_id = self.allocate_transient_id()?;
        let catalog = self.catalog();

        let cluster = catalog.resolve_target_cluster(target_cluster, session)?;

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
            catalog,
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

        Ok(PeekStageOptimizeDeprecated {
            validity,
            source,
            finishing,
            copy_to,
            view_id,
            index_id,
            source_ids,
            cluster_id: cluster.id(),
            when,
            target_replica,
            timeline_context,
            in_immediate_multi_stmt_txn,
        })
    }

    #[deprecated = "This is being replaced by peek_stage_optimize (see #20569)."]
    async fn peek_stage_optimize_deprecated(
        &mut self,
        ctx: ExecuteContext,
        mut stage: PeekStageOptimizeDeprecated,
    ) {
        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let catalog = self.owned_catalog();
        let compute = ComputeInstanceSnapshot::new(&self.controller, stage.cluster_id)
            .expect("compute instance does not exist");
        let internal_cmd_tx = self.internal_cmd_tx.clone();

        // TODO: Is there a way to avoid making two dataflow_builders (the second is in
        // optimize_peek)?
        let id_bundle = self
            .dataflow_builder(stage.cluster_id)
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
                    stage.cluster_id,
                    &stage.timeline_context,
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

        mz_ore::task::spawn_blocking(
            || "optimize peek",
            move || match Self::optimize_peek_deprecated(
                catalog.state(),
                compute,
                ctx.session(),
                stats,
                id_bundle,
                stage,
            ) {
                Ok(stage) => {
                    let stage = PeekStageDeprecated::Timestamp(stage);
                    // Ignore errors if the coordinator has shut down.
                    let _ = internal_cmd_tx.send(Message::PeekStageDeprecatedReady { ctx, stage });
                }
                Err(err) => ctx.retire(Err(err)),
            },
        );
    }

    #[deprecated = "This is being replaced by optimize_peek (see #20569)."]
    fn optimize_peek_deprecated(
        catalog: &CatalogState,
        compute: ComputeInstanceSnapshot,
        session: &Session,
        stats: Box<dyn StatisticsOracle>,
        id_bundle: CollectionIdBundle,
        PeekStageOptimizeDeprecated {
            validity,
            source,
            finishing,
            copy_to,
            view_id,
            index_id,
            source_ids,
            cluster_id,
            when,
            target_replica,
            timeline_context,
            in_immediate_multi_stmt_txn,
        }: PeekStageOptimizeDeprecated,
    ) -> Result<PeekStageTimestampDeprecated, AdapterError> {
        let optimizer = Optimizer::logical_optimizer(&mz_transform::typecheck::empty_context());
        let source = optimizer.optimize(source)?;
        let mut builder = DataflowBuilder::new(catalog, compute);

        // We create a dataflow and optimize it, to determine if we can avoid building it.
        // This can happen if the result optimizes to a constant, or to a `Get` expression
        // around a maintained arrangement.
        let typ = source.typ();
        let key: Vec<MirScalarExpr> = typ
            .default_key()
            .iter()
            .map(|k| MirScalarExpr::Column(*k))
            .collect();
        // The assembled dataflow contains a view and an index of that view.
        let mut dataflow = DataflowDesc::new(format!("oneshot-select-{}", view_id));
        builder.import_view_into_dataflow(&view_id, &source, &mut dataflow)?;

        // Resolve all unmaterializable function calls except mz_now(), because we don't yet have a
        // timestamp.
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Deferred,
            session,
            catalog_state: catalog,
        };
        dataflow.visit_children(
            |r| prep_relation_expr(r, style),
            |s| prep_scalar_expr(s, style),
        )?;

        dataflow.export_index(
            index_id,
            IndexDesc {
                on_id: view_id,
                key: key.clone(),
            },
            typ.clone(),
        );

        // Optimize the dataflow across views, and any other ways that appeal.
        let dataflow_metainfo = mz_transform::optimize_dataflow(&mut dataflow, &builder, &*stats)?;

        Ok(PeekStageTimestampDeprecated {
            validity,
            dataflow,
            finishing,
            copy_to,
            view_id,
            index_id,
            source_ids,
            cluster_id,
            id_bundle,
            when,
            target_replica,
            timeline_context,
            in_immediate_multi_stmt_txn,
            key,
            typ,
            dataflow_metainfo,
        })
    }

    #[deprecated = "This is being replaced by peek_stage_timestamp (see #20569)."]
    #[tracing::instrument(level = "debug", skip_all)]
    fn peek_stage_timestamp_deprecated(
        &mut self,
        ctx: ExecuteContext,
        PeekStageTimestampDeprecated {
            validity,
            dataflow,
            finishing,
            copy_to,
            view_id,
            index_id,
            source_ids,
            cluster_id,
            id_bundle,
            when,
            target_replica,
            timeline_context,
            in_immediate_multi_stmt_txn,
            key,
            typ,
            dataflow_metainfo,
        }: PeekStageTimestampDeprecated,
    ) -> Option<(ExecuteContext, PeekStageFinishDeprecated)> {
        match self.recent_timestamp(ctx.session(), source_ids.iter().cloned()) {
            Some(fut) => {
                let internal_cmd_tx = self.internal_cmd_tx.clone();
                let conn_id = ctx.session().conn_id().clone();
                self.pending_real_time_recency_timestamp.insert(
                    conn_id.clone(),
                    RealTimeRecencyContext::PeekDeprecated {
                        ctx,
                        finishing,
                        copy_to,
                        dataflow,
                        cluster_id,
                        when,
                        target_replica,
                        view_id,
                        index_id,
                        timeline_context,
                        source_ids,
                        in_immediate_multi_stmt_txn,
                        key,
                        typ,
                        dataflow_metainfo,
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
                PeekStageFinishDeprecated {
                    validity,
                    finishing,
                    copy_to,
                    dataflow,
                    cluster_id,
                    id_bundle: Some(id_bundle),
                    when,
                    target_replica,
                    view_id,
                    index_id,
                    timeline_context,
                    source_ids,
                    real_time_recency_ts: None,
                    key,
                    typ,
                    dataflow_metainfo,
                },
            )),
        }
    }

    #[deprecated = "This is being replaced by peek_stage_finish (see #20569)."]
    #[tracing::instrument(level = "debug", skip_all)]
    async fn peek_stage_finish_deprecated(
        &mut self,
        ctx: &mut ExecuteContext,
        PeekStageFinishDeprecated {
            validity: _,
            finishing,
            copy_to,
            dataflow,
            cluster_id,
            id_bundle,
            when,
            target_replica,
            view_id,
            index_id,
            timeline_context,
            source_ids,
            real_time_recency_ts,
            key,
            typ,
            dataflow_metainfo,
        }: PeekStageFinishDeprecated,
    ) -> Result<ExecuteResponse, AdapterError> {
        let id_bundle = id_bundle.unwrap_or_else(|| {
            self.index_oracle(cluster_id)
                .sufficient_collections(&source_ids)
        });
        let peek_plan = self
            .plan_peek_deprecated(
                dataflow,
                ctx.session_mut(),
                &when,
                cluster_id,
                view_id,
                index_id,
                timeline_context,
                source_ids,
                &id_bundle,
                real_time_recency_ts,
                key,
                typ,
                &finishing,
            )
            .await?;

        let determination = peek_plan.determination.clone();

        let max_query_result_size = std::cmp::min(
            ctx.session().vars().max_query_result_size(),
            self.catalog().system_config().max_result_size(),
        );
        // Implement the peek, and capture the response.
        let resp = self
            .implement_peek_plan(
                ctx.extra_mut(),
                peek_plan,
                finishing,
                cluster_id,
                target_replica,
                max_query_result_size,
            )
            .await?;

        if ctx.session().vars().emit_timestamp_notice() {
            let explanation =
                self.explain_timestamp(ctx.session(), cluster_id, &id_bundle, determination);
            ctx.session()
                .add_notice(AdapterNotice::QueryTimestamp { explanation });
        }
        self.emit_optimizer_notices(ctx.session(), &dataflow_metainfo.optimizer_notices);

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
    #[deprecated = "This is being replaced by sequence_peek_timestamp (see #20569)."]
    async fn sequence_peek_timestamp_deprecated(
        &mut self,
        session: &mut Session,
        when: &QueryWhen,
        cluster_id: ClusterId,
        timeline_context: TimelineContext,
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
                        timestamp_context: TimestampContext::TimelineTimestamp(_, _),
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

    #[deprecated = "This is being replaced by plan_peek (see #20569)."]
    async fn plan_peek_deprecated(
        &mut self,
        mut dataflow: DataflowDescription<OptimizedMirRelationExpr>,
        session: &mut Session,
        when: &QueryWhen,
        cluster_id: ClusterId,
        view_id: GlobalId,
        index_id: GlobalId,
        timeline_context: TimelineContext,
        source_ids: BTreeSet<GlobalId>,
        id_bundle: &CollectionIdBundle,
        real_time_recency_ts: Option<Timestamp>,
        key: Vec<MirScalarExpr>,
        typ: RelationType,
        finishing: &RowSetFinishing,
    ) -> Result<PlannedPeek, AdapterError> {
        let conn_id = session.conn_id().clone();
        let determination = self
            .sequence_peek_timestamp_deprecated(
                session,
                when,
                cluster_id,
                timeline_context,
                id_bundle,
                &source_ids,
                real_time_recency_ts,
            )
            .await?;

        // Now that we have a timestamp, set the as of and resolve calls to mz_now().
        dataflow.set_as_of(determination.timestamp_context.antichain());
        let catalog_state = self.catalog().state();
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Time(determination.timestamp_context.timestamp_or_default()),
            session,
            catalog_state,
        };
        dataflow.visit_children(
            |r| prep_relation_expr(r, style),
            |s| prep_scalar_expr(s, style),
        )?;

        let (permutation, thinning) = permutation_for_arrangement(&key, typ.arity());

        // At this point, `dataflow_plan` contains our best optimized dataflow.
        // We will check the plan to see if there is a fast path to escape full dataflow construction.
        let peek_plan = self.create_peek_plan(
            dataflow,
            view_id,
            cluster_id,
            index_id,
            key,
            permutation,
            thinning.len(),
            finishing,
        )?;

        Ok(PlannedPeek {
            plan: peek_plan,
            determination,
            conn_id,
            source_arity: typ.arity(),
            source_ids,
        })
    }

    /// Run the query optimization explanation pipeline. This function must be called with
    /// an `OptimizerTrace` `tracing` subscriber, using `.with_subscriber(...)`.
    /// The `root_dispatch` should be the global `tracing::Dispatch`.
    ///
    /// This should mirror the operational semantics of
    /// `Coordinator::explain_query_optimizer_pipeline`.
    //
    // WARNING, ENTERING SPOOKY ZONE 3.0
    //
    // You must be careful when altering this function. Any async call (so, anything that uses
    // `.await` should be wrapped with `.with_subscriber(root_dispatch)`.
    //
    // `tracing` has limitations that mean that any `Span` created under the `OptimizerTrace`
    // subscriber that _leaves_ this function will almost assuredly cause a panic inside `tracing`.
    // This is because `Span`s track the `Subscriber` they were created under, but certain actions
    // (like an ordinary `Span` exit) will call a method on the _thread-local_ `Subscriber`, which
    // may be backed with a different `Registry`.
    //
    // At first glance, there is no obvious way this method leaks `Span`s, but ANY tokio
    // resource (like `oneshot` channels) create `Span`s if tokio is built with `tokio_unstable`
    // and the `tracing` feature. This method has been audited to make sure ALL such
    // cases are dispatched inside the global `root_dispatch`, and **any change to this method
    // needs to ensure this invariant is upheld.**
    //
    // It is a bit wonky to have this method under a specialized `Dispatch`, but ensuring
    // all `.await` points inside it use the passed `root_dispatch`, but splitting the method
    // into pieces to allow us to control the `Dispatch` for various pieces at a higher-level
    // would be very hard to read. Additionally, once the issues with `broken` are resolved
    // (as discussed in <https://github.com/MaterializeInc/materialize/pull/21809>), this
    // can be simplified, as only a _singular_ `Registry` will be in use.
    #[deprecated = "This is being replaced by explain_query_optimizer_pipeline (see #20569)."]
    #[tracing::instrument(target = "optimizer", level = "trace", name = "optimize", skip_all)]
    pub(crate) async fn explain_query_optimizer_pipeline_deprecated(
        &mut self,
        raw_plan: mz_sql::plan::HirRelationExpr,
        broken: bool,
        target_cluster: TargetCluster,
        session: &mut Session,
        finishing: &Option<RowSetFinishing>,
        explain_config: &mz_repr::explain::ExplainConfig,
        root_dispatch: tracing::Dispatch,
    ) -> Result<
        (
            UsedIndexes,
            Option<FastPathPlan>,
            DataflowMetainfo,
            BTreeMap<GlobalId, TransientItem>,
        ),
        AdapterError,
    > {
        use mz_repr::explain::trace_plan;

        if broken {
            tracing::warn!("EXPLAIN ... BROKEN <query> is known to leak memory, use with caution");
        }

        let catalog = self.catalog();
        let target_cluster_id = catalog.resolve_target_cluster(target_cluster, session)?.id;
        let system_config = catalog.system_config();
        let optimizer_config = OptimizerConfig::from((system_config, explain_config));

        // Execute the various stages of the optimization pipeline
        // -------------------------------------------------------

        // Trace the pipeline input under `optimize/raw`.
        tracing::span!(target: "optimizer", Level::DEBUG, "raw").in_scope(|| {
            trace_plan(&raw_plan);
        });

        // Execute the `optimize/hir_to_mir` stage.
        let decorrelated_plan = catch_unwind(broken, "hir_to_mir", || {
            raw_plan.lower((system_config, explain_config))
        })?;

        let mut timeline_context =
            self.validate_timeline_context(decorrelated_plan.depends_on())?;
        if matches!(timeline_context, TimelineContext::TimestampIndependent)
            && decorrelated_plan.contains_temporal()
        {
            // If the source IDs are timestamp independent but the query contains temporal functions,
            // then the timeline context needs to be upgraded to timestamp dependent. This is
            // required because `source_ids` doesn't contain functions.
            timeline_context = TimelineContext::TimestampDependent;
        }

        let source_ids = decorrelated_plan.depends_on();
        let id_bundle = self
            .index_oracle(target_cluster_id)
            .sufficient_collections(&source_ids);

        // Execute the `optimize/local` stage.
        let optimized_plan = catch_unwind(broken, "local", || {
            let _span = tracing::span!(target: "optimizer", Level::DEBUG, "local").entered();
            let optimized_plan = self.view_optimizer.optimize(decorrelated_plan)?;
            trace_plan(optimized_plan.as_inner());
            Ok::<_, AdapterError>(optimized_plan)
        })?;

        // Acquire a timestamp (necessary for loading statistics).
        let timestamp_ctx = self
            .sequence_peek_timestamp_deprecated(
                session,
                &QueryWhen::Immediately,
                target_cluster_id,
                timeline_context,
                &id_bundle,
                &source_ids,
                None, // no real-time recency
            )
            .await?
            .timestamp_context;

        // Load cardinality statistics.
        //
        // TODO: proper stats needs exact timestamp at the moment. However, we
        // don't want to resolve the timestamp twice, so we need to figure out a
        // way to get somewhat stale stats.
        let stats = self
            .statistics_oracle(session, &source_ids, timestamp_ctx.antichain(), true)
            .with_subscriber(root_dispatch)
            .await?;

        let state = self.catalog().state();

        // Execute the `optimize/global` stage.
        let (mut df, df_metainfo) = catch_unwind(broken, "global", || {
            let mut df_builder = self.dataflow_builder(target_cluster_id);

            let mut df = DataflowDesc::new("explanation".to_string());
            df_builder.import_view_into_dataflow(&GlobalId::Explain, &optimized_plan, &mut df)?;
            df_builder.reoptimize_imported_views(&mut df, &optimizer_config)?;

            // Resolve all unmaterializable function calls except mz_now(),
            // because in line with the `sequence_~` method we pretend that we
            // don't have a timestamp yet.
            let style = ExprPrepStyle::OneShot {
                logical_time: EvalTime::Deferred,
                session,
                catalog_state: state,
            };
            df.visit_children(
                |r| prep_relation_expr(r, style),
                |s| prep_scalar_expr(s, style),
            )?;

            // Optimize the dataflow across views, and any other ways that appeal.
            let df_metainfo = mz_transform::optimize_dataflow(
                &mut df,
                &self.index_oracle(target_cluster_id),
                stats.as_ref(),
            )?;

            Ok::<_, AdapterError>((df, df_metainfo))
        })?;

        // Collect the list of indexes used by the dataflow at this point
        let mut used_indexes = UsedIndexes::new(
            df
                .index_imports
                .iter()
                .map(|(id, _index_import)| {
                    (*id, df_metainfo.index_usage_types.get(id).expect("prune_and_annotate_dataflow_index_imports should have been called already").clone())
                })
                .collect(),
        );

        // Determine if fast path plan will be used for this explainee.
        let fast_path_plan = {
            df.set_as_of(timestamp_ctx.antichain());

            // Resolve all unmaterializable function including mz_now().
            let style = ExprPrepStyle::OneShot {
                logical_time: EvalTime::Time(timestamp_ctx.timestamp_or_default()),
                session,
                catalog_state: state,
            };
            df.visit_children(
                |r| prep_relation_expr(r, style),
                |s| prep_scalar_expr(s, style),
            )?;
            peek::create_fast_path_plan(
                &mut df,
                GlobalId::Explain,
                finishing.as_ref(),
                self.catalog.system_config().persist_fast_path_limit(),
            )?
        };

        if let Some(fast_path_plan) = &fast_path_plan {
            used_indexes = fast_path_plan.used_indexes(finishing);
        }

        // We have the opportunity to name an `until` frontier that will prevent work we needn't perform.
        // By default, `until` will be `Antichain::new()`, which prevents no updates and is safe.
        if let Some(as_of) = df.as_of.as_ref() {
            if !as_of.is_empty() {
                if let Some(next) = as_of.as_option().and_then(|as_of| as_of.checked_add(1)) {
                    df.until = timely::progress::Antichain::from_elem(next);
                }
            }
        }

        // Execute the `optimize/finalize_dataflow` stage.
        let df = catch_unwind(broken, "finalize_dataflow", || {
            self.finalize_dataflow(df, target_cluster_id)
        })?;

        // Trace the resulting plan for the top-level `optimize` path.
        trace_plan(&df);

        // Return objects that need to be passed to the `ExplainContext`
        // when rendering explanations for the various trace entries.
        Ok((used_indexes, fast_path_plan, df_metainfo, BTreeMap::new()))
    }
}

#[derive(Debug)]
pub enum PeekStageDeprecated {
    Validate(PeekStageValidateDeprecated),
    Optimize(PeekStageOptimizeDeprecated),
    Timestamp(PeekStageTimestampDeprecated),
    Finish(PeekStageFinishDeprecated),
}

impl PeekStageDeprecated {
    fn validity(&mut self) -> Option<&mut PlanValidity> {
        match self {
            PeekStageDeprecated::Validate(_) => None,
            PeekStageDeprecated::Optimize(PeekStageOptimizeDeprecated { validity, .. })
            | PeekStageDeprecated::Timestamp(PeekStageTimestampDeprecated { validity, .. })
            | PeekStageDeprecated::Finish(PeekStageFinishDeprecated { validity, .. }) => {
                Some(validity)
            }
        }
    }
}

#[derive(Debug)]
pub struct PeekStageValidateDeprecated {
    pub plan: mz_sql::plan::SelectPlan,
    pub target_cluster: TargetCluster,
}

#[derive(Debug)]
pub struct PeekStageOptimizeDeprecated {
    pub validity: PlanValidity,
    pub source: MirRelationExpr,
    pub finishing: RowSetFinishing,
    pub copy_to: Option<CopyFormat>,
    pub view_id: GlobalId,
    pub index_id: GlobalId,
    pub source_ids: BTreeSet<GlobalId>,
    pub cluster_id: ClusterId,
    pub when: QueryWhen,
    pub target_replica: Option<ReplicaId>,
    pub timeline_context: TimelineContext,
    pub in_immediate_multi_stmt_txn: bool,
}

#[derive(Debug)]
pub struct PeekStageTimestampDeprecated {
    pub validity: PlanValidity,
    pub dataflow: DataflowDescription<OptimizedMirRelationExpr>,
    pub finishing: RowSetFinishing,
    pub copy_to: Option<CopyFormat>,
    pub view_id: GlobalId,
    pub index_id: GlobalId,
    pub source_ids: BTreeSet<GlobalId>,
    pub cluster_id: ClusterId,
    pub id_bundle: CollectionIdBundle,
    pub when: QueryWhen,
    pub target_replica: Option<ReplicaId>,
    pub timeline_context: TimelineContext,
    pub in_immediate_multi_stmt_txn: bool,
    pub key: Vec<MirScalarExpr>,
    pub typ: RelationType,
    pub dataflow_metainfo: DataflowMetainfo,
}

#[derive(Debug)]
pub struct PeekStageFinishDeprecated {
    pub validity: PlanValidity,
    pub finishing: RowSetFinishing,
    pub copy_to: Option<CopyFormat>,
    pub dataflow: DataflowDescription<OptimizedMirRelationExpr>,
    pub cluster_id: ClusterId,
    pub id_bundle: Option<CollectionIdBundle>,
    pub when: QueryWhen,
    pub target_replica: Option<ReplicaId>,
    pub view_id: GlobalId,
    pub index_id: GlobalId,
    pub timeline_context: TimelineContext,
    pub source_ids: BTreeSet<GlobalId>,
    pub real_time_recency_ts: Option<mz_repr::Timestamp>,
    pub key: Vec<MirScalarExpr>,
    pub typ: RelationType,
    pub dataflow_metainfo: DataflowMetainfo,
}
