// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;
use mz_controller_types::ClusterId;
use mz_expr::CollectionPlan;
use mz_ore::instrument;
use mz_repr::explain::ExplainFormat;
use mz_repr::{Datum, Row};
use mz_sql::plan::{self};
use mz_sql::session::metadata::SessionMetadata;
use tracing::{Instrument, Span};

use crate::coord::sequencer::inner::return_if_err;
use crate::coord::timestamp_selection::{TimestampDetermination, TimestampSource};
use crate::coord::{
    Coordinator, ExplainTimestampFinish, ExplainTimestampOptimize, ExplainTimestampRealTimeRecency,
    ExplainTimestampStage, Message, PlanValidity, StageResult, Staged, TargetCluster,
};
use crate::error::AdapterError;
use crate::optimize::{self, Optimize};
use crate::session::{RequireLinearization, Session};
use crate::{CollectionIdBundle, ExecuteContext, TimelineContext, TimestampExplanation};

impl Staged for ExplainTimestampStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            ExplainTimestampStage::Optimize(stage) => &mut stage.validity,
            ExplainTimestampStage::RealTimeRecency(stage) => &mut stage.validity,
            ExplainTimestampStage::Finish(stage) => &mut stage.validity,
        }
    }

    async fn stage(
        self,
        coord: &mut Coordinator,
        ctx: &mut ExecuteContext,
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        match self {
            ExplainTimestampStage::Optimize(stage) => coord.explain_timestamp_optimize(stage),
            ExplainTimestampStage::RealTimeRecency(stage) => {
                coord
                    .explain_timestamp_real_time_recency(ctx.session(), stage)
                    .await
            }
            ExplainTimestampStage::Finish(stage) => {
                coord
                    .explain_timestamp_finish(ctx.session_mut(), stage)
                    .await
            }
        }
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::ExplainTimestampStageReady {
            ctx,
            span,
            stage: self,
        }
    }

    fn cancel_enabled(&self) -> bool {
        true
    }
}

impl Coordinator {
    #[instrument]
    pub async fn sequence_explain_timestamp(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::ExplainTimestampPlan,
        target_cluster: TargetCluster,
    ) {
        let stage = return_if_err!(
            self.explain_timestamp_validity(ctx.session(), plan, target_cluster),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    fn explain_timestamp_validity(
        &self,
        session: &Session,
        plan: plan::ExplainTimestampPlan,
        target_cluster: TargetCluster,
    ) -> Result<ExplainTimestampStage, AdapterError> {
        let cluster = self
            .catalog()
            .resolve_target_cluster(target_cluster, session)?;
        let cluster_id = cluster.id;
        let dependencies = plan
            .raw_plan
            .depends_on()
            .into_iter()
            .map(|id| self.catalog().resolve_item_id(&id))
            .collect();
        let validity = PlanValidity::new(
            self.catalog().transient_revision(),
            dependencies,
            Some(cluster_id),
            None,
            session.role_metadata().clone(),
        );
        Ok(ExplainTimestampStage::Optimize(ExplainTimestampOptimize {
            validity,
            plan,
            cluster_id,
        }))
    }

    #[instrument]
    fn explain_timestamp_optimize(
        &self,
        ExplainTimestampOptimize {
            validity,
            plan,
            cluster_id,
        }: ExplainTimestampOptimize,
    ) -> Result<StageResult<Box<ExplainTimestampStage>>, AdapterError> {
        // Collect optimizer parameters.
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        let mut optimizer = optimize::view::Optimizer::new(optimizer_config, None);

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize explain timestamp",
            move || {
                span.in_scope(|| {
                    let plan::ExplainTimestampPlan {
                        format,
                        raw_plan,
                        when,
                    } = plan;

                    // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local)
                    let optimized_plan = optimizer.optimize(raw_plan)?;

                    let stage =
                        ExplainTimestampStage::RealTimeRecency(ExplainTimestampRealTimeRecency {
                            validity,
                            format,
                            optimized_plan,
                            cluster_id,
                            when,
                        });
                    Ok(Box::new(stage))
                })
            },
        )))
    }

    #[instrument]
    async fn explain_timestamp_real_time_recency(
        &self,
        session: &Session,
        ExplainTimestampRealTimeRecency {
            validity,
            format,
            optimized_plan,
            cluster_id,
            when,
        }: ExplainTimestampRealTimeRecency,
    ) -> Result<StageResult<Box<ExplainTimestampStage>>, AdapterError> {
        let source_ids = optimized_plan.depends_on();
        let source_items: Vec<_> = source_ids
            .iter()
            .map(|gid| self.catalog().resolve_item_id(gid))
            .collect();
        let fut = self
            .determine_real_time_recent_timestamp(session, source_items.into_iter())
            .await?;

        match fut {
            Some(fut) => {
                let span = Span::current();
                Ok(StageResult::Handle(mz_ore::task::spawn(
                    || "explain timestamp real time recency",
                    async move {
                        let real_time_recency_ts = fut.await?;
                        let stage = ExplainTimestampStage::Finish(ExplainTimestampFinish {
                            validity,
                            format,
                            optimized_plan,
                            cluster_id,
                            source_ids,
                            when,
                            real_time_recency_ts: Some(real_time_recency_ts),
                        });
                        Ok(Box::new(stage))
                    }
                    .instrument(span),
                )))
            }
            None => Ok(StageResult::Immediate(Box::new(
                ExplainTimestampStage::Finish(ExplainTimestampFinish {
                    validity,
                    format,
                    optimized_plan,
                    cluster_id,
                    source_ids,
                    when,
                    real_time_recency_ts: None,
                }),
            ))),
        }
    }

    pub(crate) fn explain_timestamp(
        &self,
        session: &Session,
        cluster_id: ClusterId,
        id_bundle: &CollectionIdBundle,
        determination: TimestampDetermination<mz_repr::Timestamp>,
    ) -> TimestampExplanation<mz_repr::Timestamp> {
        let mut sources = Vec::new();
        {
            let storage_ids = id_bundle.storage_ids.iter().cloned().collect_vec();
            let frontiers = self
                .controller
                .storage
                .collections_frontiers(storage_ids)
                .expect("missing collection");

            for (id, since, upper) in frontiers {
                let name = self
                    .catalog()
                    .try_get_entry_by_global_id(&id)
                    .map(|item| item.name())
                    .map(|name| {
                        self.catalog()
                            .resolve_full_name(name, Some(session.conn_id()))
                            .to_string()
                    })
                    .unwrap_or_else(|| id.to_string());
                sources.push(TimestampSource {
                    name: format!("{name} ({id}, storage)"),
                    read_frontier: since.elements().to_vec(),
                    write_frontier: upper.elements().to_vec(),
                });
            }
        }
        {
            if let Some(compute_ids) = id_bundle.compute_ids.get(&cluster_id) {
                let catalog = self.catalog();
                for id in compute_ids {
                    let frontiers = self
                        .controller
                        .compute
                        .collection_frontiers(*id, Some(cluster_id))
                        .expect("id does not exist");
                    let name = catalog
                        .try_get_entry_by_global_id(id)
                        .map(|item| item.name())
                        .map(|name| {
                            catalog
                                .resolve_full_name(name, Some(session.conn_id()))
                                .to_string()
                        })
                        .unwrap_or_else(|| id.to_string());
                    sources.push(TimestampSource {
                        name: format!("{name} ({id}, compute)"),
                        read_frontier: frontiers.read_frontier.to_vec(),
                        write_frontier: frontiers.write_frontier.to_vec(),
                    });
                }
            }
        }
        let respond_immediately = determination.respond_immediately();
        TimestampExplanation {
            determination,
            sources,
            session_wall_time: session.pcx().wall_time,
            respond_immediately,
        }
    }

    #[instrument]
    async fn explain_timestamp_finish(
        &mut self,
        session: &mut Session,
        ExplainTimestampFinish {
            validity: _,
            format,
            optimized_plan,
            cluster_id,
            source_ids,
            when,
            real_time_recency_ts,
        }: ExplainTimestampFinish,
    ) -> Result<StageResult<Box<ExplainTimestampStage>>, AdapterError> {
        let id_bundle = self
            .index_oracle(cluster_id)
            .sufficient_collections(source_ids.iter().copied());

        let is_json = match format {
            ExplainFormat::Text => false,
            ExplainFormat::Json => true,
            ExplainFormat::Dot => {
                return Err(AdapterError::Unsupported("EXPLAIN TIMESTAMP AS DOT"));
            }
        };
        let mut timeline_context = self.validate_timeline_context(source_ids.iter().copied())?;
        if matches!(timeline_context, TimelineContext::TimestampIndependent)
            && optimized_plan.contains_temporal()
        {
            // If the source IDs are timestamp independent but the query contains temporal functions,
            // then the timeline context needs to be upgraded to timestamp dependent. This is
            // required because `source_ids` doesn't contain functions.
            timeline_context = TimelineContext::TimestampDependent;
        }

        let oracle_read_ts = self.oracle_read_ts(session, &timeline_context, &when).await;

        let determination = self.sequence_peek_timestamp(
            session,
            &when,
            cluster_id,
            timeline_context,
            oracle_read_ts,
            &id_bundle,
            &source_ids,
            real_time_recency_ts,
            RequireLinearization::NotRequired,
        )?;
        let explanation = self.explain_timestamp(session, cluster_id, &id_bundle, determination);

        let s = if is_json {
            serde_json::to_string_pretty(&explanation).expect("failed to serialize explanation")
        } else {
            explanation.to_string()
        };
        let rows = vec![Row::pack_slice(&[Datum::from(s.as_str())])];
        Ok(StageResult::Response(Self::send_immediate_rows(rows)))
    }
}
