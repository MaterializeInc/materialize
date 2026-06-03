// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `CREATE MATERIALIZED VIEW` statements.
//!
//! Note that, in contrast to other optimization pipelines, timestamp selection is not part of
//! MV optimization. Instead users are expected to separately set the as-of on the optimized
//! `DataflowDescription` received from `GlobalLirPlan::unapply`. Reasons for choosing to exclude
//! timestamp selection from the MV optimization pipeline are:
//!
//!  (a) MVs don't support non-empty `until` frontiers, so they don't provide opportunity for
//!      optimizations based on the selected timestamp.
//!  (b) We want to generate dataflow plans early during environment bootstrapping, before we have
//!      access to all information required for timestamp selection.
//!
//! None of this is set in stone though. If we find an opportunity for optimizing MVs based on
//! their timestamps, we'll want to make timestamp selection part of the MV optimization again and
//! find a different approach to bootstrapping.
//!
//! See also MaterializeInc/materialize#22940.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_compute_types::dataflows::ChangelogMode;
use mz_compute_types::plan::Plan;
use mz_compute_types::sinks::{
    ComputeSinkConnection, ComputeSinkDesc, MaterializedViewSinkConnection,
};
use mz_expr::visit::Visit;
use mz_expr::{
    Eval, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr, UnmaterializableFunc,
};
use mz_repr::adt::interval::Interval;
use mz_repr::explain::trace_plan;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{
    ColumnName, Datum, GlobalId, RelationDesc, ReprScalarType, RowArena, SqlRelationType, Timestamp,
};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::HirRelationExpr;
use mz_transform::TransformCtx;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{SharedTypecheckingContext, empty_typechecking_context};
use timely::progress::Antichain;

use crate::coord::infer_sql_type_for_catalog;
use crate::optimize::dataflows::{
    ComputeInstanceSnapshot, DataflowBuilder, ExprPrep, ExprPrepMaintained,
};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizeMode, OptimizerCatalog,
    OptimizerConfig, OptimizerError, optimize_mir_local, trace_plan,
};

pub struct Optimizer {
    /// A representation typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: SharedTypecheckingContext,
    /// A snapshot of the catalog state.
    catalog: Arc<dyn OptimizerCatalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A durable GlobalId to be used with the exported materialized view sink.
    sink_id: GlobalId,
    /// A transient GlobalId to be used when constructing the dataflow.
    view_id: GlobalId,
    /// The resulting column names.
    column_names: Vec<ColumnName>,
    /// Output columns that are asserted to be not null in the `CREATE VIEW`
    /// statement.
    non_null_assertions: Vec<usize>,
    /// Refresh schedule, e.g., `REFRESH EVERY '1 day'`
    refresh_schedule: Option<RefreshSchedule>,
    /// A human-readable name exposed internally (useful for debugging).
    debug_name: String,
    /// Optimizer config.
    config: OptimizerConfig,
    /// Optimizer metrics.
    metrics: OptimizerMetrics,
    /// The time spent performing optimization so far.
    duration: Duration,
}

impl Optimizer {
    pub fn new(
        catalog: Arc<dyn OptimizerCatalog>,
        compute_instance: ComputeInstanceSnapshot,
        sink_id: GlobalId,
        view_id: GlobalId,
        column_names: Vec<ColumnName>,
        non_null_assertions: Vec<usize>,
        refresh_schedule: Option<RefreshSchedule>,
        debug_name: String,
        config: OptimizerConfig,
        metrics: OptimizerMetrics,
    ) -> Self {
        Self {
            typecheck_ctx: empty_typechecking_context(),
            catalog,
            compute_instance,
            sink_id,
            view_id,
            column_names,
            non_null_assertions,
            refresh_schedule,
            debug_name,
            config,
            metrics,
            duration: Default::default(),
        }
    }
}

/// The (sealed intermediate) result after HIR ⇒ MIR lowering and decorrelation
/// and MIR optimization.
#[derive(Clone, Debug)]
pub struct LocalMirPlan {
    expr: MirRelationExpr,
    df_meta: DataflowMetainfo,
    typ: SqlRelationType,
}

/// The (sealed intermediate) result after:
///
/// 1. embedding a [`LocalMirPlan`] into a [`MirDataflowDescription`],
/// 2. transitively inlining referenced views, and
/// 3. jointly optimizing the `MIR` plans in the [`MirDataflowDescription`].
#[derive(Clone, Debug)]
pub struct GlobalMirPlan {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl GlobalMirPlan {
    pub fn df_desc(&self) -> &MirDataflowDescription {
        &self.df_desc
    }
}

/// The (final) result after MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
#[derive(Clone, Debug)]
pub struct GlobalLirPlan {
    df_desc: LirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl GlobalLirPlan {
    pub fn df_desc(&self) -> &LirDataflowDescription {
        &self.df_desc
    }

    pub fn df_meta(&self) -> &DataflowMetainfo {
        &self.df_meta
    }

    pub fn desc(&self) -> &RelationDesc {
        let sink_exports = &self.df_desc.sink_exports;
        let sink = sink_exports.values().next().expect("valid sink");
        &sink.from_desc
    }
}

impl Optimize<HirRelationExpr> for Optimizer {
    type To = LocalMirPlan;

    fn optimize(&mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        // Trace the pipeline input under `optimize/raw`.
        trace_plan!(at: "raw", &expr);

        // HIR ⇒ MIR lowering and decorrelation
        let mir_expr = expr.clone().lower(&self.config, Some(&self.metrics))?;

        // MIR ⇒ MIR optimization (local)
        let mut df_meta = DataflowMetainfo::default();
        let mut transform_ctx = TransformCtx::local(
            &self.config.features,
            &self.typecheck_ctx,
            &mut df_meta,
            Some(&mut self.metrics),
            Some(self.view_id),
        );
        let mir_expr = optimize_mir_local(mir_expr, &mut transform_ctx)?.into_inner();
        let typ = infer_sql_type_for_catalog(&expr, &mir_expr);

        self.duration += time.elapsed();

        // Return the (sealed) plan at the end of this optimization step.
        Ok(LocalMirPlan {
            expr: mir_expr,
            df_meta,
            typ,
        })
    }
}

impl LocalMirPlan {
    pub fn expr(&self) -> OptimizedMirRelationExpr {
        OptimizedMirRelationExpr(self.expr.clone())
    }
}

/// This is needed only because the pipeline in the bootstrap code starts from an
/// [`OptimizedMirRelationExpr`] attached to a [`mz_catalog::memory::objects::CatalogItem`].
impl Optimize<(OptimizedMirRelationExpr, SqlRelationType)> for Optimizer {
    type To = GlobalMirPlan;

    fn optimize(
        &mut self,
        (expr, typ): (OptimizedMirRelationExpr, SqlRelationType),
    ) -> Result<Self::To, OptimizerError> {
        let expr = expr.into_inner();
        let df_meta = DataflowMetainfo::default();
        self.optimize(LocalMirPlan { expr, df_meta, typ })
    }
}

impl Optimize<LocalMirPlan> for Optimizer {
    type To = GlobalMirPlan;

    fn optimize(&mut self, plan: LocalMirPlan) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        let expr = OptimizedMirRelationExpr(plan.expr);
        let mut df_meta = plan.df_meta;

        let mut rel_typ = plan.typ;
        for &i in self.non_null_assertions.iter() {
            rel_typ.column_types[i].nullable = false;
        }
        let rel_desc = RelationDesc::new(rel_typ, self.column_names.clone());

        let mut df_builder = {
            let compute = self.compute_instance.clone();
            DataflowBuilder::new(&*self.catalog, compute).with_config(&self.config)
        };
        let mut df_desc = MirDataflowDescription::new(self.debug_name.clone());

        df_desc.refresh_schedule.clone_from(&self.refresh_schedule);

        df_builder.import_view_into_dataflow(
            &self.view_id,
            &expr,
            &mut df_desc,
            &self.config.features,
        )?;
        df_builder.maybe_reoptimize_imported_views(&mut df_desc, &self.config)?;

        let sink_description = ComputeSinkDesc {
            from: self.view_id,
            from_desc: rel_desc.clone(),
            connection: ComputeSinkConnection::MaterializedView(MaterializedViewSinkConnection {
                value_desc: rel_desc,
                storage_metadata: (),
            }),
            with_snapshot: true,
            up_to: Antichain::default(),
            non_null_assertions: self.non_null_assertions.clone(),
            refresh_schedule: self.refresh_schedule.clone(),
        };
        df_desc.export_sink(self.sink_id, sink_description);

        // `CHANGES`: mark changelog reads in the dataflow so their source
        // imports are read as maintained changelogs (snapshot skipped, deltas
        // replayed from a coordinator-chosen start; see `ChangelogMode`).
        // Unlike the peek path there is no query time to resolve the bound
        // against; instead we extract the window lag `i` from the
        // `mz_now()`-relative bound, and the read start is resolved later,
        // wherever the dataflow `as_of` is decided (the sequencer at creation,
        // as-of selection at bootstrap, command-history reduction on replica
        // reconnect).
        //
        // Because the window slides forever, each changelog read is wrapped in
        // the temporal filter `mz_now() < mz_timestamp + i`, which retracts
        // changes once they age past the window's trailing edge. This is what
        // bounds the MV's state and keeps aggregations over it correct, and
        // its strictness (`<`) aligns the retained window with the open delta
        // replay interval `(start, now]`, making a restart reproduce the
        // persisted contents exactly. The predicate is spelled in the
        // `mz_now() BINOP <non-temporal>` normal form the temporal MFP
        // machinery requires, with the interval arithmetic on the column side
        // (`mz_timestamp` itself deliberately supports no arithmetic).
        let mut changelog_windows = BTreeMap::new();
        for build in df_desc.objects_to_build.iter_mut() {
            let mut result = Ok(());
            build.plan.as_inner_mut().visit_mut_post(&mut |e| {
                if let MirRelationExpr::Changes { id, bound, typ, .. } = e {
                    if self.refresh_schedule.is_some() {
                        // REFRESH rounds the dataflow `as_of` and `until` in
                        // ways the sliding changelog start does not compose
                        // with yet.
                        result = Err(OptimizerError::UnsupportedTemporalExpression(
                            "CHANGES is not supported in materialized views with a refresh \
                             schedule"
                                .to_string(),
                        ));
                        return;
                    }
                    let window = match changelog_window(bound) {
                        Ok(window) => window,
                        Err(err) => {
                            result = Err(err);
                            return;
                        }
                    };
                    // Multiple reads of the same input use the widest window
                    // for the import's read hold and start; each read's own
                    // filter still enforces its own window.
                    changelog_windows
                        .entry(*id)
                        .and_modify(|w| *w = std::cmp::max(*w, window))
                        .or_insert(window);

                    // The `mz_timestamp` column precedes `mz_diff` at the end.
                    let ts_col = typ.arity() - 2;
                    let micros = u64::from(window)
                        .checked_mul(1000)
                        .and_then(|m| i64::try_from(m).ok());
                    let Some(micros) = micros else {
                        result = Err(OptimizerError::UnsupportedTemporalExpression(
                            "the AS OF bound of CHANGES in a materialized view trails mz_now() \
                             by too large an amount"
                                .to_string(),
                        ));
                        return;
                    };
                    let lag = Interval::new(0, 0, micros);
                    let predicate =
                        MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow)
                            .call_binary(
                                MirScalarExpr::column(ts_col)
                                    .call_unary(mz_expr::func::CastMzTimestampToTimestampTz)
                                    .call_binary(
                                        MirScalarExpr::literal_ok(
                                            Datum::Interval(lag),
                                            ReprScalarType::Interval,
                                        ),
                                        mz_expr::func::AddTimestampTzInterval,
                                    )
                                    .call_unary(mz_expr::func::CastTimestampTzToMzTimestamp),
                                mz_expr::func::Lt,
                            );
                    let input = e.take_dangerous();
                    *e = input.filter(vec![predicate]);
                }
            })?;
            result?;
        }
        for (id, window) in changelog_windows {
            df_desc.set_source_changelog(
                &id,
                ChangelogMode::Maintained {
                    window,
                    start: None,
                },
            );
        }

        // Prepare expressions in the assembled dataflow.
        let style = ExprPrepMaintained;
        df_desc.visit_children(
            |r| style.prep_relation_expr(r),
            |s| style.prep_scalar_expr(s),
        )?;

        // Construct TransformCtx for global optimization.
        let mut transform_ctx = TransformCtx::global(
            &df_builder,
            &mz_transform::EmptyStatisticsOracle, // TODO: wire proper stats
            &self.config.features,
            &self.typecheck_ctx,
            &mut df_meta,
            Some(&mut self.metrics),
        );
        // Run global optimization.
        mz_transform::optimize_dataflow(&mut df_desc, &mut transform_ctx, false)?;

        if self.config.mode == OptimizeMode::Explain {
            // Collect the list of indexes used by the dataflow at this point.
            trace_plan!(at: "global", &df_meta.used_indexes(&df_desc));
        }

        self.duration += time.elapsed();

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalMirPlan { df_desc, df_meta })
    }
}

impl Optimize<GlobalMirPlan> for Optimizer {
    type To = GlobalLirPlan;

    fn optimize(&mut self, plan: GlobalMirPlan) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        let GlobalMirPlan {
            mut df_desc,
            df_meta,
        } = plan;

        // Ensure all expressions are normalized before finalizing.
        for build in df_desc.objects_to_build.iter_mut() {
            normalize_lets(&mut build.plan.0, &self.config.features)?
        }

        // Finalize the dataflow. This includes:
        // - MIR ⇒ LIR lowering
        // - LIR ⇒ LIR transforms
        let df_desc = Plan::finalize_dataflow(df_desc, &self.config.features)?;

        // Trace the pipeline output under `optimize`.
        trace_plan(&df_desc);

        self.duration += time.elapsed();
        self.metrics
            .observe_e2e_optimization_time("materialized_view", self.duration);

        // Return the plan at the end of this `optimize` step.
        Ok(GlobalLirPlan { df_desc, df_meta })
    }
}

impl GlobalLirPlan {
    /// Unwraps the parts of the final result of the optimization pipeline.
    pub fn unapply(self) -> (LirDataflowDescription, DataflowMetainfo) {
        (self.df_desc, self.df_meta)
    }
}

/// Extracts the sliding-window lag `i` from an `mz_now()`-relative `CHANGES`
/// bound, defined by `bound(t) = t - i`.
///
/// The bound is evaluated at two reference times; anything whose lag is not a
/// constant shift of `mz_now()` is rejected, since the maintained changelog
/// machinery (a constant-lag read hold and a constant-lag read start) cannot
/// represent it.
fn changelog_window(bound: &MirScalarExpr) -> Result<Timestamp, OptimizerError> {
    let eval_at = |t: Timestamp| -> Result<Timestamp, OptimizerError> {
        let mut expr = bound.clone();
        expr.visit_mut_post(&mut |e| {
            if let MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow) = e {
                *e = MirScalarExpr::literal_ok(Datum::MzTimestamp(t), ReprScalarType::MzTimestamp);
            }
        })?;
        let arena = RowArena::new();
        match expr.eval(&[], &arena)? {
            Datum::MzTimestamp(ts) => Ok(ts),
            other => Err(OptimizerError::Internal(format!(
                "CHANGES bound did not evaluate to an mz_timestamp: {other:?}"
            ))),
        }
    };

    // Two reference times chosen so that calendar-dependent shifts produce
    // different lags: subtracting a month interval from 2020-03-15 spans
    // February (29 days in 2020) while subtracting it from 2020-04-15 spans
    // March (31 days), so month intervals — whose millisecond lag varies with
    // the calendar — are detected and rejected. Fixed intervals (days and
    // smaller) shift both times by the same amount.
    let t1 = Timestamp::from(1_584_230_400_000u64); // 2020-03-15 00:00:00 UTC
    let t2 = Timestamp::from(1_586_908_800_000u64); // 2020-04-15 00:00:00 UTC
    let lag = |t: Timestamp, b: Timestamp| u64::from(t).checked_sub(u64::from(b));
    let lag1 = lag(t1, eval_at(t1)?);
    let lag2 = lag(t2, eval_at(t2)?);
    match (lag1, lag2) {
        (Some(lag1), Some(lag2)) if lag1 == lag2 => Ok(Timestamp::from(lag1)),
        _ => Err(OptimizerError::UnsupportedTemporalExpression(
            "the AS OF bound of CHANGES in a materialized view must trail mz_now() by a \
             constant amount, e.g. mz_now() - INTERVAL '30 minutes' (month intervals vary \
             with the calendar and are not supported)"
                .to_string(),
        )),
    }
}
