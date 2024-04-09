// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `SELECT` statements.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_compute_types::dataflows::IndexDesc;
use mz_compute_types::plan::Plan;
use mz_compute_types::ComputeInstanceId;
use mz_expr::{MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr, RowSetFinishing};
use mz_repr::explain::trace_plan;
use mz_repr::{GlobalId, RelationType, Timestamp};
use mz_sql::plan::HirRelationExpr;
use mz_sql::session::metadata::SessionMetadata;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::{StatisticsOracle, TransformCtx};
use timely::progress::Antichain;
use tracing::{debug_span, warn};

use crate::catalog::Catalog;
use crate::coord::peek::{create_fast_path_plan, PeekDataflowPlan, PeekPlan};
use crate::optimize::dataflows::{
    prep_relation_expr, prep_scalar_expr, ComputeInstanceSnapshot, DataflowBuilder, EvalTime,
    ExprPrepStyle,
};
use crate::optimize::metrics::OptimizerMetrics;
use crate::optimize::{
    optimize_mir_local, trace_plan, MirDataflowDescription, Optimize, OptimizeMode,
    OptimizerConfig, OptimizerError,
};
use crate::TimestampContext;

pub struct Optimizer {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// Optional row-set finishing to be applied to the final result.
    finishing: RowSetFinishing,
    /// A transient GlobalId to be used when constructing the dataflow.
    select_id: GlobalId,
    /// A transient GlobalId to be used when constructing a PeekPlan.
    index_id: GlobalId,
    /// Optimizer config.
    config: OptimizerConfig,
    /// Optimizer metrics.
    metrics: OptimizerMetrics,
    /// The time spent performing optimization so far.
    duration: Duration,
}

impl Optimizer {
    pub fn new(
        catalog: Arc<Catalog>,
        compute_instance: ComputeInstanceSnapshot,
        finishing: RowSetFinishing,
        select_id: GlobalId,
        index_id: GlobalId,
        config: OptimizerConfig,
        metrics: OptimizerMetrics,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            finishing,
            select_id,
            index_id,
            config,
            metrics,
            duration: Default::default(),
        }
    }

    pub fn cluster_id(&self) -> ComputeInstanceId {
        self.compute_instance.instance_id()
    }

    pub fn finishing(&self) -> &RowSetFinishing {
        &self.finishing
    }

    pub fn select_id(&self) -> GlobalId {
        self.select_id
    }

    pub fn index_id(&self) -> GlobalId {
        self.index_id
    }
}

// A bogey `Debug` implementation that hides fields. This is needed to make the
// `event!` call in `sequence_peek_stage` not emit a lot of data.
//
// For now, we skip almost all fields, but we might revisit that bit if it turns
// out that we really need those for debugging purposes.
impl Debug for Optimizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptimizePeek")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Marker type for [`LocalMirPlan`] representing an optimization result without
/// context.
pub struct Unresolved;

/// The (sealed intermediate) result after HIR ⇒ MIR lowering and decorrelation
/// and MIR optimization.
#[derive(Clone)]
pub struct LocalMirPlan<T = Unresolved> {
    expr: MirRelationExpr,
    df_meta: DataflowMetainfo,
    context: T,
}

/// Marker type for [`LocalMirPlan`] structs representing an optimization result
/// with attached environment context required for the next optimization stage.
pub struct Resolved<'s> {
    timestamp_ctx: TimestampContext<Timestamp>,
    stats: Box<dyn StatisticsOracle>,
    session: &'s dyn SessionMetadata,
}

/// The (final) result after
///
/// 1. embedding a [`LocalMirPlan`] into a `DataflowDescription`,
/// 2. transitively inlining referenced views,
/// 3. timestamp resolution,
/// 4. optimizing the resulting `DataflowDescription` with `MIR` plans.
/// 5. MIR ⇒ LIR lowering, and
/// 6. optimizing the resulting `DataflowDescription` with `LIR` plans.
///
///  MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
#[derive(Debug)]
pub struct GlobalLirPlan {
    peek_plan: PeekPlan,
    df_meta: DataflowMetainfo,
    typ: RelationType,
}

impl Optimize<HirRelationExpr> for Optimizer {
    type To = LocalMirPlan;

    fn optimize(&mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        // Trace the pipeline input under `optimize/raw`.
        trace_plan!(at: "raw", &expr);

        // HIR ⇒ MIR lowering and decorrelation
        let expr = expr.lower(&self.config)?;

        // MIR ⇒ MIR optimization (local)
        let mut df_meta = DataflowMetainfo::default();
        let mut transform_ctx =
            TransformCtx::local(&self.config.features, &self.typecheck_ctx, &mut df_meta);
        let expr = optimize_mir_local(expr, &mut transform_ctx)?.into_inner();

        self.duration += time.elapsed();

        // Return the (sealed) plan at the end of this optimization step.
        Ok(LocalMirPlan {
            expr,
            df_meta,
            context: Unresolved,
        })
    }
}

impl LocalMirPlan<Unresolved> {
    /// Produces the [`LocalMirPlan`] with [`Resolved`] contextual information
    /// required for the next stage.
    pub fn resolve(
        self,
        timestamp_ctx: TimestampContext<Timestamp>,
        session: &dyn SessionMetadata,
        stats: Box<dyn StatisticsOracle>,
    ) -> LocalMirPlan<Resolved> {
        LocalMirPlan {
            expr: self.expr,
            df_meta: self.df_meta,
            context: Resolved {
                timestamp_ctx,
                session,
                stats,
            },
        }
    }
}

impl<'s> Optimize<LocalMirPlan<Resolved<'s>>> for Optimizer {
    type To = GlobalLirPlan;

    fn optimize(&mut self, plan: LocalMirPlan<Resolved<'s>>) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        let LocalMirPlan {
            expr,
            mut df_meta,
            context:
                Resolved {
                    timestamp_ctx,
                    stats,
                    session,
                },
        } = plan;

        let expr = OptimizedMirRelationExpr(expr);

        // We create a dataflow and optimize it, to determine if we can avoid building it.
        // This can happen if the result optimizes to a constant, or to a `Get` expression
        // around a maintained arrangement.
        let typ = expr.typ();
        let key = typ
            .default_key()
            .iter()
            .map(|k| MirScalarExpr::Column(*k))
            .collect();

        // The assembled dataflow contains a view and an index of that view.
        let mut df_builder = {
            let catalog = self.catalog.state();
            let compute = self.compute_instance.clone();
            DataflowBuilder::new(catalog, compute).with_config(&self.config)
        };

        let debug_name = format!("oneshot-select-{}", self.select_id);
        let mut df_desc = MirDataflowDescription::new(debug_name.to_string());

        df_builder.import_view_into_dataflow(&self.select_id, &expr, &mut df_desc)?;
        df_builder.maybe_reoptimize_imported_views(&mut df_desc, &self.config)?;

        // Resolve all unmaterializable function calls except mz_now(), because
        // we don't yet have a timestamp.
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Deferred,
            session,
            catalog_state: self.catalog.state(),
        };
        df_desc.visit_children(
            |r| prep_relation_expr(r, style),
            |s| prep_scalar_expr(s, style),
        )?;

        // TODO: Instead of conditioning here we should really
        // reconsider how to render multi-plan peek dataflows. The main
        // difficulty here is rendering the optional finishing bit.
        if self.config.mode != OptimizeMode::Explain {
            df_desc.export_index(
                self.index_id,
                IndexDesc {
                    on_id: self.select_id,
                    key,
                },
                typ.clone(),
            );
        }

        // Set the `as_of` and `until` timestamps for the dataflow.
        df_desc.set_as_of(timestamp_ctx.antichain());

        // Use the opportunity to name an `until` frontier that will prevent
        // work we needn't perform. By default, `until` will be
        // `Antichain::new()`, which prevents no updates and is safe.
        //
        // If `timestamp_ctx.antichain()` is empty, `timestamp_ctx.timestamp()`
        // will return `None` and we use the default (empty) `until`. Otherwise,
        // we expect to be able to set `until = as_of + 1` without an overflow.
        if let Some(as_of) = timestamp_ctx.timestamp() {
            if let Some(until) = as_of.checked_add(1) {
                df_desc.until = Antichain::from_elem(until);
            } else {
                warn!(as_of = %as_of, "as_of + 1 overflow");
            }
        }

        // Construct TransformCtx for global optimization.
        let mut transform_ctx = TransformCtx::global(
            &df_builder,
            &*stats,
            &self.config.features,
            &self.typecheck_ctx,
            &mut df_meta,
        );
        // Run global optimization.
        mz_transform::optimize_dataflow(&mut df_desc, &mut transform_ctx)?;

        if self.config.mode == OptimizeMode::Explain {
            // Collect the list of indexes used by the dataflow at this point.
            trace_plan!(at: "global", &df_meta.used_indexes(&df_desc));
        }

        // Get the single timestamp representing the `as_of` time.
        let as_of = df_desc
            .as_of
            .clone()
            .expect("as_of antichain")
            .into_option()
            .expect("unique as_of element");

        // Resolve all unmaterializable function calls including mz_now().
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Time(as_of),
            session,
            catalog_state: self.catalog.state(),
        };
        df_desc.visit_children(
            |r| prep_relation_expr(r, style),
            |s| prep_scalar_expr(s, style),
        )?;

        // TODO: use the following code once we can be sure that the
        // index_exports always exist.
        //
        // let typ = self.df_desc
        //     .index_exports
        //     .first_key_value()
        //     .map(|(_key, (_desc, typ))| typ.clone())
        //     .expect("GlobalMirPlan type");

        let peek_plan = match create_fast_path_plan(
            &mut df_desc,
            self.select_id,
            Some(&self.finishing),
            self.config.features.persist_fast_path_limit,
        )? {
            Some(plan) if !self.config.no_fast_path => {
                if self.config.mode == OptimizeMode::Explain {
                    // Trace the `used_indexes` for the FastPathPlan.
                    debug_span!(target: "optimizer", "fast_path").in_scope(|| {
                        // Fast path plans come with an updated finishing.
                        let finishing = if !self.finishing.is_trivial(typ.arity()) {
                            Some(&self.finishing)
                        } else {
                            None
                        };
                        trace_plan(&plan.used_indexes(finishing));
                    });
                }
                // Trace the FastPathPlan.
                trace_plan!(at: "fast_path", &plan);

                // Trace the pipeline output under `optimize`.
                trace_plan(&plan);

                // Build the PeekPlan
                PeekPlan::FastPath(plan)
            }
            _ => {
                // Ensure all expressions are normalized before finalizing.
                for build in df_desc.objects_to_build.iter_mut() {
                    normalize_lets(&mut build.plan.0)?
                }

                // Finalize the dataflow. This includes:
                // - MIR ⇒ LIR lowering
                // - LIR ⇒ LIR transforms
                let df_desc = Plan::finalize_dataflow(df_desc, &self.config.features)?;

                // Trace the pipeline output under `optimize`.
                trace_plan(&df_desc);

                // Build the PeekPlan
                PeekPlan::SlowPath(PeekDataflowPlan::new(df_desc, self.index_id(), &typ))
            }
        };

        self.duration += time.elapsed();
        let label = match &peek_plan {
            PeekPlan::FastPath(_) => "peek:fast_path",
            PeekPlan::SlowPath(_) => "peek:slow_path",
        };
        self.metrics
            .observe_e2e_optimization_time(label, self.duration);

        Ok(GlobalLirPlan {
            peek_plan,
            df_meta,
            typ,
        })
    }
}

impl GlobalLirPlan {
    /// Unwraps the parts of the final result of the optimization pipeline.
    pub fn unapply(self) -> (PeekPlan, DataflowMetainfo, RelationType) {
        (self.peek_plan, self.df_meta, self.typ)
    }
}
