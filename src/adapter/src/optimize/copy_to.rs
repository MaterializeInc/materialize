// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `COPY TO` statements.

use std::fmt::Debug;
use std::sync::Arc;

use http::Uri;
use mz_compute_types::plan::Plan;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, S3OneshotSinkConnection};
use mz_compute_types::ComputeInstanceId;
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_pgcopy::CopyFormatParams;
use mz_repr::explain::trace_plan;
use mz_repr::{GlobalId, RelationDesc, Timestamp};
use mz_sql::plan::HirRelationExpr;
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::connections::Connection;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::{Optimizer as TransformOptimizer, StatisticsOracle};
use timely::progress::Antichain;
use tracing::{span, warn, Level};

use crate::catalog::Catalog;
use crate::optimize::dataflows::{
    prep_relation_expr, prep_scalar_expr, ComputeInstanceSnapshot, DataflowBuilder, EvalTime,
    ExprPrepStyle,
};
use crate::optimize::{
    trace_plan, LirDataflowDescription, MirDataflowDescription, Optimize, OptimizeMode,
    OptimizerConfig, OptimizerError,
};
use crate::session::Session;
use crate::TimestampContext;

pub struct Optimizer {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A transient GlobalId to be used when constructing the dataflow.
    select_id: GlobalId,
    /// The [`RelationDesc`] for the optimized statement.
    desc: RelationDesc,
    /// The destination [`Uri`].
    copy_to_uri: Uri,
    /// The connection to be used by the the `COPY TO` sink.
    copy_to_connection: Connection<ReferencedConnection>,
    /// Formatting parameters for the `COPY TO` sink.
    #[allow(dead_code)] // TODO(#7256): remove if not used when the epic is closed.
    copy_to_format_params: CopyFormatParams<'static>,
    // Optimizer config.
    config: OptimizerConfig,
}

impl Optimizer {
    pub fn new(
        catalog: Arc<Catalog>,
        compute_instance: ComputeInstanceSnapshot,
        select_id: GlobalId,
        desc: RelationDesc,
        copy_to_uri: http::Uri,
        copy_to_connection: Connection<ReferencedConnection>,
        copy_to_format_params: CopyFormatParams<'static>,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            select_id,
            desc,
            copy_to_uri,
            copy_to_connection,
            copy_to_format_params,
            config,
        }
    }

    pub fn cluster_id(&self) -> ComputeInstanceId {
        self.compute_instance.instance_id()
    }

    pub fn copy_to_uri(&self) -> &Uri {
        &self.copy_to_uri
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
            .finish()
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
    context: T,
}

/// Marker type for [`LocalMirPlan`] structs representing an optimization result
/// with attached environment context required for the next optimization stage.
pub struct Resolved<'s> {
    timestamp_ctx: TimestampContext<Timestamp>,
    stats: Box<dyn StatisticsOracle>,
    session: &'s Session,
}

/// The (sealed intermediate) result after:
///
/// 1. embedding a [`LocalMirPlan`] into a [`MirDataflowDescription`],
/// 2. transitively inlining referenced views,
/// 3. timestamp resolution, and
/// 4. jointly optimizing the `MIR` plans in the [`MirDataflowDescription`].
#[derive(Clone)]
pub struct GlobalMirPlan {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
}

/// The (final) result after MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
#[derive(Debug)]
pub struct GlobalLirPlan {
    df_desc: LirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl Optimize<HirRelationExpr> for Optimizer {
    type To = LocalMirPlan;

    fn optimize(&mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        // Trace the pipeline input under `optimize/raw`.
        trace_plan!(at: "raw", &expr);

        // HIR ⇒ MIR lowering and decorrelation
        let expr = expr.lower(&self.config)?;

        // MIR ⇒ MIR optimization (local)
        let expr = span!(target: "optimizer", Level::DEBUG, "local").in_scope(|| {
            #[allow(deprecated)]
            let optimizer = TransformOptimizer::logical_optimizer(&self.typecheck_ctx);
            let expr = optimizer.optimize(expr)?.into_inner();

            // Trace the result of this phase.
            trace_plan(&expr);

            Ok::<_, OptimizerError>(expr)
        })?;

        // Return the (sealed) plan at the end of this optimization step.
        Ok(LocalMirPlan {
            expr,
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
        session: &Session,
        stats: Box<dyn StatisticsOracle>,
    ) -> LocalMirPlan<Resolved> {
        LocalMirPlan {
            expr: self.expr,
            context: Resolved {
                timestamp_ctx,
                session,
                stats,
            },
        }
    }
}

impl<'s> Optimize<LocalMirPlan<Resolved<'s>>> for Optimizer {
    type To = GlobalMirPlan;

    fn optimize(&mut self, plan: LocalMirPlan<Resolved<'s>>) -> Result<Self::To, OptimizerError> {
        let LocalMirPlan {
            expr,
            context:
                Resolved {
                    timestamp_ctx,
                    stats,
                    session,
                },
        } = plan;

        let expr = OptimizedMirRelationExpr(expr);

        // The assembled dataflow contains a view and a sink on that view.
        let mut df_builder = {
            let catalog = self.catalog.state();
            let compute = self.compute_instance.clone();
            DataflowBuilder::new(catalog, compute).with_config(&self.config)
        };

        let debug_name = format!("oneshot-select-{}", self.select_id);
        let mut df_desc = MirDataflowDescription::new(debug_name.to_string());

        df_builder.import_view_into_dataflow(&self.select_id, &expr, &mut df_desc)?;
        df_builder.reoptimize_imported_views(&mut df_desc, &self.config)?;

        // Creating an S3 sink as currently only s3 sinks are supported. It
        // might be possible in the future for COPY TO to write to different
        // sinks, which should be set here depending upon the url scheme.
        let connection = match &self.copy_to_connection {
            Connection::Aws(aws_connection) => {
                ComputeSinkConnection::S3Oneshot(S3OneshotSinkConnection {
                    aws_connection: aws_connection.clone(),
                    prefix: self.copy_to_uri.to_string(),
                })
            }
            _ => {
                // Currently only s3 sinks are supported. It was already validated in planning that this
                // is an aws connection.
                let msg = "only aws connection is supported in COPY TO";
                return Err(OptimizerError::Internal(msg.to_string()));
            }
        };
        let sink_desc = ComputeSinkDesc {
            from_desc: self.desc.clone(),
            from: self.select_id,
            connection,
            with_snapshot: true,
            // This will get updated  when the GlobalMirPlan is resolved with as_of below.
            up_to: Default::default(),
            // No `FORCE NOT NULL` for copy_to.
            non_null_assertions: Vec::new(),
            // No `REFRESH` for copy_to.
            refresh_schedule: None,
        };
        df_desc.export_sink(self.select_id, sink_desc);

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

        // Set the `as_of` and `until` timestamps for the dataflow.
        df_desc.set_as_of(timestamp_ctx.antichain());

        // Use the the opportunity to name an `until` frontier that will prevent
        // work we needn't perform. By default, `until` will be
        // `Antichain::new()`, which prevents no updates and is safe.
        //
        // If `timestamp_ctx.antichain()` is empty, `timestamp_ctx.timestamp()`
        // will return `None` and we use the default (empty) `until`. Otherwise,
        // we expect to be able to set `until = as_of + 1` without an overflow.
        if let Some(as_of) = timestamp_ctx.timestamp() {
            if let Some(until) = as_of.checked_add(1) {
                df_desc.until = Antichain::from_elem(until);
                // Also updating the sink up_to
                for (_, sink) in &mut df_desc.sink_exports {
                    sink.up_to = df_desc.until.clone();
                }
            } else {
                warn!(as_of = %as_of, "as_of + 1 overflow");
            }
        }

        let df_meta = mz_transform::optimize_dataflow(
            &mut df_desc,
            &df_builder,
            &*stats,
            self.config.enable_eager_delta_joins,
        )?;

        if self.config.mode == OptimizeMode::Explain {
            // Collect the list of indexes used by the dataflow at this point.
            trace_plan!(at: "global", &df_meta.used_indexes(&df_desc));
        }

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalMirPlan { df_desc, df_meta })
    }
}

impl Optimize<GlobalMirPlan> for Optimizer {
    type To = GlobalLirPlan;

    fn optimize(&mut self, plan: GlobalMirPlan) -> Result<Self::To, OptimizerError> {
        let GlobalMirPlan {
            mut df_desc,
            df_meta,
        } = plan;

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

        // Ensure all expressions are normalized before finalizing.
        for build in df_desc.objects_to_build.iter_mut() {
            normalize_lets(&mut build.plan.0)?
        }

        // Finalize the dataflow. This includes:
        // - MIR ⇒ LIR lowering
        // - LIR ⇒ LIR transforms
        let df_desc = Plan::finalize_dataflow(
            df_desc,
            self.config.enable_consolidate_after_union_negate,
            self.config.enable_specialized_arrangements,
            self.config.enable_reduce_mfp_fusion,
        )
        .map_err(OptimizerError::Internal)?;

        // Trace the pipeline output under `optimize`.
        trace_plan(&df_desc);

        Ok(GlobalLirPlan { df_desc, df_meta })
    }
}

impl GlobalLirPlan {
    /// Unwraps the parts of the final result of the optimization pipeline.
    pub fn unapply(self) -> (LirDataflowDescription, DataflowMetainfo) {
        (self.df_desc, self.df_meta)
    }
}
