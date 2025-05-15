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
use std::time::{Duration, Instant};

use mz_compute_types::ComputeInstanceId;
use mz_compute_types::plan::Plan;
use mz_compute_types::sinks::{
    ComputeSinkConnection, ComputeSinkDesc, CopyToS3OneshotSinkConnection,
};
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_repr::explain::trace_plan;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::HirRelationExpr;
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_types::connections::Connection;
use mz_storage_types::sinks::S3UploadInfo;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{SharedContext as TypecheckContext, empty_context};
use mz_transform::{StatisticsOracle, TransformCtx};
use timely::progress::Antichain;
use tracing::warn;

use crate::TimestampContext;
use crate::catalog::Catalog;
use crate::coord::CopyToContext;
use crate::optimize::dataflows::{
    ComputeInstanceSnapshot, DataflowBuilder, EvalTime, ExprPrepStyle, prep_relation_expr,
    prep_scalar_expr,
};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizeMode, OptimizerConfig,
    OptimizerError, optimize_mir_local, trace_plan,
};

pub struct Optimizer {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A transient GlobalId to be used when constructing the dataflow.
    select_id: GlobalId,
    /// Data required to do a COPY TO query.
    copy_to_context: CopyToContext,
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
        select_id: GlobalId,
        copy_to_context: CopyToContext,
        config: OptimizerConfig,
        metrics: OptimizerMetrics,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            select_id,
            copy_to_context,
            config,
            metrics,
            duration: Default::default(),
        }
    }

    pub fn cluster_id(&self) -> ComputeInstanceId {
        self.compute_instance.instance_id()
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
#[derive(Debug)]
pub struct GlobalLirPlan {
    df_desc: LirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl GlobalLirPlan {
    pub fn df_desc(&self) -> &LirDataflowDescription {
        &self.df_desc
    }

    pub fn sink_id(&self) -> GlobalId {
        let sink_exports = &self.df_desc.sink_exports;
        let sink_id = sink_exports.keys().next().expect("valid sink");
        *sink_id
    }
}

impl Optimize<HirRelationExpr> for Optimizer {
    type To = LocalMirPlan;

    fn optimize(&mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        // Trace the pipeline input under `optimize/raw`.
        trace_plan!(at: "raw", &expr);

        // HIR ⇒ MIR lowering and decorrelation
        let expr = expr.lower(&self.config, Some(&self.metrics))?;

        // MIR ⇒ MIR optimization (local)
        let mut df_meta = DataflowMetainfo::default();
        let mut transform_ctx = TransformCtx::local(
            &self.config.features,
            &self.typecheck_ctx,
            &mut df_meta,
            Some(&self.metrics),
            Some(self.select_id),
        );
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

        // The assembled dataflow contains a view and a sink on that view.
        let mut df_builder = {
            let catalog = self.catalog.state();
            let compute = self.compute_instance.clone();
            DataflowBuilder::new(catalog, compute).with_config(&self.config)
        };

        let debug_name = format!("copy-to-{}", self.select_id);
        let mut df_desc = MirDataflowDescription::new(debug_name.to_string());

        df_builder.import_view_into_dataflow(
            &self.select_id,
            &expr,
            &mut df_desc,
            &self.config.features,
        )?;
        df_builder.maybe_reoptimize_imported_views(&mut df_desc, &self.config)?;

        // Creating an S3 sink as currently only s3 sinks are supported. It
        // might be possible in the future for COPY TO to write to different
        // sinks, which should be set here depending upon the url scheme.
        let connection = match &self.copy_to_context.connection {
            Connection::Aws(aws_connection) => {
                ComputeSinkConnection::CopyToS3Oneshot(CopyToS3OneshotSinkConnection {
                    upload_info: S3UploadInfo {
                        uri: self.copy_to_context.uri.to_string(),
                        max_file_size: self.copy_to_context.max_file_size,
                        desc: self.copy_to_context.desc.clone(),
                        format: self.copy_to_context.format.clone(),
                    },
                    aws_connection: aws_connection.clone(),
                    connection_id: self.copy_to_context.connection_id,
                    output_batch_count: self
                        .copy_to_context
                        .output_batch_count
                        .expect("output_batch_count should be set in sequencer"),
                })
            }
            _ => {
                // Currently only s3 sinks are supported. It was already validated in planning that this
                // is an aws connection.
                let msg = "only aws connection is supported in COPY TO";
                return Err(OptimizerError::Internal(msg.to_string()));
            }
        };
        let sink_description = ComputeSinkDesc {
            from_desc: self.copy_to_context.desc.clone(),
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
        df_desc.export_sink(self.select_id, sink_description);

        // Prepare expressions in the assembled dataflow.
        //
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
                // Also updating the sink up_to
                for (_, sink) in &mut df_desc.sink_exports {
                    sink.up_to.clone_from(&df_desc.until);
                }
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
            Some(&self.metrics),
        );
        // Run global optimization.
        mz_transform::optimize_dataflow(&mut df_desc, &mut transform_ctx, false)?;

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
            .observe_e2e_optimization_time("copy_to", self.duration);

        Ok(GlobalLirPlan { df_desc, df_meta })
    }
}

impl GlobalLirPlan {
    /// Unwraps the parts of the final result of the optimization pipeline.
    pub fn unapply(self) -> (LirDataflowDescription, DataflowMetainfo) {
        (self.df_desc, self.df_meta)
    }
}
