// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `CREATE METRIC SINK` statements.
//!
//! A metric sink exports the rows of an existing collection (table, source, view, materialized
//! view, or index) into the in-process Prometheus metrics registry. Like `CREATE INDEX`, it does
//! not introduce a new relational expression to lower from HIR: the pipeline starts directly from
//! the `GlobalId` of the collection to export. Unlike a materialized view sink, there is no
//! persist shard, so this pipeline has no storage-metadata stage.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_compute_types::plan::LirRelationExpr;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, MetricSinkConnection};
use mz_repr::GlobalId;
use mz_repr::explain::trace_plan;
use mz_sql::names::QualifiedItemName;
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_transform::TransformCtx;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{SharedTypecheckingContext, empty_typechecking_context};
use timely::progress::Antichain;

use crate::optimize::dataflows::{
    ComputeInstanceSnapshot, DataflowBuilder, ExprPrep, ExprPrepMaintained,
};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizerCatalog, OptimizerConfig,
    OptimizerError,
};

pub struct Optimizer {
    /// A representation typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: SharedTypecheckingContext,
    /// A snapshot of the catalog state.
    catalog: Arc<dyn OptimizerCatalog>,
    /// A snapshot of the cluster that will run the dataflow.
    compute_instance: ComputeInstanceSnapshot,
    /// A durable GlobalId to be used with the exported metric sink.
    sink_id: GlobalId,
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
        config: OptimizerConfig,
        metrics: OptimizerMetrics,
    ) -> Self {
        Self {
            typecheck_ctx: empty_typechecking_context(),
            catalog,
            compute_instance,
            sink_id,
            config,
            metrics,
            duration: Default::default(),
        }
    }
}

/// A wrapper of metric sink parts needed to start the optimization process.
pub struct MetricSink {
    name: QualifiedItemName,
    from: GlobalId,
}

impl MetricSink {
    /// Construct a new [`MetricSink`]. Arguments are recorded as-is.
    pub fn new(name: QualifiedItemName, from: GlobalId) -> Self {
        Self { name, from }
    }
}

/// The (sealed intermediate) result after:
///
/// 1. embedding a [`MetricSink`] into a [`MirDataflowDescription`],
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
}

impl Optimize<MetricSink> for Optimizer {
    type To = GlobalMirPlan;

    fn optimize(&mut self, metric_sink: MetricSink) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        let from_entry = self.catalog.get_entry(&metric_sink.from);
        let full_name = self
            .catalog
            .resolve_full_name(&metric_sink.name, from_entry.conn_id());
        let from_desc = from_entry
            .relation_desc()
            .expect("can only create a metric sink on items with a valid description")
            .into_owned();

        let mut df_builder = {
            let compute = self.compute_instance.clone();
            DataflowBuilder::new(&*self.catalog, compute).with_config(&self.config)
        };
        let mut df_desc = MirDataflowDescription::new(full_name.to_string());

        df_builder.import_into_dataflow(&metric_sink.from, &mut df_desc, &self.config.features)?;
        df_builder.maybe_reoptimize_imported_views(&mut df_desc, &self.config)?;

        let sink_description = ComputeSinkDesc {
            from: metric_sink.from,
            from_desc: from_desc.clone(),
            connection: ComputeSinkConnection::MetricSink(MetricSinkConnection {}),
            with_snapshot: true,
            up_to: Antichain::new(),
            non_null_assertions: Vec::new(),
            refresh_schedule: None,
        };
        df_desc.export_sink(self.sink_id, sink_description);

        // Prepare expressions in the assembled dataflow.
        let style = ExprPrepMaintained;
        df_desc.visit_children(
            |r| style.prep_relation_expr(r),
            |s| style.prep_scalar_expr(s),
        )?;

        // Construct TransformCtx for global optimization.
        let mut df_meta = DataflowMetainfo::default();
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
        let df_desc = LirRelationExpr::finalize_dataflow(
            df_desc,
            &self.config.features,
            Some(self.metrics.lowering()),
        )?;

        // Trace the pipeline output under `optimize`.
        trace_plan(&df_desc);

        self.duration += time.elapsed();
        self.metrics
            .observe_e2e_optimization_time("metric_sink", self.duration);

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
