// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `SUBSCRIBE` statements.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use differential_dataflow::lattice::Lattice;
use mz_adapter_types::connection::ConnectionId;
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::plan::Plan;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection};
use mz_ore::collections::CollectionExt;
use mz_ore::soft_assert_or_log;
use mz_repr::{GlobalId, RelationDesc, Timestamp};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::SubscribeFrom;
use mz_transform::TransformCtx;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{SharedContext as TypecheckContext, empty_context};
use timely::progress::Antichain;

use crate::CollectionIdBundle;
use crate::optimize::dataflows::{
    ComputeInstanceSnapshot, DataflowBuilder, ExprPrepStyle, dataflow_import_id_bundle,
    prep_relation_expr, prep_scalar_expr,
};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizeMode, OptimizerCatalog,
    OptimizerConfig, OptimizerError, optimize_mir_local, trace_plan,
};

pub struct Optimizer {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<dyn OptimizerCatalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A transient GlobalId to be used for the exported sink.
    sink_id: GlobalId,
    /// A transient GlobalId to be used when constructing a dataflow for
    /// `SUBSCRIBE FROM <SELECT>` variants.
    view_id: GlobalId,
    /// The id of the session connection in which the optimizer will run.
    conn_id: Option<ConnectionId>,
    /// Should the plan produce an initial snapshot?
    with_snapshot: bool,
    /// Sink timestamp.
    up_to: Option<Timestamp>,
    /// A human-readable name exposed internally (useful for debugging).
    debug_name: String,
    /// Optimizer config.
    config: OptimizerConfig,
    /// Optimizer metrics.
    metrics: OptimizerMetrics,
    /// The time spent performing optimization so far.
    duration: Duration,
}

// A bogey `Debug` implementation that hides fields. This is needed to make the
// `event!` call in `sequence_peek_stage` not emit a lot of data.
//
// For now, we skip almost all fields, but we might revisit that bit if it turns
// out that we really need those for debugging purposes.
impl std::fmt::Debug for Optimizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Optimizer")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Optimizer {
    pub fn new(
        catalog: Arc<dyn OptimizerCatalog>,
        compute_instance: ComputeInstanceSnapshot,
        view_id: GlobalId,
        sink_id: GlobalId,
        conn_id: Option<ConnectionId>,
        with_snapshot: bool,
        up_to: Option<Timestamp>,
        debug_name: String,
        config: OptimizerConfig,
        metrics: OptimizerMetrics,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            view_id,
            sink_id,
            conn_id,
            with_snapshot,
            up_to,
            debug_name,
            config,
            metrics,
            duration: Default::default(),
        }
    }

    pub fn cluster_id(&self) -> ComputeInstanceId {
        self.compute_instance.instance_id()
    }

    pub fn up_to(&self) -> Option<Timestamp> {
        self.up_to.clone()
    }
}

/// The (sealed intermediate) result after:
///
/// 1. embedding a [`SubscribeFrom`] plan into a [`MirDataflowDescription`],
/// 2. transitively inlining referenced views, and
/// 3. jointly optimizing the `MIR` plans in the [`MirDataflowDescription`].
#[derive(Clone, Debug)]
pub struct GlobalMirPlan<T: Clone> {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
    phantom: PhantomData<T>,
}

impl<T: Clone> GlobalMirPlan<T> {
    /// Computes the [`CollectionIdBundle`] of the wrapped dataflow.
    pub fn id_bundle(&self, compute_instance_id: ComputeInstanceId) -> CollectionIdBundle {
        dataflow_import_id_bundle(&self.df_desc, compute_instance_id)
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
    pub fn sink_id(&self) -> GlobalId {
        let sink_exports = &self.df_desc.sink_exports;
        let sink_id = sink_exports.keys().next().expect("valid sink");
        *sink_id
    }

    pub fn as_of(&self) -> Option<Timestamp> {
        self.df_desc.as_of.clone().map(|as_of| as_of.into_element())
    }

    pub fn sink_desc(&self) -> &ComputeSinkDesc {
        let sink_exports = &self.df_desc.sink_exports;
        let sink_desc = sink_exports.values().next().expect("valid sink");
        sink_desc
    }
}

/// Marker type for [`GlobalMirPlan`] structs representing an optimization
/// result without a resolved timestamp.
#[derive(Clone, Debug)]
pub struct Unresolved;

/// Marker type for [`GlobalMirPlan`] structs representing an optimization
/// result with a resolved timestamp.
///
/// The actual timestamp value is set in the [`MirDataflowDescription`] of the
/// surrounding [`GlobalMirPlan`] when we call `resolve()`.
#[derive(Clone, Debug)]
pub struct Resolved;

impl Optimize<SubscribeFrom> for Optimizer {
    type To = GlobalMirPlan<Unresolved>;

    fn optimize(&mut self, plan: SubscribeFrom) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        let mut df_builder = {
            let compute = self.compute_instance.clone();
            DataflowBuilder::new(&*self.catalog, compute).with_config(&self.config)
        };
        let mut df_desc = MirDataflowDescription::new(self.debug_name.clone());
        let mut df_meta = DataflowMetainfo::default();

        match plan {
            SubscribeFrom::Id(from_id) => {
                let from = self.catalog.get_entry(&from_id);
                let from_desc = from
                    .desc(
                        &self
                            .catalog
                            .resolve_full_name(from.name(), self.conn_id.as_ref()),
                    )
                    .expect("subscribes can only be run on items with descs")
                    .into_owned();

                df_builder.import_into_dataflow(&from_id, &mut df_desc, &self.config.features)?;
                df_builder.maybe_reoptimize_imported_views(&mut df_desc, &self.config)?;

                // Make SinkDesc
                let sink_description = ComputeSinkDesc {
                    from: from_id,
                    from_desc,
                    connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
                    with_snapshot: self.with_snapshot,
                    up_to: self.up_to.map(Antichain::from_elem).unwrap_or_default(),
                    // No `FORCE NOT NULL` for subscribes
                    non_null_assertions: vec![],
                    // No `REFRESH` for subscribes
                    refresh_schedule: None,
                };
                df_desc.export_sink(self.sink_id, sink_description);
            }
            SubscribeFrom::Query { expr, desc } => {
                // TODO: Change the `expr` type to be `HirRelationExpr` and run
                // HIR ⇒ MIR lowering and decorrelation here. This would allow
                // us implement something like `EXPLAIN RAW PLAN FOR SUBSCRIBE.`
                //
                // let expr = expr.lower(&self.config)?;

                // MIR ⇒ MIR optimization (local)
                let mut transform_ctx = TransformCtx::local(
                    &self.config.features,
                    &self.typecheck_ctx,
                    &mut df_meta,
                    Some(&self.metrics),
                    Some(self.view_id),
                );
                let expr = optimize_mir_local(expr, &mut transform_ctx)?;

                df_builder.import_view_into_dataflow(
                    &self.view_id,
                    &expr,
                    &mut df_desc,
                    &self.config.features,
                )?;
                df_builder.maybe_reoptimize_imported_views(&mut df_desc, &self.config)?;

                // Make SinkDesc
                let sink_description = ComputeSinkDesc {
                    from: self.view_id,
                    from_desc: RelationDesc::new(expr.typ(), desc.iter_names()),
                    connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
                    with_snapshot: self.with_snapshot,
                    up_to: self.up_to.map(Antichain::from_elem).unwrap_or_default(),
                    // No `FORCE NOT NULL` for subscribes
                    non_null_assertions: vec![],
                    // No `REFRESH` for subscribes
                    refresh_schedule: None,
                };
                df_desc.export_sink(self.sink_id, sink_description);
            }
        };

        // Prepare expressions in the assembled dataflow.
        let style = ExprPrepStyle::Index;
        df_desc.visit_children(
            |r| prep_relation_expr(r, style),
            |s| prep_scalar_expr(s, style),
        )?;

        // Construct TransformCtx for global optimization.
        let mut transform_ctx = TransformCtx::global(
            &df_builder,
            &mz_transform::EmptyStatisticsOracle, // TODO: wire proper stats
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

        self.duration += time.elapsed();

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalMirPlan {
            df_desc,
            df_meta,
            phantom: PhantomData::<Unresolved>,
        })
    }
}

impl GlobalMirPlan<Unresolved> {
    /// Produces the [`GlobalMirPlan`] with [`Resolved`] timestamp.
    ///
    /// We need to resolve timestamps before the `GlobalMirPlan ⇒ GlobalLirPlan`
    /// optimization stage in order to profit from possible single-time
    /// optimizations in the `Plan::finalize_dataflow` call.
    pub fn resolve(mut self, as_of: Antichain<Timestamp>) -> GlobalMirPlan<Resolved> {
        // A dataflow description for a `SUBSCRIBE` statement should not have
        // index exports.
        soft_assert_or_log!(
            self.df_desc.index_exports.is_empty(),
            "unexpectedly setting until for a DataflowDescription with an index",
        );

        // Set the `as_of` timestamp for the dataflow.
        self.df_desc.set_as_of(as_of);

        // The only outputs of the dataflow are sinks, so we might be able to
        // turn off the computation early, if they all have non-trivial
        // `up_to`s.
        self.df_desc.until = Antichain::from_elem(Timestamp::MIN);
        for (_, sink) in &self.df_desc.sink_exports {
            self.df_desc.until.join_assign(&sink.up_to);
        }

        GlobalMirPlan {
            df_desc: self.df_desc,
            df_meta: self.df_meta,
            phantom: PhantomData::<Resolved>,
        }
    }
}

impl Optimize<GlobalMirPlan<Resolved>> for Optimizer {
    type To = GlobalLirPlan;

    fn optimize(&mut self, plan: GlobalMirPlan<Resolved>) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        let GlobalMirPlan {
            mut df_desc,
            df_meta,
            phantom: _,
        } = plan;

        // Ensure all expressions are normalized before finalizing.
        for build in df_desc.objects_to_build.iter_mut() {
            normalize_lets(&mut build.plan.0, &self.config.features)?
        }

        // Finalize the dataflow. This includes:
        // - MIR ⇒ LIR lowering
        // - LIR ⇒ LIR transforms
        let df_desc = Plan::finalize_dataflow(df_desc, &self.config.features)?;

        self.duration += time.elapsed();
        self.metrics
            .observe_e2e_optimization_time("subscribe", self.duration);

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
