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

use differential_dataflow::lattice::Lattice;
use mz_adapter_types::connection::ConnectionId;
use mz_compute_types::plan::Plan;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection};
use mz_compute_types::ComputeInstanceId;
use mz_ore::soft_assert_or_log;
use mz_repr::explain::trace_plan;
use mz_repr::{GlobalId, RelationDesc, Timestamp};
use mz_sql::plan::SubscribeFrom;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::Optimizer as TransformOptimizer;
use timely::progress::Antichain;
use tracing::{span, Level};

use crate::catalog::Catalog;
use crate::coord::dataflows::{
    dataflow_import_id_bundle, ComputeInstanceSnapshot, DataflowBuilder,
};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizerConfig, OptimizerError,
};
use crate::CollectionIdBundle;

pub struct Optimizer {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A transient GlobalId to be used when constructing the dataflow.
    transient_id: GlobalId,
    /// The id of the session connection in which the optimizer will run.
    conn_id: ConnectionId,
    /// Should the plan produce an initial snapshot?
    with_snapshot: bool,
    /// Sink timestamp.
    up_to: Option<Timestamp>,
    // Optimizer config.
    config: OptimizerConfig,
}

impl Optimizer {
    pub fn new(
        catalog: Arc<Catalog>,
        compute_instance: ComputeInstanceSnapshot,
        transient_id: GlobalId,
        conn_id: ConnectionId,
        with_snapshot: bool,
        up_to: Option<Timestamp>,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            transient_id,
            conn_id,
            with_snapshot,
            up_to,
            config,
        }
    }

    pub fn cluster_id(&self) -> ComputeInstanceId {
        self.compute_instance.instance_id()
    }
}

/// The (sealed intermediate) result after:
///
/// 1. embedding a [`SubscribeFrom`] plan into a [`MirDataflowDescription`],
/// 2. transitively inlining referenced views, and
/// 3. jointly optimizing the `MIR` plans in the [`MirDataflowDescription`].
#[derive(Clone)]
pub struct GlobalMirPlan<T: Clone> {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
    phantom: PhantomData<T>,
}

impl<T: Clone> GlobalMirPlan<T> {
    pub fn df_desc(&self) -> &MirDataflowDescription {
        &self.df_desc
    }

    #[allow(dead_code)] // This will be needed for EXPLAIN SUBSCRIBE
    pub fn df_meta(&self) -> &DataflowMetainfo {
        &self.df_meta
    }

    /// Computes the [`CollectionIdBundle`] of the wrapped dataflow.
    pub fn id_bundle(&self, compute_instance_id: ComputeInstanceId) -> CollectionIdBundle {
        dataflow_import_id_bundle(&self.df_desc, compute_instance_id)
    }
}

/// The (final) result after MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
#[derive(Clone)]
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

    pub fn sink_id(&self) -> GlobalId {
        let sink_exports = &self.df_desc.sink_exports;
        let sink_id = sink_exports.keys().next().expect("valid sink");
        *sink_id
    }

    pub fn sink_desc(&self) -> &ComputeSinkDesc {
        let sink_exports = &self.df_desc.sink_exports;
        let sink_desc = sink_exports.values().next().expect("valid sink");
        sink_desc
    }
}

/// Marker type for [`GlobalMirPlan`] structs representing an optimization
/// result without a resolved timestamp.
#[derive(Clone)]
pub struct Unresolved;

/// Marker type for [`GlobalMirPlan`] structs representing an optimization
/// result with a resolved timestamp.
///
/// The actual timestamp value is set in the [`MirDataflowDescription`] of the
/// surrounding [`GlobalMirPlan`] when we call `resolve()`.
#[derive(Clone)]
pub struct Resolved;

impl Optimize<SubscribeFrom> for Optimizer {
    type To = GlobalMirPlan<Unresolved>;

    fn optimize(&mut self, plan: SubscribeFrom) -> Result<Self::To, OptimizerError> {
        let sink_name = format!("subscribe-{}", self.transient_id);

        let (df_desc, df_meta) = match plan {
            SubscribeFrom::Id(from_id) => {
                let from = self.catalog.get_entry(&from_id);
                let from_desc = from
                    .desc(
                        &self
                            .catalog
                            .state()
                            .resolve_full_name(from.name(), Some(&self.conn_id)),
                    )
                    .expect("subscribes can only be run on items with descs")
                    .into_owned();

                // Make SinkDesc
                let sink_id = self.transient_id;
                let sink_desc = ComputeSinkDesc {
                    from: from_id,
                    from_desc,
                    connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
                    with_snapshot: self.with_snapshot,
                    up_to: self.up_to.map(Antichain::from_elem).unwrap_or_default(),
                    // No `FORCE NOT NULL` for subscribes
                    non_null_assertions: vec![],
                    // No `REFRESH EVERY` for subscribes
                    refresh_schedule: None,
                };

                let mut df_builder =
                    DataflowBuilder::new(self.catalog.state(), self.compute_instance.clone());

                let (df_desc, df_meta) =
                    df_builder.build_sink_dataflow(sink_name, sink_id, sink_desc)?;

                (df_desc, df_meta)
            }
            SubscribeFrom::Query { expr, desc } => {
                // TODO: Change the `expr` type to be `HirRelationExpr` and run
                // HIR ⇒ MIR lowering and decorrelation here. This would allow
                // us implement something like `EXPLAIN RAW PLAN FOR SUBSCRIBE.`
                //
                // let expr = expr.lower(&self.config)?;

                // MIR ⇒ MIR optimization (local)
                let expr = span!(target: "optimizer", Level::DEBUG, "local").in_scope(|| {
                    let optimizer = TransformOptimizer::logical_optimizer(&self.typecheck_ctx);
                    let expr = optimizer.optimize(expr)?;

                    // Trace the result of this phase.
                    trace_plan(&expr);

                    Ok::<_, OptimizerError>(expr)
                })?;

                let from_desc = RelationDesc::new(expr.typ(), desc.iter_names());
                let from_id = self.transient_id;

                // Make SinkDesc
                let sink_desc = ComputeSinkDesc {
                    from: from_id,
                    from_desc,
                    connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
                    with_snapshot: self.with_snapshot,
                    up_to: self.up_to.map(Antichain::from_elem).unwrap_or_default(),
                    // No `FORCE NOT NULL` for subscribes
                    non_null_assertions: vec![],
                    // No `REFRESH EVERY` for subscribes
                    refresh_schedule: None,
                };

                let mut df_builder =
                    DataflowBuilder::new(self.catalog.state(), self.compute_instance.clone());

                let mut df_desc = MirDataflowDescription::new(sink_name);

                df_builder.import_view_into_dataflow(&from_id, &expr, &mut df_desc)?;
                df_builder.reoptimize_imported_views(&mut df_desc, &self.config)?;

                let df_meta =
                    df_builder.build_sink_dataflow_into(&mut df_desc, from_id, sink_desc)?;

                (df_desc, df_meta)
            }
        };

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
        // A datalfow description for a `SUBSCRIBE` statement should not have
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
        let GlobalMirPlan {
            mut df_desc,
            df_meta,
            phantom: _,
        } = plan;

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
        )
        .map_err(OptimizerError::Internal)?;

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
