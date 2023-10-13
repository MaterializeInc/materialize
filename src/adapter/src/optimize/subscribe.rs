// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `SUBSCRIBE` statements.

#![allow(unused)] // TODO: remove

use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use maplit::btreemap;
use mz_compute_types::plan::Plan;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection};
use mz_compute_types::ComputeInstanceId;
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_ore::soft_assert_or_log;
use mz_repr::explain::trace_plan;
use mz_repr::{GlobalId, RelationDesc, Timestamp};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::Optimizer as TransformOptimizer;
use timely::progress::Antichain;
use timely::PartialOrder;
use tracing::{span, Level};

use crate::catalog::{Catalog, CatalogEntry};
use crate::coord::dataflows::{ComputeInstanceSnapshot, DataflowBuilder};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizerConfig, OptimizerError,
};
use crate::session::Session;
use crate::CollectionIdBundle;

pub struct OptimizeSubscribe<'ctx> {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the compute instance that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A transient GlobalId to be used when constructing the dataflow.
    subscribe_id: GlobalId,
    /// An instance of the session that in which the optimizer will run.
    session: &'ctx mut Session,
    /// Should the plan produce an initial snapshot?
    with_snapshot: bool,
    /// Timestamps
    as_of: Timestamp,
    up_to: Antichain<Timestamp>,
    // Optimizer config.
    config: OptimizerConfig,
}

/// The starting point of the optimization. Each variant represents a specific
/// type of comutation that can be subscribed.
///
/// Variants can be constructed and descructed publicly because a client needs
/// one in order to start the optimziation pipeline.
pub enum RawPlan {
    FromQuery {
        /// An unoptimized plan.
        expr: MirRelationExpr,
        /// A RelationDescription of the subscribe result
        desc: RelationDesc,
    },
    FromId {
        /// A catalog entry.
        from: CatalogEntry,
    },
}

/// The (sealed intermediate) result after:
///
/// 1. embedding an optimized [`RawPlan`] into a [`MirDataflowDescription`],
/// 2. transitively inlining referenced views, and
/// 3. jointly optimizing the `MIR` plans in the [`MirDataflowDescription`].
pub struct GlobalMirPlan<T> {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
    ts_info: T,
}

/// Timestamp information type for [`GlobalMirPlan`] structs representing an
/// optimization result without a resolved timestamp.
pub struct Unresolved {
    as_of: Timestamp,
    compute_instance_id: ComputeInstanceId,
}

/// Timestamp information type for [`GlobalMirPlan`] structs representing an
/// optimization result with a resolved timestamp.
///
/// The actual timestamp value is set in the [`MirDataflowDescription`] of the
/// surrounding [`GlobalMirPlan`] when we call `resolve()`.
pub struct Resolved;

/// The (final) result after MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
pub struct GlobalLirPlan {
    df_desc: LirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl<'ctx> OptimizeSubscribe<'ctx> {
    pub fn new(
        catalog: Arc<Catalog>,
        compute_instance: ComputeInstanceSnapshot,
        exported_sink_id: GlobalId,
        subscribe_id: GlobalId,
        session: &'ctx mut Session,
        with_snapshot: bool,
        as_of: Timestamp,
        up_to: Antichain<Timestamp>,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            subscribe_id,
            session,
            with_snapshot,
            as_of,
            up_to,
            config,
        }
    }
}

impl<'ctx> Optimize<'ctx, RawPlan> for OptimizeSubscribe<'ctx> {
    type To = GlobalMirPlan<Unresolved>;

    fn optimize<'s: 'ctx>(&'s mut self, plan: RawPlan) -> Result<Self::To, OptimizerError> {
        let sink_name = format!("subscribe-{}", self.subscribe_id);

        let (df_desc, df_meta) = match plan {
            RawPlan::FromQuery { expr, desc } => {
                // TODO: expr: HirRelationExpr
                // HIR ⇒ MIR lowering and decorrelation
                // let config = mz_sql::plan::OptimizerConfig {};
                // let expr = expr.optimize_and_lower(&config)?;

                // MIR ⇒ MIR optimization (local)
                let expr = span!(target: "optimizer", Level::TRACE, "local").in_scope(|| {
                    let optimizer = TransformOptimizer::logical_optimizer(&self.typecheck_ctx);
                    let expr = optimizer.optimize(expr)?.into_inner();

                    // Trace the result of this phase.
                    trace_plan(&expr);

                    Ok::<_, OptimizerError>(expr)
                })?;

                // I'm not sure why this is needed - possibly for refining the type?
                // TODO: figure out if we should be doing this elsewhere.
                let from_desc = RelationDesc::new(expr.typ(), desc.iter_names());

                // Make SinkDesc
                let sink_desc = ComputeSinkDesc {
                    from: self.subscribe_id,
                    from_desc,
                    connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
                    with_snapshot: self.with_snapshot,
                    up_to: self.up_to.clone(),
                    // No `FORCE NOT NULL` for subscribes
                    non_null_assertions: vec![],
                };

                let mut df_builder =
                    DataflowBuilder::new(self.catalog.state(), self.compute_instance.clone());

                let mut df_desc = MirDataflowDescription::new(sink_name);

                df_builder.import_view_into_dataflow(
                    &self.subscribe_id,
                    &OptimizedMirRelationExpr(expr),
                    &mut df_desc,
                )?;

                let df_meta = df_builder.build_sink_dataflow_into(
                    &mut df_desc,
                    self.subscribe_id,
                    sink_desc,
                )?;

                (df_desc, df_meta)
            }
            RawPlan::FromId { from } => {
                let from_desc = from
                    .desc(
                        &self
                            .catalog
                            .state()
                            .resolve_full_name(from.name(), Some(self.session.conn_id())),
                    )
                    .expect("subscribes can only be run on items with descs")
                    .into_owned();

                // Make SinkDesc
                let sink_desc = ComputeSinkDesc {
                    from: self.subscribe_id,
                    from_desc,
                    connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
                    with_snapshot: self.with_snapshot,
                    up_to: self.up_to.clone(),
                    // No `FORCE NOT NULL` for subscribes
                    non_null_assertions: vec![],
                };

                let mut df_builder =
                    DataflowBuilder::new(self.catalog.state(), self.compute_instance.clone());

                let (df_desc, df_meta) =
                    df_builder.build_sink_dataflow(sink_name, self.subscribe_id, sink_desc)?;

                (df_desc, df_meta)
            }
        };

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalMirPlan {
            df_desc,
            df_meta,
            ts_info: Unresolved {
                as_of: self.as_of,
                compute_instance_id: self.compute_instance.instance_id(),
            },
        })
    }
}

impl<T: Clone> GlobalMirPlan<T> {
    pub fn df_desc(&self) -> &MirDataflowDescription {
        &self.df_desc
    }

    pub fn df_meta(&self) -> &DataflowMetainfo {
        &self.df_meta
    }
}

impl GlobalMirPlan<Unresolved> {
    /// Produces the [`GlobalMirPlan`] with [`Resolved`] timestamp required for
    /// the next stage.
    pub fn resolve(mut self, since: Antichain<Timestamp>) -> GlobalMirPlan<Resolved> {
        let as_of = self.ts_info.as_of;

        // Ensure that the dataflow's `as_of` is at least `since`. It should not
        // be possible to request an invalid time, as subscribe checks that
        // their AS OF is >= since.
        assert!(
            PartialOrder::less_equal(&since, &Antichain::from_elem(as_of)),
            "Dataflow {} requested as_of ({:?}) not >= since ({:?})",
            self.df_desc.debug_name,
            as_of,
            since
        );

        // A datalfow description for a `SUBSCRIBE` statement should not have
        // index exports.
        soft_assert_or_log!(
            self.df_desc.index_exports.is_empty(),
            "unexpectedly setting until for a DataflowDescription with an index",
        );

        // Set the `as_of` timestamp for the dataflow.
        self.df_desc.set_as_of(Antichain::from_elem(as_of));

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
            ts_info: Resolved,
        }
    }

    /// Computes the [`CollectionIdBundle`] of the wrapped dataflow.
    pub fn id_bundle(&self) -> CollectionIdBundle {
        let storage_ids = self.df_desc.source_imports.keys().copied().collect();
        let compute_ids = self.df_desc.index_imports.keys().copied().collect();
        CollectionIdBundle {
            storage_ids,
            compute_ids: btreemap! {self.compute_instance_id() => compute_ids},
        }
    }

    /// Returns the [`ComputeInstanceId`] against which we should resolve the
    /// timestamp for the next stage.
    pub fn compute_instance_id(&self) -> ComputeInstanceId {
        self.ts_info.compute_instance_id
    }
}

impl<'ctx> Optimize<'ctx, GlobalMirPlan<Resolved>> for OptimizeSubscribe<'ctx> {
    type To = GlobalLirPlan;

    fn optimize<'s: 'ctx>(
        &'s mut self,
        plan: GlobalMirPlan<Resolved>,
    ) -> Result<Self::To, OptimizerError> {
        let GlobalMirPlan {
            mut df_desc,
            df_meta,
            ts_info: _,
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
            false, // we are not in a monotonic context here
            self.config.enable_specialized_arrangements,
        )
        .map_err(OptimizerError::Internal)?;

        // Return the plan at the end of this `optimize` step.
        Ok(GlobalLirPlan { df_desc, df_meta })
    }
}

impl GlobalLirPlan {
    pub fn unapply(self) -> (LirDataflowDescription, DataflowMetainfo) {
        (self.df_desc, self.df_meta)
    }

    pub fn df_desc(&self) -> &LirDataflowDescription {
        &self.df_desc
    }

    pub fn df_meta(&self) -> &DataflowMetainfo {
        &self.df_meta
    }

    pub fn desc(&self) -> RelationDesc {
        let sink_exports = &self.df_desc.sink_exports;
        let sink = sink_exports.values().next().expect("valid sink");
        sink.from_desc.clone()
    }
}
