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
use maplit::btreemap;
use mz_adapter_types::connection::ConnectionId;
use mz_compute_types::plan::Plan;
use mz_compute_types::sinks::{
    ComputeSinkConnection, ComputeSinkDesc, S3SinkConnection,
};
use mz_compute_types::ComputeInstanceId;
use mz_ore::soft_assert_or_log;

use mz_repr::{GlobalId, Timestamp};

use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};

use timely::progress::Antichain;


use crate::catalog::Catalog;
use crate::coord::dataflows::{ComputeInstanceSnapshot, DataflowBuilder};
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
    // Optimizer config.
    config: OptimizerConfig,
}

impl Optimizer {
    pub fn new(
        catalog: Arc<Catalog>,
        compute_instance: ComputeInstanceSnapshot,
        transient_id: GlobalId,
        conn_id: ConnectionId,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            transient_id,
            conn_id,
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
        let storage_ids = self.df_desc.source_imports.keys().copied().collect();
        let compute_ids = self.df_desc.index_imports.keys().copied().collect();
        CollectionIdBundle {
            storage_ids,
            compute_ids: btreemap! {compute_instance_id => compute_ids},
        }
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

impl Optimize<GlobalId> for Optimizer {
    type To = GlobalMirPlan<Unresolved>;

    fn optimize(&mut self, from_id: GlobalId) -> Result<Self::To, OptimizerError> {
        let sink_name = format!("s3-{}", self.transient_id);

        let from = self.catalog.get_entry(&from_id);
        let from_desc = from
            .desc(
                &self
                    .catalog
                    .state()
                    .resolve_full_name(from.name(), Some(&self.conn_id)),
            )
            .expect("s3 can only be run on items with descs")
            .into_owned();

        // Make SinkDesc
        let sink_id = self.transient_id;
        let sink_desc = ComputeSinkDesc {
            from: from_id,
            from_desc,
            connection: ComputeSinkConnection::S3(S3SinkConnection::default()),
            with_snapshot: true,
            up_to: Default::default(),
            // No `FORCE NOT NULL` for subscribes
            non_null_assertions: vec![],
        };

        let mut df_builder =
            DataflowBuilder::new(self.catalog.state(), self.compute_instance.clone());

        let (df_desc, df_meta) = df_builder.build_sink_dataflow(sink_name, sink_id, sink_desc)?;

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
