// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use maplit::btreemap;
use mz_adapter_types::compaction::CompactionWindow;
use mz_adapter_types::connection::ConnectionId;
use mz_catalog::memory::objects::{CatalogItem, MaterializedView};
use mz_expr::refresh_schedule::RefreshSchedule;
use mz_expr::CollectionPlan;
use mz_ore::collections::CollectionExt;
use mz_ore::instrument;
use mz_ore::soft_panic_or_log;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::optimize::OverrideFrom;
use mz_repr::ColumnName;
use mz_repr::Datum;
use mz_repr::Row;
use mz_sql::ast::ExplainStage;
use mz_sql::catalog::CatalogError;
use mz_sql::names::{ObjectId, ResolvedIds};
use mz_sql::plan;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast;
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_client::controller::{CollectionDescription, DataSource, DataSourceOther};
use timely::progress::Antichain;
use tracing::Span;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateMaterializedViewExplain, CreateMaterializedViewFinish,
    CreateMaterializedViewOptimize, CreateMaterializedViewStage, ExplainContext,
    ExplainPlanContext, Message, PlanValidity, StageResult, Staged,
};
use crate::error::AdapterError;
use crate::explain::explain_dataflow;
use crate::explain::explain_plan;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::util::ResultExt;
use crate::ReadHolds;
use crate::{catalog, AdapterNotice, CollectionIdBundle, ExecuteContext, TimestampProvider};

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_create_continually_insert(
        &mut self,
        session: &Session,
        plan: plan::CreateContinuallyInsertPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::CreateContinuallyInsertPlan {
            name,
            target_table_id,
            retract_from_table_id,
            continually_insert:
                plan::ContinuallyInsert {
                    cluster_id, expr, ..
                },
            ..
        } = &plan;

        // Collect optimizer parameters.
        let compute_instance = self
            .instance_snapshot(*cluster_id)
            .expect("compute instance does not exist");

        // Validate any references in our expression. We do
        // this on the unoptimized plan to better reflect what the user typed.
        // We want to reject queries that depend on log sources, for example,
        // even if we can *technically* optimize that reference away.
        let expr_depends_on = expr.depends_on();

        let validity = PlanValidity {
            transient_revision: self.catalog().transient_revision(),
            dependency_ids: expr_depends_on.clone(),
            cluster_id: Some(*cluster_id),
            replica_id: None,
            role_metadata: session.role_metadata().clone(),
        };

        let debug_name = self.catalog().resolve_full_name(name, None).to_string();
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(*cluster_id).config.features());

        // Transient ID for any dataflows we might have to render.
        let view_id = self.allocate_transient_id()?;

        // We're making up a task ID because we're not storing the task in the catalog. Only for this PoC, of course!
        let task_id = self.allocate_transient_id()?;

        // We're not using this, but the optimizer wants it.
        let connection_id = session.conn_id().clone();

        let raw_expr = expr.clone();

        let column_names = (0..expr.arity())
            .map(|i| ColumnName::from(format!("column_{}", i)))
            .collect_vec();

        // Build an optimizer for this CONTINUAL TASK INSERT
        let mut optimizer = optimize::continual_task_insert::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            task_id,
            target_table_id.clone(),
            retract_from_table_id.clone(),
            view_id,
            column_names,
            debug_name,
            optimizer_config,
            self.optimizer_metrics(),
        );
        // let mut optimizer = optimize::subscribe::Optimizer::new(
        //     self.owned_catalog(),
        //     compute_instance,
        //     view_id,
        //     dummy_sink_id,
        //     connection_id,
        //     true, /* with snapshot */
        //     None, /* no UP TO */
        //     debug_name,
        //     optimizer_config,
        //     self.optimizer_metrics(),
        // );

        // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local and global)
        let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
        let global_mir_plan = optimizer.catch_unwind_optimize(local_mir_plan.clone())?;
        // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
        let global_lir_plan = optimizer.catch_unwind_optimize(global_mir_plan.clone())?;

        // tracing::info!("local_mir_plan:\n {:#?}", local_mir_plan);
        // tracing::info!("global_mir_plan:\n {:#?}", global_mir_plan);
        tracing::info!("global_lir_plan:\n {:#?}", global_lir_plan);

        todo!()
    }
}
