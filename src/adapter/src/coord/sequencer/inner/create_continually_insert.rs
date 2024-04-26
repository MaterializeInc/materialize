// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;
use mz_expr::CollectionPlan;
use mz_ore::instrument;
use mz_repr::optimize::OverrideFrom;
use mz_repr::ColumnName;
use mz_sql::names::ResolvedIds;
use mz_sql::plan;
use mz_sql::session::metadata::SessionMetadata;
use timely::progress::Antichain;

use crate::command::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::ReadHolds;
use crate::{CollectionIdBundle, TimestampProvider};

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_create_continually_insert(
        &mut self,
        session: &Session,
        plan: plan::CreateContinuallyInsertPlan,
        _resolved_ids: ResolvedIds,
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
        let _expr_depends_on = expr.depends_on();

        // let validity = PlanValidity {
        //     transient_revision: self.catalog().transient_revision(),
        //     dependency_ids: expr_depends_on.clone(),
        //     cluster_id: Some(*cluster_id),
        //     replica_id: None,
        //     role_metadata: session.role_metadata().clone(),
        // };

        let debug_name = self.catalog().resolve_full_name(name, None).to_string();
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(*cluster_id).config.features());

        // Transient ID for any dataflows we might have to render.
        let view_id = self.allocate_transient_id()?;

        // We're making up a task ID because we're not storing the task in the catalog. Only for this PoC, of course!
        let task_id = self.allocate_transient_id()?;

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

        // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local and global)
        let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
        let global_mir_plan = optimizer.catch_unwind_optimize(local_mir_plan.clone())?;
        // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
        let global_lir_plan = optimizer.catch_unwind_optimize(global_mir_plan.clone())?;

        // tracing::info!("local_mir_plan:\n {:#?}", local_mir_plan);
        // tracing::info!("global_mir_plan:\n {:#?}", global_mir_plan);
        tracing::info!("global_lir_plan:\n {:#?}", global_lir_plan);

        // Timestamp selection
        let id_bundle = dataflow_import_id_bundle(global_lir_plan.df_desc(), cluster_id.clone());

        let read_holds_owned;
        let read_holds = if let Some(txn_reads) = self.txn_read_holds.get(session.conn_id()) {
            // In some cases, for example when REFRESH is used, the preparatory
            // stages will already have acquired ReadHolds, we can re-use those.

            txn_reads
        } else {
            // No one has acquired holds, make sure we can determine an as_of
            // and render our dataflow below.
            read_holds_owned = self.acquire_read_holds(&id_bundle);
            &read_holds_owned
        };

        let dataflow_as_of = self.select_task_timestamps(id_bundle, read_holds)?;

        tracing::info!(?dataflow_as_of, "continual task timestamp selection",);

        // WIP: We're not adding the TASK to the catalog plus a million other things we'd have to be doing...

        let (mut df_desc, _df_meta) = global_lir_plan.unapply();

        df_desc.set_as_of(dataflow_as_of.clone());

        self.ship_dataflow(df_desc, cluster_id.clone()).await;

        Ok(ExecuteResponse::CreatedContinuallyInsert)
    }

    /// Select the initial `dataflow_as_of`, `storage_as_of`, and `until` frontiers for a
    /// materialized view.
    fn select_task_timestamps(
        &self,
        id_bundle: CollectionIdBundle,
        read_holds: &ReadHolds<mz_repr::Timestamp>,
    ) -> Result<Antichain<mz_repr::Timestamp>, AdapterError> {
        assert!(
            id_bundle.difference(&read_holds.id_bundle()).is_empty(),
            "we must have read holds for all involved collections"
        );

        let least_valid_read = self.least_valid_read(read_holds);

        Ok(least_valid_read)
    }
}
