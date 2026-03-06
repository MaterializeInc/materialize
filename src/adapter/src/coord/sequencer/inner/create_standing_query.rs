// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_catalog::memory::objects::{CatalogItem, StandingQuery};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_expr::OptimizedMirRelationExpr;
use mz_ore::instrument;
use mz_repr::GlobalId;
use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
use mz_sql::names::ResolvedIds;
use mz_sql::plan;
use mz_sql::plan::HirRelationExpr;
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_client::controller::CollectionDescription;
use mz_transform::dataflow::DataflowMetainfo;

use crate::command::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize};
use crate::util::ResultExt;
use crate::{ExecuteContext, catalog};

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_create_standing_query(
        &mut self,
        ctx: &mut ExecuteContext,
        plan: plan::CreateStandingQueryPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::CreateStandingQueryPlan {
            name,
            standing_query:
                plan::StandingQuery {
                    create_sql,
                    expr: raw_expr,
                    dependencies,
                    column_names,
                    desc,
                    params: _params,
                    cluster_id,
                },
            if_not_exists: _if_not_exists,
        } = plan;

        // Allocate IDs.
        let id_ts = self.get_catalog_write_ts().await;
        let (item_id, global_id) = self.catalog().allocate_user_id(id_ts).await?;

        // Optimize the expression using the MV optimizer.
        let (optimized_plan, mut physical_plan, raw_metainfo, optimizer_features) =
            self.optimize_create_standing_query(&raw_expr, &column_names, global_id, cluster_id)?;

        // Create a metainfo with rendered notices, preallocating a transient
        // GlobalId for each.
        let notice_ids = std::iter::repeat_with(|| self.allocate_transient_id())
            .map(|(_item_id, global_id)| global_id)
            .take(raw_metainfo.optimizer_notices.len())
            .collect();
        let metainfo = self
            .catalog()
            .render_notices(raw_metainfo, notice_ids, Some(global_id));

        // Timestamp selection.
        let mut id_bundle = dataflow_import_id_bundle(&physical_plan, cluster_id);
        id_bundle.storage_ids.remove(&global_id);
        let read_holds = self.acquire_read_holds(&id_bundle);
        let as_of = read_holds.least_valid_read();
        physical_plan.set_as_of(as_of.clone());
        physical_plan.set_initial_as_of(as_of.clone());

        // Extract the optimized MIR expression from the dataflow.
        let optimized_expr = optimized_plan
            .objects_to_build
            .last()
            .expect("optimizer must produce at least one object")
            .plan
            .clone();

        let item = StandingQuery {
            create_sql,
            global_id,
            raw_expr: raw_expr.into(),
            optimized_expr: optimized_expr.into(),
            desc: desc.clone(),
            resolved_ids: resolved_ids.clone(),
            dependencies,
            cluster_id,
        };

        let ops = vec![catalog::Op::CreateItem {
            id: item_id,
            name: name.clone(),
            item: CatalogItem::StandingQuery(item),
            owner_id: *ctx.session().current_role_id(),
        }];

        let () = self
            .catalog_transact_with_side_effects(Some(ctx), ops, move |coord, _ctx| {
                Box::pin(async move {
                    let catalog = coord.catalog_mut();
                    catalog.set_optimized_plan(global_id, optimized_plan);
                    catalog.set_physical_plan(global_id, physical_plan.clone());
                    catalog.set_dataflow_metainfo(global_id, metainfo);
                    catalog.cache_expressions(global_id, None, optimizer_features);

                    coord
                        .controller
                        .storage
                        .create_collections(
                            coord.catalog.state().storage_metadata(),
                            None,
                            vec![(
                                global_id,
                                CollectionDescription::for_other(desc, Some(as_of)),
                            )],
                        )
                        .await
                        .unwrap_or_terminate("cannot fail to append");

                    coord.ship_dataflow(physical_plan, cluster_id, None).await;
                })
            })
            .await?;

        Ok(ExecuteResponse::CreatedStandingQuery)
    }

    fn optimize_create_standing_query(
        &self,
        raw_expr: &HirRelationExpr,
        column_names: &[mz_repr::ColumnName],
        output_id: GlobalId,
        cluster_id: mz_controller_types::ClusterId,
    ) -> Result<
        (
            DataflowDescription<OptimizedMirRelationExpr>,
            DataflowDescription<Plan>,
            DataflowMetainfo,
            OptimizerFeatures,
        ),
        AdapterError,
    > {
        let catalog = self.owned_catalog().as_optimizer_catalog();
        let (_, view_id) = self.allocate_transient_id();
        let compute_instance = self
            .instance_snapshot(cluster_id)
            .expect("compute instance does not exist");
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(cluster_id).config.features());
        let optimizer_features = optimizer_config.features.clone();

        let mut optimizer = optimize::materialized_view::Optimizer::new(
            catalog,
            compute_instance,
            output_id,
            view_id,
            column_names.to_vec(),
            Vec::new(), // non_null_assertions
            None,       // refresh_schedule
            format!("standing-query-{output_id}"),
            optimizer_config,
            self.optimizer_metrics(),
            Default::default(), // force_non_monotonic
        );

        // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local and global)
        let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr.clone())?;
        let global_mir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;
        let optimized_plan = global_mir_plan.df_desc().clone();
        // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
        let global_lir_plan = optimizer.catch_unwind_optimize(global_mir_plan)?;
        let (physical_plan, metainfo) = global_lir_plan.unapply();

        Ok((optimized_plan, physical_plan, metainfo, optimizer_features))
    }
}
