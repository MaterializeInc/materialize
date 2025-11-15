// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use maplit::btreeset;
use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::memory::objects::{CatalogItem, ReplacementMaterializedView};
use mz_ore::collections::CollectionExt;
use mz_ore::instrument;
use mz_sql::catalog::ObjectType;
use mz_sql::names::ResolvedIds;
use mz_sql::plan;
use mz_sql::plan::AlterMaterializedViewApplyReplacementPlan;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast;
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_client::controller::{CollectionDescription, DataSource};
use tracing::Span;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateReplacementMaterializedViewFinish,
    CreateReplacementMaterializedViewOptimize, CreateReplacementMaterializedViewStage,
    ExplainContext, Message, PlanValidity, StageResult, Staged,
};
use crate::error::AdapterError;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::session::Session;
use crate::util::ResultExt;
use crate::{ExecuteContext, catalog};

impl Staged for CreateReplacementMaterializedViewStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            Self::Optimize(stage) => &mut stage.validity,
            Self::Finish(stage) => &mut stage.validity,
        }
    }

    async fn stage(
        self,
        coord: &mut Coordinator,
        ctx: &mut ExecuteContext,
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        match self {
            CreateReplacementMaterializedViewStage::Optimize(stage) => {
                coord
                    .create_replacement_materialized_view_optimize(stage)
                    .await
            }
            CreateReplacementMaterializedViewStage::Finish(stage) => {
                coord
                    .create_replacement_materialized_view_finish(ctx, stage)
                    .await
            }
        }
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::CreateReplacementMaterializedViewStageReady {
            ctx,
            span,
            stage: self,
        }
    }

    fn cancel_enabled(&self) -> bool {
        true
    }
}

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_create_replacement_materialized_view(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateReplacementMaterializedViewPlan,
        resolved_ids: ResolvedIds,
    ) {
        let stage = return_if_err!(
            self.create_replacement_materialized_view_validate(ctx.session(), plan, resolved_ids,),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    fn create_replacement_materialized_view_validate(
        &mut self,
        session: &Session,
        plan: plan::CreateReplacementMaterializedViewPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<CreateReplacementMaterializedViewStage, AdapterError> {
        let plan::CreateReplacementMaterializedViewPlan {
            materialized_view:
                plan::MaterializedView {
                    expr,
                    cluster_id,
                    refresh_schedule,
                    ..
                },
            ambiguous_columns,
            ..
        } = &plan;

        let validity = self.create_materialized_view_validate_inner(
            session,
            &resolved_ids,
            expr,
            cluster_id,
            refresh_schedule,
            ambiguous_columns,
            &ExplainContext::None,
        )?;

        Ok(CreateReplacementMaterializedViewStage::Optimize(
            CreateReplacementMaterializedViewOptimize {
                validity,
                plan,
                resolved_ids,
            },
        ))
    }

    #[instrument]
    async fn create_replacement_materialized_view_optimize(
        &mut self,
        CreateReplacementMaterializedViewOptimize {
            validity,
            plan,
            resolved_ids,
        }: CreateReplacementMaterializedViewOptimize,
    ) -> Result<StageResult<Box<CreateReplacementMaterializedViewStage>>, AdapterError> {
        let plan::CreateReplacementMaterializedViewPlan {
            name,
            materialized_view:
                plan::MaterializedView {
                    column_names,
                    cluster_id,
                    non_null_assertions,
                    refresh_schedule,
                    ..
                },
            ..
        } = &plan;

        let (item_id, global_id, optimizer) = self
            .create_materialized_view_optimize_common(
                name,
                column_names.clone(),
                cluster_id,
                non_null_assertions.clone(),
                refresh_schedule.clone(),
                &ExplainContext::None,
            )
            .await?;

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize create replacement materialized view",
            move || {
                span.in_scope(|| {
                    let raw_expr = plan.materialized_view.expr.clone();

                    let (local_mir_plan, global_mir_plan, global_lir_plan) =
                        Self::create_materialized_view_call_optimizer(
                            optimizer,
                            raw_expr,
                            &ExplainContext::None,
                        )?;

                    let finish = CreateReplacementMaterializedViewFinish {
                        item_id,
                        global_id,
                        validity,
                        plan,
                        resolved_ids,
                        local_mir_plan,
                        global_mir_plan,
                        global_lir_plan,
                    };
                    let stage = CreateReplacementMaterializedViewStage::Finish(finish);

                    Ok(Box::new(stage))
                })
            },
        )))
    }

    #[instrument]
    async fn create_replacement_materialized_view_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        stage: CreateReplacementMaterializedViewFinish,
    ) -> Result<StageResult<Box<CreateReplacementMaterializedViewStage>>, AdapterError> {
        let CreateReplacementMaterializedViewFinish {
            item_id,
            global_id,
            plan:
                plan::CreateReplacementMaterializedViewPlan {
                    name,
                    replaces,
                    materialized_view:
                        plan::MaterializedView {
                            mut create_sql,
                            expr: raw_expr,
                            dependencies,
                            cluster_id,
                            non_null_assertions,
                            compaction_window,
                            refresh_schedule,
                            ..
                        },
                    ..
                },
            resolved_ids,
            local_mir_plan,
            global_mir_plan,
            global_lir_plan,
            ..
        } = stage;

        let Some(mv) = self.catalog().get_entry(&replaces).materialized_view() else {
            return Err(AdapterError::internal(
                "create materialized view",
                "original SQL should roundtrip",
            ));
        };

        if &mv.desc.latest() != global_lir_plan.desc() {
            return Err(AdapterError::ChangedPlan(
                "Schemas are incompatible".to_string(),
            ));
        }

        // Timestamp selection
        let id_bundle = dataflow_import_id_bundle(global_lir_plan.df_desc(), cluster_id);

        let read_holds_owned;
        let read_holds = if let Some(txn_reads) = self.txn_read_holds.get(ctx.session().conn_id()) {
            // In some cases, for example when REFRESH is used, the preparatory
            // stages will already have acquired ReadHolds, we can re-use those.

            txn_reads
        } else {
            // No one has acquired holds, make sure we can determine an as_of
            // and render our dataflow below.
            read_holds_owned = self.acquire_read_holds(&id_bundle);
            &read_holds_owned
        };

        let (dataflow_as_of, storage_as_of, until) =
            self.select_timestamps(id_bundle, refresh_schedule.as_ref(), read_holds)?;

        tracing::info!(
            dataflow_as_of = ?dataflow_as_of,
            storage_as_of = ?storage_as_of,
            until = ?until,
            "materialized view timestamp selection",
        );

        let initial_as_of = storage_as_of.clone();

        // Update the `create_sql` with the selected `as_of`. This is how we make sure the `as_of`
        // is persisted to the catalog and can be relied on during bootstrapping.
        // This has to be the `storage_as_of`, because bootstrapping uses this in
        // `bootstrap_storage_collections`.
        if let Some(storage_as_of_ts) = storage_as_of.as_option() {
            let stmt = mz_sql::parse::parse(&create_sql)
                .map_err(|_| {
                    AdapterError::internal(
                        "create materialized view",
                        "original SQL should roundtrip",
                    )
                })?
                .into_element()
                .ast;
            let ast::Statement::CreateReplacementMaterializedView(mut stmt) = stmt else {
                panic!("unexpected statement type");
            };
            stmt.as_of = Some(storage_as_of_ts.into());
            create_sql = stmt.to_ast_string_stable();
        }

        let ops = vec![catalog::Op::CreateItem {
            id: item_id,
            name: name.clone(),
            item: CatalogItem::ReplacementMaterializedView(ReplacementMaterializedView {
                create_sql,
                replaces,
                raw_expr: raw_expr.into(),
                optimized_expr: local_mir_plan.expr().into(),
                desc: global_lir_plan.desc().clone(),
                global_id,
                resolved_ids,
                dependencies,
                cluster_id,
                non_null_assertions,
                custom_logical_compaction_window: compaction_window,
                refresh_schedule: refresh_schedule.clone(),
                initial_as_of: Some(initial_as_of.clone()),
            }),
            owner_id: *ctx.session().current_role_id(),
        }];

        // Pre-allocate a vector of transient GlobalIds for each notice.
        let notice_ids = std::iter::repeat_with(|| self.allocate_transient_id())
            .map(|(_item_id, global_id)| global_id)
            .take(global_lir_plan.df_meta().optimizer_notices.len())
            .collect::<Vec<_>>();

        self.catalog_transact_with_side_effects(Some(ctx), ops, move |coord, ctx| {
            Box::pin(async move {
                let output_desc = global_lir_plan.desc().clone();
                let (mut df_desc, df_meta) = global_lir_plan.unapply();

                // Save plan structures.
                coord
                    .catalog_mut()
                    .set_optimized_plan(global_id, global_mir_plan.df_desc().clone());
                coord
                    .catalog_mut()
                    .set_physical_plan(global_id, df_desc.clone());

                let notice_builtin_updates_fut = coord
                    .process_dataflow_metainfo(df_meta, global_id, ctx, notice_ids)
                    .await;

                df_desc.set_as_of(dataflow_as_of.clone());
                df_desc.set_initial_as_of(initial_as_of);
                df_desc.until = until;

                let storage_metadata = coord.catalog.state().storage_metadata();

                // Announce the creation of the materialized view source.
                coord
                    .controller
                    .storage
                    .create_collections(
                        storage_metadata,
                        None,
                        vec![(
                            global_id,
                            CollectionDescription {
                                desc: output_desc,
                                data_source: DataSource::Other,
                                since: Some(storage_as_of),
                                status_collection_id: None,
                                timeline: None,
                            },
                        )],
                    )
                    .await
                    .unwrap_or_terminate("cannot fail to append");

                coord
                    .initialize_storage_read_policies(
                        btreeset![item_id],
                        compaction_window.unwrap_or(CompactionWindow::Default),
                    )
                    .await;

                coord
                    .ship_dataflow_and_notice_builtin_table_updates(
                        df_desc,
                        cluster_id,
                        notice_builtin_updates_fut,
                    )
                    .await;
            })
        })
        .await?;

        Ok(StageResult::Response(
            ExecuteResponse::CreatedReplacementMaterializedView,
        ))
    }

    pub(crate) async fn sequence_alter_materialized_view_apply_replacement(
        &mut self,
        ctx: &mut ExecuteContext,
        AlterMaterializedViewApplyReplacementPlan {
            id,
            replacement_id,
        }: AlterMaterializedViewApplyReplacementPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mv = self
            .catalog()
            .get_entry(&id)
            .materialized_view()
            .ok_or_else(|| {
                AdapterError::internal(
                    "alter materialized view apply replacement",
                    "replacement id should refer to a materialized view",
                )
            })?;
        let rmv = self
            .catalog()
            .get_entry(&replacement_id)
            .replacement_materialized_view()
            .ok_or_else(|| {
                AdapterError::internal(
                    "alter materialized view apply replacement",
                    "replacement id should refer to a replacement materialized view",
                )
            })?;
        let rmv_cluster = rmv.cluster_id;
        let sink_id = rmv.global_id();
        self.catalog_transact_with_side_effects(
            Some(ctx),
            vec![catalog::Op::AlterMaterializedViewApplyReplacement {
                id,
                replacement_id,
                replaced_id: mv.global_id_writes(),
                cluster_id: mv.cluster_id,
            }],
            move |coord, _ctx| {
                Box::pin(async move {
                    coord.allow_writes(rmv_cluster, sink_id);
                })
            },
        )
        .await?;
        Ok(ExecuteResponse::AlteredObject(ObjectType::MaterializedView))
    }
}
