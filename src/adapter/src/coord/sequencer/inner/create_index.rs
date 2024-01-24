// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use maplit::btreemap;
use mz_catalog::memory::objects::{CatalogItem, Index};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::explain::{trace_plan, ExprHumanizerExt, TransientItem, UsedIndexes};
use mz_repr::{Datum, Row};
use mz_sql::catalog::CatalogError;
use mz_sql::names::ResolvedIds;
use mz_sql::plan;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateIndexExplain, CreateIndexFinish, CreateIndexOptimize, CreateIndexStage,
    CreateIndexValidate, ExplainContext, Message, PlanValidity,
};
use crate::error::AdapterError;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::{catalog, AdapterNotice, ExecuteContext, TimestampProvider};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_create_index(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateIndexPlan,
        resolved_ids: ResolvedIds,
    ) {
        self.execute_create_index_stage(
            ctx,
            CreateIndexStage::Validate(CreateIndexValidate {
                plan,
                resolved_ids,
                explain_ctx: None,
            }),
            OpenTelemetryContext::obtain(),
        )
        .await;
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn explain_create_index(
        &mut self,
        ctx: ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
    ) {
        let plan::Explainee::Statement(stmt) = explainee else {
            // This is currently asserted in the `sequence_explain_plan` code that
            // calls this method.
            unreachable!()
        };
        let plan::ExplaineeStatement::CreateIndex { broken, plan } = stmt else {
            // This is currently asserted in the `sequence_explain_plan` code that
            // calls this method.
            unreachable!()
        };

        // Create an OptimizerTrace instance to collect plans emitted when
        // executing the optimizer pipeline.
        let optimizer_trace = OptimizerTrace::new(broken, stage.path());

        // Not used in the EXPLAIN path so it's OK to generate a dummy value.
        let resolved_ids = ResolvedIds(Default::default());

        self.execute_create_index_stage(
            ctx,
            CreateIndexStage::Validate(CreateIndexValidate {
                plan,
                resolved_ids,
                explain_ctx: Some(ExplainContext {
                    broken,
                    config,
                    format,
                    stage,
                    desc: None,
                    optimizer_trace,
                }),
            }),
            OpenTelemetryContext::obtain(),
        )
        .await;
    }

    /// Processes as many `create index` stages as possible.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn execute_create_index_stage(
        &mut self,
        mut ctx: ExecuteContext,
        mut stage: CreateIndexStage,
        otel_ctx: OpenTelemetryContext,
    ) {
        use CreateIndexStage::*;

        // Process the current stage and allow for processing the next.
        loop {
            // Always verify plan validity. This is cheap, and prevents programming errors
            // if we move any stages off thread.
            if let Some(validity) = stage.validity() {
                return_if_err!(validity.check(self.catalog()), ctx);
            }

            (ctx, stage) = match stage {
                Validate(stage) => {
                    let next =
                        return_if_err!(self.create_index_validate(ctx.session(), stage), ctx);
                    (ctx, CreateIndexStage::Optimize(next))
                }
                Optimize(stage) => {
                    self.create_index_optimize(ctx, stage, otel_ctx).await;
                    return;
                }
                Finish(stage) => {
                    let result = self.create_index_finish(&mut ctx, stage).await;
                    ctx.retire(result);
                    return;
                }
                Explain(stage) => {
                    let result = self.create_index_explain(&mut ctx, stage);
                    ctx.retire(result);
                    return;
                }
            }
        }
    }

    fn create_index_validate(
        &mut self,
        session: &Session,
        CreateIndexValidate {
            plan,
            resolved_ids,
            explain_ctx,
        }: CreateIndexValidate,
    ) -> Result<CreateIndexOptimize, AdapterError> {
        let plan::CreateIndexPlan {
            index: plan::Index { on, cluster_id, .. },
            ..
        } = &plan;

        let validity = PlanValidity {
            transient_revision: self.catalog().transient_revision(),
            dependency_ids: BTreeSet::from_iter(std::iter::once(*on)),
            cluster_id: Some(*cluster_id),
            replica_id: None,
            role_metadata: session.role_metadata().clone(),
        };

        Ok(CreateIndexOptimize {
            validity,
            plan,
            resolved_ids,
            explain_ctx,
        })
    }

    async fn create_index_optimize(
        &mut self,
        ctx: ExecuteContext,
        CreateIndexOptimize {
            validity,
            plan,
            resolved_ids,
            explain_ctx,
        }: CreateIndexOptimize,
        otel_ctx: OpenTelemetryContext,
    ) {
        let plan::CreateIndexPlan {
            index: plan::Index { cluster_id, .. },
            ..
        } = &plan;

        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let internal_cmd_tx = self.internal_cmd_tx.clone();

        // Collect optimizer parameters.
        let compute_instance = self
            .instance_snapshot(*cluster_id)
            .expect("compute instance does not exist");
        let exported_index_id = if explain_ctx.is_some() {
            return_if_err!(self.allocate_transient_id(), ctx)
        } else {
            return_if_err!(self.catalog_mut().allocate_user_id().await, ctx)
        };
        let optimizer_config = if let Some(explain_ctx) = explain_ctx.as_ref() {
            optimize::OptimizerConfig::from((self.catalog().system_config(), &explain_ctx.config))
        } else {
            optimize::OptimizerConfig::from(self.catalog().system_config())
        };

        // Build an optimizer for this INDEX.
        let mut optimizer = optimize::index::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            exported_index_id,
            optimizer_config,
        );

        mz_ore::task::spawn_blocking(
            || "optimize create index",
            move || {
                let mut pipeline = || -> Result<(
                    optimize::index::GlobalMirPlan,
                    optimize::index::GlobalLirPlan,
                    UsedIndexes,
                ), AdapterError> {
                    // In `explain_~` contexts, set the trace-derived dispatch
                    // as default while optimizing.
                    let _dispatch_guard = if let Some(explain_ctx) = explain_ctx.as_ref() {
                        let dispatch = tracing::Dispatch::from(&explain_ctx.optimizer_trace);
                        Some(tracing::dispatcher::set_default(&dispatch))
                    } else {
                        None
                    };

                    let _span_guard = tracing::debug_span!(target: "optimizer", "optimize").entered();

                    // MIR ⇒ MIR optimization (global)
                    let index_plan =
                        optimize::index::Index::new(&plan.name, &plan.index.on, &plan.index.keys);
                    let global_mir_plan = optimizer.catch_unwind_optimize(index_plan)?;

                    // Collect the list of indexes used by the dataflow at this point
                    let used_indexes = {
                        let df_desc = global_mir_plan.df_desc();
                        let df_meta = global_mir_plan.df_meta();
                        UsedIndexes::new(
                            df_desc
                                .index_imports
                                .iter()
                                .map(|(id, _index_import)| {
                                    (*id, df_meta.index_usage_types.get(id).expect("prune_and_annotate_dataflow_index_imports should have been called already").clone())
                                })
                                .collect(),
                        )
                    };

                    // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                    let global_lir_plan = optimizer.catch_unwind_optimize(global_mir_plan.clone())?;

                    // Trace the resulting plan for the top-level `optimize` path.
                    trace_plan(global_lir_plan.df_desc());

                    Ok((global_mir_plan, global_lir_plan, used_indexes))
                };

                let stage = match pipeline() {
                    Ok((global_mir_plan, global_lir_plan, used_indexes)) => {
                        if let Some(explain_ctx) = explain_ctx {
                            let (_, df_meta) = global_lir_plan.unapply();
                            CreateIndexStage::Explain(CreateIndexExplain {
                                validity,
                                exported_index_id,
                                plan,
                                df_meta,
                                used_indexes,
                                explain_ctx,
                            })
                        } else {
                            CreateIndexStage::Finish(CreateIndexFinish {
                                validity,
                                exported_index_id,
                                plan,
                                resolved_ids,
                                global_mir_plan,
                                global_lir_plan,
                            })
                        }
                    }
                    // Internal optimizer errors are handled differently
                    // depending on the caller.
                    Err(err) => {
                        let Some(explain_ctx) = explain_ctx else {
                            // In `sequence_~` contexts, immediately retire the
                            // execution with the error.
                            return ctx.retire(Err(err.into()));
                        };

                        if explain_ctx.broken {
                            // In `EXPLAIN BROKEN` contexts, just log the error
                            // and move to the next stage with default
                            // parameters.
                            tracing::error!("error while handling EXPLAIN statement: {}", err);
                            CreateIndexStage::Explain(CreateIndexExplain {
                                validity,
                                exported_index_id,
                                plan,
                                df_meta: Default::default(),
                                used_indexes: Default::default(),
                                explain_ctx,
                            })
                        } else {
                            // In regular `EXPLAIN` contexts, immediately retire
                            // the execution with the error.
                            return ctx.retire(Err(err.into()));
                        }
                    }
                };

                let _ = internal_cmd_tx.send(Message::CreateIndexStageReady {
                    ctx,
                    otel_ctx,
                    stage,
                });
            },
        );
    }

    async fn create_index_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        CreateIndexFinish {
            exported_index_id,
            plan:
                plan::CreateIndexPlan {
                    name,
                    index:
                        plan::Index {
                            create_sql,
                            on,
                            keys,
                            cluster_id,
                        },
                    options,
                    if_not_exists,
                },
            resolved_ids,
            global_mir_plan,
            global_lir_plan,
            ..
        }: CreateIndexFinish,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = vec![catalog::Op::CreateItem {
            id: exported_index_id,
            oid: self.catalog_mut().allocate_oid()?,
            name: name.clone(),
            item: CatalogItem::Index(Index {
                create_sql,
                keys,
                on,
                conn_id: None,
                resolved_ids,
                cluster_id,
                is_retained_metrics_object: false,
                custom_logical_compaction_window: None,
            }),
            owner_id: *self.catalog().get_entry(&on).owner_id(),
        }];

        // Pre-allocate a vector of transient GlobalIds for each notice.
        let notice_ids = std::iter::repeat_with(|| self.allocate_transient_id())
            .take(global_lir_plan.df_meta().optimizer_notices.len())
            .collect::<Result<Vec<_>, _>>()?;

        let transact_result = self
            .catalog_transact_with_side_effects(Some(ctx.session()), ops, |coord| async {
                // Save plan structures.
                coord
                    .catalog_mut()
                    .set_optimized_plan(exported_index_id, global_mir_plan.df_desc().clone());
                coord
                    .catalog_mut()
                    .set_physical_plan(exported_index_id, global_lir_plan.df_desc().clone());

                let (mut df_desc, df_meta) = global_lir_plan.unapply();

                // Timestamp selection
                let id_bundle = dataflow_import_id_bundle(&df_desc, cluster_id);
                let since = coord.least_valid_read(&id_bundle);
                df_desc.set_as_of(since);

                // Emit notices.
                coord.emit_optimizer_notices(ctx.session(), &df_meta.optimizer_notices);

                // Return a metainfo with rendered notices.
                let df_meta =
                    coord
                        .catalog()
                        .render_notices(df_meta, notice_ids, Some(exported_index_id));
                coord
                    .catalog_mut()
                    .set_dataflow_metainfo(exported_index_id, df_meta.clone());

                if coord.catalog().state().system_config().enable_mz_notices() {
                    // Initialize a container for builtin table updates.
                    let mut builtin_table_updates =
                        Vec::with_capacity(df_meta.optimizer_notices.len());
                    // Collect optimization hint updates.
                    coord.catalog().state().pack_optimizer_notices(
                        &mut builtin_table_updates,
                        df_meta.optimizer_notices.iter(),
                        1,
                    );
                    // Write collected optimization hints to the builtin tables.
                    let builtin_updates_fut = coord
                        .builtin_table_update()
                        .execute(builtin_table_updates)
                        .await;

                    let ship_dataflow_fut = coord.ship_dataflow(df_desc, cluster_id);

                    futures::future::join(builtin_updates_fut, ship_dataflow_fut).await;
                } else {
                    coord.ship_dataflow(df_desc, cluster_id).await;
                }

                coord
                    .set_index_options(exported_index_id, options)
                    .expect("index enabled");
            })
            .await;

        match transact_result {
            Ok(_) => Ok(ExecuteResponse::CreatedIndex),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                ctx.session()
                    .add_notice(AdapterNotice::ObjectAlreadyExists {
                        name: name.item,
                        ty: "index",
                    });
                Ok(ExecuteResponse::CreatedIndex)
            }
            Err(err) => Err(err),
        }
    }

    fn create_index_explain(
        &mut self,
        ctx: &mut ExecuteContext,
        CreateIndexExplain {
            exported_index_id,
            plan: plan::CreateIndexPlan { name, index, .. },
            df_meta,
            used_indexes,
            explain_ctx:
                ExplainContext {
                    broken,
                    config,
                    format,
                    stage,
                    optimizer_trace,
                    ..
                },
            ..
        }: CreateIndexExplain,
    ) -> Result<ExecuteResponse, AdapterError> {
        let session_catalog = self.catalog().for_session(ctx.session());
        let expr_humanizer = {
            let on_entry = self.catalog.get_entry(&index.on);
            let full_name = self.catalog.resolve_full_name(&name, on_entry.conn_id());
            let on_desc = on_entry
                .desc(&full_name)
                .expect("can only create indexes on items with a valid description");

            let transient_items = btreemap! {
                exported_index_id => TransientItem::new(
                    Some(full_name.to_string()),
                    Some(full_name.item.to_string()),
                    Some(on_desc.iter_names().map(|c| c.to_string()).collect()),
                )
            };
            ExprHumanizerExt::new(transient_items, &session_catalog)
        };

        let trace = optimizer_trace.drain_all(
            format,
            &config,
            &expr_humanizer,
            None,
            used_indexes,
            df_meta,
        )?;

        let rows = match stage.path() {
            None => {
                // For the `Trace` (pseudo-)stage, return the entire trace as
                // triples of (time, path, plan) values.
                let rows = trace
                    .into_iter()
                    .map(|entry| {
                        // The trace would have to take over 584 years to overflow a u64.
                        let span_duration = u64::try_from(entry.span_duration.as_nanos());
                        Row::pack_slice(&[
                            Datum::from(span_duration.unwrap_or(u64::MAX)),
                            Datum::from(entry.path.as_str()),
                            Datum::from(entry.plan.as_str()),
                        ])
                    })
                    .collect();
                rows
            }
            Some(path) => {
                // For everything else, return the plan for the stage identified
                // by the corresponding path.
                let row = trace
                    .into_iter()
                    .find(|entry| entry.path == path)
                    .map(|entry| Row::pack_slice(&[Datum::from(entry.plan.as_str())]))
                    .ok_or_else(|| {
                        let stmt_kind = plan::ExplaineeStatementKind::CreateIndex;
                        if !stmt_kind.supports(&stage) {
                            // Print a nicer error for unsupported stages.
                            AdapterError::Unstructured(anyhow::anyhow!(format!(
                                "cannot EXPLAIN {stage} FOR {stmt_kind}"
                            )))
                        } else {
                            // We don't expect this stage to be missing.
                            AdapterError::Internal(format!(
                                "stage `{path}` not present in the collected optimizer trace",
                            ))
                        }
                    })?;
                vec![row]
            }
        };

        if broken {
            tracing_core::callsite::rebuild_interest_cache();
        }

        Ok(Self::send_immediate_rows(rows))
    }
}
