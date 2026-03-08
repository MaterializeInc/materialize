// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use maplit::btreemap;
use mz_catalog::memory::objects::{CatalogItem, StandingQuery};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_expr::OptimizedMirRelationExpr;
use mz_ore::instrument;
use mz_ore::treat_as_equal::TreatAsEqual;
use mz_repr::GlobalId;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
use mz_repr::{ColumnName, RelationDesc, SqlColumnType, SqlRelationType, SqlScalarType};
use mz_sql::names::ResolvedIds;
use mz_sql::plan;
use mz_sql::plan::{ColumnRef, HirRelationExpr, HirScalarExpr, JoinKind};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_client::controller::CollectionDescription;
use mz_transform::dataflow::DataflowMetainfo;
use timely::progress::Timestamp as TimelyTimestamp;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{Coordinator, ExplainContext, ExplainPlanContext};
use crate::error::AdapterError;
use crate::explain::optimizer_trace::OptimizerTrace;
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
                    params,
                    cluster_id,
                },
            if_not_exists: _if_not_exists,
        } = plan;

        // Allocate IDs for the standing query and the internal parameter collection.
        let id_ts = self.get_catalog_write_ts().await;
        let ids = self.catalog().allocate_user_ids(2, id_ts).await?;
        let (item_id, global_id) = ids[0];
        let (_param_item_id, param_collection_id) = ids[1];

        // Build the parameter collection's RelationDesc and SqlRelationType.
        let param_desc = Self::build_param_collection_desc(&params);
        let param_typ = param_desc.typ().clone();

        // Rewrite the user's query HIR to join with the parameter collection.
        // This replaces $N parameter references with column references to the
        // param collection and prepends request_id to the output.
        let (rewritten_expr, rewritten_column_names) = Self::rewrite_standing_query_hir(
            &raw_expr,
            &column_names,
            param_collection_id,
            &param_typ,
            &params,
        );

        // Optimize using the standing query optimizer, which pre-imports the
        // param collection as an extra source.
        let extra_source_imports = BTreeMap::from([(param_collection_id, param_typ)]);
        let (optimized_plan, mut physical_plan, raw_metainfo, optimizer_features) = self
            .optimize_create_standing_query(
                &rewritten_expr,
                &rewritten_column_names,
                global_id,
                cluster_id,
                extra_source_imports,
            )?;

        // Create a metainfo with rendered notices, preallocating a transient
        // GlobalId for each.
        let notice_ids = std::iter::repeat_with(|| self.allocate_transient_id())
            .map(|(_item_id, global_id)| global_id)
            .take(raw_metainfo.optimizer_notices.len())
            .collect();
        let metainfo = self
            .catalog()
            .render_notices(raw_metainfo, notice_ids, Some(global_id));

        // Note: timestamp selection is deferred to after param collection creation
        // in the side effects callback, because the param collection's since
        // is set at creation time and must be included in the as_of.

        // Extract the optimized MIR expression from the dataflow.
        let optimized_expr = optimized_plan
            .objects_to_build
            .last()
            .expect("optimizer must produce at least one object")
            .plan
            .clone();

        // The desc stored in the catalog is the *output* desc (without request_id).
        let item = StandingQuery {
            create_sql,
            global_id,
            raw_expr: raw_expr.into(),
            optimized_expr: optimized_expr.into(),
            desc: desc.clone(),
            params,
            param_collection_id,
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

                    // Create the internal parameter collection. We use DataSource::Other
                    // so it is NOT registered with the collection manager's append-only
                    // write task. Instead we hold a WriteHandle directly and do our own
                    // compare_and_append at the shard's current upper, avoiding the 1s
                    // idle tick and now()-based timestamp inflation.
                    let register_ts = coord.get_local_write_ts().await.timestamp;
                    coord
                        .controller
                        .storage
                        .create_collections(
                            coord.catalog.state().storage_metadata(),
                            Some(register_ts),
                            vec![(
                                param_collection_id,
                                CollectionDescription {
                                    desc: param_desc.clone(),
                                    data_source: mz_storage_client::controller::DataSource::Other,
                                    since: None,
                                    timeline: Some(
                                        mz_storage_types::sources::Timeline::EpochMilliseconds,
                                    ),
                                    primary: None,
                                },
                            )],
                        )
                        .await
                        .unwrap_or_terminate("cannot fail to create param collection");
                    coord.apply_local_write(register_ts).await;

                    // Open our own WriteHandle for the param collection shard.
                    let param_metadata = coord
                        .controller
                        .storage
                        .collection_metadata(param_collection_id)
                        .expect("param collection must exist");
                    let param_write_handle = coord
                        .persist_client
                        .open_writer(
                            param_metadata.data_shard,
                            std::sync::Arc::new(param_desc),
                            std::sync::Arc::new(mz_persist_types::codec_impls::UnitSchema),
                            mz_persist_client::Diagnostics {
                                shard_name: param_collection_id.to_string(),
                                handle_purpose: format!(
                                    "standing query param writer for {}",
                                    param_collection_id
                                ),
                            },
                        )
                        .await
                        .expect("valid persist usage");
                    // Compute the as_of from input frontiers.
                    use crate::optimize::dataflows::dataflow_import_id_bundle;
                    let mut id_bundle = dataflow_import_id_bundle(&physical_plan, cluster_id);
                    id_bundle.storage_ids.remove(&global_id);
                    let read_holds = coord.acquire_read_holds(&id_bundle);
                    let as_of = read_holds.least_valid_read();
                    physical_plan.set_as_of(as_of.clone());
                    physical_plan.set_initial_as_of(as_of);

                    // The subscribe can't produce output until the param shard
                    // upper advances past the as_of. Initialize the watch
                    // channel with as_of + 1 so the batcher immediately
                    // advances the param shard past the subscribe's snapshot
                    // point.
                    let initial_upper_target = physical_plan
                        .as_of
                        .as_ref()
                        .and_then(|a| a.as_option().copied())
                        .map(|ts| ts.step_forward())
                        .unwrap_or_else(mz_repr::Timestamp::minimum);

                    tracing::info!(
                        "standing query {global_id}: initial_upper_target={initial_upper_target}, as_of={:?}",
                        physical_plan.as_of.as_ref().map(|a| a.elements()),
                    );

                    let (subscribe_tx, subscribe_rx) = tokio::sync::mpsc::unbounded_channel();
                    let (flush_tx, flush_rx) = tokio::sync::mpsc::unbounded_channel();
                    let (advance_upper_tx, advance_upper_rx) =
                        tokio::sync::watch::channel(initial_upper_target);
                    let sq_client = crate::standing_query_client::StandingQueryExecuteClient::new(
                        item_id,
                        global_id,
                        param_write_handle,
                        flush_tx,
                        advance_upper_rx,
                    );
                    crate::coord::standing_query_handler::spawn_standing_query_handler(
                        global_id,
                        sq_client.clone(),
                        subscribe_rx,
                        flush_rx,
                    );

                    coord.ship_dataflow(physical_plan, cluster_id, None).await;

                    // Register the active standing query state.
                    use crate::coord::standing_query_state::ActiveStandingQuery;
                    // Build the input bundle for ongoing upper tracking.
                    // Remove the param collection — we don't want circular
                    // dependency on ourselves.
                    let mut input_bundle = id_bundle;
                    input_bundle.storage_ids.remove(&param_collection_id);

                    coord.active_standing_queries.insert(
                        global_id,
                        ActiveStandingQuery {
                            item_id,
                            cluster_id,
                            input_ids: input_bundle,
                            client: sq_client,
                            subscribe_tx,
                            advance_upper_tx,
                        },
                    );
                })
            })
            .await?;

        Ok(ExecuteResponse::CreatedStandingQuery)
    }

    /// Rewrite the user's query HIR to join with the parameter collection.
    ///
    /// Takes the user's query as-is (supporting arbitrary complexity: joins,
    /// subqueries, aggregations, etc.) and produces:
    /// ```text
    ///   Project { request_id, user_col_1, ..., user_col_N }
    ///     Join {
    ///       left: Get(params),        -- [request_id, param_1, ..., param_K]
    ///       right: <user_query>,      -- with $N replaced by correlated refs to left
    ///       on: TRUE,
    ///       kind: Inner,
    ///     }
    /// ```
    ///
    /// The user's query contains `Parameter(N)` references (from `$N` in the
    /// SQL). These are replaced with correlated column references (`level: depth + 1`)
    /// that point to the corresponding columns in the params collection (the
    /// left side of the join). The HIR lowering pass handles decorrelation of
    /// the resulting correlated join automatically.
    ///
    /// In the params collection:
    ///   column 0 is `request_id`
    ///   column N is `param_N` (corresponding to `$N`)
    ///
    /// In the join result:
    ///   columns `0..param_arity` are from the params collection
    ///   columns `param_arity..param_arity+user_arity` are from the user query
    pub(crate) fn rewrite_standing_query_hir(
        raw_expr: &HirRelationExpr,
        column_names: &[ColumnName],
        param_collection_id: GlobalId,
        param_typ: &SqlRelationType,
        _params: &[(String, SqlScalarType)],
    ) -> (HirRelationExpr, Vec<ColumnName>) {
        let user_arity = raw_expr.arity();
        let param_arity = param_typ.column_types.len(); // request_id + param_1..param_K

        // Construct the param collection Get (left side of join).
        let param_get = HirRelationExpr::Get {
            id: mz_expr::Id::Global(param_collection_id),
            typ: param_typ.clone(),
        };

        // Clone the user's query and replace Parameter(N) with correlated
        // column references to the params collection (left side of join).
        // In the right side of a Join, `level: 1` refers to the left side's
        // columns. Inside nested subqueries or join-right-sides within the
        // user query, the depth increases, so we use `level: depth + 1`.
        let mut user_expr = raw_expr.clone();
        #[allow(deprecated)]
        let _ = user_expr.visit_scalar_expressions_mut(
            0,
            &mut |scalar: &mut HirScalarExpr, depth: usize| {
                #[allow(deprecated)]
                let _ = scalar.visit_recursively_mut(
                    depth,
                    &mut |depth: usize, e: &mut HirScalarExpr| {
                        if let HirScalarExpr::Parameter(n, _name) = e {
                            // $N (1-based) maps to column N in the params collection.
                            // From within the right side of our join, the params
                            // (left side) are `depth + 1` scopes up.
                            *e = HirScalarExpr::Column(
                                ColumnRef {
                                    level: depth + 1,
                                    column: *n,
                                },
                                TreatAsEqual(None),
                            );
                        }
                        Ok::<_, ()>(())
                    },
                );
                Ok::<_, ()>(())
            },
        );

        // Build a correlated inner join: params × user_query.
        // The user_expr now contains correlated references to the left side.
        let joined = HirRelationExpr::Join {
            left: Box::new(param_get),
            right: Box::new(user_expr),
            on: HirScalarExpr::literal_true(),
            kind: JoinKind::Inner,
        };

        // Project: request_id first (column 0), then the user's output columns
        // (which start at param_arity in the joined relation).
        let mut outputs = vec![0]; // request_id
        outputs.extend(param_arity..param_arity + user_arity);
        let projected = HirRelationExpr::Project {
            input: Box::new(joined),
            outputs,
        };

        // Build column names: request_id + original column names.
        let mut new_column_names = vec![ColumnName::from("request_id")];
        new_column_names.extend(column_names.iter().cloned());

        (projected, new_column_names)
    }

    /// Build the RelationDesc for a standing query's parameter collection.
    ///
    /// Schema: `(request_id UInt64, param_1 T1, param_2 T2, ...)`
    pub(crate) fn build_param_collection_desc(params: &[(String, SqlScalarType)]) -> RelationDesc {
        let mut desc = RelationDesc::builder();
        desc = desc.with_column(
            ColumnName::from("request_id"),
            SqlColumnType {
                scalar_type: SqlScalarType::UInt64,
                nullable: false,
            },
        );
        for (param_name, param_type) in params {
            desc = desc.with_column(
                ColumnName::from(param_name.as_str()),
                SqlColumnType {
                    scalar_type: param_type.clone(),
                    nullable: true,
                },
            );
        }
        desc.finish()
    }

    pub(crate) fn optimize_create_standing_query(
        &self,
        raw_expr: &HirRelationExpr,
        column_names: &[mz_repr::ColumnName],
        output_id: GlobalId,
        cluster_id: mz_controller_types::ClusterId,
        extra_source_imports: BTreeMap<GlobalId, SqlRelationType>,
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

        let mut optimizer = optimize::standing_query::Optimizer::new(
            catalog,
            compute_instance,
            output_id,
            view_id,
            column_names.to_vec(),
            format!("standing-query-{output_id}"),
            optimizer_config,
            self.optimizer_metrics(),
            extra_source_imports,
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

    /// EXPLAIN CREATE STANDING QUERY
    ///
    /// Shows the optimized plan for the standing query dataflow, including the
    /// rewritten HIR that joins the user's query with the parameter collection.
    pub(crate) async fn explain_create_standing_query(
        &self,
        ctx: ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
    ) {
        let plan::Explainee::Statement(stmt) = explainee else {
            unreachable!()
        };
        let plan::ExplaineeStatement::CreateStandingQuery { broken, plan } = stmt else {
            unreachable!()
        };

        let plan::CreateStandingQueryPlan {
            name,
            standing_query:
                plan::StandingQuery {
                    expr: raw_expr,
                    column_names,
                    params,
                    cluster_id,
                    ..
                },
            ..
        } = plan;

        // Allocate transient IDs for the explain path.
        let (_item_id, global_id) = self.allocate_transient_id();
        let (_param_item_id, param_collection_id) = self.allocate_transient_id();

        // Build the parameter collection desc and type.
        let param_desc = Self::build_param_collection_desc(&params);
        let param_typ = param_desc.typ().clone();

        // Rewrite the user's query HIR to join with the parameter collection.
        let (rewritten_expr, rewritten_column_names) = Self::rewrite_standing_query_hir(
            &raw_expr,
            &column_names,
            param_collection_id,
            &param_typ,
            &params,
        );

        // Create an OptimizerTrace to collect plans emitted during optimization.
        let optimizer_trace = OptimizerTrace::new(stage.paths());

        let extra_source_imports = BTreeMap::from([(param_collection_id, param_typ)]);

        // Run the optimizer pipeline within the trace dispatcher.
        let result: Result<DataflowMetainfo, AdapterError> = {
            let _dispatch_guard = optimizer_trace.as_guard();

            let catalog = self.owned_catalog().as_optimizer_catalog();
            let (_, view_id) = self.allocate_transient_id();
            let compute_instance = self
                .instance_snapshot(cluster_id)
                .expect("compute instance does not exist");
            let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
                .override_from(&self.catalog.get_cluster(cluster_id).config.features())
                .override_from(&ExplainContext::Plan(ExplainPlanContext {
                    broken,
                    config: config.clone(),
                    format: format.clone(),
                    stage: stage.clone(),
                    replan: None,
                    desc: None,
                    optimizer_trace: OptimizerTrace::new(None),
                }));

            let mut optimizer = optimize::standing_query::Optimizer::new(
                catalog,
                compute_instance,
                global_id,
                view_id,
                rewritten_column_names.clone(),
                format!("standing-query-{global_id}"),
                optimizer_config,
                self.optimizer_metrics(),
                extra_source_imports,
            );

            match || -> Result<DataflowMetainfo, AdapterError> {
                let local_mir_plan = optimizer.catch_unwind_optimize(rewritten_expr.clone())?;
                let global_mir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;
                let global_lir_plan = optimizer.catch_unwind_optimize(global_mir_plan)?;
                let (_physical_plan, metainfo) = global_lir_plan.unapply();
                Ok(metainfo)
            }() {
                Ok(metainfo) => Ok(metainfo),
                Err(err) => {
                    if broken {
                        tracing::error!("error while handling EXPLAIN statement: {}", err);
                        Ok(Default::default())
                    } else {
                        Err(err)
                    }
                }
            }
        };

        let df_meta = return_if_err!(result, ctx);

        // Build the humanizer with transient item names.
        let session_catalog = self.catalog().for_session(ctx.session());
        let expr_humanizer = {
            let full_name = self.catalog().resolve_full_name(&name, None);
            let transient_items = btreemap! {
                global_id => TransientItem::new(
                    Some(full_name.into_parts()),
                    Some(rewritten_column_names.iter().map(|c| c.to_string()).collect()),
                ),
                param_collection_id => TransientItem::new(
                    Some(vec![GlobalId::Explain.to_string()]),
                    Some(param_desc.iter_names().map(|c| c.to_string()).collect()),
                ),
            };
            ExprHumanizerExt::new(transient_items, &session_catalog)
        };

        let target_cluster = self.catalog().get_cluster(cluster_id);

        let features = OptimizerFeatures::from(self.catalog().system_config())
            .override_from(&target_cluster.config.features())
            .override_from(&config.features);

        let result = optimizer_trace
            .into_rows(
                format,
                &config,
                &features,
                &expr_humanizer,
                None,
                Some(target_cluster),
                df_meta,
                stage,
                plan::ExplaineeStatementKind::CreateStandingQuery,
                None,
            )
            .await;

        let rows = return_if_err!(result, ctx);
        ctx.retire(Ok(Self::send_immediate_rows(rows)));
    }
}
