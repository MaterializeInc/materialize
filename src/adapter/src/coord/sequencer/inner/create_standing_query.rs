// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_catalog::memory::objects::{CatalogItem, StandingQuery};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_expr::OptimizedMirRelationExpr;
use mz_ore::instrument;
use mz_ore::treat_as_equal::TreatAsEqual;
use mz_repr::GlobalId;
use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
use mz_repr::{ColumnName, RelationDesc, SqlColumnType, SqlRelationType, SqlScalarType};
use mz_sql::names::ResolvedIds;
use mz_sql::plan;
use mz_sql::plan::{ColumnRef, HirRelationExpr, HirScalarExpr, JoinKind};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_client::controller::CollectionDescription;
use mz_transform::dataflow::DataflowMetainfo;

use crate::command::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;
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

                    // Create the internal parameter collection (a table-like collection).
                    let register_ts = coord.get_local_write_ts().await.timestamp;
                    coord
                        .controller
                        .storage
                        .create_collections(
                            coord.catalog.state().storage_metadata(),
                            Some(register_ts),
                            vec![(
                                param_collection_id,
                                CollectionDescription::for_table(param_desc),
                            )],
                        )
                        .await
                        .unwrap_or_terminate("cannot fail to create param collection");
                    coord.apply_local_write(register_ts).await;

                    // Now that the param collection exists, compute the as_of.
                    use crate::optimize::dataflows::dataflow_import_id_bundle;
                    let mut id_bundle = dataflow_import_id_bundle(&physical_plan, cluster_id);
                    // The standing query's own ID is not a storage collection.
                    id_bundle.storage_ids.remove(&global_id);
                    let read_holds = coord.acquire_read_holds(&id_bundle);
                    let as_of = read_holds.least_valid_read();
                    physical_plan.set_as_of(as_of.clone());
                    physical_plan.set_initial_as_of(as_of);

                    coord.ship_dataflow(physical_plan, cluster_id, None).await;

                    // Register the standing query for subscribe response handling.
                    // The subscribe sink ID is the standing query's global_id.
                    use crate::coord::standing_query_state::ActiveStandingQuery;
                    coord.active_standing_queries.insert(
                        global_id,
                        ActiveStandingQuery {
                            item_id,
                            output_id: global_id,
                            param_collection_id,
                            cluster_id,
                            subscribe_sink_id: global_id,
                            batch_buffer: Vec::new(),
                            request_map: BTreeMap::new(),
                            in_flight: BTreeMap::new(),
                            result_buffer: BTreeMap::new(),
                            param_rows: BTreeMap::new(),
                            pending_retractions: Vec::new(),
                        },
                    );
                })
            })
            .await?;

        Ok(ExecuteResponse::CreatedStandingQuery)
    }

    /// Rewrite the user's query HIR to join with the parameter collection.
    ///
    /// Transforms:
    /// ```text
    ///   Project { outputs }
    ///     Filter { predicates: [col_a = $1, col_b = $2, static_filter] }
    ///       Get { orders }
    /// ```
    /// Into:
    /// ```text
    ///   Project { request_id, original_outputs }
    ///     Filter { predicates: [col_a = params.param_1, col_b = params.param_2, static_filter] }
    ///       Join { left: Get(orders), right: Get(params), on: TRUE, kind: Inner }
    /// ```
    ///
    /// The key transformation:
    /// 1. Decompose the user's expression into its base relation and the
    ///    operators above it (filters, maps, projects).
    /// 2. Insert a cross join between the base relation and the param collection.
    /// 3. Replace `Parameter(N)` with column references to `params.param_N`.
    /// 4. Prepend `request_id` to the output projection.
    ///
    /// In the joined relation:
    ///   columns `0..base_arity` are from the base relation (e.g. orders)
    ///   column `base_arity + 0` is `request_id`
    ///   column `base_arity + N` is `param_N` (corresponding to `$N`)
    fn rewrite_standing_query_hir(
        raw_expr: &HirRelationExpr,
        column_names: &[ColumnName],
        param_collection_id: GlobalId,
        param_typ: &SqlRelationType,
        _params: &[(String, SqlScalarType)],
    ) -> (HirRelationExpr, Vec<ColumnName>) {
        // Decompose the user's expression. For a standing query, the planner
        // produces:
        //   Project { Filter { Get { target } } }
        // or just:
        //   Filter { Get { target } }
        // We need to find the innermost Get (the base relation) so we can
        // insert the cross join below the filters.
        //
        // Strategy: find the base Get, wrap it in a cross join with params,
        // then re-apply the filters/projects on top with parameter references
        // replaced by column refs into the joined relation.

        // For v1, the expression structure is restricted (single FROM, no joins).
        // Extract the base Get and the filter predicates + project outputs.
        let (base_get, predicates, project_outputs) = Self::decompose_simple_query(raw_expr);
        let base_arity = match &base_get {
            HirRelationExpr::Get { typ, .. } => typ.column_types.len(),
            _ => panic!("expected Get as base relation"),
        };

        // Construct the param collection Get.
        let param_get = HirRelationExpr::Get {
            id: mz_expr::Id::Global(param_collection_id),
            typ: param_typ.clone(),
        };

        // Cross join: base_relation × params
        let joined = HirRelationExpr::Join {
            left: Box::new(base_get),
            right: Box::new(param_get),
            on: HirScalarExpr::literal_true(),
            kind: JoinKind::Inner,
        };

        // Replace $N in predicates with column references to params.
        let mut rewritten_predicates = predicates;
        for pred in rewritten_predicates.iter_mut() {
            Self::replace_param_in_scalar(pred, base_arity);
        }

        // Apply filters on top of the join.
        let filtered = if rewritten_predicates.is_empty() {
            joined
        } else {
            HirRelationExpr::Filter {
                input: Box::new(joined),
                predicates: rewritten_predicates,
            }
        };

        // Project: request_id first, then original output columns.
        // The original project_outputs reference columns in the base relation
        // (0..base_arity), which are still at the same positions in the joined
        // relation. request_id is at position base_arity.
        let request_id_col = base_arity;
        let mut outputs = vec![request_id_col];
        match project_outputs {
            Some(original_outputs) => outputs.extend(original_outputs),
            None => outputs.extend(0..base_arity),
        }
        let projected = HirRelationExpr::Project {
            input: Box::new(filtered),
            outputs,
        };

        // Build column names: request_id + original column names.
        let mut new_column_names = vec![ColumnName::from("request_id")];
        new_column_names.extend(column_names.iter().cloned());

        (projected, new_column_names)
    }

    /// Decompose a simple standing query expression into its components.
    ///
    /// Returns (base_get, filter_predicates, optional_project_outputs).
    /// Handles:
    ///   Get
    ///   Filter { Get }
    ///   Project { Filter { Get } }
    ///   Project { Get }
    fn decompose_simple_query(
        expr: &HirRelationExpr,
    ) -> (HirRelationExpr, Vec<HirScalarExpr>, Option<Vec<usize>>) {
        match expr {
            HirRelationExpr::Project { input, outputs } => match input.as_ref() {
                HirRelationExpr::Filter {
                    input: inner,
                    predicates,
                } => match inner.as_ref() {
                    get @ HirRelationExpr::Get { .. } => {
                        (get.clone(), predicates.clone(), Some(outputs.clone()))
                    }
                    _ => panic!("standing query: expected Get inside Filter inside Project"),
                },
                get @ HirRelationExpr::Get { .. } => {
                    (get.clone(), Vec::new(), Some(outputs.clone()))
                }
                _ => panic!("standing query: expected Filter or Get inside Project"),
            },
            HirRelationExpr::Filter {
                input, predicates, ..
            } => match input.as_ref() {
                get @ HirRelationExpr::Get { .. } => (get.clone(), predicates.clone(), None),
                _ => panic!("standing query: expected Get inside Filter"),
            },
            get @ HirRelationExpr::Get { .. } => (get.clone(), Vec::new(), None),
            _ => panic!("standing query: expected Project, Filter, or Get at top level"),
        }
    }

    /// Replace `Parameter(N)` in a scalar expression with a column reference
    /// to the param collection.
    fn replace_param_in_scalar(expr: &mut HirScalarExpr, base_arity: usize) {
        #[allow(deprecated)]
        let _ = expr.visit_recursively_mut(0, &mut |_depth: usize, e: &mut HirScalarExpr| {
            if let HirScalarExpr::Parameter(n, _name) = e {
                // $N (1-based) maps to param_N in the param collection,
                // which is at column base_arity + N in the joined relation.
                *e = HirScalarExpr::Column(
                    ColumnRef {
                        level: 0,
                        column: base_arity + *n,
                    },
                    TreatAsEqual(None),
                );
            }
            Ok::<_, ()>(())
        });
    }

    /// Build the RelationDesc for a standing query's parameter collection.
    ///
    /// Schema: `(request_id UUID, param_1 T1, param_2 T2, ...)`
    fn build_param_collection_desc(params: &[(String, SqlScalarType)]) -> RelationDesc {
        let mut desc = RelationDesc::builder();
        desc = desc.with_column(
            ColumnName::from("request_id"),
            SqlColumnType {
                scalar_type: SqlScalarType::Uuid,
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

    fn optimize_create_standing_query(
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
}
