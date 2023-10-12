// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains alternative implementations of `sequence_~` methods
//! that optimize plans using the new optimizer API. The `sequence_plan` method
//! in the parent module will delegate to these methods only if the
//! `enable_unified_optimizer_api` feature flag is off. Once we have gained
//! enough confidence that the new methods behave equally well as the old ones,
//! we will deprecate the old methods and move the implementations here to the
//! `inner` module.

use std::collections::BTreeMap;

use differential_dataflow::lattice::Lattice;
use mz_controller_types::ClusterId;
use mz_expr::CollectionPlan;
use mz_repr::explain::{TransientItem, UsedIndexes};
use mz_repr::{ColumnName, GlobalId, RelationDesc, Timestamp};
use mz_sql::catalog::CatalogError;
use mz_sql::names::{ObjectId, QualifiedItemName, ResolvedIds};
use mz_sql::plan::{self, MaterializedView};
use mz_storage_client::controller::{CollectionDescription, DataSource, DataSourceOther};
use mz_transform::dataflow::DataflowMetainfo;
use timely::progress::Antichain;
use tracing::Level;

use crate::catalog::CatalogItem;
use crate::coord::dataflows::DataflowBuilder;
use crate::coord::peek::FastPathPlan;
use crate::coord::sequencer::inner::catch_unwind;
use crate::coord::{Coordinator, DEFAULT_LOGICAL_COMPACTION_WINDOW_TS};
use crate::session::Session;
use crate::util::ResultExt;
use crate::{catalog, AdapterError, AdapterNotice, ExecuteResponse, TimestampProvider};

impl Coordinator {
    /// This should mirror the operational semantics of
    /// `Coordinator::sequence_create_materialized_view`.
    #[deprecated = "This is being replaced by sequence_create_materialized_view (see #20569)."]
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_materialized_view_deprecated(
        &mut self,
        session: &mut Session,
        plan: plan::CreateMaterializedViewPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::CreateMaterializedViewPlan {
            name,
            materialized_view:
                MaterializedView {
                    create_sql,
                    expr: raw_expr,
                    column_names,
                    cluster_id,
                    non_null_assertions,
                },
            replace: _,
            drop_ids,
            if_not_exists,
            ambiguous_columns,
        } = plan;

        self.ensure_cluster_can_host_compute_item(&name, cluster_id)?;

        // Validate any references in the materialized view's expression. We do
        // this on the unoptimized plan to better reflect what the user typed.
        // We want to reject queries that depend on log sources, for example,
        // even if we can *technically* optimize that reference away.
        let expr_depends_on = raw_expr.depends_on();
        self.validate_timeline_context(expr_depends_on.iter().cloned())?;
        self.validate_system_column_references(ambiguous_columns, &expr_depends_on)?;
        // Materialized views are not allowed to depend on log sources, as replicas
        // are not producing the same definite collection for these.
        // TODO(teskje): Remove this check once arrangement-based log sources
        // are replaced with persist-based ones.
        let log_names = expr_depends_on
            .iter()
            .flat_map(|id| self.catalog().introspection_dependencies(*id))
            .map(|id| self.catalog().get_entry(&id).name().item.clone())
            .collect::<Vec<_>>();
        if !log_names.is_empty() {
            return Err(AdapterError::InvalidLogDependency {
                object_type: "materialized view".into(),
                log_names,
            });
        }

        // Allocate IDs for the materialized view in the catalog.
        let id = self.catalog_mut().allocate_user_id().await?;
        let oid = self.catalog_mut().allocate_oid()?;
        // Allocate a unique ID that can be used by the dataflow builder to
        // connect the view dataflow to the storage sink.
        let internal_view_id = self.allocate_transient_id()?;
        let decorrelated_expr = raw_expr.optimize_and_lower(&plan::OptimizerConfig {})?;
        let optimized_expr = self.view_optimizer.optimize(decorrelated_expr)?;
        let desc = RelationDesc::new(optimized_expr.typ(), column_names);
        let debug_name = self.catalog().resolve_full_name(&name, None).to_string();

        // Pick the least valid read timestamp as the as-of for the view
        // dataflow. This makes the materialized view include the maximum possible
        // amount of historical detail.
        let id_bundle = self
            .index_oracle(cluster_id)
            .sufficient_collections(&expr_depends_on);
        let as_of = self.least_valid_read(&id_bundle);

        let mut ops = Vec::new();
        ops.extend(
            drop_ids
                .into_iter()
                .map(|id| catalog::Op::DropObject(ObjectId::Item(id))),
        );
        ops.push(catalog::Op::CreateItem {
            id,
            oid,
            name: name.clone(),
            item: CatalogItem::MaterializedView(catalog::MaterializedView {
                create_sql,
                optimized_expr,
                desc: desc.clone(),
                resolved_ids,
                cluster_id,
                non_null_assertions,
            }),
            owner_id: *session.current_role_id(),
        });

        match self
            .catalog_transact_with(Some(session.conn_id()), ops, |txn| {
                // Create a dataflow that materializes the view query and sinks
                // it to storage.
                let CatalogItem::MaterializedView(mv) = txn.catalog.get_entry(&id).item() else {
                    unreachable!()
                };

                let mut builder = txn.dataflow_builder(cluster_id);
                let (df, df_metainfo) = builder.build_materialized_view(
                    id,
                    internal_view_id,
                    debug_name,
                    &mv.optimized_expr,
                    &mv.desc,
                    &mv.non_null_assertions,
                )?;

                Ok((df, df_metainfo))
            })
            .await
        {
            Ok((mut df, df_metainfo)) => {
                self.emit_optimizer_notices(session, &df_metainfo.optimizer_notices);
                // Announce the creation of the materialized view source.
                self.controller
                    .storage
                    .create_collections(
                        None,
                        vec![(
                            id,
                            CollectionDescription {
                                desc,
                                data_source: DataSource::Other(DataSourceOther::Compute),
                                since: Some(as_of.clone()),
                                status_collection_id: None,
                            },
                        )],
                    )
                    .await
                    .unwrap_or_terminate("cannot fail to append");

                self.initialize_storage_read_policies(
                    vec![id],
                    Some(DEFAULT_LOGICAL_COMPACTION_WINDOW_TS),
                )
                .await;

                self.catalog_mut().set_optimized_plan(id, df.clone());
                self.catalog_mut().set_dataflow_metainfo(id, df_metainfo);

                df.set_as_of(as_of);
                let df = self.must_ship_dataflow(df, cluster_id).await;
                self.catalog_mut().set_physical_plan(id, df);

                Ok(ExecuteResponse::CreatedMaterializedView)
            }
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: name.item,
                    ty: "materialized view",
                });
                Ok(ExecuteResponse::CreatedMaterializedView)
            }
            Err(err) => Err(err),
        }
    }

    /// Run the MV optimization explanation pipeline. This function must be called with
    /// an `OptimizerTrace` `tracing` subscriber, using `.with_subscriber(...)`.
    /// The `root_dispatch` should be the global `tracing::Dispatch`.
    ///
    /// This should mirror the operational semantics of
    /// `Coordinator::explain_create_materialized_view_optimizer_pipeline`.
    ///
    /// WARNING, ENTERING SPOOKY ZONE 3.0
    ///
    /// Please read the docs on `explain_query_optimizer_pipeline` before changing this function.
    ///
    /// Currently this method does not need to use the global `Dispatch` like
    /// `explain_query_optimizer_pipeline`, but it is passed in case changes to this function
    /// require it.
    #[deprecated = "This is being replaced by explain_create_materialized_view_optimizer_pipeline (see #20569)."]
    #[tracing::instrument(target = "optimizer", level = "trace", name = "optimize", skip_all)]
    pub(crate) async fn explain_create_materialized_view_optimizer_pipeline_deprecated(
        &mut self,
        name: QualifiedItemName,
        raw_plan: mz_sql::plan::HirRelationExpr,
        column_names: Vec<ColumnName>,
        target_cluster_id: ClusterId,
        broken: bool,
        non_null_assertions: &Vec<usize>,
        _root_dispatch: tracing::Dispatch,
    ) -> Result<
        (
            UsedIndexes,
            Option<FastPathPlan>,
            DataflowMetainfo,
            BTreeMap<GlobalId, TransientItem>,
        ),
        AdapterError,
    > {
        use mz_repr::explain::trace_plan;

        if broken {
            tracing::warn!("EXPLAIN ... BROKEN <query> is known to leak memory, use with caution");
        }

        let full_name = self.catalog().resolve_full_name(&name, None);

        // Initialize optimizer context
        // ----------------------------

        let compute_instance = self
            .instance_snapshot(target_cluster_id)
            .expect("compute instance does not exist");
        let exported_sink_id = self.allocate_transient_id()?;
        let internal_view_id = self.allocate_transient_id()?;
        let debug_name = full_name.to_string();
        let as_of = {
            let id_bundle = self
                .index_oracle(target_cluster_id)
                .sufficient_collections(&raw_plan.depends_on());
            self.least_valid_read(&id_bundle)
        };

        // Create a transient catalog item
        // -------------------------------

        let mut transient_items = BTreeMap::new();
        transient_items.insert(exported_sink_id, {
            TransientItem::new(
                Some(full_name.to_string()),
                Some(full_name.item.to_string()),
                Some(column_names.iter().map(|c| c.to_string()).collect()),
            )
        });

        // Execute the various stages of the optimization pipeline
        // -------------------------------------------------------

        // Trace the pipeline input under `optimize/raw`.
        tracing::span!(target: "optimizer", Level::TRACE, "raw").in_scope(|| {
            trace_plan(&raw_plan);
        });

        // Execute the `optimize/hir_to_mir` stage.
        let decorrelated_plan = catch_unwind(broken, "hir_to_mir", || {
            raw_plan.optimize_and_lower(&plan::OptimizerConfig {})
        })?;

        // Execute the `optimize/local` stage.
        let optimized_plan = catch_unwind(broken, "local", || {
            let _span = tracing::span!(target: "optimizer", Level::TRACE, "local").entered();
            let optimized_plan = self.view_optimizer.optimize(decorrelated_plan)?;
            trace_plan(optimized_plan.as_inner());
            Ok::<_, AdapterError>(optimized_plan)
        })?;

        // Execute the `optimize/global` stage.
        let (mut df, df_metainfo) = catch_unwind(broken, "global", || {
            let mut df_builder = DataflowBuilder::new(self.catalog().state(), compute_instance);
            df_builder.build_materialized_view(
                exported_sink_id,
                internal_view_id,
                debug_name,
                &optimized_plan,
                &RelationDesc::new(optimized_plan.typ(), column_names),
                non_null_assertions,
            )
        })?;

        // Collect the list of indexes used by the dataflow at this point
        let used_indexes = UsedIndexes::new(
            df
                .index_imports
                .iter()
                .map(|(id, _index_import)| {
                    (*id, df_metainfo.index_usage_types.get(id).expect("prune_and_annotate_dataflow_index_imports should have been called already").clone())
                })
                .collect(),
        );

        df.set_as_of(as_of);

        // In the actual sequencing of `CREATE MATERIALIZED VIEW` statements,
        // the following DataflowDescription manipulations happen in the
        // `ship_dataflow` call. However, since we don't want to ship, we have
        // temporary duplicated the code below. This will be resolved once we
        // implement the proposal from #20569.

        // If the only outputs of the dataflow are sinks, we might be able to
        // turn off the computation early, if they all have non-trivial
        // `up_to`s.
        //
        // TODO: This should always be the case here so we can demote
        // the outer if to a soft assert.
        if df.index_exports.is_empty() {
            df.until = Antichain::from_elem(Timestamp::MIN);
            for (_, sink) in &df.sink_exports {
                df.until.join_assign(&sink.up_to);
            }
        }

        // Execute the `optimize/finalize_dataflow` stage.
        let df = catch_unwind(broken, "finalize_dataflow", || {
            self.finalize_dataflow(df, target_cluster_id)
        })?;

        // Trace the resulting plan for the top-level `optimize` path.
        trace_plan(&df);

        // Return objects that need to be passed to the `ExplainContext`
        // when rendering explanations for the various trace entries.
        Ok((used_indexes, None, df_metainfo, transient_items))
    }
}
