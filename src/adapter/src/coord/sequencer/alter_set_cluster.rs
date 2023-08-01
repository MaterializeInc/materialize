// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::iter::once;

use mz_ore::collections::CollectionExt;
use mz_repr::RelationDesc;
use mz_sql::catalog::{CatalogItem as SqlCatalogItem, ObjectType};
use mz_sql::plan::{
    AlterSetClusterPlan, CreateMaterializedViewPlan, OptimizerConfig, Params, Plan,
};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{IfExistsBehavior, RawClusterName, Statement};
use mz_transform::Optimizer;

use crate::catalog::CatalogItem;
use crate::coord::Coordinator;
use crate::session::Session;
use crate::util::ResultExt;
use crate::{catalog, AdapterError, ExecuteResponse};

impl Coordinator {
    /// Convert a [`AlterSetClusterPlan`] to a sequence of catalog operators and adjust state.
    pub(super) async fn sequence_alter_set_cluster(
        &mut self,
        session: &Session,
        AlterSetClusterPlan { id, set_cluster }: AlterSetClusterPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let entry = self.catalog().get_entry(&id);

        let system_catalog = self.catalog().for_system_session();
        let debug_name = self
            .catalog
            .resolve_full_name(entry.name(), None)
            .to_string();

        // Since the catalog serializes the items using only their creation statement
        // and context, we need to parse and rewrite the cluster option in that statement.
        // (And then make any other changes to the object definition to match.)
        let mut stmt = mz_sql::parse::parse(entry.create_sql())
            .expect("invalid create sql persisted to catalog")
            .into_element()
            .ast;

        // Patch AST.
        let stmt_in_cluster = match &mut stmt {
            Statement::CreateIndex(s) => &mut s.in_cluster,
            Statement::CreateMaterializedView(s) => &mut s.in_cluster,
            Statement::CreateSink(s) => &mut s.in_cluster,
            Statement::CreateSource(s) => &mut s.in_cluster,
            // Planner produced wrong plan.
            _ => coord_bail!("object {id} does not have an associated cluster"),
        };
        *stmt_in_cluster = Some(RawClusterName::Resolved(set_cluster.to_string()));

        // Update catalog item with new cluster.
        let create_sql = stmt.to_ast_string_stable();
        let stmt = mz_sql::parse::parse(&create_sql)?.into_element().ast;
        let (mut stmt, resolved_ids) = mz_sql::names::resolve(&system_catalog, stmt)?;

        match &mut stmt {
            Statement::CreateMaterializedView(s) => {
                // Planning for materialized views fails if the object already exists.
                // Force to skip behavior, which is OK because it still lowers the query.
                // We need to be careful to not save the new behavior in the catalog. Planning
                // changes this to Error.
                s.if_exists = IfExistsBehavior::Skip;
            }
            _ => {}
        }

        let plan =
            mz_sql::plan::plan(None, &system_catalog, stmt, &Params::empty(), &resolved_ids)?;

        match entry.item() {
            CatalogItem::Index(_old_index) => Err(AdapterError::Unsupported("ALTER SET CLUSTER")),
            CatalogItem::MaterializedView(old_mv) => {
                if !self.is_compute_cluster(set_cluster) {
                    let cluster_name = self.catalog().get_cluster(set_cluster).name.clone();
                    return Err(AdapterError::BadItemInStorageCluster { cluster_name });
                }

                let old_cluster_id = old_mv.cluster_id;

                let (optimized_expr, desc, create_sql) = match plan {
                    Plan::CreateMaterializedView(CreateMaterializedViewPlan {
                        materialized_view: mv,
                        ..
                    }) => {
                        let optimizer =
                            Optimizer::logical_optimizer(&mz_transform::typecheck::empty_context());
                        let decorrelated_expr = mv.expr.optimize_and_lower(&OptimizerConfig {})?;
                        let optimized_expr = optimizer.optimize(decorrelated_expr)?;
                        let desc = RelationDesc::new(optimized_expr.typ(), mv.column_names);
                        assert_eq!(mv.cluster_id, set_cluster);

                        (optimized_expr, desc, mv.create_sql)
                    }
                    _ => {
                        return Err(catalog::Error::new(catalog::ErrorKind::Corruption {
                            detail: "catalog entry generated inappropriate plan".to_string(),
                        })
                        .into())
                    }
                };

                let item = CatalogItem::MaterializedView(catalog::MaterializedView {
                    create_sql,
                    optimized_expr,
                    desc,
                    resolved_ids,
                    cluster_id: set_cluster,
                });

                // Allocate a unique ID that can be used by the dataflow builder to
                // connect the view dataflow to the storage sink.
                let internal_view_id = self.allocate_transient_id()?;
                let op = catalog::Op::AlterSetCluster { id, item };
                // Update catalog
                let (mut dataflow, df_metainfo) = self
                    .catalog_transact_with(Some(session.conn_id()), vec![op], |txn| {
                        // Create a dataflow that materializes the view query and sinks
                        // it to storage.
                        let CatalogItem::MaterializedView(mv) = txn.catalog.get_entry(&id).item()
                        else {
                            unreachable!()
                        };

                        let mut builder = txn.dataflow_builder(set_cluster);
                        builder.build_materialized_view(
                            id,
                            internal_view_id,
                            debug_name,
                            &mv.optimized_expr,
                            &mv.desc,
                        )
                    })
                    .await?;
                self.emit_optimizer_notices(session, &df_metainfo.optimizer_notices);

                self.catalog_mut().set_optimized_plan(id, dataflow.clone());
                self.catalog_mut().set_dataflow_metainfo(id, df_metainfo);

                // Pick the least valid read timestamp as the as-of for the view
                // dataflow. This makes the materialized view include the maximum possible
                // amount of historical detail.
                let as_of = self.bootstrap_materialized_view_as_of(&dataflow, set_cluster);
                dataflow.set_as_of(as_of);

                self.migrate_compute_ids_in_timeline(once((old_cluster_id, id)), set_cluster);

                // Send the dataflow to the new cluster.
                let dataflow_plan = self.must_finalize_dataflow(dataflow, set_cluster);
                self.controller
                    .active_compute()
                    .create_dataflow(set_cluster, dataflow_plan.clone())
                    .unwrap_or_terminate("cannot fail to create dataflows");
                self.catalog_mut().set_physical_plan(id, dataflow_plan);

                self.force_drop_collections(vec![id], old_cluster_id);

                Ok(ExecuteResponse::AlteredObject(ObjectType::MaterializedView))
            }
            CatalogItem::Sink(_old_sink) => Err(AdapterError::Unsupported("ALTER SET CLUSTER")),
            CatalogItem::Source(old_source) => {
                match &old_source.data_source {
                    catalog::DataSourceDesc::Ingestion(_ingestion) => {
                        Err(AdapterError::Unsupported("ALTER SET CLUSTER"))
                    }
                    catalog::DataSourceDesc::Source
                    | catalog::DataSourceDesc::Introspection(_)
                    | catalog::DataSourceDesc::Progress
                    | catalog::DataSourceDesc::Webhook { .. } => {
                        // Planner produced wrong plan.
                        coord_bail!("source {id} does not have an associated cluster");
                    }
                }
            }
            // Planner produced wrong plan.
            _ => coord_bail!("object {id} does not have an associated cluster"),
        }
    }
}
