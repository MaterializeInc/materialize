// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use mz_catalog::memory::objects::{
    CatalogEntry, CatalogItem, MaterializedView, Table, TableDataSource,
};
use mz_compute_types::sinks::{
    ComputeSinkConnection, ContinualTaskConnection, PersistSinkConnection,
};
use mz_expr::visit::Visit;
use mz_expr::{Id, LocalId};
use mz_ore::instrument;
use mz_repr::adt::mz_acl_item::PrivilegeMap;
use mz_repr::optimize::OverrideFrom;
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{self, HirRelationExpr};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_client::controller::{CollectionDescription, DataSource, DataSourceOther};

use crate::catalog;
use crate::command::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::util::ResultExt;

// TODO(ct): Big oof. Dedup a bunch of this with MVs.
impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_create_continual_task(
        &mut self,
        session: &Session,
        plan: plan::CreateContinualTaskPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::CreateContinualTaskPlan {
            name,
            desc,
            input_id,
            continual_task:
                plan::MaterializedView {
                    create_sql,
                    cluster_id,
                    mut expr,
                    column_names,
                    non_null_assertions,
                    compaction_window: _,
                    refresh_schedule,
                    as_of: _,
                },
        } = plan;

        // Collect optimizer parameters.
        let compute_instance = self
            .instance_snapshot(cluster_id)
            .expect("compute instance does not exist");

        let debug_name = self.catalog().resolve_full_name(&name, None).to_string();
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(cluster_id).config.features());

        let view_id = self.allocate_transient_id();
        let catalog_mut = self.catalog_mut();
        let sink_id = catalog_mut.allocate_user_id().await?;

        // Put a placeholder in the catalog so the optimizer can find something
        // for the sink_id.
        let fake_entry = CatalogEntry {
            item: CatalogItem::Table(Table {
                create_sql: Some(create_sql.clone()),
                desc: desc.clone(),
                conn_id: None,
                resolved_ids: resolved_ids.clone(),
                custom_logical_compaction_window: None,
                is_retained_metrics_object: false,
                data_source: TableDataSource::TableWrites {
                    defaults: Vec::new(),
                },
            }),
            referenced_by: Vec::new(),
            used_by: Vec::new(),
            id: sink_id,
            oid: 0,
            name: name.clone(),
            owner_id: *session.current_role_id(),
            privileges: PrivilegeMap::new(),
        };
        catalog_mut.hack_add_ct(sink_id, fake_entry);

        // Build an optimizer for this CONTINUAL TASK.
        let mut optimizer = optimize::materialized_view::Optimizer::new(
            Arc::new(catalog_mut.clone()),
            compute_instance,
            sink_id,
            view_id,
            column_names,
            non_null_assertions,
            refresh_schedule.clone(),
            debug_name,
            optimizer_config,
            self.optimizer_metrics(),
        );

        // Replace our placeholder fake ctes with the real output id, now that
        // we have it.
        expr.visit_mut_post(&mut |expr| match expr {
            HirRelationExpr::Get { id, .. } if *id == Id::Local(LocalId::new(0)) => {
                *id = Id::Global(sink_id);
            }
            _ => {}
        })?;

        // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local and global)
        let raw_expr = expr.clone();
        let local_mir_plan = optimizer.catch_unwind_optimize(expr)?;
        let global_mir_plan = optimizer.catch_unwind_optimize(local_mir_plan.clone())?;
        // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
        let global_lir_plan = optimizer.catch_unwind_optimize(global_mir_plan.clone())?;
        let (mut df_desc, _df_meta) = global_lir_plan.unapply();

        // Timestamp selection
        let mut id_bundle = dataflow_import_id_bundle(&df_desc, cluster_id.clone());
        // Can't acquire a read hold on ourselves because we don't exist yet.
        //
        // It is not necessary to take a read hold on the CT output in the
        // coordinator, since the current scheme takes read holds in the
        // coordinator only to ensure inputs don't get compacted until the
        // compute controller has installed its own read holds, which happens
        // below with the `ship_dataflow` call.
        id_bundle.storage_ids.remove(&sink_id);
        let read_holds = self.acquire_read_holds(&id_bundle);
        let as_of = read_holds.least_valid_read();
        df_desc.set_as_of(as_of.clone());

        // TODO(ct): HACKs
        for sink in df_desc.sink_exports.values_mut() {
            match &mut sink.connection {
                ComputeSinkConnection::Persist(PersistSinkConnection {
                    storage_metadata, ..
                }) => {
                    sink.connection =
                        ComputeSinkConnection::ContinualTask(ContinualTaskConnection {
                            input_id,
                            storage_metadata: *storage_metadata,
                        })
                }
                _ => unreachable!("MV should produce persist sink connection"),
            }
        }

        let ops = vec![catalog::Op::CreateItem {
            id: sink_id,
            name: name.clone(),
            item: CatalogItem::MaterializedView(MaterializedView {
                // TODO(ct): This doesn't give the `DELETE FROM` / `INSERT INTO`
                // names the `[u1 AS "materialize"."public"."append_only"]`
                // style expansion. Bug?
                create_sql,
                raw_expr: Arc::new(raw_expr),
                optimized_expr: Arc::new(local_mir_plan.expr()),
                desc: desc.clone(),
                resolved_ids,
                cluster_id,
                non_null_assertions: Vec::new(),
                custom_logical_compaction_window: None,
                refresh_schedule,
                initial_as_of: Some(as_of.clone()),
            }),
            owner_id: *session.current_role_id(),
        }];

        let () = self
            .catalog_transact_with_side_effects(Some(session), ops, |coord| async {
                coord
                    .controller
                    .storage
                    .create_collections(
                        coord.catalog.state().storage_metadata(),
                        None,
                        vec![(
                            sink_id,
                            CollectionDescription {
                                desc,
                                data_source: DataSource::Other(DataSourceOther::Compute),
                                since: Some(as_of),
                                status_collection_id: None,
                            },
                        )],
                    )
                    .await
                    .unwrap_or_terminate("cannot fail to append");

                coord.ship_dataflow(df_desc, cluster_id.clone(), None).await;
            })
            .await?;
        Ok(ExecuteResponse::CreatedContinualTask)
    }
}
