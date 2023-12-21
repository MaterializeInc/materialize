// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_catalog::memory::objects::CatalogItem;
use mz_sql::catalog::CatalogError;
use mz_sql::names::ResolvedIds;
use mz_sql::plan;

use crate::command::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::{catalog, AdapterNotice, TimestampProvider};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_create_index_off_thread(
        &mut self,
        session: &mut Session,
        plan: plan::CreateIndexPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        ::tracing::info!("sequence_create_index_off_thread");

        let plan::CreateIndexPlan {
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
        } = plan;

        self.ensure_cluster_can_host_compute_item(&name, cluster_id)?;

        // Collect optimizer parameters.
        let compute_instance = self
            .instance_snapshot(cluster_id)
            .expect("compute instance does not exist");
        let id = self.catalog_mut().allocate_user_id().await?;
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this INDEX.
        let mut optimizer = optimize::index::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            id,
            optimizer_config,
        );

        // MIR ⇒ MIR optimization (global)
        let index_plan = optimize::index::Index::new(&name, &on, &keys);
        let global_mir_plan = optimizer.optimize(index_plan)?;
        // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
        let global_lir_plan = optimizer.optimize(global_mir_plan.clone())?;

        let index = mz_catalog::memory::objects::Index {
            create_sql,
            keys,
            on,
            conn_id: None,
            resolved_ids,
            cluster_id,
            is_retained_metrics_object: false,
            custom_logical_compaction_window: None,
        };

        let oid = self.catalog_mut().allocate_oid()?;
        let on = self.catalog().get_entry(&index.on);
        // Indexes have the same owner as their parent relation.
        let owner_id = *on.owner_id();
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name: name.clone(),
            item: CatalogItem::Index(index),
            owner_id,
        };

        let transact_result = self
            .catalog_transact_with_side_effects(Some(session), vec![op], |coord| async {
                // Save plan structures.
                coord
                    .catalog_mut()
                    .set_optimized_plan(id, global_mir_plan.df_desc().clone());
                coord
                    .catalog_mut()
                    .set_physical_plan(id, global_lir_plan.df_desc().clone());

                let (mut df_desc, df_meta) = global_lir_plan.unapply();

                // Timestamp selection
                let id_bundle = dataflow_import_id_bundle(&df_desc, cluster_id);
                let since = coord.least_valid_read(&id_bundle);
                df_desc.set_as_of(since);

                // Emit notices.
                coord.emit_optimizer_notices(session, &df_meta.optimizer_notices);

                // Notices rendering
                let df_meta = coord.catalog().render_notices(df_meta, Some(id));
                coord
                    .catalog_mut()
                    .set_dataflow_metainfo(id, df_meta.clone());

                if coord.catalog().state().system_config().enable_mz_notices() {
                    // Initialize a container for builtin table updates.
                    let mut builtin_table_updates =
                        Vec::with_capacity(df_meta.optimizer_notices.len());
                    // Collect optimization hint updates.
                    coord.catalog().pack_optimizer_notices(
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

                coord.set_index_options(id, options).expect("index enabled");
            })
            .await;

        match transact_result {
            Ok(_) => Ok(ExecuteResponse::CreatedIndex),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: name.item,
                    ty: "index",
                });
                Ok(ExecuteResponse::CreatedIndex)
            }
            Err(err) => Err(err),
        }
    }
}
