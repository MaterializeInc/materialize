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
    CatalogCollectionEntry, CatalogEntry, CatalogItem, ContinualTask, Table, TableDataSource,
};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_compute_types::sinks::{
    ComputeSinkConnection, ContinualTaskConnection, MaterializedViewSinkConnection,
};
use mz_expr::OptimizedMirRelationExpr;
use mz_ore::collections::CollectionExt;
use mz_ore::instrument;
use mz_repr::adt::mz_acl_item::PrivilegeMap;
use mz_repr::optimize::OverrideFrom;
use mz_repr::{
    GlobalId, RelationVersion, RelationVersionSelector, Timestamp, VersionedRelationDesc,
};
use mz_sql::ast::visit_mut::{VisitMut, VisitMutNode};
use mz_sql::ast::{Raw, RawItemName};
use mz_sql::names::{FullItemName, PartialItemName, ResolvedIds};
use mz_sql::normalize::unresolved_item_name;
use mz_sql::plan;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast::Statement;
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_client::controller::{CollectionDescription, DataSource};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::OptimizerNotice;

use crate::command::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize, OptimizerCatalog};
use crate::util::ResultExt;
use crate::{ExecuteContext, catalog};

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_create_continual_task(
        &mut self,
        ctx: &mut ExecuteContext,
        plan: plan::CreateContinualTaskPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let desc = plan.desc.clone();
        let name = plan.name.clone();
        let cluster_id = plan.continual_task.cluster_id;

        // Put a placeholder in the catalog so the optimizer can find something
        // for the sink_id.
        let id_ts = self.get_catalog_write_ts().await;
        let (item_id, global_id) = self.catalog().allocate_user_id(id_ts).await?;
        let collections = [(RelationVersion::root(), global_id)].into_iter().collect();

        let entry = CatalogEntry {
            item: CatalogItem::Table(Table {
                create_sql: None,
                desc: VersionedRelationDesc::new(desc.clone()),
                collections,
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
            id: item_id,
            oid: 0,
            name: name.clone(),
            owner_id: *ctx.session().current_role_id(),
            privileges: PrivilegeMap::new(),
        };
        let bootstrap_catalog = ContinualTaskCatalogBootstrap {
            delegate: self.owned_catalog().as_optimizer_catalog(),
            sink_id: global_id,
            entry: CatalogCollectionEntry {
                entry,
                version: RelationVersionSelector::Latest,
            },
        };

        // Construct the CatalogItem for this CT and optimize it.
        let mut item = crate::continual_task::ct_item_from_plan(plan, global_id, resolved_ids)?;
        let full_name = bootstrap_catalog.resolve_full_name(&name, Some(ctx.session().conn_id()));
        let (optimized_plan, mut physical_plan, metainfo) = self.optimize_create_continual_task(
            &item,
            global_id,
            Arc::new(bootstrap_catalog),
            full_name.to_string(),
        )?;

        // Timestamp selection
        let mut id_bundle = dataflow_import_id_bundle(&physical_plan, cluster_id.clone());
        // Can't acquire a read hold on ourselves because we don't exist yet.
        //
        // It is not necessary to take a read hold on the CT output in the
        // coordinator, since the current scheme takes read holds in the
        // coordinator only to ensure inputs don't get compacted until the
        // compute controller has installed its own read holds, which happens
        // below with the `ship_dataflow` call.
        id_bundle.storage_ids.remove(&global_id);
        let read_holds = self.acquire_read_holds(&id_bundle);
        let as_of = read_holds.least_valid_read();
        physical_plan.set_as_of(as_of.clone());
        // Used in dataflow rendering to avoid the snapshot for CTs that are
        // restarted after they have committed the snapshot output.
        physical_plan.set_initial_as_of(as_of.clone());

        // Rewrite `create_sql` to reference self with the fully qualified name.
        // This is normally done when `create_sql` is created at plan time, but
        // we didn't have the necessary info in name resolution.
        item.create_sql = update_create_sql(&item.create_sql, &full_name, as_of.as_option());

        let ops = vec![catalog::Op::CreateItem {
            id: item_id,
            name: name.clone(),
            item: CatalogItem::ContinualTask(item),
            owner_id: *ctx.session().current_role_id(),
        }];

        let () = self
            .catalog_transact_with_side_effects(Some(ctx), ops, move |coord, _ctx| {
                Box::pin(async move {
                    let catalog = coord.catalog_mut();
                    catalog.set_optimized_plan(global_id, optimized_plan);
                    catalog.set_physical_plan(global_id, physical_plan.clone());
                    catalog.set_dataflow_metainfo(global_id, metainfo);

                    coord
                        .controller
                        .storage
                        .create_collections(
                            coord.catalog.state().storage_metadata(),
                            None,
                            vec![(
                                global_id,
                                CollectionDescription {
                                    desc,
                                    data_source: DataSource::Other,
                                    since: Some(as_of),
                                    status_collection_id: None,
                                    timeline: None,
                                },
                            )],
                        )
                        .await
                        .unwrap_or_terminate("cannot fail to append");

                    coord.ship_dataflow(physical_plan, cluster_id, None).await;
                    coord.allow_writes(cluster_id, vec![global_id]);
                })
            })
            .await?;
        Ok(ExecuteResponse::CreatedContinualTask)
    }

    pub fn optimize_create_continual_task(
        &self,
        ct: &ContinualTask,
        output_id: GlobalId,
        catalog: Arc<dyn OptimizerCatalog>,
        debug_name: String,
    ) -> Result<
        (
            DataflowDescription<OptimizedMirRelationExpr>,
            DataflowDescription<Plan>,
            DataflowMetainfo<Arc<OptimizerNotice>>,
        ),
        AdapterError,
    > {
        let catalog = Arc::new(NoIndexCatalog { delegate: catalog });

        let (_, view_id) = self.allocate_transient_id();
        let compute_instance = self
            .instance_snapshot(ct.cluster_id)
            .expect("compute instance does not exist");
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(ct.cluster_id).config.features());
        let non_null_assertions = Vec::new();
        let refresh_schedule = None;
        // Continual Tasks turn an "input" into diffs by inserting retractions,
        // which removes any monotonicity properties the collection otherwise
        // would have had.
        let force_non_monotonic = [ct.input_id].into();
        let mut optimizer = optimize::materialized_view::Optimizer::new(
            catalog,
            compute_instance,
            output_id,
            view_id,
            ct.desc.iter_names().cloned().collect(),
            non_null_assertions,
            refresh_schedule,
            debug_name,
            optimizer_config,
            self.optimizer_metrics(),
            force_non_monotonic,
        );

        // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local and global)
        let local_mir_plan = optimizer.catch_unwind_optimize((*ct.raw_expr).clone())?;
        let global_mir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;
        let optimized_plan = global_mir_plan.df_desc().clone();
        // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
        let global_lir_plan = optimizer.catch_unwind_optimize(global_mir_plan)?;
        let (mut physical_plan, metainfo) = global_lir_plan.unapply();

        // The MV optimizer is hardcoded to output a PersistSinkConnection.
        // Sniff it and swap for the ContinualTaskSink. If/when we split out a
        // Continual Task optimizer, this won't be necessary, and in the
        // meantime, it seems undesirable to burden the MV optimizer with a
        // configuration for this.
        for sink in physical_plan.sink_exports.values_mut() {
            match &mut sink.connection {
                ComputeSinkConnection::MaterializedView(MaterializedViewSinkConnection {
                    storage_metadata,
                    ..
                }) => {
                    sink.with_snapshot = ct.with_snapshot;
                    sink.connection =
                        ComputeSinkConnection::ContinualTask(ContinualTaskConnection {
                            input_id: ct.input_id,
                            storage_metadata: *storage_metadata,
                        })
                }
                _ => unreachable!("MV should produce persist sink connection"),
            }
        }

        // Create a metainfo with rendered notices, preallocating a transient
        // GlobalId for each.
        let notice_ids = std::iter::repeat_with(|| self.allocate_transient_id())
            .map(|(_item_id, global_id)| global_id)
            .take(metainfo.optimizer_notices.len())
            .collect();
        let metainfo = self
            .catalog()
            .render_notices(metainfo, notice_ids, Some(output_id));

        Ok((optimized_plan, physical_plan, metainfo))
    }
}

/// An implementation of [OptimizerCatalog] with a placeholder for the continual
/// task to solve the self-referential CT bootstrapping problem.
#[derive(Debug)]
struct ContinualTaskCatalogBootstrap {
    delegate: Arc<dyn OptimizerCatalog>,
    sink_id: GlobalId,
    entry: CatalogCollectionEntry,
}

impl OptimizerCatalog for ContinualTaskCatalogBootstrap {
    fn get_entry(&self, id: &GlobalId) -> CatalogCollectionEntry {
        if self.sink_id == *id {
            return self.entry.clone();
        }
        self.delegate.get_entry(id)
    }

    fn get_entry_by_item_id(&self, id: &mz_repr::CatalogItemId) -> &CatalogEntry {
        self.delegate.get_entry_by_item_id(id)
    }

    fn resolve_full_name(
        &self,
        name: &mz_sql::names::QualifiedItemName,
        conn_id: Option<&mz_adapter_types::connection::ConnectionId>,
    ) -> mz_sql::names::FullItemName {
        self.delegate.resolve_full_name(name, conn_id)
    }

    fn get_indexes_on(
        &self,
        id: GlobalId,
        cluster: mz_controller_types::ClusterId,
    ) -> Box<dyn Iterator<Item = (GlobalId, &mz_catalog::memory::objects::Index)> + '_> {
        self.delegate.get_indexes_on(id, cluster)
    }
}

fn update_create_sql(
    create_sql: &str,
    ct_name: &FullItemName,
    as_of: Option<&Timestamp>,
) -> String {
    struct ReplaceName(PartialItemName);
    impl<'ast> VisitMut<'ast, Raw> for ReplaceName {
        fn visit_item_name_mut(&mut self, node: &'ast mut RawItemName) {
            let Ok(name) = unresolved_item_name(node.name().clone()) else {
                return;
            };
            if name.matches(&self.0) {
                *(node.name_mut()) = self.0.clone().into();
            }
        }
    }

    let mut ast = mz_sql_parser::parser::parse_statements(create_sql)
        .expect("non-system items must be parseable")
        .into_element()
        .ast;
    match &mut ast {
        Statement::CreateContinualTask(stmt) => {
            // Replace any self-references in the statements with the full name,
            // now that we have it.
            stmt.visit_mut(&mut ReplaceName(PartialItemName::from(ct_name.clone())));
            // Also fill in the initial as_of.
            if let Some(as_of) = as_of {
                stmt.as_of = Some(as_of.into());
            }
        }
        _ => unreachable!("should be CREATE CONTINUAL TASK statement"),
    }
    ast.to_ast_string_stable()
}

/// An [OptimizerCatalog] impl that ignores any indexes that exist.
///
/// TODO(ct3): At the moment, the dataflow rendering for CTs only knows how to
/// turn persist_sources into CT inputs. If the optimizer decides to use an
/// existing index in the cluster, it won't work. It seems tricky/invasive to
/// fix the render bug, so for now pretend indexes don't exist for CTs. Remove
/// this once we fix the bug.
#[derive(Debug)]
struct NoIndexCatalog {
    delegate: Arc<dyn OptimizerCatalog>,
}

impl OptimizerCatalog for NoIndexCatalog {
    fn get_entry(&self, id: &GlobalId) -> CatalogCollectionEntry {
        self.delegate.get_entry(id)
    }

    fn get_entry_by_item_id(&self, id: &mz_repr::CatalogItemId) -> &CatalogEntry {
        self.delegate.get_entry_by_item_id(id)
    }

    fn resolve_full_name(
        &self,
        name: &mz_sql::names::QualifiedItemName,
        conn_id: Option<&mz_adapter_types::connection::ConnectionId>,
    ) -> mz_sql::names::FullItemName {
        self.delegate.resolve_full_name(name, conn_id)
    }

    fn get_indexes_on(
        &self,
        _id: GlobalId,
        _cluster: mz_controller_types::ClusterId,
    ) -> Box<dyn Iterator<Item = (GlobalId, &mz_catalog::memory::objects::Index)> + '_> {
        Box::new(std::iter::empty())
    }
}
