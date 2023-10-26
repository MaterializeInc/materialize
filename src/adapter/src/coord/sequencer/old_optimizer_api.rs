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
use mz_compute_types::dataflows::{DataflowDesc, IndexDesc};
use mz_ore::soft_panic_or_log;
use mz_repr::explain::{TransientItem, UsedIndexes};
use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::CatalogError;
use mz_sql::names::{QualifiedItemName, ResolvedIds};
use mz_sql::plan::{self, Index};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::optimizer_notices::OptimizerNotice;
use timely::progress::Antichain;

use crate::catalog::CatalogItem;
use crate::coord::dataflows::{
    prep_relation_expr, prep_scalar_expr, DataflowBuilder, ExprPrepStyle,
};
use crate::coord::peek::FastPathPlan;
use crate::coord::sequencer::inner::catch_unwind;
use crate::coord::Coordinator;
use crate::session::Session;
use crate::{catalog, AdapterError, AdapterNotice, ExecuteResponse};

impl Coordinator {
    // Indexes
    // -----------------

    /// This should mirror the operational semantics of
    /// `Coordinator::sequence_create_index`.
    #[deprecated = "This is being replaced by sequence_create_index (see #20569)."]
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_index_deprecated(
        &mut self,
        session: &mut Session,
        plan: plan::CreateIndexPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::CreateIndexPlan {
            name,
            index,
            options,
            if_not_exists,
        } = plan;

        // An index must be created on a specific cluster.
        let cluster_id = index.cluster_id;

        self.ensure_cluster_can_host_compute_item(&name, cluster_id)?;

        let empty_key = index.keys.is_empty();

        let id = self.catalog_mut().allocate_user_id().await?;
        let index = catalog::Index {
            create_sql: index.create_sql,
            keys: index.keys,
            on: index.on,
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
        match self
            .catalog_transact_with(Some(session.conn_id()), vec![op], |txn| {
                let mut builder = txn.dataflow_builder(cluster_id);
                let (df, df_metainfo) = builder.build_index_dataflow(id)?;
                Ok((df, df_metainfo))
            })
            .await
        {
            Ok((df, mut df_metainfo)) => {
                if empty_key {
                    df_metainfo.push_optimizer_notice_dedup(OptimizerNotice::IndexKeyEmpty);
                }

                self.emit_optimizer_notices(session, &df_metainfo.optimizer_notices);

                self.catalog_mut().set_optimized_plan(id, df.clone());
                self.catalog_mut().set_dataflow_metainfo(id, df_metainfo);

                let df = self.must_ship_dataflow(df, cluster_id).await;
                self.catalog_mut().set_physical_plan(id, df);

                self.set_index_options(id, options).expect("index enabled");
                Ok(ExecuteResponse::CreatedIndex)
            }
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
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

    /// Run the index optimization explanation pipeline. This function must be called with
    /// an `OptimizerTrace` `tracing` subscriber, using `.with_subscriber(...)`.
    /// The `root_dispatch` should be the global `tracing::Dispatch`.
    ///
    /// This should mirror the operational semantics of
    /// `Coordinator::explain_create_index_optimizer_pipeline`.
    //
    // WARNING, ENTERING SPOOKY ZONE 3.0
    //
    // Please read the docs on `explain_query_optimizer_pipeline` before changing this function.
    //
    // Currently this method does not need to use the global `Dispatch` like
    // `explain_query_optimizer_pipeline`, but it is passed in case changes to this function
    // require it.
    #[deprecated = "This is being replaced by explain_create_index_optimizer_pipeline (see #20569)."]
    #[tracing::instrument(target = "optimizer", level = "trace", name = "optimize", skip_all)]
    pub(crate) async fn explain_create_index_optimizer_pipeline_deprecated(
        &mut self,
        name: QualifiedItemName,
        index: Index,
        broken: bool,
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

        // Initialize helper context
        // -------------------------

        let target_cluster_id = index.cluster_id;
        let compute_instance = self
            .instance_snapshot(target_cluster_id)
            .expect("compute instance does not exist");
        let exported_index_id = self.allocate_transient_id()?;
        let state = self.catalog().state();
        let on_entry = self.catalog.get_entry(&index.on);
        let full_name = self.catalog.resolve_full_name(&name, on_entry.conn_id());
        let on_desc = on_entry
            .desc(&full_name)
            .expect("can only create indexes on items with a valid description");

        // Create a transient catalog item
        // -------------------------------

        let mut transient_items = BTreeMap::new();
        transient_items.insert(exported_index_id, {
            TransientItem::new(
                Some(full_name.to_string()),
                Some(full_name.item.to_string()),
                Some(on_desc.iter_names().map(|c| c.to_string()).collect()),
            )
        });

        // Global optimization
        // -------------------
        let (mut df, df_metainfo) = catch_unwind(broken, "global", || {
            // This code should mirror the sequence of steps performed in
            // `build_index_dataflow`. We cannot call `build_index_dataflow`
            // here directly because it assumes that the index is present in the
            // catalog as an `IndexEntry`. However, in the condtext of `EXPLAIN
            // CREATE` we don't want to do actually modify the catalog state.

            let mut df_builder = DataflowBuilder::new(self.catalog().state(), compute_instance);
            let mut df = DataflowDesc::new(full_name.to_string());

            df_builder.import_into_dataflow(&index.on, &mut df)?;

            for desc in df.objects_to_build.iter_mut() {
                prep_relation_expr(state, &mut desc.plan, ExprPrepStyle::Index)?;
            }

            let mut index_description = IndexDesc {
                on_id: index.on,
                key: index.keys.clone(),
            };

            for key in index_description.key.iter_mut() {
                prep_scalar_expr(state, key, ExprPrepStyle::Index)?;
            }

            df.export_index(exported_index_id, index_description, on_desc.typ().clone());

            // Optimize the dataflow across views, and any other ways that appeal.
            let df_metainfo = mz_transform::optimize_dataflow(
                &mut df,
                &df_builder,
                &mz_transform::EmptyStatisticsOracle,
            )?;

            Ok::<_, AdapterError>((df, df_metainfo))
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

        // Finalization
        // ------------

        // In the actual sequencing of `CREATE INDEX` statements, the following
        // DataflowDescription manipulations happen in the `ship_dataflow` call.
        // However, since we don't want to ship, we have temporary duplicated
        // the code below. This will be resolved once we implement the proposal
        // from #20569.

        // If the only outputs of the dataflow are sinks, we might be able to
        // turn off the computation early, if they all have non-trivial
        // `up_to`s.
        //
        // TODO: This should never be the case here so we can probably remove
        // the entire thing.
        if df.index_exports.is_empty() {
            soft_panic_or_log!("unexpectedly setting df.until for an index");
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
