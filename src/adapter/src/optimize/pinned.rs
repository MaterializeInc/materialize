// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A pinned LIR plan is a durable record of a dataflow's plan;
//! we must _instantiate_ that plan with other runtime information
//! to actually run the dataflow.
//!
//! [`CatalogState`] is the [`InstantiationContext`] for
//! `PinnedDataflow::instantiate`, supplying the export descriptors
//! not recorded in the pinned plan. We look this information up in the
//! catalog items; each lookup reproduces what we would have built in the
//! first place.

use mz_catalog::memory::objects::CatalogItem;
use mz_compute_types::plan::pinned::{
    IndexInfo, InstantiationContext, MaterializedViewInfo, MetricSinkInfo,
};
use mz_expr::MirRelationExpr;
use mz_repr::{GlobalId, ReprRelationType};

use crate::catalog::CatalogState;
use crate::optimize::metric_sink::shape_metric_sink_source;

impl InstantiationContext for CatalogState {
    fn index(&self, id: GlobalId) -> Option<IndexInfo> {
        let index = self.try_get_entry_by_global_id(&id)?.index()?;
        let on_desc = self.try_get_desc_by_global_id(&index.on)?;
        Some(IndexInfo {
            on: index.on,
            keys: index.keys.to_vec(),
            typ: ReprRelationType::from(on_desc.typ()),
        })
    }

    fn materialized_view(&self, id: GlobalId) -> Option<MaterializedViewInfo> {
        let mv = self.try_get_entry_by_global_id(&id)?.materialized_view()?;
        // Only the writing collection is fed by a sink. An older version's id
        // resolves to the same item but names no sink.
        if mv.global_id_writes() != id {
            return None;
        }
        Some(MaterializedViewInfo {
            desc: mv.desc.latest(),
            non_null_assertions: mv.non_null_assertions.clone(),
            refresh_schedule: mv.refresh_schedule.clone(),
        })
    }

    fn metric_sink(&self, id: GlobalId) -> Option<MetricSinkInfo> {
        let CatalogItem::MetricSink(sink) = self.try_get_entry_by_global_id(&id)?.item() else {
            return None;
        };
        let from_desc = self.try_get_desc_by_global_id(&sink.from)?;
        // The sink reads the shaped view the optimizer builds over `from`,
        // whose description depends only on the source description and the
        // prefix. The expression the shaping also returns is not needed here.
        let source =
            MirRelationExpr::global_get(sink.from, ReprRelationType::from(from_desc.typ()));
        let (_shaped_expr, shaped_desc) =
            shape_metric_sink_source(source, &from_desc, &sink.prefix);
        Some(MetricSinkInfo {
            // A user sink passes no label to the optimizer, which then labels
            // the sink with its id.
            label: id.to_string(),
            from_desc: shaped_desc,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::Duration;

    use mz_catalog::memory::objects::CatalogItem;
    use mz_compute_types::plan::pinned::PinnedDataflow;
    use mz_controller_types::ClusterId;
    use mz_ore::metrics::MetricsRegistry;
    use mz_repr::{CatalogItemId, GlobalId};
    use mz_sql::names::{
        DatabaseId, ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, SchemaId,
        SchemaSpecifier,
    };
    use mz_sql::optimizer_metrics::OptimizerMetrics;
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
    use mz_sql::session::vars::VarInput;

    use crate::catalog::{Catalog, LocalExpressionCache, Op};
    use crate::optimize::dataflows::ComputeInstanceSnapshot;
    use crate::optimize::{self, LirDataflowDescription, Optimize, OptimizerConfig};

    /// Checks that the original dataflow rebuilds correctly in the given catalog.
    ///
    /// Returns the pinned form so a test can inspect what was stored.
    fn assert_round_trips(catalog: &Catalog, original: LirDataflowDescription) -> PinnedDataflow {
        let pinned = PinnedDataflow::try_from(original.clone()).expect("pinnable");
        let rebuilt = pinned
            .clone()
            .instantiate(catalog.state(), original.debug_name.clone())
            .expect("instantiates");
        assert_eq!(rebuilt, original);
        pinned
    }

    /// TEST FIXTURE
    /// Creates the item `create_sql` defines in `materialize.public`, owned by
    /// the system role, and returns its entry's item.
    async fn create_item(
        catalog: &mut Catalog,
        id: u64,
        name: &str,
        create_sql: &str,
    ) -> CatalogItem {
        let item_id = CatalogItemId::User(id);
        let item = catalog
            .state()
            .deserialize_item(
                GlobalId::User(id),
                create_sql,
                &BTreeMap::new(),
                &mut LocalExpressionCache::Closed,
                None,
            )
            .unwrap_or_else(|err| panic!("cannot deserialize {create_sql}: {err}"));
        let commit_ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                commit_ts,
                None,
                vec![Op::CreateItem {
                    id: item_id,
                    name: QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Id(DatabaseId::User(1)),
                            schema_spec: SchemaSpecifier::Id(SchemaId::User(3)),
                        },
                        item: name.to_string(),
                    },
                    item,
                    owner_id: MZ_SYSTEM_ROLE_ID,
                }],
            )
            .await
            .expect("transact succeeds");
        catalog.get_entry(&item_id).item().clone()
    }

    /// TEST FIXTURE
    fn cluster() -> ClusterId {
        ClusterId::user(1).expect("valid cluster id")
    }

    fn optimizer_parts(
        catalog: &Catalog,
    ) -> (ComputeInstanceSnapshot, OptimizerConfig, OptimizerMetrics) {
        (
            ComputeInstanceSnapshot::new_from_parts(cluster(), BTreeSet::new()),
            OptimizerConfig::from(catalog.system_config()),
            OptimizerMetrics::register_into(&MetricsRegistry::new(), Duration::MAX),
        )
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn index_round_trips() {
        Catalog::with_debug(|mut catalog| async move {
            let index_gid = GlobalId::User(1);
            let CatalogItem::Index(index) = create_item(
                &mut catalog,
                1,
                "i",
                "CREATE INDEX \"i\" IN CLUSTER \"quickstart\" ON \"mz_catalog\".\"mz_tables\" (\"id\")",
            )
            .await
            else {
                panic!("expected an index");
            };
            let catalog = Arc::new(catalog);

            let (compute, config, metrics) = optimizer_parts(&catalog);
            let mut optimizer = optimize::index::Optimizer::new(
                Arc::<Catalog>::clone(&catalog),
                compute,
                index_gid,
                config,
                metrics,
            );
            let name = catalog.get_entry(&CatalogItemId::User(1)).name().clone();
            let global_mir_plan = optimizer
                .optimize(optimize::index::Index::new(
                    name,
                    index.on,
                    index.keys.to_vec(),
                ))
                .expect("MIR optimization succeeds");
            let global_lir_plan = optimizer
                .optimize(global_mir_plan)
                .expect("LIR optimization succeeds");
            let (df_desc, _) = global_lir_plan.unapply();

            assert_round_trips(&catalog, df_desc);
        })
        .await
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn materialized_view_round_trips() {
        Catalog::with_debug(|mut catalog| async move {
            let CatalogItem::MaterializedView(mv) = create_item(
                &mut catalog,
                1,
                "mv",
                "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv\" \
                 IN CLUSTER \"quickstart\" WITH (ASSERT NOT NULL \"name\") \
                 AS SELECT \"id\", \"name\" FROM \"mz_catalog\".\"mz_tables\"",
            )
            .await
            else {
                panic!("expected a materialized view");
            };
            let catalog = Arc::new(catalog);

            let (compute, config, metrics) = optimizer_parts(&catalog);
            let mut optimizer = optimize::materialized_view::Optimizer::new(
                Arc::<Catalog>::clone(&catalog),
                compute,
                mv.global_id_writes(),
                GlobalId::Transient(1),
                mv.desc.latest().iter_names().cloned().collect(),
                mv.non_null_assertions.clone(),
                mv.refresh_schedule.clone(),
                "mv".to_string(),
                config,
                metrics,
            );
            let local_mir_plan = optimizer
                .optimize((*mv.raw_expr).clone())
                .expect("local MIR optimization succeeds");
            let global_mir_plan = optimizer
                .optimize(local_mir_plan)
                .expect("global MIR optimization succeeds");
            let global_lir_plan = optimizer
                .optimize(global_mir_plan)
                .expect("LIR optimization succeeds");
            let (df_desc, _) = global_lir_plan.unapply();

            assert_round_trips(&catalog, df_desc);
        })
        .await
    }

    /// A temporal filter pushed into the source read comes back from lowering
    /// as an `mz_now()` predicate, which the pinned form must carry as a bound.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn temporal_filter_materialized_view_round_trips() {
        Catalog::with_debug(|mut catalog| async move {
            let CatalogItem::MaterializedView(mv) = create_item(
                &mut catalog,
                1,
                "mv",
                "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv\" \
                 IN CLUSTER \"quickstart\" \
                 AS SELECT \"id\" FROM \"mz_catalog\".\"mz_tables\" WHERE mz_now() >= 1000",
            )
            .await
            else {
                panic!("expected a materialized view");
            };
            let catalog = Arc::new(catalog);

            let (compute, config, metrics) = optimizer_parts(&catalog);
            let mut optimizer = optimize::materialized_view::Optimizer::new(
                Arc::<Catalog>::clone(&catalog),
                compute,
                mv.global_id_writes(),
                GlobalId::Transient(1),
                mv.desc.latest().iter_names().cloned().collect(),
                mv.non_null_assertions.clone(),
                mv.refresh_schedule.clone(),
                "mv".to_string(),
                config,
                metrics,
            );
            let local_mir_plan = optimizer
                .optimize((*mv.raw_expr).clone())
                .expect("local MIR optimization succeeds");
            let global_mir_plan = optimizer
                .optimize(local_mir_plan)
                .expect("global MIR optimization succeeds");
            let global_lir_plan = optimizer
                .optimize(global_mir_plan)
                .expect("LIR optimization succeeds");
            let (df_desc, _) = global_lir_plan.unapply();

            let source = df_desc
                .source_imports
                .values()
                .next()
                .expect("reads mz_tables");
            let operators = source
                .desc
                .arguments
                .operators
                .as_ref()
                .expect("filter pushed into the source read");
            assert!(
                operators
                    .predicates
                    .iter()
                    .any(|(_, predicate)| predicate.contains_temporal()),
                "the pushed-down MFP carries the temporal filter"
            );

            let pinned = assert_round_trips(&catalog, df_desc);

            // The pinned form holds the filter as a bound of the source's
            // `MfpPlan`. `LirScalarExpr` cannot express `mz_now()`, so the
            // type alone guarantees it appears nowhere else.
            let pinned_source = pinned
                .source_imports
                .values()
                .next()
                .expect("pins the mz_tables read");
            let (_, lower_bounds, upper_bounds) = pinned_source
                .operators
                .clone()
                .expect("pins the pushed-down operators")
                .into_parts();
            assert_eq!(
                (lower_bounds.len(), upper_bounds.len()),
                (1, 0),
                "mz_now() >= 1000 is pinned as one lower bound"
            );
        })
        .await
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn metric_sink_round_trips() {
        Catalog::with_debug(|mut catalog| async move {
            catalog
                .system_config_mut()
                .set("enable_metric_sink", VarInput::Flat("on"))
                .expect("flag exists");
            create_item(
                &mut catalog,
                1,
                "shaped",
                "CREATE VIEW \"materialize\".\"public\".\"shaped\" AS \
                 SELECT 'n'::text AS metric_name, 'gauge'::text AS metric_type, \
                 NULL::map[text=>text] AS labels, NULL::double AS value, 'h'::text AS help",
            )
            .await;
            let sink_gid = GlobalId::User(2);
            let CatalogItem::MetricSink(sink) = create_item(
                &mut catalog,
                2,
                "ms",
                "CREATE METRIC SINK \"materialize\".\"public\".\"ms\" IN CLUSTER \"quickstart\" \
                 FROM \"materialize\".\"public\".\"shaped\" WITH (PREFIX = 'mz_metric_sink_ms_')",
            )
            .await
            else {
                panic!("expected a metric sink");
            };
            let catalog = Arc::new(catalog);

            let (compute, config, metrics) = optimizer_parts(&catalog);
            let mut optimizer = optimize::metric_sink::Optimizer::new(
                Arc::<Catalog>::clone(&catalog),
                compute,
                GlobalId::Transient(1),
                sink_gid,
                config,
                metrics,
            );
            let global_mir_plan = optimizer
                .optimize(optimize::metric_sink::MetricSink::new(
                    "ms".to_string(),
                    optimize::metric_sink::MetricSinkFrom::Id(sink.from),
                    sink.prefix.clone(),
                    None,
                ))
                .expect("MIR optimization succeeds");
            let global_lir_plan = optimizer
                .optimize(global_mir_plan)
                .expect("LIR optimization succeeds");
            let (df_desc, _) = global_lir_plan.unapply();

            assert_round_trips(&catalog, df_desc);
        })
        .await
    }
}
