// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use mz_catalog::memory::objects::{CatalogItem, Index, MaterializedView};
use mz_compute_types::plan::pinned::{InstantiateError, PinnedDataflow};
use mz_compute_types::plan::scalar::LirScalarExpr;
use mz_controller_types::ClusterId;
use mz_expr::MfpPlan;
use mz_ore::metrics::MetricsRegistry;
use mz_repr::global_id::TransientIdGen;
use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::names::{
    DatabaseId, ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, SchemaId,
    SchemaSpecifier,
};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::VarInput;

use super::CatalogInstantiationContext;
use crate::catalog::{Catalog, DropObjectInfo, LocalExpressionCache, Op};
use crate::optimize::dataflows::ComputeInstanceSnapshot;
use crate::optimize::{self, LirDataflowDescription, Optimize, OptimizerConfig};

/// Pins `original`, instantiates it against `catalog`, and checks that
/// the result is `original` again, and that pinning the result gives the
/// same pinned form.
///
/// The optimizers in these tests take their internal view id from
/// [`internal_view_id`], the first id of a fresh generator, and
/// instantiation renumbers from another fresh generator, so the rebuilt
/// description carries the same ids and compares equal.
///
/// Returns the pinned form for further inspection.
fn assert_round_trips(catalog: &Catalog, original: LirDataflowDescription) -> PinnedDataflow {
    let ctx = CatalogInstantiationContext {
        catalog: catalog.state(),
        transient_ids: &TransientIdGen::new(),
    };
    let pinned = PinnedDataflow::try_from(original.clone()).expect("pinnable");
    let rebuilt = pinned
        .clone()
        .instantiate(&ctx, original.debug_name.clone())
        .expect("instantiates");
    assert_eq!(
        rebuilt, original,
        "pin then instantiate is the identity for {}",
        original.debug_name
    );
    let repinned = PinnedDataflow::try_from(rebuilt).expect("pinnable");
    assert_eq!(
        repinned, pinned,
        "instantiate then pin is the identity for {}",
        original.debug_name
    );
    pinned
}

/// The internal view id an optimizer is handed, as a fresh process would
/// allocate it.
fn internal_view_id() -> GlobalId {
    TransientIdGen::new().allocate_id().1
}

/// Runs the index optimizer pipeline over the catalog item.
fn optimize_index(
    catalog: &Arc<Catalog>,
    name: QualifiedItemName,
    index: &Index,
    indexes: BTreeSet<GlobalId>,
) -> LirDataflowDescription {
    let (compute, config, metrics) = optimizer_parts(catalog, index.cluster_id, indexes);
    let mut optimizer = optimize::index::Optimizer::new(
        Arc::<Catalog>::clone(catalog),
        compute,
        index.global_id,
        config,
        metrics,
    );
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
    global_lir_plan.unapply().0
}

/// Runs the materialized view optimizer pipeline over the catalog item.
fn optimize_materialized_view(
    catalog: &Arc<Catalog>,
    mv: &MaterializedView,
    debug_name: String,
    indexes: BTreeSet<GlobalId>,
) -> LirDataflowDescription {
    let (compute, config, metrics) = optimizer_parts(catalog, mv.cluster_id, indexes);
    let mut optimizer = optimize::materialized_view::Optimizer::new(
        Arc::<Catalog>::clone(catalog),
        compute,
        mv.global_id_writes(),
        internal_view_id(),
        mv.desc.latest().iter_names().cloned().collect(),
        mv.non_null_assertions.clone(),
        mv.refresh_schedule.clone(),
        debug_name,
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
    global_lir_plan.unapply().0
}

/// The pushed-down operators of the dataflow's only source import.
fn source_operators(df: &LirDataflowDescription) -> &MfpPlan<LirScalarExpr> {
    let source = df.source_imports.values().next().expect("reads a source");
    source
        .desc
        .arguments
        .operators
        .as_ref()
        .expect("filter pushed into the source read")
}

/// TEST FIXTURE
/// Creates the item `create_sql` defines in `materialize.public`, owned by
/// the system role, and returns its entry's item.
async fn create_item(catalog: &mut Catalog, id: u64, name: &str, create_sql: &str) -> CatalogItem {
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
/// Drops the item [`create_item`] created under `id`. The catalog does not
/// check for dependents, so this leaves any plan reading it dangling.
async fn drop_item(catalog: &mut Catalog, id: u64) {
    let commit_ts = catalog.current_upper().await;
    catalog
        .transact(
            None,
            commit_ts,
            None,
            vec![Op::DropObjects(vec![DropObjectInfo::Item(
                CatalogItemId::User(id),
            )])],
        )
        .await
        .expect("transact succeeds");
}

/// TEST FIXTURE
/// The optimizer's inputs, with `indexes` the indexes `cluster` has.
fn optimizer_parts(
    catalog: &Catalog,
    cluster: ClusterId,
    indexes: BTreeSet<GlobalId>,
) -> (ComputeInstanceSnapshot, OptimizerConfig, OptimizerMetrics) {
    (
        ComputeInstanceSnapshot::new_from_parts(cluster, indexes),
        OptimizerConfig::from(catalog.system_config()),
        OptimizerMetrics::register_into(&MetricsRegistry::new(), Duration::MAX),
    )
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn index_round_trips() {
    Catalog::with_debug(|mut catalog| async move {
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
        let name = catalog.get_entry(&CatalogItemId::User(1)).name().clone();
        let df_desc = optimize_index(&catalog, name, &index, BTreeSet::new());

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
        let df_desc = optimize_materialized_view(&catalog, &mv, "mv".to_string(), BTreeSet::new());

        assert_round_trips(&catalog, df_desc);
    })
    .await
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn refresh_every_materialized_view_round_trips() {
    Catalog::with_debug(|mut catalog| async move {
        catalog
            .system_config_mut()
            .set("enable_refresh_every_mvs", VarInput::Flat("on"))
            .expect("flag exists");
        let CatalogItem::MaterializedView(mv) = create_item(
            &mut catalog,
            1,
            "mv",
            "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv\" \
             IN CLUSTER \"quickstart\" WITH (REFRESH EVERY '1 hour' ALIGNED TO 0) \
             AS SELECT \"id\" FROM \"mz_catalog\".\"mz_tables\"",
        )
        .await
        else {
            panic!("expected a materialized view");
        };
        assert!(mv.refresh_schedule.is_some(), "the item carries a schedule");
        let catalog = Arc::new(catalog);
        let df_desc = optimize_materialized_view(&catalog, &mv, "mv".to_string(), BTreeSet::new());

        assert_eq!(df_desc.refresh_schedule, mv.refresh_schedule);
        assert_eq!(
            df_desc.sink_exports[&mv.global_id_writes()].refresh_schedule,
            mv.refresh_schedule
        );

        assert_round_trips(&catalog, df_desc);
    })
    .await
}

/// A temporal filter pushed into the source read comes out of lowering as
/// a bound of the source's plan, and the pinned form must keep it.
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
        let df_desc = optimize_materialized_view(&catalog, &mv, "mv".to_string(), BTreeSet::new());
        let (_, lower_bounds, upper_bounds) = source_operators(&df_desc).clone().into_parts();
        assert_eq!(
            (lower_bounds.len(), upper_bounds.len()),
            (1, 0),
            "lowering carries mz_now() >= 1000 as one lower bound"
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

/// The MIR optimizer pushes `mz_now() <= e` into the source read next to
/// the `mz_now() < step_mz_timestamp(e)` it derives from it. Both plan to
/// the same bound, and lowering's plan must carry it once.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn inclusive_temporal_filter_materialized_view_round_trips() {
    Catalog::with_debug(|mut catalog| async move {
        create_item(
            &mut catalog,
            1,
            "events",
            "CREATE TABLE \"materialize\".\"public\".\"events\" (\"ts\" timestamp)",
        )
        .await;
        let CatalogItem::MaterializedView(mv) = create_item(
            &mut catalog,
            2,
            "mv",
            "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv\" \
             IN CLUSTER \"quickstart\" \
             AS SELECT \"ts\" FROM \"materialize\".\"public\".\"events\" \
             WHERE mz_now() <= \"ts\"",
        )
        .await
        else {
            panic!("expected a materialized view");
        };
        let catalog = Arc::new(catalog);
        let df_desc = optimize_materialized_view(&catalog, &mv, "mv".to_string(), BTreeSet::new());
        let (_, lower_bounds, upper_bounds) = source_operators(&df_desc).clone().into_parts();
        assert_eq!(
            (lower_bounds.len(), upper_bounds.len()),
            (0, 1),
            "the equivalent inclusive and exclusive spellings plan to one upper bound"
        );

        assert_round_trips(&catalog, df_desc);
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

        let (compute, config, metrics) =
            optimizer_parts(&catalog, sink.cluster_id, BTreeSet::new());
        let mut optimizer = optimize::metric_sink::Optimizer::new(
            Arc::<Catalog>::clone(&catalog),
            compute,
            internal_view_id(),
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
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn dropped_index_import_is_detected() {
    Catalog::with_debug(|mut catalog| async move {
        create_item(
            &mut catalog,
            1,
            "events",
            "CREATE TABLE \"materialize\".\"public\".\"events\" (\"a\" int)",
        )
        .await;
        let index_gid = GlobalId::User(2);
        create_item(
            &mut catalog,
            2,
            "i",
            "CREATE INDEX \"i\" IN CLUSTER \"quickstart\" \
             ON \"materialize\".\"public\".\"events\" (\"a\")",
        )
        .await;
        let CatalogItem::MaterializedView(mv) = create_item(
            &mut catalog,
            3,
            "mv",
            "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv\" \
             IN CLUSTER \"quickstart\" \
             AS SELECT \"a\" FROM \"materialize\".\"public\".\"events\"",
        )
        .await
        else {
            panic!("expected a materialized view");
        };
        let catalog = Arc::new(catalog);
        let df_desc = optimize_materialized_view(
            &catalog,
            &mv,
            "mv".to_string(),
            BTreeSet::from([index_gid]),
        );
        assert!(
            df_desc.index_imports.contains_key(&index_gid),
            "the plan reads the index"
        );
        let pinned = assert_round_trips(&catalog, df_desc);

        let mut catalog = Arc::try_unwrap(catalog).expect("the optimizer released the catalog");
        drop_item(&mut catalog, 2).await;
        let ctx = CatalogInstantiationContext {
            catalog: catalog.state(),
            transient_ids: &TransientIdGen::new(),
        };
        assert_eq!(
            pinned.instantiate(&ctx, "mv".to_string()),
            Err(InstantiateError::MissingCatalogItem {
                id: index_gid,
                kind: "index"
            })
        );
    })
    .await
}

/// A collection recreated under the same id with another type stands in
/// for a builtin whose schema migrated.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn retyped_source_import_is_detected() {
    Catalog::with_debug(|mut catalog| async move {
        let table_gid = GlobalId::User(1);
        create_item(
            &mut catalog,
            1,
            "events",
            "CREATE TABLE \"materialize\".\"public\".\"events\" (\"a\" int)",
        )
        .await;
        let CatalogItem::MaterializedView(mv) = create_item(
            &mut catalog,
            2,
            "mv",
            "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv\" \
             IN CLUSTER \"quickstart\" \
             AS SELECT \"a\" FROM \"materialize\".\"public\".\"events\"",
        )
        .await
        else {
            panic!("expected a materialized view");
        };
        let catalog = Arc::new(catalog);
        let df_desc = optimize_materialized_view(&catalog, &mv, "mv".to_string(), BTreeSet::new());
        assert!(
            df_desc.source_imports.contains_key(&table_gid),
            "the plan reads the table"
        );
        let pinned = assert_round_trips(&catalog, df_desc);

        let mut catalog = Arc::try_unwrap(catalog).expect("the optimizer released the catalog");
        drop_item(&mut catalog, 1).await;
        let ctx = CatalogInstantiationContext {
            catalog: catalog.state(),
            transient_ids: &TransientIdGen::new(),
        };
        assert_eq!(
            pinned.clone().instantiate(&ctx, "mv".to_string()),
            Err(InstantiateError::MissingCatalogItem {
                id: table_gid,
                kind: "collection"
            })
        );

        create_item(
            &mut catalog,
            1,
            "events",
            "CREATE TABLE \"materialize\".\"public\".\"events\" (\"a\" text)",
        )
        .await;
        let ctx = CatalogInstantiationContext {
            catalog: catalog.state(),
            transient_ids: &TransientIdGen::new(),
        };
        assert!(matches!(
            pinned.instantiate(&ctx, "mv".to_string()),
            Err(InstantiateError::ImportDrift { id, .. }) if id == table_gid
        ));
    })
    .await
}
/// Applying a replacement leaves the target's id resolving to the merged
/// item, but only the replacement's id still names a sink.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn applied_replacement_instantiates_only_under_its_own_id() {
    Catalog::with_debug(|mut catalog| async move {
        catalog
            .system_config_mut()
            .set(
                "enable_replacement_materialized_views",
                VarInput::Flat("on"),
            )
            .expect("flag exists");
        create_item(
            &mut catalog,
            1,
            "events",
            "CREATE TABLE \"materialize\".\"public\".\"events\" (\"a\" int)",
        )
        .await;
        let target_gid = GlobalId::User(2);
        let CatalogItem::MaterializedView(target) = create_item(
            &mut catalog,
            2,
            "t",
            "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"t\" \
             IN CLUSTER \"quickstart\" \
             AS SELECT \"a\" FROM \"materialize\".\"public\".\"events\"",
        )
        .await
        else {
            panic!("expected a materialized view");
        };
        // A replacement writes into its target's shard.
        catalog
            .register_collection_shard(target_gid)
            .await
            .expect("registers the target's shard");
        // `"a" * 1` keeps the target's schema, nullability and keys
        // included, which `CREATE REPLACEMENT` requires.
        let replacement_gid = GlobalId::User(3);
        let CatalogItem::MaterializedView(replacement) = create_item(
            &mut catalog,
            3,
            "r",
            "CREATE REPLACEMENT MATERIALIZED VIEW \"materialize\".\"public\".\"r\" \
             FOR \"materialize\".\"public\".\"t\" IN CLUSTER \"quickstart\" \
             AS SELECT \"a\" * 1 AS \"a\" FROM \"materialize\".\"public\".\"events\"",
        )
        .await
        else {
            panic!("expected a materialized view");
        };
        assert_eq!(
            replacement.desc.latest().typ(),
            target.desc.latest().typ(),
            "the replacement keeps the target's type"
        );

        let catalog = Arc::new(catalog);
        let target_df =
            optimize_materialized_view(&catalog, &target, "t".to_string(), BTreeSet::new());
        let target_pinned = assert_round_trips(&catalog, target_df);
        let replacement_df =
            optimize_materialized_view(&catalog, &replacement, "r".to_string(), BTreeSet::new());
        let replacement_pinned = assert_round_trips(&catalog, replacement_df.clone());

        let mut catalog = Arc::try_unwrap(catalog).expect("the optimizer released the catalog");
        let commit_ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                commit_ts,
                None,
                vec![Op::AlterMaterializedViewApplyReplacement {
                    id: CatalogItemId::User(2),
                    replacement_id: CatalogItemId::User(3),
                }],
            )
            .await
            .expect("transact succeeds");
        let merged = catalog
            .try_get_entry_by_global_id(&target_gid)
            .expect("the target's id still resolves")
            .materialized_view()
            .expect("to the merged materialized view");
        assert_eq!(merged.global_id_writes(), replacement_gid);

        let ctx = CatalogInstantiationContext {
            catalog: catalog.state(),
            transient_ids: &TransientIdGen::new(),
        };
        assert_eq!(
            replacement_pinned.instantiate(&ctx, "r".to_string()),
            Ok(replacement_df),
            "a plan pinned while the replacement was pending survives the apply"
        );
        assert_eq!(
            target_pinned.instantiate(&ctx, "t".to_string()),
            Err(InstantiateError::MissingCatalogItem {
                id: target_gid,
                kind: "materialized view"
            }),
            "a plan pinned for the target must not bring back its dropped writer"
        );
    })
    .await
}

/// Every builtin index and materialized view, optimized as its cluster
/// would optimize it, with the indexes that cluster has. The catalog adds a
/// builtin view's declared keys to the ones it infers, so this is where the
/// pinned sink type and the catalog's description disagree.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn builtin_items_round_trip() {
    Catalog::with_debug(|catalog| async move {
        let catalog = Arc::new(catalog);
        let mut indexes_by_cluster: BTreeMap<ClusterId, BTreeSet<GlobalId>> = BTreeMap::new();
        for entry in catalog.entries() {
            if let CatalogItem::Index(index) = entry.item() {
                indexes_by_cluster
                    .entry(index.cluster_id)
                    .or_default()
                    .insert(index.global_id);
            }
        }
        let cluster_indexes = |cluster: ClusterId| {
            indexes_by_cluster
                .get(&cluster)
                .cloned()
                .unwrap_or_default()
        };

        let (mut indexes, mut views) = (0, 0);
        for entry in catalog.entries() {
            if !entry.id().is_system() {
                continue;
            }
            let debug_name = catalog.resolve_full_name(entry.name(), None).to_string();
            match entry.item() {
                CatalogItem::Index(index) => {
                    // Bootstrap plans indexes in id order, so an index
                    // sees only the indexes created before it.
                    let mut available = cluster_indexes(index.cluster_id);
                    available.retain(|id| *id < index.global_id);
                    let df_desc = optimize_index(&catalog, entry.name().clone(), index, available);
                    assert_round_trips(&catalog, df_desc);
                    indexes += 1;
                }
                CatalogItem::MaterializedView(mv) => {
                    let df_desc = optimize_materialized_view(
                        &catalog,
                        mv,
                        debug_name,
                        cluster_indexes(mv.cluster_id),
                    );
                    assert_round_trips(&catalog, df_desc);
                    views += 1;
                }
                _ => {}
            }
        }
        assert!(
            indexes > 0 && views > 0,
            "the debug catalog has builtin indexes ({indexes}) and materialized views ({views})"
        );
    })
    .await
}
