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

use mz_catalog::builtin::BUILTINS;
use mz_catalog::memory::objects::{CatalogItem, Index, MaterializedView};
use mz_compute_types::plan::join::JoinPlan;
use mz_compute_types::plan::pinned::{
    InlinedBuiltin, InlinedBuiltinKind, InstantiateError, PinError, PinnedDataflow, PinningContext,
};
use mz_compute_types::plan::reduce::ReducePlan;
use mz_compute_types::plan::scalar::LirScalarExpr;
use mz_compute_types::plan::top_k::TopKPlan;
use mz_compute_types::plan::{LirRelationExpr, LirRelationNode};
use mz_controller_types::ClusterId;
use mz_expr::MfpPlan;
use mz_ore::metrics::MetricsRegistry;
use mz_repr::global_id::TransientIdGen;
use mz_repr::namespaces::UNSTABLE_SCHEMAS;
use mz_repr::{CatalogItemId, GlobalId, SqlRelationType, SqlScalarType};
use mz_sql::catalog::SessionCatalog;
use mz_sql::func::{Func, FuncImpl};
use mz_sql::names::{
    DatabaseId, ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, SchemaId,
    SchemaSpecifier,
};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::{
    CoercibleScalarExpr, ExprContext, HirScalarExpr, PlanContext, QueryContext, QueryLifetime,
    Scope, StatementContext,
};
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
    let pinned = PinnedDataflow::pin(original.clone(), catalog.state()).expect("pinnable");
    let rebuilt = pinned
        .clone()
        .instantiate(&ctx, original.debug_name.clone())
        .expect("instantiates");
    assert_eq!(
        rebuilt, original,
        "pin then instantiate is the identity for {}",
        original.debug_name
    );
    let repinned = PinnedDataflow::pin(rebuilt, catalog.state()).expect("pinnable");
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
/// Creates `materialize.public.tables (id text, name text)` under id 1.
async fn create_tables(catalog: &mut Catalog) {
    create_item(
        catalog,
        1,
        "tables",
        "CREATE TABLE \"materialize\".\"public\".\"tables\" (\"id\" text, \"name\" text)",
    )
    .await;
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
            vec![Op::DropObjects {
                objects: vec![DropObjectInfo::Item(CatalogItemId::User(id))],
                cascade: false,
            }],
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
        create_tables(&mut catalog).await;
        let CatalogItem::Index(index) = create_item(
            &mut catalog,
            2,
            "i",
            "CREATE INDEX \"i\" IN CLUSTER \"quickstart\" \
             ON \"materialize\".\"public\".\"tables\" (\"id\")",
        )
        .await
        else {
            panic!("expected an index");
        };
        let catalog = Arc::new(catalog);
        let name = catalog.get_entry(&CatalogItemId::User(2)).name().clone();
        let df_desc = optimize_index(&catalog, name, &index, BTreeSet::new());

        assert_round_trips(&catalog, df_desc);
    })
    .await
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn materialized_view_round_trips() {
    Catalog::with_debug(|mut catalog| async move {
        create_tables(&mut catalog).await;
        let CatalogItem::MaterializedView(mv) = create_item(
            &mut catalog,
            2,
            "mv",
            "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv\" \
             IN CLUSTER \"quickstart\" WITH (ASSERT NOT NULL \"name\") \
             AS SELECT \"id\", \"name\" FROM \"materialize\".\"public\".\"tables\"",
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
        create_tables(&mut catalog).await;
        let CatalogItem::MaterializedView(mv) = create_item(
            &mut catalog,
            2,
            "mv",
            "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv\" \
             IN CLUSTER \"quickstart\" WITH (REFRESH EVERY '1 hour' ALIGNED TO 0) \
             AS SELECT \"id\" FROM \"materialize\".\"public\".\"tables\"",
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
        create_tables(&mut catalog).await;
        let CatalogItem::MaterializedView(mv) = create_item(
            &mut catalog,
            2,
            "mv",
            "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv\" \
             IN CLUSTER \"quickstart\" \
             AS SELECT \"id\" FROM \"materialize\".\"public\".\"tables\" WHERE mz_now() >= 1000",
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
            .expect("pins the tables read");
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
/// for a materialized view whose inferred type changed between releases.
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
/// would optimize it, with the indexes that cluster has.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn builtin_items_refuse_to_pin() {
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

        let assert_refused = |df_desc: LirDataflowDescription, name: &str| {
            assert!(
                matches!(
                    PinnedDataflow::pin(df_desc, catalog.state()),
                    Err(PinError::BuiltinCollection(_))
                ),
                "{name} refuses to pin"
            );
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
                    assert_refused(df_desc, &debug_name);
                    indexes += 1;
                }
                CatalogItem::MaterializedView(mv) => {
                    let df_desc = optimize_materialized_view(
                        &catalog,
                        mv,
                        debug_name.clone(),
                        cluster_indexes(mv.cluster_id),
                    );
                    assert_refused(df_desc, &debug_name);
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

/// Labels every LIR operator in the dataflow's builds, with the join,
/// reduce, and top-k strategy the plan chose.
fn operators(df: &LirDataflowDescription) -> BTreeSet<&'static str> {
    let mut seen = BTreeSet::new();
    let mut todo: Vec<&LirRelationExpr> = df.objects_to_build.iter().map(|b| &b.plan).collect();
    while let Some(expr) = todo.pop() {
        seen.insert(match &expr.node {
            LirRelationNode::Constant { .. } => "Constant",
            LirRelationNode::Get { .. } => "Get",
            LirRelationNode::Let { .. } => "Let",
            LirRelationNode::LetRec { .. } => "LetRec",
            LirRelationNode::Mfp { .. } => "Mfp",
            LirRelationNode::FlatMap { .. } => "FlatMap",
            LirRelationNode::Join { plan, .. } => match plan {
                JoinPlan::Linear(_) => "Join::Linear",
                JoinPlan::Delta(_) => "Join::Delta",
            },
            LirRelationNode::Reduce { plan, .. } => match plan {
                ReducePlan::Distinct => "Reduce::Distinct",
                ReducePlan::Accumulable(_) => "Reduce::Accumulable",
                ReducePlan::Hierarchical(_) => "Reduce::Hierarchical",
                ReducePlan::Basic(_) => "Reduce::Basic",
            },
            LirRelationNode::TopK { top_k_plan, .. } => match top_k_plan {
                TopKPlan::MonotonicTop1(_) => "TopK::MonotonicTop1",
                TopKPlan::MonotonicTopK(_) => "TopK::MonotonicTopK",
                TopKPlan::Basic(_) => "TopK::Basic",
            },
            LirRelationNode::Negate { .. } => "Negate",
            LirRelationNode::Threshold { .. } => "Threshold",
            LirRelationNode::Union { .. } => "Union",
            LirRelationNode::ArrangeBy { .. } => "ArrangeBy",
        });
        todo.extend(expr.node.children());
    }
    seen
}

/// User indexes and materialized views whose plans together contain every
/// LIR operator, and each join, reduce, and top-k strategy, except the
/// monotonic top-k plans, which need a monotonic input that a table is not.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn user_items_cover_every_operator() {
    Catalog::with_debug(|mut catalog| async move {
        let mut next_id = 0;
        let mut create = async |catalog: &mut Catalog, name: &str, sql: String| {
            next_id += 1;
            let item = create_item(catalog, next_id, name, &sql).await;
            (next_id, item)
        };
        for (name, columns) in [
            ("t1", "\"a\" int, \"b\" int"),
            ("t2", "\"a\" int, \"c\" text"),
            ("t3", "\"a\" int, \"d\" int"),
        ] {
            create(
                &mut catalog,
                name,
                format!("CREATE TABLE \"materialize\".\"public\".\"{name}\" ({columns})"),
            )
            .await;
        }
        // Indexes on every join key let the optimizer pick a delta join.
        let mut indexes = BTreeSet::new();
        for table in ["t1", "t2", "t3"] {
            let name = format!("{table}_a");
            let (id, _) = create(
                &mut catalog,
                &name,
                format!(
                    "CREATE INDEX \"{name}\" IN CLUSTER \"quickstart\" \
                     ON \"materialize\".\"public\".\"{table}\" (\"a\")"
                ),
            )
            .await;
            indexes.insert(GlobalId::User(id));
        }
        create(
            &mut catalog,
            "v",
            "CREATE VIEW \"materialize\".\"public\".\"v\" AS \
             SELECT \"a\", sum(\"b\") AS \"s\" FROM \"materialize\".\"public\".\"t1\" \
             GROUP BY \"a\""
                .to_string(),
        )
        .await;
        let (view_index_id, view_index) = create(
            &mut catalog,
            "v_a",
            "CREATE INDEX \"v_a\" IN CLUSTER \"quickstart\" \
             ON \"materialize\".\"public\".\"v\" (\"a\")"
                .to_string(),
        )
        .await;
        let CatalogItem::Index(view_index) = view_index else {
            panic!("expected an index");
        };

        let views = [
            (
                "linear_join",
                "SELECT t1.b, t2.c FROM materialize.public.t1 AS t1 \
                 JOIN materialize.public.t2 AS t2 ON t1.b = t2.a",
            ),
            (
                "delta_join",
                "SELECT t1.b, t2.c, t3.d FROM materialize.public.t1 AS t1 \
                 JOIN materialize.public.t2 AS t2 ON t1.a = t2.a \
                 JOIN materialize.public.t3 AS t3 ON t1.a = t3.a",
            ),
            (
                "accumulable",
                "SELECT a, sum(b), count(*) FROM materialize.public.t1 AS t1 GROUP BY a",
            ),
            (
                "hierarchical",
                "SELECT a, min(b), max(b) FROM materialize.public.t1 AS t1 GROUP BY a",
            ),
            (
                "basic_reduce",
                "SELECT a, string_agg(c, ',') FROM materialize.public.t2 AS t2 GROUP BY a",
            ),
            (
                "distinct",
                "SELECT DISTINCT b FROM materialize.public.t1 AS t1",
            ),
            // The map on top of the top-k cannot fuse into it, so the plan
            // has a standalone `Mfp`.
            (
                "top_k",
                "SELECT a, b + 1 FROM \
                 (SELECT a, b FROM materialize.public.t1 AS t1 ORDER BY b LIMIT 3) AS top",
            ),
            (
                "except",
                "SELECT a FROM materialize.public.t1 AS t1 \
                 EXCEPT SELECT a FROM materialize.public.t2 AS t2",
            ),
            (
                "flat_map",
                "SELECT a, generate_series(1, b) FROM materialize.public.t1 AS t1",
            ),
            (
                "shared_cte",
                "WITH x AS (SELECT a, count(*) AS n FROM materialize.public.t1 AS t1 GROUP BY a) \
                 SELECT x1.a, x2.n FROM x AS x1 JOIN x AS x2 ON x1.n = x2.a",
            ),
            (
                "recursive",
                "WITH MUTUALLY RECURSIVE r (n int) AS \
                 (SELECT a FROM materialize.public.t1 AS t1 \
                 UNION SELECT n + 1 FROM r WHERE n < 10) \
                 SELECT n FROM r",
            ),
            ("constant", "SELECT 1 AS x"),
            (
                "through_view_index",
                "SELECT a, s FROM materialize.public.v AS v WHERE a = 1",
            ),
        ];
        let mut mvs = Vec::new();
        for (name, query) in views {
            let (_, item) = create(
                &mut catalog,
                name,
                format!(
                    "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"{name}\" \
                     IN CLUSTER \"quickstart\" AS {query}"
                ),
            )
            .await;
            let CatalogItem::MaterializedView(mv) = item else {
                panic!("expected a materialized view");
            };
            mvs.push((name, mv));
        }

        let catalog = Arc::new(catalog);
        let mut seen = BTreeSet::new();
        let view_index_name = catalog
            .get_entry(&CatalogItemId::User(view_index_id))
            .name()
            .clone();
        let df_desc = optimize_index(&catalog, view_index_name, &view_index, indexes.clone());
        seen.extend(operators(&df_desc));
        assert_round_trips(&catalog, df_desc);

        indexes.insert(view_index.global_id);
        let mut index_imports = 0;
        for (name, mv) in &mvs {
            let df_desc =
                optimize_materialized_view(&catalog, mv, name.to_string(), indexes.clone());
            seen.extend(operators(&df_desc));
            index_imports += df_desc.index_imports.len();
            assert_round_trips(&catalog, df_desc);
        }

        assert!(index_imports > 0, "some plan reads an index");
        let expected = BTreeSet::from([
            "ArrangeBy",
            "Constant",
            "FlatMap",
            "Get",
            "Join::Delta",
            "Join::Linear",
            "Let",
            "LetRec",
            "Mfp",
            "Negate",
            "Reduce::Accumulable",
            "Reduce::Basic",
            "Reduce::Distinct",
            "Reduce::Hierarchical",
            "Threshold",
            "TopK::Basic",
            "Union",
        ]);
        let missing: Vec<_> = expected.difference(&seen).collect();
        assert!(missing.is_empty(), "no plan contains {missing:?}");
    })
    .await
}

fn inlined(kind: InlinedBuiltinKind, schema: &str, name: &str) -> InlinedBuiltin {
    InlinedBuiltin {
        kind,
        schema: schema.to_string(),
        name: name.to_string(),
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn pin_records_builtins_inlined_through_views() {
    const ZONES: u64 = 2;
    const MV: u64 = 3;
    const MV_ON_TABLE: u64 = 4;
    const MV_ON_MV: u64 = 5;
    const UNKNOWN: u64 = 99;
    Catalog::with_debug(|mut catalog| async move {
        create_tables(&mut catalog).await;
        create_item(
            &mut catalog,
            ZONES,
            "zones",
            "CREATE VIEW \"materialize\".\"public\".\"zones\" AS \
             SELECT \"name\" FROM \"mz_catalog\".\"mz_timezone_names\"",
        )
        .await;
        let CatalogItem::MaterializedView(mv) = create_item(
            &mut catalog,
            MV,
            "mv",
            "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv\" \
             IN CLUSTER \"quickstart\" AS \
             SELECT \"pg_catalog\".\"regexp_split_to_table\"(\"name\", '/') AS \"part\" \
             FROM \"materialize\".\"public\".\"zones\"",
        )
        .await
        else {
            panic!("expected a materialized view");
        };
        create_item(
            &mut catalog,
            MV_ON_TABLE,
            "mv_on_table",
            "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv_on_table\" \
             IN CLUSTER \"quickstart\" AS \
             SELECT \"id\" FROM \"materialize\".\"public\".\"tables\"",
        )
        .await;
        create_item(
            &mut catalog,
            MV_ON_MV,
            "mv_on_mv",
            "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv_on_mv\" \
             IN CLUSTER \"quickstart\" AS \
             SELECT \"part\" FROM \"materialize\".\"public\".\"mv\"",
        )
        .await;
        let catalog = Arc::new(catalog);

        let expected = BTreeSet::from([
            inlined(InlinedBuiltinKind::View, "mz_catalog", "mz_timezone_names"),
            inlined(
                InlinedBuiltinKind::Func,
                "pg_catalog",
                "regexp_split_to_table",
            ),
        ]);
        let df_desc = optimize_materialized_view(&catalog, &mv, "mv".to_string(), BTreeSet::new());
        let pinned = assert_round_trips(&catalog, df_desc);
        assert_eq!(pinned.inlined_builtins, expected);

        let state = catalog.state();
        assert_eq!(
            state.inlined_builtins(GlobalId::User(MV_ON_TABLE)),
            Ok(BTreeSet::new()),
            "a table is an import, not inlined"
        );
        assert_eq!(
            state.inlined_builtins(GlobalId::User(MV_ON_MV)),
            Ok(BTreeSet::new()),
            "another materialized view is an import, not inlined"
        );
        assert_eq!(
            state.inlined_builtins(GlobalId::User(UNKNOWN)),
            Err(PinError::UnknownExport(GlobalId::User(UNKNOWN)))
        );
    })
    .await
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn pin_rejects_unstable_dependencies() {
    const DIRECT: u64 = 2;
    const THROUGH_VIEW: u64 = 3;
    Catalog::with_debug(|mut catalog| async move {
        catalog
            .system_config_mut()
            .set("unsafe_enable_unstable_dependencies", VarInput::Flat("on"))
            .expect("flag exists");
        create_item(
            &mut catalog,
            DIRECT,
            "direct",
            "CREATE VIEW \"materialize\".\"public\".\"direct\" AS \
             SELECT \"mz_internal\".\"mz_connection_oid\"('c') AS \"oid\"",
        )
        .await;
        create_item(
            &mut catalog,
            THROUGH_VIEW,
            "through_view",
            "CREATE VIEW \"materialize\".\"public\".\"through_view\" AS \
             SELECT \"oid\" FROM \"materialize\".\"public\".\"direct\"",
        )
        .await;
        let state = catalog.state();
        for id in [GlobalId::User(DIRECT), GlobalId::User(THROUGH_VIEW)] {
            assert_eq!(
                state.inlined_builtins(id),
                Err(PinError::UnstableDependency {
                    export: id,
                    dependency: "mz_internal.mz_connection_oid".to_string(),
                }),
            );
        }
    })
    .await
}

/// The views `id` reads, directly or through other views, including `id`
/// itself, or `None` if any of them reads a collection.
fn constant_view_closure(catalog: &Catalog, id: CatalogItemId) -> Option<BTreeSet<CatalogItemId>> {
    let mut views = BTreeSet::new();
    let mut todo = vec![id];
    while let Some(id) = todo.pop() {
        let entry = catalog.get_entry(&id);
        match entry.item() {
            CatalogItem::View(_) => {
                if views.insert(id) {
                    todo.extend(entry.uses());
                }
            }
            CatalogItem::Func(_) | CatalogItem::Type(_) => {}
            _ => return None,
        }
    }
    Some(views)
}

/// Ensures that the set of known-constant views does not change out from under us.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn constant_builtin_views() {
    const ZONES: u64 = 2;
    const ZONES_AND_TABLES: u64 = 3;
    Catalog::with_debug(|mut catalog| async move {
        create_tables(&mut catalog).await;
        // Roots are stable views only, see `BUILTINS::constant_views`.
        let mut constant = BTreeSet::new();
        for entry in catalog.entries() {
            let stable = !catalog
                .state()
                .is_unstable_schema_specifier(entry.name().qualifiers.schema_spec);
            if entry.id().is_system() && stable && matches!(entry.item(), CatalogItem::View(_)) {
                if let Some(views) = constant_view_closure(&catalog, entry.id()) {
                    constant.extend(views);
                }
            }
        }
        let constant: BTreeSet<String> = constant
            .into_iter()
            .map(|id| {
                let entry = catalog.get_entry(&id);
                catalog.resolve_full_name(entry.name(), None).to_string()
            })
            .collect();
        let listed: BTreeSet<String> = mz_catalog::builtin::BUILTINS::constant_views()
            .map(|view| format!("{}.{}", view.schema, view.name))
            .collect();
        assert_eq!(
            constant, listed,
            "BUILTINS::constant_views in src/catalog/src/builtin.rs must list exactly the \
             constant builtin views a user plan can inline"
        );

        // A collection read anywhere below a view makes it non-constant.
        create_item(
            &mut catalog,
            ZONES,
            "zones",
            "CREATE VIEW \"materialize\".\"public\".\"zones\" AS \
             SELECT \"name\" FROM \"mz_catalog\".\"mz_timezone_names\"",
        )
        .await;
        create_item(
            &mut catalog,
            ZONES_AND_TABLES,
            "zones_and_tables",
            "CREATE VIEW \"materialize\".\"public\".\"zones_and_tables\" AS \
             SELECT \"z\".\"name\" FROM \"materialize\".\"public\".\"zones\" AS \"z\", \
             \"materialize\".\"public\".\"tables\" AS \"t\"",
        )
        .await;
        assert!(constant_view_closure(&catalog, CatalogItemId::User(ZONES)).is_some());
        assert!(constant_view_closure(&catalog, CatalogItemId::User(ZONES_AND_TABLES)).is_none());
    })
    .await
}

/// The catalog items planning resolves inside the SQL body of `imp`, called
/// with typed NULL arguments. An `anyarray` parameter is bound to `int4[]`.
fn sql_body_items<R>(catalog: &Catalog, imp: &FuncImpl<R>) -> BTreeSet<CatalogItemId> {
    let conn_catalog = catalog.for_system_session();
    let pcx = PlanContext::zero();
    let scx = StatementContext::new(Some(&pcx), &conn_catalog);
    let qcx = QueryContext::root(&scx, QueryLifetime::OneShot);
    let ecx = ExprContext {
        qcx: &qcx,
        name: "sql_body_items",
        scope: &Scope::empty(),
        relation_type: &SqlRelationType::empty(),
        allow_aggregates: false,
        allow_subqueries: true,
        allow_parameters: false,
        allow_windows: false,
    };
    let details = imp.details();
    let args = details
        .arg_typs
        .iter()
        .map(|name| {
            let typ = if *name == "anyarray" {
                SqlScalarType::Array(Box::new(SqlScalarType::Int32))
            } else {
                let oid = conn_catalog.get_system_type(name).oid();
                let pg = mz_pgrepr::Type::from_oid(oid).unwrap_or_else(|_| {
                    panic!("no concrete type to bind parameter type {name} to")
                });
                SqlScalarType::try_from(&pg).expect("pg types map to SQL types")
            };
            CoercibleScalarExpr::Coerced(HirScalarExpr::literal_null(typ))
        })
        .collect();
    if let Err(err) = (imp.op.build)(&ecx, args, &imp.params, vec![]) {
        panic!("planning SQL-defined builtin {} failed: {err}", details.oid);
    }
    let items = scx
        .sql_impl_resolved_ids
        .lock()
        .expect("not poisoned")
        .items()
        .copied()
        .collect();
    items
}

/// A pin records only the builtins its exports reference through views, not
/// what a SQL-defined function's body references. So a SQL body that inlines a
/// SQL-defined function or a view must also read a collection, which makes
/// every plan calling it unpinnable.
///
/// NOTE: If this fails because a body legitimately inlines another builtin,
/// make `CatalogState::inlined_builtins` follow SQL bodies instead: resolve
/// each `imp.sql` with `names::resolve` (types are not needed) and push the
/// resolved ids onto the walk. Builtins in unstable schemas reached that way
/// would then also need recording in the builtin registry.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn sql_bodies_that_inline_builtins_read_a_collection() {
    Catalog::with_debug(|mut catalog| async move {
        catalog
            .system_config_mut()
            .set("enable_date_bin_hopping", VarInput::Flat("on"))
            .expect("flag exists");
        let is_inlined = |id: &CatalogItemId| match catalog.get_entry(id).item() {
            CatalogItem::Func(func) => func.inner.func_impls().iter().any(|imp| imp.sql.is_some()),
            CatalogItem::View(_) => true,
            _ => false,
        };
        let reads_collection = |id: &CatalogItemId| match catalog.get_entry(id).item() {
            CatalogItem::Func(_) | CatalogItem::Type(_) => false,
            CatalogItem::View(_) => constant_view_closure(&catalog, *id).is_none(),
            _ => true,
        };
        let check = |name: String, items: BTreeSet<CatalogItemId>| -> bool {
            let inlines = items.iter().any(is_inlined);
            assert!(
                !inlines || items.iter().any(reads_collection),
                "{name} inlines a SQL-defined function or view but reads no collection, \
                 so a plan calling it can be pinned without recording what it inlines"
            );
            inlines
        };

        let mut inlining = 0;
        for func in BUILTINS::funcs().filter(|func| !UNSTABLE_SCHEMAS.contains(&func.schema)) {
            let name = |oid| format!("{}.{} (oid {oid})", func.schema, func.name);
            let reaches = match func.inner {
                Func::Scalar(impls) => impls
                    .iter()
                    .filter(|imp| imp.details().sql.is_some())
                    .map(|imp| check(name(imp.oid), sql_body_items(&catalog, imp)))
                    .filter(|reaches| *reaches)
                    .count(),
                Func::Table(impls) => impls
                    .iter()
                    .filter(|imp| imp.details().sql.is_some())
                    .map(|imp| check(name(imp.oid), sql_body_items(&catalog, imp)))
                    .filter(|reaches| *reaches)
                    .count(),
                Func::Aggregate(_) | Func::ScalarWindow(_) | Func::ValueWindow(_) => 0,
            };
            inlining += reaches;
        }
        assert!(
            inlining > 0,
            "no stable SQL body inlines a SQL-defined function or view, so this test \
             no longer exercises the case it guards"
        );
    })
    .await
}
