// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Native execution of generated log-fed metric plans, not curated SQL planning
//! or process-restart acceptance coverage.

use std::collections::BTreeSet;
use std::time::Duration;

use differential_dataflow::Hashable;
use itertools::Itertools;
use mz_catalog::catalog::{Catalog, CatalogError, Op};
use mz_catalog::durable::DurableCatalogError;
use mz_catalog::durable::objects::ReplicaPlanOwner;
use mz_catalog::expr_cache::{GlobalExpressions, expression_build_version};
use mz_catalog::memory::error::ErrorKind;
use mz_catalog::memory::objects::CatalogItem;
use mz_compute_client::logging::{ComputeLog, LogVariant};
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_types::dataflows::{DataflowDescription, IndexDesc};
use mz_compute_types::plan::LirRelationExpr;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, MetricSinkConnection};
use mz_expr::{MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr};
use mz_ore::cast::CastLossy;
use mz_ore::metrics::MetricsRegistry;
use mz_repr::{
    Datum, GlobalId, RelationDesc, RelationVersion, ReprRelationType, Row, SqlScalarType,
};
use timely::progress::Antichain;
use tokio::time::timeout;
use uuid::Uuid;

use super::tests::{Fixture, in_child, start_runtime};

const LABEL: &str = "mz_curated_native_metric_test";
const METRIC: &str = "mz_native_follower_log_metric";

#[mz_ore::test(tokio::test)]
async fn written_metric_survives_disconnect_and_export_replacement() {
    if !in_child(
        "MZ_CLUSTERD_NATIVE_METRIC_TEST_CHILD",
        "catalog_follower::execution::metric_tests::written_metric_survives_disconnect_and_export_replacement",
        Duration::from_secs(120),
    )
    .await
    {
        return;
    }
    Box::pin(run_metric_replacement()).await;
    std::process::exit(0);
}

async fn run_metric_replacement() {
    let mut fixture = Box::pin(Fixture::new((1, 15_000), 20_000, false)).await;
    let build = expression_build_version(fixture.config.build_info).to_string();
    let owner = ReplicaPlanOwner {
        replica_id: fixture.config.replica_id,
        name: LABEL.into(),
    };
    let old = GlobalId::Transient(90_000);
    // Exercise different export-ID hash partitions on two workers. Registration
    // ownership must follow the stable label, not these transient identifiers.
    let new = (90_001..90_100)
        .map(GlobalId::Transient)
        .find(|id| id.hashed() % 2 != old.hashed() % 2)
        .expect("an export ID on the other worker");
    let log = LogVariant::Compute(ComputeLog::DataflowCurrent);
    let log_index = fixture
        .writer
        .try_get_cluster(fixture.config.cluster_id)
        .unwrap()
        .log_indexes[&log];
    let old_revision = Uuid::new_v4();
    let new_revision = Uuid::new_v4();
    // Both revisions are in the follower's named expression shard before either
    // selection is committed. No writer or expression-store handle is needed to
    // generate or deliver bytes during the disconnected phase.
    fixture
        .store
        .write_plans(vec![
            (
                old,
                old_revision,
                metric_plan(&fixture.writer, log_index, old, 1.0),
            ),
            (
                new,
                new_revision,
                metric_plan(&fixture.writer, log_index, new, 2.0),
            ),
        ])
        .await
        .unwrap();
    commit(
        &mut fixture.writer,
        vec![selection(
            old,
            &build,
            None,
            Some(old_revision),
            Some(owner.clone()),
            log_index,
        )],
    )
    .await;
    assert!(fixture.writer.try_get_entry_by_global_id(&old).is_none());

    let registry = MetricsRegistry::new();
    let factory = start_runtime(fixture.config, fixture.clients, registry.clone()).await;
    let mut query = factory();
    query
        .send(ComputeCommand::HelloQuery {
            nonce: Uuid::new_v4(),
        })
        .await
        .unwrap();
    assert!(matches!(
        query.recv().await.unwrap(),
        Some(ComputeResponse::QueryReady)
    ));
    // This connection performs no reads, so it needs no historical read grant.
    drop(query);
    drop(factory);
    drop(fixture.writer);
    drop(fixture.store);

    let first = wait_metric(&registry, 1.0, 0.0).await;
    wait_metric(&registry, 1.0, first).await;

    // A separate catalog client changes only the durable selection. It never
    // sends maintained compute commands. Retraction and selection share a commit.
    commit(
        &mut fixture.observer,
        vec![
            selection(
                old,
                &build,
                Some(old_revision),
                None,
                Some(owner.clone()),
                log_index,
            ),
            selection(
                new,
                &build,
                None,
                Some(new_revision),
                Some(owner),
                log_index,
            ),
        ],
    )
    .await;
    assert_eq!(fixture.observer.state().written_plan(old, &build), None);
    assert_eq!(
        fixture.observer.state().written_plan(new, &build),
        Some(new_revision)
    );
    drop(fixture.observer);
    let replaced = wait_metric(&registry, 2.0, 0.0).await;
    wait_metric(&registry, 2.0, replaced).await;
}

fn selection(
    id: GlobalId,
    build: &str,
    expected_revision: Option<Uuid>,
    revision: Option<Uuid>,
    replica_owner: Option<ReplicaPlanOwner>,
    log_index: GlobalId,
) -> Op {
    Op::SetWrittenPlan {
        id,
        build_version: build.into(),
        expected_revision,
        revision,
        imports: if revision.is_some() {
            BTreeSet::from([log_index])
        } else {
            BTreeSet::new()
        },
        replica_owner,
    }
}

fn metric_plan(
    catalog: &Catalog,
    log_index: GlobalId,
    export: GlobalId,
    value: f64,
) -> GlobalExpressions {
    let entry = catalog.try_get_entry_by_global_id(&log_index).unwrap();
    let CatalogItem::Index(index) = entry.item() else {
        panic!("log arrangement is an index")
    };
    let log_desc = catalog
        .state()
        .try_get_desc_by_global_id(&index.on)
        .unwrap();
    let desc = RelationDesc::builder()
        .with_column("metric_name", SqlScalarType::String.nullable(false))
        .with_column(
            "labels",
            SqlScalarType::Map {
                value_type: Box::new(SqlScalarType::String),
                custom_id: None,
            }
            .nullable(false),
        )
        .with_column("value", SqlScalarType::Float64.nullable(false))
        .with_column("help", SqlScalarType::String.nullable(false))
        .with_column("metric_kind", SqlScalarType::Int32.nullable(false))
        .with_column("name_valid", SqlScalarType::Bool.nullable(false))
        .finish();
    let mut row = Row::default();
    let mut packer = row.packer();
    packer.push(Datum::String(METRIC));
    packer.push_dict_with(|_| ());
    packer.push(Datum::Float64(value.into()));
    packer.push(Datum::String("Native follower log-fed regression gauge"));
    packer.push(Datum::Int32(0));
    packer.push(Datum::True);
    let arity = log_desc.arity();
    let repr_type = ReprRelationType::from(desc.typ());
    let scalars = row
        .iter()
        .zip_eq(repr_type.column_types)
        .map(|(datum, typ)| MirScalarExpr::literal_ok(datum, typ.scalar_type))
        .collect();
    // Mapping literals over a live log collection preserves its progress. There
    // is no constant input whose completed frontier could fake ongoing work.
    let expr = MirRelationExpr::global_get(index.on, log_desc.typ().into())
        .map(scalars)
        .project((arity..arity + desc.arity()).collect());
    let from = match export {
        GlobalId::Transient(id) => GlobalId::Transient(id + 10_000),
        _ => unreachable!(),
    };
    let mut mir = DataflowDescription::new(LABEL.into());
    mir.import_index(
        log_index,
        IndexDesc {
            on_id: index.on,
            key: index.keys.to_vec(),
        },
        log_desc.typ().into(),
        false,
    );
    mir.insert_plan(from, OptimizedMirRelationExpr::declare_optimized(expr));
    mir.export_sink(
        export,
        ComputeSinkDesc {
            from,
            from_desc: desc,
            connection: ComputeSinkConnection::MetricSink(MetricSinkConnection {
                label: LABEL.into(),
            }),
            with_snapshot: true,
            up_to: Antichain::new(),
            non_null_assertions: Vec::new(),
            refresh_schedule: None,
        },
    );
    let features = Default::default();
    let physical_plan = LirRelationExpr::finalize_dataflow(mir.clone(), &features, None).unwrap();
    assert!(physical_plan.source_imports.is_empty());
    assert!(physical_plan.index_exports.is_empty());
    assert_eq!(
        physical_plan
            .index_imports
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![log_index]
    );
    GlobalExpressions {
        global_mir: mir,
        physical_plan,
        dataflow_metainfos: Default::default(),
        optimizer_features: features,
        item_version: RelationVersion::root(),
    }
}

async fn wait_metric(registry: &MetricsRegistry, value: f64, after: f64) -> f64 {
    timeout(Duration::from_secs(25), async {
        loop {
            let families = registry.gather();
            let samples = families
                .iter()
                .filter(|family| family.name() == METRIC)
                .flat_map(|family| family.get_metric())
                .collect::<Vec<_>>();
            let frontier = families
                .iter()
                .filter(|family| family.name() == "mz_compute_metric_sink_frontier_ms")
                .flat_map(|family| family.get_metric())
                .find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "sink" && label.value() == LABEL)
                })
                .map(|metric| metric.get_gauge().value());
            if samples.len() == 1
                && samples[0].get_gauge().value() == value
                && let Some(frontier) = frontier
                && frontier > after
                && frontier < f64::cast_lossy(u64::MAX)
            {
                for name in [
                    "errors",
                    "skipped",
                    "conflicts",
                    "collisions",
                    "null_values",
                ] {
                    let family_name = format!("mz_compute_metric_sink_{name}");
                    let health = families
                        .iter()
                        .find(|family| family.name() == family_name)
                        .unwrap();
                    let health = health
                        .get_metric()
                        .iter()
                        .find(|metric| {
                            metric
                                .get_label()
                                .iter()
                                .any(|label| label.name() == "sink" && label.value() == LABEL)
                        })
                        .unwrap();
                    assert_eq!(health.get_gauge().value(), 0.0, "{family_name}");
                }
                return frontier;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("log-fed series and finite advancing metric frontier")
}

async fn commit(catalog: &mut Catalog, ops: Vec<Op>) {
    timeout(Duration::from_secs(10), async {
        loop {
            catalog.sync_to_current_updates().await.unwrap();
            let ts = catalog.current_upper().await;
            match catalog.transact(None, ts, None, ops.clone()).await {
                Err(CatalogError::Catalog(error))
                    if matches!(
                        error.kind,
                        ErrorKind::Durable(DurableCatalogError::CatalogOutOfSync { .. })
                    ) =>
                {
                    continue;
                }
                result => {
                    result.unwrap();
                    return;
                }
            }
        }
    })
    .await
    .expect("commit metric selection alongside follower publications");
}
