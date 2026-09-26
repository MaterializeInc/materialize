// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::catalog_follower::tests::{BUILD, create_table, debug_catalog, store};
use mz_compute_types::dataflows::{IndexDesc, IndexImport};
use mz_persist_client::PersistClient;
use mz_repr::{ReprRelationType, SqlRelationType, Timestamp};

fn source_plan(id: GlobalId, source: GlobalId) -> DataflowDescription<LirRelationExpr, ()> {
    let mut imports = DataflowDescription::new("imports".into());
    imports.import_source(source, SqlRelationType::empty(), false);
    let mut plan = DataflowDescription::new("dependence".into());
    plan.source_imports = imports.source_imports;
    plan.index_exports.insert(
        id,
        (
            IndexDesc {
                on_id: source,
                key: vec![],
            },
            ReprRelationType::empty(),
        ),
    );
    plan
}

#[mz_ore::test(tokio::test)]
async fn pending_dag_refresh_and_installed_precedence() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let mut catalog = debug_catalog(&persist, None).await;
    let (_, table) = create_table(&mut catalog, "time_input").await;
    let producer_id = GlobalId::Transient(1);
    let consumer_id = GlobalId::Transient(2);
    let mut producer = source_plan(producer_id, table);
    let schedule = RefreshSchedule {
        everies: vec![],
        ats: vec![Timestamp::from(1000)],
    };
    producer.refresh_schedule = Some(schedule.clone());
    let mut consumer = source_plan(consumer_id, table);
    consumer.source_imports.clear();
    consumer.index_imports.insert(
        producer_id,
        IndexImport {
            desc: IndexDesc {
                on_id: table,
                key: vec![],
            },
            typ: ReprRelationType::empty(),
            monotonic: false,
            with_snapshot: true,
        },
    );
    // Consumer first exercises dependency resolution independent of batch order.
    let mut plans = [consumer, producer];
    resolve(&catalog, &store, BUILD, &mut plans, &BTreeMap::new())
        .await
        .expect("pending producer");
    let expected = Some(TimeDependence::new(Some(schedule), vec![]));
    assert_eq!(plans[0].time_dependence, expected);
    assert_eq!(plans[1].time_dependence, expected);

    resolve(
        &catalog,
        &store,
        BUILD,
        &mut plans,
        &BTreeMap::from([(producer_id, None)]),
    )
    .await
    .expect("installed producer");
    assert_eq!(plans[0].time_dependence, None);
    assert_eq!(plans[1].time_dependence, expected);
}

#[mz_ore::test(tokio::test)]
async fn only_used_prerequisites_are_required_and_errors_leave_batch_unchanged() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let catalog = debug_catalog(&persist, None).await;
    let missing = GlobalId::Transient(10);
    let mut unused = source_plan(GlobalId::Transient(11), missing);
    unused.index_exports.clear();
    let mut plans = [unused];
    resolve(&catalog, &store, BUILD, &mut plans, &BTreeMap::new())
        .await
        .expect("unused missing import");
    assert_eq!(plans[0].time_dependence, None);

    plans[0].time_dependence = Some(TimeDependence::default());
    let mut batch = vec![
        plans[0].clone(),
        source_plan(GlobalId::Transient(12), missing),
    ];
    assert!(
        resolve(&catalog, &store, BUILD, &mut batch, &BTreeMap::new())
            .await
            .is_err()
    );
    assert_eq!(batch[0].time_dependence, Some(TimeDependence::default()));
}
