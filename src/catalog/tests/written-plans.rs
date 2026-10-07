// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![recursion_limit = "256"]

use std::collections::BTreeMap;

use mz_catalog::durable::{
    CatalogError, DurableCatalogError, TestCatalogStateBuilder, Transaction, test_bootstrap_args,
};
use mz_catalog::memory::objects::StateUpdateKind;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::PersistClient;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::names::SchemaId;
use uuid::Uuid;

const ID: GlobalId = GlobalId::User(1000);
const ITEM: CatalogItemId = CatalogItemId::User(1000);

fn insert_item(txn: &mut Transaction<'_>) {
    txn.insert_item(
        ITEM,
        20000,
        ID,
        SchemaId::User(1),
        "v",
        "CREATE MATERIALIZED VIEW v AS SELECT 1".into(),
        RoleId::User(1),
        Vec::new(),
        BTreeMap::new(),
        None,
    )
    .unwrap();
}

async fn commit(mut txn: Transaction<'_>) {
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.unwrap();
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn written_plans_are_atomic_and_build_isolated() {
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation();
    let mut state = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    state.sync_to_current_updates().await.unwrap();
    let mut observer = builder
        .clone()
        .unwrap_build()
        .await
        .open_read_only(&test_bootstrap_args())
        .await
        .unwrap();
    observer.sync_to_current_updates().await.unwrap();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let mut txn = state.transaction().await.unwrap();
    insert_item(&mut txn);
    txn.set_written_plan(ID, "build-a", Some(a)).unwrap();
    txn.set_written_plan(ID, "build-b", Some(b)).unwrap();
    assert_eq!(txn.get_written_plan(ID, "build-a"), Some(a));
    assert_eq!(txn.get_written_plan(ID, "build-b"), Some(b));
    assert_eq!(txn.get_written_plan(ID, "unknown"), None);
    assert_eq!(txn.get_written_plans().count(), 2);
    commit(txn).await;
    let updates = observer.sync_to_current_updates().await.unwrap();
    assert_eq!(updates.len(), 3);
    assert!(
        updates
            .iter()
            .any(|u| matches!(u.kind, StateUpdateKind::Item(_)))
    );
    assert_eq!(
        updates
            .iter()
            .filter(|u| matches!(u.kind, StateUpdateKind::WrittenPlan(_)))
            .count(),
        2
    );
    assert!(updates.iter().all(|u| u.ts == updates[0].ts));
    let committed = state.snapshot().await.unwrap();

    let mut txn = state.transaction().await.unwrap();
    txn.remove_item(ITEM).unwrap();
    txn.set_written_plan(ID, "build-a", None).unwrap();
    assert_eq!(txn.get_written_plan(ID, "build-b"), Some(b));
    drop(txn);
    assert_eq!(state.snapshot().await.unwrap(), committed);

    let mut txn = state.transaction().await.unwrap();
    txn.set_written_plan(ID, "build-a", Some(Uuid::new_v4()))
        .unwrap();
    commit(txn).await;
    let updates = observer.sync_to_current_updates().await.unwrap();
    assert_eq!(updates.len(), 2);
    assert!(
        updates
            .iter()
            .all(|u| matches!(u.kind, StateUpdateKind::WrittenPlan(_)))
    );

    let mut txn = state.transaction().await.unwrap();
    txn.remove_item(ITEM).unwrap();
    txn.set_written_plan(ID, "build-a", None).unwrap();
    commit(txn).await;
    Box::new(observer).expire().await;
    Box::new(state).expire().await;
    let mut state = builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    state.sync_to_current_updates().await.unwrap();
    let txn = state.transaction().await.unwrap();
    assert!(txn.get_item(&ITEM).is_none());
    assert_eq!(txn.get_written_plan(ID, "build-a"), None);
    // A drop never implicitly clears another build's selector.
    assert_eq!(txn.get_written_plan(ID, "build-b"), Some(b));
    drop(txn);
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn losing_cas_cannot_select_a_plan_or_change_an_item() {
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation();
    let mut first = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    first.sync_to_current_updates().await.unwrap();
    let mut txn = first.transaction().await.unwrap();
    txn.set_config("catalog_read_protection_enabled".into(), Some(1))
        .unwrap();
    insert_item(&mut txn);
    txn.set_written_plan(ID, "build-a", Some(Uuid::new_v4()))
        .unwrap();
    commit(txn).await;
    first.sync_to_current_updates().await.unwrap();
    let mut second = builder.unwrap_build().await.join().await.unwrap();
    second.sync_to_current_updates().await.unwrap();
    let mut loser = first.transaction().await.unwrap();
    loser.remove_item(ITEM).unwrap();
    loser.set_written_plan(ID, "build-a", None).unwrap();
    let winning_revision = Uuid::new_v4();
    let mut winner = second.transaction().await.unwrap();
    winner
        .set_written_plan(ID, "build-a", Some(winning_revision))
        .unwrap();
    commit(winner).await;
    let _ = loser.get_and_commit_op_updates();
    let ts = loser.upper();
    assert!(matches!(
        loser.commit(ts).await,
        Err(CatalogError::Durable(
            DurableCatalogError::CatalogOutOfSync { .. }
        ))
    ));
    first.sync_to_current_updates().await.unwrap();
    let txn = first.transaction().await.unwrap();
    assert!(txn.get_item(&ITEM).is_some());
    assert_eq!(txn.get_written_plan(ID, "build-a"), Some(winning_revision));
    drop(txn);
    Box::new(first).expire().await;
    Box::new(second).expire().await;
}
