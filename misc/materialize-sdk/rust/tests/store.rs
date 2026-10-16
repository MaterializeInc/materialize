// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The Materialize checkpoint store against a running Materialize.

// The repository's clippy rules ask for wrappers a standalone SDK cannot use.
#![allow(clippy::disallowed_methods)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::{admin, config, execute};
use materialize_sdk::core::Checkpoint;
use materialize_sdk::core::catalog::ObjectName;
use materialize_sdk::{
    CheckpointStore, CheckpointWrite, MaterializeStore, WriteResult, checkpoint_table_sql,
};
use tokio::sync::Barrier;
use tokio_postgres::NoTls;

/// A fresh checkpoint table named `name` for this test run.
async fn table(name: &str) -> ObjectName {
    let name = &format!("{name}_{}", std::process::id());
    let table = ObjectName {
        database: Some("materialize".into()),
        schema: "public".into(),
        name: name.into(),
    };
    let admin = admin().await;
    execute(
        &admin,
        &format!("DROP TABLE IF EXISTS materialize.public.{name}"),
    )
    .await;
    execute(&admin, &checkpoint_table_sql(&table)).await;
    table
}

async fn store(table: &ObjectName) -> MaterializeStore<NoTls> {
    MaterializeStore::connect(&config(), NoTls, table)
        .await
        .unwrap()
}

fn first() -> Checkpoint {
    Checkpoint::new(BTreeMap::new())
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn setup_creates_the_checkpoint_only_once() {
    let table = table("store_setup").await;
    let mut store = store(&table).await;
    let text = first().encode().unwrap();
    assert!(store.load("s").await.unwrap().is_none());
    assert!(matches!(
        store.create("s", &text).await,
        WriteResult::Applied { .. }
    ));
    assert!(matches!(
        store.create("s", "other").await,
        WriteResult::NotApplied
    ));
    assert_eq!(store.load("s").await.unwrap(), Some(text));
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_write_applies_only_over_the_text_it_expects() {
    let table = table("store_cas").await;
    let mut store = store(&table).await;
    let (take_over, owned) = CheckpointWrite::take_over(&first()).unwrap();
    let (rival, _) = CheckpointWrite::start_snapshot(&first(), BTreeMap::new(), 40).unwrap();
    assert!(matches!(
        store.create("s", &take_over.expected).await,
        WriteResult::Applied { .. }
    ));
    assert!(matches!(
        store.compare_and_set("s", &take_over).await,
        WriteResult::Applied { .. }
    ));
    assert!(
        matches!(
            store.compare_and_set("s", &rival).await,
            WriteResult::NotApplied
        ),
        "a write from the replaced text does not apply"
    );
    assert_eq!(
        store.load("s").await.unwrap(),
        Some(owned.encode().unwrap())
    );
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_write_that_only_checks_applies_while_the_text_is_unchanged() {
    let table = table("store_check").await;
    let mut store = store(&table).await;
    let (start, snapshot) = CheckpointWrite::start_snapshot(&first(), BTreeMap::new(), 40).unwrap();
    assert!(matches!(
        store.create("s", &start.expected).await,
        WriteResult::Applied { .. }
    ));
    let check = CheckpointWrite::snapshot_write(&snapshot).unwrap();
    assert!(
        matches!(
            store.compare_and_set("s", &check).await,
            WriteResult::NotApplied
        ),
        "the snapshot has not started yet"
    );
    assert!(matches!(
        store.compare_and_set("s", &start).await,
        WriteResult::Applied { .. }
    ));
    assert!(matches!(
        store.compare_and_set("s", &check).await,
        WriteResult::Applied { .. }
    ));
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn only_one_of_racing_writes_applies() {
    const RACERS: usize = 8;
    let table = table("store_race").await;
    let expected = first().encode().unwrap();
    assert!(matches!(
        store(&table).await.create("s", &expected).await,
        WriteResult::Applied { .. }
    ));
    let mut stores = Vec::new();
    for _ in 0..RACERS {
        stores.push(store(&table).await);
    }
    // Every racer is connected before any of them writes.
    let start = Arc::new(Barrier::new(RACERS));
    let mut racers = Vec::new();
    for (as_of, mut store) in (0u64..).zip(stores) {
        // Each racer leaves a different text.
        let (write, _) = CheckpointWrite::start_snapshot(&first(), BTreeMap::new(), as_of).unwrap();
        let start = Arc::clone(&start);
        racers.push(tokio::spawn(async move {
            start.wait().await;
            (store.compare_and_set("s", &write).await, write.new)
        }));
    }
    let mut applied = Vec::new();
    for racer in racers {
        match racer.await.unwrap() {
            (WriteResult::Applied { .. }, new) => applied.push(new),
            (WriteResult::NotApplied, _) => {}
            (other, _) => panic!("a racer neither applied nor lost: {other:?}"),
        }
    }
    assert_eq!(applied.len(), 1);
    let stored = store(&table).await.load("s").await.unwrap();
    assert_eq!(stored.as_ref(), applied.first());
}
