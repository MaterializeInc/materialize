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

//! The failure and recovery scenarios of the design, against a running
//! Materialize at `MATERIALIZE_URL`. Run with
//! `cargo test --test scenarios -- --ignored`.
//!
//! These drive a cursor through the protocol core, as every language package
//! does, and check each error's classification and the catalog checks that
//! resolve it.

// The repository's clippy rules ask for wrappers a standalone SDK cannot use.
#![allow(clippy::disallowed_methods)]

mod common;

use std::time::Duration;

use common::{Cursor, Failure, STEP, Target, admin, config, execute, lookup, rows, table, value};
use materialize_sdk::core::catalog;
use materialize_sdk::core::classify::{ObjectOutcome, resolve_object_check};
use materialize_sdk::core::{CatalogObject, Envelope, IdentityChange, Start, compare};
use materialize_sdk::{Classification, ErrorKind, Frontier, Subscribe};
use tokio_postgres::Client;

const SNAPSHOT: Start = Start::Snapshot { as_of: None };

/// Long enough that no test outlives the history it resumes from.
const RETAINED: &str = "WITH (RETAIN HISTORY = FOR '1h')";

fn subscribe(object: &CatalogObject) -> Subscribe {
    Subscribe::object(object.object.clone())
}

/// The error that ends a subscription expected to fail, at open or later.
async fn fails(subscribe: Subscribe, start: Start) -> Failure {
    let mut cursor = match Cursor::open(&config(), subscribe, start).await {
        Ok(cursor) => cursor,
        Err(failure) => return failure,
    };
    let mut target = match start {
        Start::Snapshot { .. } => Target::default(),
        Start::Resume { frontier } => Target::restored(frontier, Default::default()),
    };
    match cursor.drain(&mut target).await {
        Ok(()) => panic!("the stream ended without an error"),
        Err(failure) => failure,
    }
}

/// The indexes on the session's cluster that serve `object`.
async fn indexes(client: &Client, object: &CatalogObject) -> Vec<String> {
    let cluster = value(client, "SHOW cluster").await;
    let sql = catalog::indexes_sql(&object.object.id, &cluster);
    let (columns, found) = rows(client, &sql).await;
    catalog::parse_indexes(&columns, found).unwrap()
}

/// The read frontier of the object with catalog id `id`.
async fn read_frontier(client: &Client, id: &str) -> Option<u64> {
    let (columns, found) = rows(client, &catalog::read_frontier_sql(id)).await;
    catalog::parse_read_frontier(&columns, found).unwrap()
}

async fn resume_margin(
    client: &Client,
    object: &CatalogObject,
    frontier: Frontier,
) -> Option<Duration> {
    let read_frontier = read_frontier(client, &object.object.id)
        .await
        .expect("a readable object");
    catalog::resume_margin(frontier, read_frontier)
}

/// How the object `stored`'s name resolves to now differs from `stored`.
async fn identity_change(client: &Client, stored: &CatalogObject) -> IdentityChange {
    let name = &stored.object;
    let current = lookup(client, &name.schema, &name.name).await;
    let fingerprint = |object: &CatalogObject| object.fingerprint(Envelope::Diff).unwrap();
    compare(
        &fingerprint(stored),
        current.as_ref().map(fingerprint).as_ref(),
    )
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_resume_continues_the_target_from_its_frontier() {
    let admin = admin().await;
    let relation = "materialize.public.scenario_resume";
    let object = table(&admin, "scenario_resume", &format!("(id text) {RETAINED}")).await;
    execute(&admin, &format!("INSERT INTO {relation} VALUES ('a')")).await;
    let mut target = Target::default();
    let mut cursor = Cursor::open(&config(), subscribe(&object), SNAPSHOT)
        .await
        .unwrap();
    cursor
        .fetch_until(&mut target, |t| t.has("a"))
        .await
        .unwrap();
    // The worker dies, and the source changes while it is down.
    drop(cursor);
    execute(&admin, &format!("INSERT INTO {relation} VALUES ('b')")).await;
    execute(&admin, &format!("DELETE FROM {relation} WHERE id = 'a'")).await;
    target.assert_matches(&admin, relation).await;
    let frontier = target.frontier.unwrap();
    assert!(resume_margin(&admin, &object, frontier).await.is_some());

    let start = Start::Resume { frontier };
    let mut cursor = Cursor::open(&config(), subscribe(&object), start)
        .await
        .unwrap();
    cursor
        .fetch_until(&mut target, |t| t.has("b") && !t.has("a"))
        .await
        .unwrap();
    target.assert_matches(&admin, relation).await;
    drop(cursor);
    execute(&admin, &format!("DROP TABLE {relation}")).await;
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_bounded_stream_ends_at_up_to() {
    let admin = admin().await;
    let relation = "materialize.public.scenario_bounded";
    let object = table(&admin, "scenario_bounded", &format!("(id text) {RETAINED}")).await;
    // A strict serializable read is timestamped after every earlier write,
    // and every later write after it.
    let now = async || -> u64 {
        value(&admin, "SELECT mz_now()::text")
            .await
            .parse()
            .unwrap()
    };
    execute(&admin, &format!("INSERT INTO {relation} VALUES ('a')")).await;
    let as_of = now().await;
    execute(&admin, &format!("INSERT INTO {relation} VALUES ('b')")).await;
    let up_to = now().await + 1;
    execute(&admin, &format!("INSERT INTO {relation} VALUES ('c')")).await;

    let bounded = subscribe(&object).with_up_to(up_to);
    let start = Start::Snapshot { as_of: Some(as_of) };
    let mut cursor = Cursor::open(&config(), bounded, start).await.unwrap();
    let mut target = Target::default();
    cursor.drain(&mut target).await.unwrap();
    assert!(cursor.is_finished());
    assert_eq!(target.frontier, Frontier::new(up_to));
    assert!(
        target.has("a") && target.has("b") && !target.has("c"),
        "{target:?}"
    );
    target.assert_matches(&admin, relation).await;
    drop(cursor);
    execute(&admin, &format!("DROP TABLE {relation}")).await;
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_resume_below_the_history_is_history_loss() {
    let admin = admin().await;
    let object = table(&admin, "scenario_history", "(id int)").await;
    let frontier = Frontier::new(1).unwrap();
    let failure = fails(subscribe(&object), Start::Resume { frontier }).await;
    assert_eq!(failure.sqlstate.as_deref(), Some("22000"), "{failure:?}");
    assert_eq!(failure.classification, Classification::CheckIndex);
    assert_eq!(indexes(&admin, &object).await, Vec::<String>::new());
    assert_eq!(resume_margin(&admin, &object, frontier).await, None);
    execute(&admin, "DROP TABLE materialize.public.scenario_history").await;
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn an_index_on_the_cluster_limits_the_history() {
    let admin = admin().await;
    let relation = "materialize.public.scenario_indexed";
    let object = table(&admin, "scenario_indexed", &format!("(id int) {RETAINED}")).await;
    execute(&admin, &format!("INSERT INTO {relation} VALUES (1)")).await;
    let mut target = Target::default();
    let mut cursor = Cursor::open(&config(), subscribe(&object), SNAPSHOT)
        .await
        .unwrap();
    cursor
        .fetch_until(&mut target, |t| t.has("1"))
        .await
        .unwrap();
    drop(cursor);
    let frontier = target.frontier.unwrap();
    let start = Start::Resume { frontier };
    let mut cursor = Cursor::open(&config(), subscribe(&object), start)
        .await
        .unwrap();
    cursor
        .fetch()
        .await
        .expect("the table's own history serves the resume");
    drop(cursor);

    execute(
        &admin,
        &format!("CREATE INDEX scenario_indexed_id ON {relation} (id)"),
    )
    .await;
    // A new index keeps its own history, so it serves the resume until its
    // read frontier passes `F - 1`.
    let index = value(
        &admin,
        "SELECT id FROM mz_catalog.mz_indexes WHERE name = 'scenario_indexed_id'",
    )
    .await;
    let deadline = tokio::time::Instant::now() + STEP;
    while !read_frontier(&admin, &index)
        .await
        .is_some_and(|t| t > frontier.resume_as_of())
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the index did not compact past F - 1"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let failure = fails(subscribe(&object), start).await;
    assert_eq!(
        failure.classification,
        Classification::CheckIndex,
        "{failure:?}"
    );
    assert_eq!(indexes(&admin, &object).await, ["scenario_indexed_id"]);
    assert!(
        resume_margin(&admin, &object, frontier).await.is_some(),
        "the storage collection still holds the history"
    );
    execute(&admin, &format!("DROP TABLE {relation}")).await;
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn dropping_the_object_fails_the_stream() {
    let admin = admin().await;
    let object = table(&admin, "scenario_dropped", "(id int)").await;
    let mut target = Target::default();
    let mut cursor = Cursor::open(&config(), subscribe(&object), SNAPSHOT)
        .await
        .unwrap();
    cursor
        .fetch_until(&mut target, |t| t.frontier.is_some())
        .await
        .unwrap();
    execute(&admin, "DROP TABLE materialize.public.scenario_dropped").await;
    let failure = cursor.drain(&mut target).await.unwrap_err();
    assert_eq!(failure.sqlstate.as_deref(), Some("42704"), "{failure:?}");
    let Classification::CheckObject { otherwise } = failure.classification else {
        panic!("{failure:?}");
    };
    let change = identity_change(&admin, &object).await;
    assert_eq!(change, IdentityChange::Dropped);
    assert_eq!(
        resolve_object_check(otherwise, change),
        ObjectOutcome::Error {
            kind: ErrorKind::ObjectDropped
        }
    );
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_dataflow_error_poisons_the_stream() {
    let admin = admin().await;
    let relation = "materialize.public.scenario_poisoned";
    table(&admin, "scenario_poisoned", "(id int)").await;
    execute(&admin, &format!("INSERT INTO {relation} VALUES (1)")).await;
    let query = Subscribe::query(format!("SELECT 1 / id AS v FROM {relation}"));
    let mut target = Target::default();
    let mut cursor = Cursor::open(&config(), query, SNAPSHOT).await.unwrap();
    cursor
        .fetch_until(&mut target, |t| t.has("1"))
        .await
        .unwrap();
    execute(&admin, &format!("INSERT INTO {relation} VALUES (0)")).await;
    let failure = cursor.drain(&mut target).await.unwrap_err();
    assert!(failure.message.contains("division by zero"), "{failure:?}");
    assert_eq!(
        failure.classification,
        Classification::Kind {
            kind: ErrorKind::StreamPoisoned
        }
    );
    drop(cursor);
    execute(&admin, &format!("DROP TABLE {relation}")).await;
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL with ALTER TABLE ADD COLUMN enabled"]
async fn a_column_added_before_the_first_fetch_is_a_schema_change() {
    let admin = admin().await;
    let relation = "materialize.public.scenario_altered";
    let object = table(&admin, "scenario_altered", "(id int)").await;
    let mut cursor = Cursor::open(&config(), subscribe(&object), SNAPSHOT)
        .await
        .unwrap();
    execute(&admin, &format!("ALTER TABLE {relation} ADD COLUMN v text")).await;
    let failure = cursor.fetch().await.unwrap_err();
    assert_eq!(failure.sqlstate.as_deref(), Some("0A000"), "{failure:?}");
    let Classification::CheckObject { otherwise } = failure.classification else {
        panic!("{failure:?}");
    };
    let change = identity_change(&admin, &object).await;
    assert_eq!(change, IdentityChange::SchemaChanged);
    assert_eq!(
        resolve_object_check(otherwise, change),
        ObjectOutcome::Error {
            kind: ErrorKind::SchemaMismatch
        }
    );
    drop(cursor);
    execute(&admin, &format!("DROP TABLE {relation}")).await;
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_swapped_name_is_caught_only_by_the_identity_check() {
    let admin = admin().await;
    for (schema, id) in [("scenario_swap_a", 1), ("scenario_swap_b", 2)] {
        execute(
            &admin,
            &format!("DROP SCHEMA IF EXISTS materialize.{schema} CASCADE"),
        )
        .await;
        execute(&admin, &format!("CREATE SCHEMA materialize.{schema}")).await;
        execute(
            &admin,
            &format!("CREATE TABLE materialize.{schema}.t (id int) {RETAINED}"),
        )
        .await;
        execute(
            &admin,
            &format!("INSERT INTO materialize.{schema}.t VALUES ({id})"),
        )
        .await;
    }
    let stored = lookup(&admin, "scenario_swap_a", "t").await.unwrap();
    let mut target = Target::default();
    let mut cursor = Cursor::open(&config(), subscribe(&stored), SNAPSHOT)
        .await
        .unwrap();
    cursor
        .fetch_until(&mut target, |t| t.has("1"))
        .await
        .unwrap();
    drop(cursor);
    execute(
        &admin,
        "ALTER SCHEMA materialize.scenario_swap_a SWAP WITH scenario_swap_b",
    )
    .await;

    // Resuming by catalog id still reads the table the target holds, now
    // under the other name, without an error.
    let old = "materialize.scenario_swap_b.t";
    execute(&admin, &format!("INSERT INTO {old} VALUES (3)")).await;
    let start = Start::Resume {
        frontier: target.frontier.unwrap(),
    };
    let mut cursor = Cursor::open(&config(), subscribe(&stored), start)
        .await
        .unwrap();
    cursor
        .fetch_until(&mut target, |t| t.has("3"))
        .await
        .unwrap();
    target.assert_matches(&admin, old).await;
    drop(cursor);
    assert_eq!(
        identity_change(&admin, &stored).await,
        IdentityChange::Swapped
    );
    for schema in ["scenario_swap_a", "scenario_swap_b"] {
        execute(&admin, &format!("DROP SCHEMA materialize.{schema} CASCADE")).await;
    }
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_replaced_view_resumes_under_its_new_id() {
    let admin = admin().await;
    let base = "materialize.public.scenario_replaced_base";
    let view = "materialize.public.scenario_replaced";
    table(&admin, "scenario_replaced_base", "(id int)").await;
    execute(&admin, &format!("INSERT INTO {base} VALUES (1)")).await;
    execute(
        &admin,
        &format!("CREATE MATERIALIZED VIEW {view} {RETAINED} AS SELECT id FROM {base}"),
    )
    .await;
    let stored = lookup(&admin, "public", "scenario_replaced").await.unwrap();
    let mut target = Target::default();
    let mut cursor = Cursor::open(&config(), subscribe(&stored), SNAPSHOT)
        .await
        .unwrap();
    cursor
        .fetch_until(&mut target, |t| t.has("1"))
        .await
        .unwrap();
    drop(cursor);
    execute(
        &admin,
        &format!(
            "CREATE REPLACEMENT MATERIALIZED VIEW materialize.public.scenario_replacement \
             FOR {view} {RETAINED} AS SELECT id + 1 AS id FROM {base}"
        ),
    )
    .await;
    execute(
        &admin,
        &format!("ALTER MATERIALIZED VIEW {view} APPLY REPLACEMENT scenario_replacement"),
    )
    .await;

    let frontier = target.frontier.unwrap();
    let start = Start::Resume { frontier };
    let failure = fails(subscribe(&stored), start).await;
    assert_eq!(failure.sqlstate.as_deref(), Some("XX000"), "{failure:?}");
    assert!(failure.message.contains("invalid id"), "{failure:?}");
    let Classification::CheckObject { otherwise } = failure.classification else {
        panic!("{failure:?}");
    };
    let change = identity_change(&admin, &stored).await;
    assert_eq!(change, IdentityChange::Replaced);
    assert_eq!(
        resolve_object_check(otherwise, change),
        ObjectOutcome::Replaced
    );
    let current = lookup(&admin, "public", "scenario_replaced").await.unwrap();
    assert!(resume_margin(&admin, &current, frontier).await.is_some());
    let mut cursor = Cursor::open(&config(), subscribe(&current), start)
        .await
        .unwrap();
    cursor
        .fetch_until(&mut target, |t| t.has("2") && !t.has("1"))
        .await
        .unwrap();
    target.assert_matches(&admin, view).await;
    drop(cursor);
    execute(&admin, &format!("DROP TABLE {base} CASCADE")).await;
}
