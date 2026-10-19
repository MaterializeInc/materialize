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

//! Durable subscriptions into in-memory sinks, against a running Materialize.

// The repository's clippy rules ask for wrappers a standalone SDK cannot use.
#![allow(clippy::disallowed_methods)]

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{STEP, admin, config, execute, rows};
use materialize_sdk::core::catalog;
use materialize_sdk::core::catalog::ObjectName;
use materialize_sdk::core::{Checkpoint, CommitConflict, Envelope};
use materialize_sdk::{
    Change, CheckpointStore, CheckpointWrite, Classification, Column, DurableOptions, DurableSink,
    Error, ErrorKind, IdentityChange, LiveOptions, MaterializeStore, Row, Sink, SinkBatch,
    SinkError, SinkType, WriteRequest, WriteResult, checkpoint_table_sql,
};
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;

/// A target with transactions: rows tagged with their generation, and the
/// checkpoint, changed together under one lock.
#[derive(Debug, Default)]
struct Target {
    checkpoint: Option<String>,
    rows: BTreeMap<(u64, Row), i64>,
    snapshot_writes: usize,
    /// How many applied snapshot chunks still report a lost response.
    lose_chunk_responses: usize,
    /// How many snapshot chunks still fail with a lost response before they
    /// apply.
    drop_chunks: usize,
    chunk_writes: usize,
}

#[derive(Clone, Debug, Default)]
struct Transactional(Arc<Mutex<Target>>);

impl Transactional {
    /// The target's rows, with their multiplicity.
    fn rows(&self) -> BTreeMap<Row, i64> {
        let target = self.0.lock().unwrap();
        let mut rows = BTreeMap::new();
        for ((_, row), count) in &target.rows {
            *rows.entry(row.clone()).or_default() += count;
        }
        rows
    }

    fn checkpoint(&self) -> Option<Checkpoint> {
        let text = self.0.lock().unwrap().checkpoint.clone()?;
        Some(Checkpoint::decode(&text).unwrap())
    }

    fn snapshot_writes(&self) -> usize {
        self.0.lock().unwrap().snapshot_writes
    }
}

/// A first checkpoint expects none, so it applies only while there is none.
fn compare_and_set(target: &mut Target, write: &CheckpointWrite) -> bool {
    if target.checkpoint != write.expected {
        return false;
    }
    target.checkpoint = Some(write.new.clone());
    true
}

impl Sink for Transactional {
    fn sink_type(&self) -> SinkType {
        SinkType::Transactional
    }

    async fn load(&mut self) -> Result<Option<String>, SinkError> {
        Ok(self.0.lock().unwrap().checkpoint.clone())
    }

    async fn write(&mut self, request: WriteRequest<'_>) -> WriteResult {
        let checkpoint = request.checkpoint().expect("a checkpoint write");
        let mut target = self.0.lock().unwrap();
        let Some(batch) = request.batch() else {
            return match compare_and_set(&mut target, checkpoint) {
                true => WriteResult::applied(),
                false => WriteResult::NotApplied,
            };
        };
        let chunk = checkpoint.expected.as_deref() == Some(checkpoint.new.as_str());
        if chunk {
            target.chunk_writes += 1;
            if target.drop_chunks > 0 {
                target.drop_chunks -= 1;
                return WriteResult::Unknown;
            }
        }
        if !compare_and_set(&mut target, checkpoint) {
            return WriteResult::NotApplied;
        }
        // A snapshot chunk leaves the checkpoint unchanged, so it can arrive
        // twice. Each row arrives once per snapshot with its full count, so
        // setting the count makes a repeat change nothing.
        for update in batch.updates() {
            if let Change::Diff { row, diff } = &update.change {
                let count = target
                    .rows
                    .entry((batch.generation(), row.clone()))
                    .or_default();
                match chunk {
                    true => *count = *diff,
                    false => *count += diff,
                }
            }
        }
        target.rows.retain(|_, count| *count != 0);
        if batch.is_snapshot() {
            target.snapshot_writes += 1;
        }
        if chunk && target.lose_chunk_responses > 0 {
            target.lose_chunk_responses -= 1;
            return WriteResult::Unknown;
        }
        WriteResult::applied()
    }

    async fn sweep_generations(&mut self, generation: u64) -> Result<(), SinkError> {
        let mut target = self.0.lock().unwrap();
        target.rows.retain(|(written, _), _| *written >= generation);
        Ok(())
    }
}

/// A target without transactions or a checkpoint, such as a webhook.
#[derive(Clone, Debug, Default)]
struct Events(Arc<Mutex<Vec<SinkBatch>>>);

impl Events {
    /// The first column of every delivered row.
    fn ids(&self) -> Vec<String> {
        let batches = self.0.lock().unwrap();
        let mut ids = Vec::new();
        for update in batches.iter().flat_map(|batch| batch.updates()) {
            if let Change::Diff { row, .. } = &update.change {
                ids.extend(row.first().cloned().flatten());
            }
        }
        ids
    }
}

impl Sink for Events {
    fn sink_type(&self) -> SinkType {
        SinkType::NonTransactional
    }

    async fn write(&mut self, request: WriteRequest<'_>) -> WriteResult {
        let WriteRequest::Batch(batch) = request else {
            unreachable!("the SDK keeps this sink's checkpoint")
        };
        self.0.lock().unwrap().push(batch.clone());
        WriteResult::applied()
    }
}

fn options() -> DurableOptions {
    DurableOptions::default().commit_interval(Duration::from_millis(100))
}

/// A fresh table with `(id int)` and an hour of history, so that a restart
/// can resume, named for this test run so that runs do not collide. Returns
/// its name and its qualified name for SQL.
async fn source(name: &str) -> (ObjectName, String) {
    let name = format!("{name}_{}", std::process::id());
    let admin = admin().await;
    common::table(&admin, &name, "(id int) WITH (RETAIN HISTORY = FOR '1h')").await;
    let relation = format!("materialize.public.{name}");
    let object = ObjectName {
        database: Some("materialize".into()),
        schema: "public".into(),
        name,
    };
    (object, relation)
}

fn spawn<S, C>(mut durable: DurableSink<S, C>) -> JoinHandle<Result<(), Error>>
where
    S: Sink + 'static,
    C: CheckpointStore + 'static,
{
    tokio::spawn(async move { durable.run(&config(), NoTls).await })
}

/// Waits until `done` holds, for at most one step.
async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + STEP;
    while !done() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The rows of `relation`, with their multiplicity.
async fn table_rows(relation: &str) -> BTreeMap<Row, i64> {
    let admin = admin().await;
    let mut counts = BTreeMap::new();
    for row in rows(&admin, &format!("SELECT * FROM {relation}")).await.1 {
        *counts.entry(row).or_default() += 1;
    }
    counts
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_transactional_sink_resumes_where_it_committed() {
    let (object, relation) = source("durable_resume").await;
    let admin = admin().await;
    execute(&admin, &format!("INSERT INTO {relation} VALUES (1), (2)")).await;
    let sink = Transactional::default();
    let mut setup = DurableSink::new("resume", sink.clone(), options());
    assert!(
        setup
            .setup(&config(), NoTls, &object, Envelope::Diff)
            .await
            .unwrap()
    );
    assert!(
        !setup
            .setup(&config(), NoTls, &object, Envelope::Diff)
            .await
            .unwrap()
    );

    let worker = spawn(DurableSink::new("resume", sink.clone(), options()));
    let expected = table_rows(&relation).await;
    until("the snapshot", || sink.rows() == expected).await;
    execute(&admin, &format!("INSERT INTO {relation} VALUES (3)")).await;
    let expected = table_rows(&relation).await;
    until("the insert", || sink.rows() == expected).await;
    worker.abort();
    let _ = worker.await;

    let generation = sink.checkpoint().unwrap().generation;
    let snapshot_writes = sink.snapshot_writes();
    execute(&admin, &format!("DELETE FROM {relation} WHERE id = 1")).await;
    execute(&admin, &format!("INSERT INTO {relation} VALUES (4)")).await;
    let worker = spawn(DurableSink::new("resume", sink.clone(), options()));
    let expected = table_rows(&relation).await;
    until("the changes made while stopped", || sink.rows() == expected).await;
    worker.abort();
    let checkpoint = sink.checkpoint().unwrap();
    assert_eq!(
        checkpoint.generation, generation,
        "the resume took no new snapshot"
    );
    assert_eq!(sink.snapshot_writes(), snapshot_writes);
    assert_eq!(checkpoint.epoch, 3, "setup, then two take-overs");
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_snapshot_chunk_whose_response_was_lost_applies_once() {
    let (object, relation) = source("durable_chunks").await;
    let admin = admin().await;
    execute(
        &admin,
        &format!("INSERT INTO {relation} VALUES (1), (2), (3)"),
    )
    .await;
    let sink = Transactional::default();
    {
        let mut target = sink.0.lock().unwrap();
        // The first chunk is lost without applying, then applies twice with
        // its response lost, so the SDK sends it three more times.
        target.lose_chunk_responses = 2;
        target.drop_chunks = 1;
    }
    // A memory limit of one byte releases each snapshot update as its own chunk.
    let options = options().live(LiveOptions::default().max_pending_bytes(Some(1)));
    DurableSink::new("chunks", sink.clone(), options.clone())
        .setup(&config(), NoTls, &object, Envelope::Diff)
        .await
        .unwrap();
    let worker = spawn(DurableSink::new("chunks", sink.clone(), options));
    until("the snapshot to complete", || {
        sink.checkpoint().unwrap().frontier.is_some()
    })
    .await;
    worker.abort();
    let (lost, dropped, chunk_writes) = {
        let target = sink.0.lock().unwrap();
        (
            target.lose_chunk_responses,
            target.drop_chunks,
            target.chunk_writes,
        )
    };
    assert_eq!((lost, dropped), (0, 0));
    assert_eq!(
        chunk_writes,
        3 + 3,
        "one chunk per row, and three sent again"
    );
    assert_eq!(sink.rows(), table_rows(&relation).await);
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_newer_worker_fences_the_older_one() {
    let (object, relation) = source("durable_fence").await;
    let admin = admin().await;
    execute(&admin, &format!("INSERT INTO {relation} VALUES (1)")).await;
    let sink = Transactional::default();
    DurableSink::new("fence", sink.clone(), options())
        .setup(&config(), NoTls, &object, Envelope::Diff)
        .await
        .unwrap();
    let older = spawn(DurableSink::new("fence", sink.clone(), options()));
    let expected = table_rows(&relation).await;
    until("the snapshot", || sink.rows() == expected).await;

    let newer = spawn(DurableSink::new("fence", sink.clone(), options()));
    until("the take-over", || sink.checkpoint().unwrap().epoch == 3).await;
    execute(&admin, &format!("INSERT INTO {relation} VALUES (2)")).await;
    let fenced = tokio::time::timeout(STEP, older).await.unwrap().unwrap();
    assert!(
        matches!(fenced, Err(Error::Conflict(CommitConflict::Fenced { .. }))),
        "{fenced:?}"
    );
    let expected = table_rows(&relation).await;
    until("the newer worker's write", || sink.rows() == expected).await;
    newer.abort();
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_sink_without_a_checkpoint_resumes_from_the_store() {
    let (object, relation) = source("durable_events").await;
    let admin = admin().await;
    execute(&admin, &format!("INSERT INTO {relation} VALUES (1)")).await;
    let table = ObjectName {
        database: Some("materialize".into()),
        schema: "public".into(),
        name: format!("durable_checkpoints_{}", std::process::id()),
    };
    execute(
        &admin,
        &format!("DROP TABLE IF EXISTS materialize.public.{}", table.name),
    )
    .await;
    execute(&admin, &checkpoint_table_sql(&table)).await;
    let store = || async {
        MaterializeStore::connect(&config(), NoTls, &table)
            .await
            .unwrap()
    };
    let events = Events::default();
    DurableSink::new("events", events.clone(), options())
        .with_store(store().await)
        .setup(&config(), NoTls, &object, Envelope::Diff)
        .await
        .unwrap();

    let worker =
        spawn(DurableSink::new("events", events.clone(), options()).with_store(store().await));
    until("the snapshot", || events.ids() == ["1"]).await;
    execute(&admin, &format!("INSERT INTO {relation} VALUES (2)")).await;
    until("the insert", || events.ids() == ["1", "2"]).await;
    let inserted_at = events.0.lock().unwrap().last().unwrap().updates()[0].timestamp;
    // The checkpoint commits after the write, so wait until it covers it.
    let deadline = tokio::time::Instant::now() + STEP;
    loop {
        let text = store().await.load("events").await.unwrap().unwrap();
        let frontier = Checkpoint::decode(&text).unwrap().frontier;
        if frontier.is_some_and(|frontier| frontier.contains(inserted_at)) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the commit"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    worker.abort();
    let _ = worker.await;

    events.0.lock().unwrap().clear();
    execute(&admin, &format!("INSERT INTO {relation} VALUES (3)")).await;
    let worker =
        spawn(DurableSink::new("events", events.clone(), options()).with_store(store().await));
    until("the insert made while stopped", || {
        events.ids().contains(&"3".to_string())
    })
    .await;
    worker.abort();
    assert_eq!(
        events.ids(),
        ["3"],
        "nothing committed before the restart arrives again"
    );
    assert!(
        events.0.lock().unwrap().iter().all(|b| !b.is_snapshot()),
        "the resume took no new snapshot"
    );
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_dropped_object_stops_the_sink_as_dropped() {
    let (object, relation) = source("durable_dropped").await;
    let target = Transactional::default();
    DurableSink::new("dropped", target.clone(), options())
        .setup(&config(), NoTls, &object, Envelope::Diff)
        .await
        .unwrap();
    execute(&admin().await, &format!("DROP TABLE {relation}")).await;
    let stopped = DurableSink::new("dropped", target, options())
        .run(&config(), NoTls)
        .await;
    assert!(
        matches!(
            stopped,
            Err(Error::ObjectChanged {
                change: IdentityChange::Dropped
            })
        ),
        "{stopped:?}"
    );
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_resume_past_an_index_history_names_the_index() {
    let (object, relation) = source("durable_indexed").await;
    let admin = admin().await;
    execute(&admin, &format!("INSERT INTO {relation} VALUES (1)")).await;
    let target = Transactional::default();
    DurableSink::new("indexed", target.clone(), options())
        .setup(&config(), NoTls, &object, Envelope::Diff)
        .await
        .unwrap();
    let worker = spawn(DurableSink::new("indexed", target.clone(), options()));
    until("the snapshot", || target.rows().len() == 1).await;
    worker.abort();
    let _ = worker.await;
    let frontier = target.checkpoint().unwrap().frontier.unwrap();

    // An index on the session's cluster keeps a second of history, so once
    // its read frontier passes the resume point, the resume fails.
    execute(
        &admin,
        &format!("CREATE INDEX durable_indexed_id ON {relation} (id)"),
    )
    .await;
    let (_, found) = rows(
        &admin,
        "SELECT id FROM mz_catalog.mz_indexes WHERE name = 'durable_indexed_id'",
    )
    .await;
    let index = found[0][0].clone().unwrap();
    let deadline = tokio::time::Instant::now() + STEP;
    loop {
        execute(&admin, &format!("INSERT INTO {relation} VALUES (2)")).await;
        let (columns, found) = rows(&admin, &catalog::read_frontier_sql(&index)).await;
        let read = catalog::parse_read_frontier(&columns, found).unwrap();
        if read.is_some_and(|read| read > frontier.resume_as_of()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the index kept its history"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let stopped = DurableSink::new("indexed", target, options())
        .run(&config(), NoTls)
        .await;
    assert!(
        matches!(
            &stopped,
            Err(Error::Server {
                classification: Classification::Kind {
                    kind: ErrorKind::IndexedTarget
                },
                ..
            })
        ),
        "{stopped:?}"
    );
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn batches_name_their_columns_with_the_key_first() {
    let name = format!("durable_columns_{}", std::process::id());
    let admin = admin().await;
    common::table(&admin, &name, "(id int, name text, region text)").await;
    execute(
        &admin,
        &format!("INSERT INTO materialize.public.{name} VALUES (1, 'a', 'eu')"),
    )
    .await;
    let object = ObjectName {
        database: Some("materialize".into()),
        schema: "public".into(),
        name,
    };
    let table = ObjectName {
        database: Some("materialize".into()),
        schema: "public".into(),
        name: format!("durable_columns_checkpoints_{}", std::process::id()),
    };
    execute(
        &admin,
        &format!("DROP TABLE IF EXISTS materialize.public.{}", table.name),
    )
    .await;
    execute(&admin, &checkpoint_table_sql(&table)).await;
    let store = || async {
        MaterializeStore::connect(&config(), NoTls, &table)
            .await
            .unwrap()
    };
    let envelope = Envelope::Upsert {
        key: vec!["region".into(), "id".into()],
    };
    let events = Events::default();
    DurableSink::new("columns", events.clone(), options())
        .with_store(store().await)
        .setup(&config(), NoTls, &object, envelope)
        .await
        .unwrap();

    let worker =
        spawn(DurableSink::new("columns", events.clone(), options()).with_store(store().await));
    until("the snapshot", || {
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|batch| !batch.updates().is_empty())
    })
    .await;
    worker.abort();
    let _ = worker.await;

    let batches = events.0.lock().unwrap();
    let batch = batches.iter().find(|b| !b.updates().is_empty()).unwrap();
    let names = |columns: &[Column]| columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
    assert_eq!(names(&batch.columns().key), ["region", "id"]);
    assert_eq!(names(&batch.columns().value), ["name"]);
    let Change::Upsert { key, value } = &batch.updates()[0].change else {
        panic!("expected an upsert, got {:?}", batch.updates()[0]);
    };
    assert_eq!(key, &[Some("eu".to_string()), Some("1".to_string())]);
    assert_eq!(value, &[Some("a".to_string())]);
}
