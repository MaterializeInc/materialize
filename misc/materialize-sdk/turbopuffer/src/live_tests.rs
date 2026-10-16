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

//! Tests against turbopuffer, with the key in `TURBOPUFFER_API_KEY`, and the
//! end-to-end tests also against a Materialize at `MATERIALIZE_URL`. Each
//! test writes only namespaces named after itself and deletes them at the end.

// The repository's clippy rules ask for wrappers a standalone SDK cannot use.
#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use materialize_sdk::core::Checkpoint;
use materialize_sdk::{
    Change, CheckpointWrite, Column, CommitConflict, DurableOptions, DurableSink, Envelope, Error,
    Frontier, ObjectName, PayloadColumns, Sink, SinkBatch, SinkError, Update, WriteResult,
};
use serde_json::{Value, json};
use tokio_postgres::NoTls;

use crate::checkpoint::Checkpoints;
use crate::client::{Client, Connection, Fault};
use crate::reader::{ReadError, Reader};
use crate::sink::{TurbopufferSink, checkpoint_namespace, data_namespace};

const STEP: Duration = Duration::from_secs(90);

fn connection() -> Connection {
    let key = std::env::var("TURBOPUFFER_API_KEY").expect("TURBOPUFFER_API_KEY is not set");
    let region = std::env::var("TURBOPUFFER_REGION").unwrap_or_else(|_| "gcp-us-central1".into());
    Connection::new(&region, key)
}

/// A name no other test run uses.
fn unique(test: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("sdk-test-{test}-{}-{nanos}", std::process::id())
}

/// Deletes the namespaces named after a test when it ends, also when it
/// fails.
struct Cleanup(String);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let name = self.0.clone();
        // A test's runtime may be shutting down, so this one runs on its own.
        let deleted = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let client = Client::new(&connection()).unwrap();
                for namespace in client.namespaces(&format!("{name}__")).await? {
                    client.delete_namespace(&namespace).await?;
                }
                Ok::<_, crate::client::Failure>(())
            })
        })
        .join();
        if !matches!(deleted, Ok(Ok(()))) {
            eprintln!("could not delete the namespaces of {}: {deleted:?}", self.0);
        }
    }
}

/// The documents of `namespace` that are not tombstones, by id.
async fn live_documents(client: &Client, namespace: &str) -> BTreeMap<String, Value> {
    let body = json!({
        "filters": ["deleted", "Eq", false],
        "rank_by": ["id", "asc"],
        "limit": 1000,
        "include_attributes": true,
    });
    let rows = client
        .query(namespace, &body)
        .await
        .unwrap()
        .unwrap_or_default();
    rows.into_iter()
        .map(|mut row| {
            let id = row["id"].as_str().unwrap().to_string();
            for meta in ["id", "mz_timestamp", "deleted", "$dist"] {
                row.as_object_mut().unwrap().remove(meta);
            }
            (id, row)
        })
        .collect()
}

async fn document(client: &Client, namespace: &str, id: &str) -> Option<Value> {
    let body = json!({
        "filters": ["id", "Eq", id],
        "rank_by": ["id", "asc"],
        "limit": 1,
        "include_attributes": true,
    });
    let rows = client.query(namespace, &body).await.unwrap()?;
    rows.into_iter().next()
}

async fn until(what: &str, mut done: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + STEP;
    while !done().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test]
#[ignore = "needs TURBOPUFFER_API_KEY"]
async fn the_checkpoint_document_is_a_compare_and_set() {
    let name = unique("checkpoint");
    let _cleanup = Cleanup(name.clone());
    let client = Client::new(&connection()).unwrap();
    let mut worker = Checkpoints::new(client.clone(), checkpoint_namespace(&name), "s".into());

    let first = Checkpoint::new(BTreeMap::new()).committed(Frontier::new(10).unwrap());
    let text = first.encode().unwrap();
    assert!(matches!(
        worker.create(&text).await,
        WriteResult::Applied { .. }
    ));
    assert!(matches!(
        worker.create(&text).await,
        WriteResult::NotApplied
    ));
    assert_eq!(worker.load().await.unwrap().unwrap().checkpoint, text);
    let mut other = Checkpoints::new(client.clone(), checkpoint_namespace(&name), "t".into());
    assert!(
        other.load().await.is_err(),
        "another sink id must not read it"
    );

    let (take_over, taken) = CheckpointWrite::take_over(&first).unwrap();
    assert!(matches!(
        worker.commit(&take_over).await,
        WriteResult::Applied { .. }
    ));

    // A newer worker commits, so the older one's next patch finds another
    // revision even though it expects the text it last wrote.
    let mut newer = Checkpoints::new(client.clone(), checkpoint_namespace(&name), "s".into());
    newer.load().await.unwrap();
    let (commit, committed) = CheckpointWrite::commit(&taken, Frontier::new(20).unwrap()).unwrap();
    assert!(matches!(
        newer.commit(&commit).await,
        WriteResult::Applied { .. }
    ));
    let (stale, _) = CheckpointWrite::commit(&taken, Frontier::new(30).unwrap()).unwrap();
    assert!(matches!(
        worker.commit(&stale).await,
        WriteResult::NotApplied
    ));
    let stored = worker.load().await.unwrap().unwrap();
    assert_eq!(
        (stored.checkpoint, stored.revision),
        (commit.new.clone(), 2)
    );

    let (lost, after_lost) =
        CheckpointWrite::commit(&committed, Frontier::new(40).unwrap()).unwrap();
    client.faults.lock().unwrap().plan.push_back(Fault::Lose);
    assert!(matches!(worker.commit(&lost).await, WriteResult::Unknown));
    assert_eq!(worker.load().await.unwrap().unwrap().checkpoint, lost.new);

    let (dropped, _) = CheckpointWrite::commit(&after_lost, Frontier::new(50).unwrap()).unwrap();
    client.faults.lock().unwrap().plan.push_back(Fault::Drop);
    assert!(matches!(
        worker.commit(&dropped).await,
        WriteResult::Unknown
    ));
    assert_eq!(worker.load().await.unwrap().unwrap().checkpoint, lost.new);
    assert!(matches!(
        worker.commit(&dropped).await,
        WriteResult::Applied { .. }
    ));
    let stored = worker.load().await.unwrap().unwrap();
    assert_eq!(
        (stored.checkpoint, stored.revision),
        (dropped.new.clone(), 4)
    );
}

#[tokio::test]
#[ignore = "needs TURBOPUFFER_API_KEY"]
async fn late_copies_of_a_create_or_a_patch_change_nothing() {
    let name = unique("late");
    let _cleanup = Cleanup(name.clone());
    let client = Client::new(&connection()).unwrap();
    let mut worker = Checkpoints::new(client.clone(), checkpoint_namespace(&name), "s".into());
    let late = || Fault::Late(Duration::from_secs(3));

    let first = Checkpoint::new(BTreeMap::new()).committed(Frontier::new(10).unwrap());
    let text = first.encode().unwrap();
    client.faults.lock().unwrap().plan.push_back(late());
    assert!(matches!(worker.create(&text).await, WriteResult::Unknown));
    assert!(matches!(
        worker.create(&text).await,
        WriteResult::Applied { .. }
    ));
    let (take_over, taken) = CheckpointWrite::take_over(&first).unwrap();
    assert!(matches!(
        worker.commit(&take_over).await,
        WriteResult::Applied { .. }
    ));

    // The same commit, sent again after its first copy got stuck, and a newer
    // commit after it. The stuck copy lands last.
    let (commit, committed) = CheckpointWrite::commit(&taken, Frontier::new(20).unwrap()).unwrap();
    client.faults.lock().unwrap().plan.push_back(late());
    assert!(matches!(worker.commit(&commit).await, WriteResult::Unknown));
    assert_eq!(worker.load().await.unwrap().unwrap().revision, 1);
    assert!(matches!(
        worker.commit(&commit).await,
        WriteResult::Applied { .. }
    ));
    let (newer, _) = CheckpointWrite::commit(&committed, Frontier::new(30).unwrap()).unwrap();
    assert!(matches!(
        worker.commit(&newer).await,
        WriteResult::Applied { .. }
    ));

    tokio::time::sleep(Duration::from_secs(6)).await;
    let stored = worker.load().await.unwrap().unwrap();
    assert_eq!((stored.checkpoint, stored.revision), (newer.new.clone(), 3));
}

#[tokio::test]
#[ignore = "needs TURBOPUFFER_API_KEY"]
async fn setup_refuses_leftover_incarnations_but_runs_again_on_its_own() {
    let name = unique("setup");
    let _cleanup = Cleanup(name.clone());
    let client = Client::new(&connection()).unwrap();
    let leftover = data_namespace(&name, 3);
    let leave = || async {
        let row = json!({"upsert_rows": [{"id": "x", "mz_timestamp": 1, "deleted": false}]});
        client.write(&leftover, row.to_string()).await.unwrap();
    };
    let text = crate::sink::tests::checkpoint(crate::sink::tests::keyed());
    let mut sink = TurbopufferSink::new(&connection(), "s", &name).unwrap();

    leave().await;
    match sink.create(&text).await {
        WriteResult::Failed(SinkError::Target { permanent, .. }) => assert!(permanent),
        other => panic!("expected a refusal, got {other:?}"),
    }
    client.delete_namespace(&leftover).await.unwrap();
    assert!(matches!(
        sink.create(&text).await,
        WriteResult::Applied { .. }
    ));
    // A running sink's incarnations do not stop its setup from running again.
    leave().await;
    assert!(matches!(sink.create(&text).await, WriteResult::NotApplied));
}

fn columns() -> PayloadColumns {
    let column = |name: &str, type_oid| Column {
        name: name.into(),
        type_oid,
        type_mod: -1,
        nullable: true,
    };
    PayloadColumns {
        key: vec![column("id", 23)],
        value: vec![column("title", 25), column("views", 20)],
    }
}

fn upsert(timestamp: u64, id: &str, title: &str, views: &str) -> Update {
    Update {
        timestamp,
        change: Change::Upsert {
            key: vec![Some(id.into())],
            value: vec![Some(title.into()), Some(views.into())],
        },
    }
}

fn delete(timestamp: u64, id: &str) -> Update {
    Update {
        timestamp,
        change: Change::Delete {
            key: vec![Some(id.into())],
        },
    }
}

#[tokio::test]
#[ignore = "needs TURBOPUFFER_API_KEY"]
async fn data_writes_are_safe_to_repeat_and_to_apply_late() {
    let name = unique("data");
    let _cleanup = Cleanup(name.clone());
    let mut sink = TurbopufferSink::new(&connection(), "s", &name)
        .unwrap()
        .filterable(["title"])
        .full_text_search(["title"]);
    let namespace = data_namespace(&name, 2);
    let batch = |updates| SinkBatch::new(updates, columns(), 2, false);
    let applied = |result: WriteResult| match result {
        WriteResult::Applied { rejected } => assert!(rejected.is_empty(), "{rejected:?}"),
        other => panic!("expected applied, got {other:?}"),
    };

    let first = batch(vec![upsert(5, "1", "a", "1"), upsert(5, "2", "b", "2")]);
    applied(sink.write(&first, None).await);
    applied(
        sink.write(&batch(vec![delete(7, "1"), upsert(7, "2", "B", "3")]), None)
            .await,
    );
    // A replay of the first batch, as after a crash before its commit.
    applied(sink.write(&first, None).await);
    let documents = live_documents(&sink.client, &namespace).await;
    assert_eq!(
        documents,
        BTreeMap::from([("2".into(), json!({"title": "B", "views": 3}))])
    );
    let tombstone = document(&sink.client, &namespace, "1").await.unwrap();
    assert_eq!(
        (&tombstone["deleted"], &tombstone["mz_timestamp"]),
        (&json!(true), &json!(7))
    );

    // A key that comes back after its tombstone.
    applied(
        sink.write(&batch(vec![upsert(9, "1", "again", "4")]), None)
            .await,
    );
    assert_eq!(
        live_documents(&sink.client, &namespace).await["1"],
        json!({"title": "again", "views": 4})
    );
    let search = json!({
        "rank_by": ["title", "BM25", "again"],
        "filters": ["And", [["deleted", "Eq", false], ["title", "Eq", "again"]]],
        "limit": 5,
    });
    let found = sink
        .client
        .query(&namespace, &search)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        found
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>(),
        [json!("1")]
    );

    // An older tombstone that lands after a newer upsert changes nothing.
    applied(
        sink.write(&batch(vec![upsert(13, "6", "f", "8")]), None)
            .await,
    );
    applied(sink.write(&batch(vec![delete(12, "6")]), None).await);
    assert!(
        live_documents(&sink.client, &namespace)
            .await
            .contains_key("6")
    );

    // A batch in three requests whose second is lost: the resend skips the
    // first, which turbopuffer acknowledged.
    sink.request_rows = 1;
    let three = batch(vec![
        upsert(11, "3", "c", "5"),
        upsert(11, "4", "d", "6"),
        upsert(11, "5", "e", "7"),
    ]);
    let sent_before = sink.client.faults.lock().unwrap().sent;
    sink.client
        .faults
        .lock()
        .unwrap()
        .plan
        .extend([Fault::Pass, Fault::Drop]);
    assert!(matches!(
        sink.write(&three, None).await,
        WriteResult::Failed(_)
    ));
    applied(sink.write(&three, None).await);
    assert_eq!(sink.client.faults.lock().unwrap().sent - sent_before, 3);
    assert_eq!(live_documents(&sink.client, &namespace).await.len(), 6);

    // A write that failed partway, then the same changes in another
    // incarnation, as when a new snapshot gets the same `AS OF`. Every
    // request goes to the new incarnation.
    let changes = vec![
        upsert(15, "7", "g", "9"),
        upsert(15, "8", "h", "10"),
        upsert(15, "9", "i", "11"),
    ];
    sink.client
        .faults
        .lock()
        .unwrap()
        .plan
        .extend([Fault::Pass, Fault::Drop]);
    assert!(matches!(
        sink.write(&batch(changes.clone()), None).await,
        WriteResult::Failed(_)
    ));
    let sent_before = sink.client.faults.lock().unwrap().sent;
    let elsewhere = SinkBatch::new(changes, columns(), 3, true);
    applied(sink.write(&elsewhere, None).await);
    assert_eq!(sink.client.faults.lock().unwrap().sent - sent_before, 3);
    assert_eq!(
        live_documents(&sink.client, &data_namespace(&name, 3))
            .await
            .len(),
        3
    );
}

#[tokio::test]
#[ignore = "needs TURBOPUFFER_API_KEY"]
async fn the_completing_commit_makes_the_snapshot_active_once_indexed() {
    let name = unique("index");
    let _cleanup = Cleanup(name.clone());
    let mut sink = TurbopufferSink::new(&connection(), "s", &name)
        .unwrap()
        .max_unindexed_bytes(0);
    let created = Checkpoint::new(BTreeMap::new());
    assert!(matches!(
        sink.create(&created.encode().unwrap()).await,
        WriteResult::Applied { .. }
    ));
    let (take_over, taken) = CheckpointWrite::take_over(&created).unwrap();
    assert!(matches!(
        sink.commit(&take_over).await,
        WriteResult::Applied { .. }
    ));
    let (start, started) = CheckpointWrite::start_snapshot(&taken, BTreeMap::new(), 5).unwrap();
    assert!(matches!(
        sink.commit(&start).await,
        WriteResult::Applied { .. }
    ));

    let rows = (0..2000).map(|i| upsert(5, &i.to_string(), &"x".repeat(200), "1"));
    let snapshot = SinkBatch::new(rows.collect(), columns(), started.generation, true);
    assert!(matches!(
        sink.write(&snapshot, None).await,
        WriteResult::Applied { .. }
    ));
    let namespace = data_namespace(&name, started.generation);
    let (complete, completed) =
        CheckpointWrite::commit(&started, Frontier::new(6).unwrap()).unwrap();
    // turbopuffer reports a backlog only for large writes, and late, so
    // whether this waits depends on turbopuffer.
    let mut waits = 0;
    loop {
        match sink.commit(&complete).await {
            WriteResult::Applied { .. } => break,
            WriteResult::Failed(SinkError::Throttled { .. }) => {
                assert!(
                    sink.client
                        .unindexed_bytes(&namespace)
                        .await
                        .unwrap()
                        .is_some()
                );
                waits += 1;
                assert!(waits < 120, "the snapshot was never indexed");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            other => panic!("expected the switch or a wait, got {other:?}"),
        }
    }
    assert_eq!(
        sink.client.unindexed_bytes(&namespace).await.unwrap(),
        Some(0)
    );
    let document = sink.checkpoints.load().await.unwrap().unwrap();
    assert_eq!(
        (document.active, document.snapshot),
        (Some(started.generation), None)
    );

    // A re-snapshot switches readers to its incarnation and retires the old.
    let (restart, restarted) =
        CheckpointWrite::start_snapshot(&completed, BTreeMap::new(), 7).unwrap();
    assert!(matches!(
        sink.commit(&restart).await,
        WriteResult::Applied { .. }
    ));
    let rows = vec![upsert(7, "a", "b", "1")];
    let snapshot = SinkBatch::new(rows, columns(), restarted.generation, true);
    assert!(matches!(
        sink.write(&snapshot, None).await,
        WriteResult::Applied { .. }
    ));
    let (complete, _) = CheckpointWrite::commit(&restarted, Frontier::new(8).unwrap()).unwrap();
    loop {
        match sink.commit(&complete).await {
            WriteResult::Applied { .. } => break,
            WriteResult::Failed(SinkError::Throttled { .. }) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            other => panic!("expected the switch or a wait, got {other:?}"),
        }
    }
    let document = sink.checkpoints.load().await.unwrap().unwrap();
    assert_eq!(
        (document.active, document.retired),
        (Some(restarted.generation), vec![started.generation])
    );
}

const MODEL: &str = "nvidia/nemotron-3-embed-8b";

fn titled(timestamp: u64, id: &str, title: Option<&str>, views: &str) -> Update {
    Update {
        timestamp,
        change: Change::Upsert {
            key: vec![Some(id.into())],
            value: vec![title.map(str::to_string), Some(views.into())],
        },
    }
}

async fn vector(client: &Client, namespace: &str, id: &str) -> Vec<f64> {
    let body = json!({
        "filters": ["id", "Eq", id],
        "rank_by": ["id", "asc"],
        "limit": 1,
        "include_attributes": ["embed_title"],
    });
    let rows = client.query(namespace, &body).await.unwrap().unwrap();
    let vector = rows[0]["embed_title"].as_array().unwrap();
    vector.iter().map(|x| x.as_f64().unwrap()).collect()
}

#[tokio::test]
#[ignore = "needs TURBOPUFFER_API_KEY"]
async fn embedded_texts_are_embedded_once_per_change() {
    let name = unique("embed");
    let _cleanup = Cleanup(name.clone());
    let mut sink = TurbopufferSink::new(&connection(), "s", &name)
        .unwrap()
        .embed("title", MODEL, 1024);
    let namespace = data_namespace(&name, 2);
    let applied = |result: WriteResult| match result {
        WriteResult::Applied { rejected } => assert!(rejected.is_empty(), "{rejected:?}"),
        other => panic!("expected applied, got {other:?}"),
    };
    let live = |updates| SinkBatch::new(updates, columns(), 2, false);

    let snapshot = SinkBatch::new(
        vec![
            titled(5, "1", Some("a cat sleeping on a windowsill"), "1"),
            titled(5, "2", Some("a dog walking in the rain"), "2"),
            titled(5, "3", Some("quarterly stock market news"), "3"),
            titled(5, "4", None, "4"),
        ],
        columns(),
        2,
        true,
    );
    applied(sink.write(&snapshot, None).await);
    let after_snapshot = sink.embedding_tokens();
    assert!(after_snapshot > 0);
    let cat = vector(&sink.client, &namespace, "1").await;
    assert_eq!(cat.len(), 1024);
    assert!(
        vector(&sink.client, &namespace, "4")
            .await
            .iter()
            .all(|x| *x == 0.0)
    );

    // A change that keeps the text is a patch, which embeds nothing and
    // keeps the vector.
    let views = live(vec![titled(
        6,
        "1",
        Some("a cat sleeping on a windowsill"),
        "10",
    )]);
    applied(sink.write(&views, None).await);
    assert_eq!(sink.embedding_tokens(), after_snapshot);
    assert_eq!(vector(&sink.client, &namespace, "1").await, cat);
    assert_eq!(
        live_documents(&sink.client, &namespace).await["1"]["views"],
        json!(10)
    );

    // A patch whose response was lost applies again when it is sent again,
    // instead of falling through to an upsert that bills embedding.
    sink.client
        .faults
        .lock()
        .unwrap()
        .plan
        .push_back(Fault::Lose);
    let more_views = live(vec![titled(
        7,
        "1",
        Some("a cat sleeping on a windowsill"),
        "11",
    )]);
    assert!(matches!(
        sink.write(&more_views, None).await,
        WriteResult::Failed(_)
    ));
    applied(sink.write(&more_views, None).await);
    assert_eq!(sink.embedding_tokens(), after_snapshot);
    assert_eq!(
        live_documents(&sink.client, &namespace).await["1"]["views"],
        json!(11)
    );

    // A changed text is embedded again.
    let dog = vector(&sink.client, &namespace, "2").await;
    applied(
        sink.write(
            &live(vec![titled(
                7,
                "2",
                Some("a puppy splashing in puddles"),
                "2",
            )]),
            None,
        )
        .await,
    );
    assert!(sink.embedding_tokens() > after_snapshot);
    assert_ne!(vector(&sink.client, &namespace, "2").await, dog);

    // A value that becomes NULL is cleared by the patch.
    let cleared = Update {
        timestamp: 8,
        change: Change::Upsert {
            key: vec![Some("2".into())],
            value: vec![Some("a puppy splashing in puddles".into()), None],
        },
    };
    let tokens = sink.embedding_tokens();
    applied(sink.write(&live(vec![cleared]), None).await);
    assert_eq!(sink.embedding_tokens(), tokens);
    let document = &live_documents(&sink.client, &namespace).await["2"];
    assert_eq!(document["title"], json!("a puppy splashing in puddles"));
    assert!(document.get("views").is_none(), "{document}");

    // Tombstones and texts that go away carry placeholders, and a text that
    // arrives later replaces one.
    applied(
        sink.write(
            &live(vec![
                delete(8, "3"),
                titled(8, "4", Some("a kitten chasing a ball of yarn"), "4"),
                titled(8, "1", None, "10"),
            ]),
            None,
        )
        .await,
    );
    assert!(
        vector(&sink.client, &namespace, "1")
            .await
            .iter()
            .all(|x| *x == 0.0)
    );
    assert!(
        vector(&sink.client, &namespace, "4")
            .await
            .iter()
            .any(|x| *x != 0.0)
    );
    let search = json!({
        "rank_by": ["title", "ANN", ["Embed", "a small cat playing"]],
        "filters": ["deleted", "Eq", false],
        "limit": 3,
    });
    let found = sink
        .client
        .query(&namespace, &search)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found[0]["id"], json!("4"), "{found:?}");
    assert!(found.iter().all(|row| row["id"] != json!("3")));

    // A replay of an older change leaves the newer document alone.
    applied(sink.write(&views, None).await);
    assert!(
        vector(&sink.client, &namespace, "1")
            .await
            .iter()
            .all(|x| *x == 0.0)
    );

    // A sink set up to embed differently refuses the namespace before it
    // writes to it.
    let mut other = TurbopufferSink::new(&connection(), "s", &name)
        .unwrap()
        .embed("title", MODEL, 512);
    match other
        .write(&live(vec![titled(9, "5", Some("x"), "1")]), None)
        .await
    {
        WriteResult::Failed(SinkError::Target { permanent, .. }) => assert!(permanent),
        result => panic!("expected a refusal, got {result:?}"),
    }
    assert_eq!(other.client.faults.lock().unwrap().sent, 0);
}

#[tokio::test]
#[ignore = "needs TURBOPUFFER_API_KEY"]
async fn a_live_write_after_an_empty_snapshot_creates_the_namespace() {
    let name = unique("empty");
    let _cleanup = Cleanup(name.clone());
    let mut sink = TurbopufferSink::new(&connection(), "s", &name)
        .unwrap()
        .embed("title", MODEL, 1024);
    let first = SinkBatch::new(
        vec![titled(5, "1", Some("hello"), "1")],
        columns(),
        2,
        false,
    );
    match sink.write(&first, None).await {
        WriteResult::Applied { rejected } => assert!(rejected.is_empty()),
        other => panic!("expected applied, got {other:?}"),
    }
    let namespace = data_namespace(&name, 2);
    assert_eq!(vector(&sink.client, &namespace, "1").await.len(), 1024);
}

#[tokio::test]
#[ignore = "needs TURBOPUFFER_API_KEY"]
async fn embedding_waits_for_room_in_the_rate_limit() {
    let name = unique("pace");
    let _cleanup = Cleanup(name.clone());
    let short = SinkBatch::new(vec![titled(5, "1", Some("a"), "1")], columns(), 2, true);
    let mut too_low = TurbopufferSink::new(&connection(), "s", &name)
        .unwrap()
        .embed("title", MODEL, 1024)
        .embedding_rate(1, 1024);
    match too_low.write(&short, None).await {
        WriteResult::Failed(SinkError::Target { permanent, .. }) => assert!(permanent),
        other => panic!("expected a refusal, got {other:?}"),
    }

    // At the lowest rate the sink allows, the second of two long texts waits
    // for the first to leave the window.
    let mut sink = TurbopufferSink::new(&connection(), "s", &name)
        .unwrap()
        .embed("title", MODEL, 1024)
        .embedding_rate(32_768, 1024);
    let long = "word ".repeat(20_000);
    let batch = SinkBatch::new(
        vec![
            titled(5, "1", Some(&long), "1"),
            titled(5, "2", Some(&long), "2"),
        ],
        columns(),
        2,
        true,
    );
    match sink.write(&batch, None).await {
        WriteResult::Failed(SinkError::Throttled {
            retry_after: Some(wait),
        }) => assert!(
            wait > Duration::ZERO && wait <= Duration::from_secs(60),
            "{wait:?}"
        ),
        other => panic!("expected a wait, got {other:?}"),
    }
    assert_eq!(sink.client.faults.lock().unwrap().sent, 1);
    assert_eq!(
        live_documents(&sink.client, &data_namespace(&name, 2))
            .await
            .len(),
        1
    );
}

/// Commits `write`, waiting while the sink waits for turbopuffer's index.
async fn commit_until_applied(sink: &mut TurbopufferSink, write: &CheckpointWrite) {
    for _ in 0..120 {
        match sink.commit(write).await {
            WriteResult::Applied { .. } => return,
            WriteResult::Failed(SinkError::Throttled { .. }) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            other => panic!("expected the commit or a wait, got {other:?}"),
        }
    }
    panic!("the commit never applied");
}

#[tokio::test]
#[ignore = "needs TURBOPUFFER_API_KEY"]
async fn the_reader_follows_the_incarnation_readers_use() {
    let name = unique("reader");
    let _cleanup = Cleanup(name.clone());
    let mut sink = TurbopufferSink::new(&connection(), "s", &name).unwrap();
    let reader = Reader::new(&connection(), &name).unwrap();
    let cached = Reader::new(&connection(), &name)
        .unwrap()
        .with_cache(Duration::from_secs(3600));
    assert_eq!(
        reader.namespace().await,
        Err(ReadError::NotReady),
        "no sink yet"
    );

    let created = Checkpoint::new(BTreeMap::new());
    assert!(matches!(
        sink.create(&created.encode().unwrap()).await,
        WriteResult::Applied { .. }
    ));
    let (take_over, taken) = CheckpointWrite::take_over(&created).unwrap();
    commit_until_applied(&mut sink, &take_over).await;
    let (start, started) = CheckpointWrite::start_snapshot(&taken, BTreeMap::new(), 5).unwrap();
    commit_until_applied(&mut sink, &start).await;
    assert_eq!(
        reader.namespace().await,
        Err(ReadError::NotReady),
        "no snapshot yet"
    );

    let first = started.generation;
    let snapshot = vec![upsert(5, "1", "a", "1"), upsert(5, "2", "b", "2")];
    let snapshot = SinkBatch::new(snapshot, columns(), first, true);
    assert!(matches!(
        sink.write(&snapshot, None).await,
        WriteResult::Applied { .. }
    ));
    let (complete, completed) =
        CheckpointWrite::commit(&started, Frontier::new(6).unwrap()).unwrap();
    commit_until_applied(&mut sink, &complete).await;
    let deleted = SinkBatch::new(vec![delete(6, "2")], columns(), first, false);
    assert!(matches!(
        sink.write(&deleted, None).await,
        WriteResult::Applied { .. }
    ));

    assert_eq!(
        reader.namespace().await.unwrap(),
        data_namespace(&name, first)
    );
    assert_eq!(
        cached.namespace().await.unwrap(),
        data_namespace(&name, first)
    );
    let query = json!({"rank_by": ["id", "asc"], "limit": 10});
    let ids = |response: Option<Value>| -> Vec<Value> {
        let rows = response.expect("a namespace")["rows"]
            .as_array()
            .unwrap()
            .clone();
        rows.into_iter().map(|row| row["id"].clone()).collect()
    };
    // The tombstone of key 2 is left out.
    assert_eq!(
        ids(reader.query(query.clone()).await.unwrap()),
        [json!("1")]
    );
    let filtered = json!({"rank_by": ["id", "asc"], "limit": 10, "filters": ["views", "Gte", 1]});
    assert_eq!(ids(reader.query(filtered).await.unwrap()), [json!("1")]);
    let multi = json!({"queries": [query.clone(), {"rank_by": ["id", "desc"], "limit": 1}]});
    let results = reader.query(multi).await.unwrap().unwrap()["results"].clone();
    let firsts: Vec<_> = results
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["rows"][0]["id"].clone())
        .collect();
    assert_eq!(firsts, [json!("1"), json!("1")], "{results}");
    assert_eq!(reader.query(json!([])).await, Err(ReadError::InvalidQuery));

    // A re-snapshot of a view that is now empty writes nothing, so its
    // incarnation has no namespace and reads as empty.
    let (restart, restarted) =
        CheckpointWrite::start_snapshot(&completed, BTreeMap::new(), 7).unwrap();
    commit_until_applied(&mut sink, &restart).await;
    let (complete, _) = CheckpointWrite::commit(&restarted, Frontier::new(8).unwrap()).unwrap();
    commit_until_applied(&mut sink, &complete).await;
    let second = restarted.generation;
    assert_eq!(
        reader.namespace().await.unwrap(),
        data_namespace(&name, second)
    );
    assert_eq!(reader.query(query.clone()).await.unwrap(), None);
    // A cached reader keeps reading the old incarnation until its cache ends,
    // or until the old incarnation is gone, when it looks the incarnation up
    // again.
    assert_eq!(
        ids(cached.query(query.clone()).await.unwrap()),
        [json!("1")]
    );
    sink.client
        .delete_namespace(&data_namespace(&name, first))
        .await
        .unwrap();
    assert_eq!(cached.query(query).await.unwrap(), None);
    assert_eq!(
        cached.namespace().await.unwrap(),
        data_namespace(&name, second)
    );
}

#[tokio::test]
#[ignore = "needs TURBOPUFFER_API_KEY"]
async fn a_long_write_yields_and_continues_where_it_stopped() {
    let name = unique("slice");
    let _cleanup = Cleanup(name.clone());
    let mut sink = TurbopufferSink::new(&connection(), "s", &name).unwrap();
    sink.request_rows = 1;
    sink.write_slice = Duration::ZERO;
    let batch = SinkBatch::new(
        vec![
            upsert(5, "1", "a", "1"),
            upsert(5, "2", "b", "2"),
            upsert(5, "3", "c", "3"),
        ],
        columns(),
        2,
        false,
    );
    // Each call sends one request, then yields before the next.
    for sent in 1..=2 {
        match sink.write(&batch, None).await {
            WriteResult::Failed(SinkError::Throttled { .. }) => {}
            other => panic!("expected the write to yield, got {other:?}"),
        }
        assert_eq!(sink.client.faults.lock().unwrap().sent, sent);
    }
    assert!(matches!(
        sink.write(&batch, None).await,
        WriteResult::Applied { .. }
    ));
    assert_eq!(sink.client.faults.lock().unwrap().sent, 3);
    assert_eq!(
        live_documents(&sink.client, &data_namespace(&name, 2))
            .await
            .len(),
        3
    );
}

/// Materialize at `MATERIALIZE_URL`.
fn materialize() -> tokio_postgres::Config {
    std::env::var("MATERIALIZE_URL")
        .expect("MATERIALIZE_URL is not set")
        .parse()
        .unwrap()
}

async fn sql(statement: &str) {
    let (client, connection) = materialize().connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    client
        .batch_execute(statement)
        .await
        .unwrap_or_else(|e| panic!("{statement}: {e}"));
}

#[tokio::test]
#[ignore = "needs TURBOPUFFER_API_KEY and a running Materialize at MATERIALIZE_URL"]
async fn a_view_converges_through_restarts_lost_responses_and_take_overs() {
    let name = unique("e2e");
    let _cleanup = Cleanup(name.clone());
    let table = format!("tp_articles_{}", std::process::id());
    sql(&format!("DROP TABLE IF EXISTS materialize.public.{table}")).await;
    sql(&format!(
        "CREATE TABLE materialize.public.{table} (id int, title text, views int8) \
         WITH (RETAIN HISTORY = FOR '1h')"
    ))
    .await;
    sql(&format!(
        "INSERT INTO materialize.public.{table} VALUES (1, 'a', 10), (2, 'b', 20)"
    ))
    .await;
    let object = ObjectName {
        database: Some("materialize".into()),
        schema: "public".into(),
        name: table.clone(),
    };
    let options = || DurableOptions::default().commit_interval(Duration::from_millis(200));
    let sink = || TurbopufferSink::new(&connection(), "e2e", &name).unwrap();
    let spawn = |sink| {
        let mut durable = DurableSink::new("e2e", sink, options());
        tokio::spawn(async move { durable.run(&materialize(), NoTls).await })
    };
    let created = DurableSink::new("e2e", sink(), options())
        .setup(
            &materialize(),
            NoTls,
            &object,
            Envelope::Upsert {
                key: vec!["id".into()],
            },
        )
        .await
        .unwrap();
    assert!(created);

    let reader = Client::new(&connection()).unwrap();
    let mut checkpoints =
        Checkpoints::new(reader.clone(), checkpoint_namespace(&name), "e2e".into());
    let mut active = None;
    let expect = |rows: &[(&str, &str, i64)]| -> BTreeMap<String, Value> {
        rows.iter()
            .map(|(id, title, views)| (id.to_string(), json!({"title": title, "views": views})))
            .collect()
    };

    let worker = spawn(sink());
    until("the snapshot to become active", async || {
        active = checkpoints.load().await.unwrap().unwrap().active;
        active.is_some()
    })
    .await;
    let namespace = data_namespace(&name, active.unwrap());
    assert_eq!(
        live_documents(&reader, &namespace).await,
        expect(&[("1", "a", 10), ("2", "b", 20)])
    );

    for change in [
        format!("UPDATE materialize.public.{table} SET title = 'B' WHERE id = 2"),
        format!("DELETE FROM materialize.public.{table} WHERE id = 1"),
        format!("INSERT INTO materialize.public.{table} VALUES (3, 'c', 30)"),
    ] {
        sql(&change).await;
    }
    let changed = expect(&[("2", "B", 20), ("3", "c", 30)]);
    until("the changes", async || {
        live_documents(&reader, &namespace).await == changed
    })
    .await;
    worker.abort();
    let _ = worker.await;

    // Changes made while no worker runs arrive after a restart, without a new
    // snapshot, while the writes after the take-over lose responses and are
    // dropped. Which write takes which fault depends on how the stream
    // batches, so only convergence is checked.
    sql(&format!(
        "INSERT INTO materialize.public.{table} VALUES (4, 'd', 40)"
    ))
    .await;
    let restarted = sink();
    restarted.client.faults.lock().unwrap().plan.extend([
        Fault::Pass,
        Fault::Lose,
        Fault::Drop,
        Fault::Pass,
        Fault::Lose,
    ]);
    let worker = spawn(restarted);
    let caught_up = expect(&[("2", "B", 20), ("3", "c", 30), ("4", "d", 40)]);
    until("the change made while stopped", async || {
        live_documents(&reader, &namespace).await == caught_up
    })
    .await;
    assert_eq!(checkpoints.load().await.unwrap().unwrap().active, active);

    // A newer worker fences the running one at its next commit.
    let newer = spawn(sink());
    let fenced = tokio::time::timeout(STEP, worker).await.unwrap().unwrap();
    assert!(
        matches!(fenced, Err(Error::Conflict(CommitConflict::Fenced { .. }))),
        "{fenced:?}"
    );
    sql(&format!(
        "DELETE FROM materialize.public.{table} WHERE id = 3"
    ))
    .await;
    let after_take_over = expect(&[("2", "B", 20), ("4", "d", 40)]);
    until("a change after the take-over", async || {
        live_documents(&reader, &namespace).await == after_take_over
    })
    .await;
    newer.abort();
    let _ = newer.await;

    sql(&format!("DROP TABLE materialize.public.{table}")).await;
}
