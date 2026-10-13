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

//! Live subscriptions against a running Materialize, at the libpq connection
//! string in `MATERIALIZE_URL`. The tests that need a server are ignored, so
//! run them with `cargo test --test live -- --ignored`.

// The repository's clippy rules ask for wrappers a standalone SDK cannot use.
#![allow(clippy::disallowed_methods)]

mod common;

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::time::Duration;

use common::{STEP, admin, config, execute, lookup, rows, table};
use materialize_sdk::{
    Change, Classification, Error, ErrorKind, Event, Frontier, LiveOptions, LiveSubscription,
    Subscribe,
};
use tokio_postgres::{Client, Config, NoTls};

async fn next(live: &mut LiveSubscription) -> Option<Result<Event, Error>> {
    tokio::time::timeout(STEP, live.next())
        .await
        .expect("an event within the step limit")
}

#[tokio::test]
async fn invalid_options_fail_before_connecting() {
    // Nothing listens here, so an attempt to connect would fail differently.
    let mut unreachable = Config::new();
    unreachable.host("127.0.0.1").port(1);
    for options in [
        LiveOptions::default().buffer(0),
        LiveOptions::default().buffer(usize::MAX),
        LiveOptions::default().max_rows(0),
        LiveOptions::default().fetch_timeout(Duration::from_secs(2 * 24 * 60 * 60)),
        LiveOptions::default()
            .max_pending_bytes(Some(100))
            .max_timestamp_bytes(Some(100)),
    ] {
        let started =
            LiveSubscription::connect(&unreachable, NoTls, Subscribe::query("SELECT 1"), options)
                .await;
        assert!(
            matches!(started, Err(Error::InvalidOption(_))),
            "{started:?}"
        );
    }
}

#[tokio::test]
async fn a_refused_connection_reports_its_cause() {
    let mut unreachable = Config::new();
    unreachable.host("127.0.0.1").port(1).user("materialize");
    let started = LiveSubscription::connect(
        &unreachable,
        NoTls,
        Subscribe::query("SELECT 1"),
        LiveOptions::default(),
    )
    .await;
    let Err(Error::Server {
        sqlstate: None,
        message,
        classification,
        ..
    }) = started
    else {
        panic!("{started:?}");
    };
    assert!(message.to_lowercase().contains("refused"), "{message}");
    assert_eq!(
        classification,
        Classification::Kind {
            kind: ErrorKind::Transient
        }
    );
}

#[tokio::test]
async fn dropping_connect_while_it_starts_closes_the_connection() {
    // A server that accepts the session and never answers `BEGIN`, so that
    // `connect` waits inside its first request until it is dropped.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (began_tx, began) = std::sync::mpsc::channel();
    let (closed_tx, closed) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let read_body = |socket: &mut std::net::TcpStream| {
            let mut length = [0; 4];
            socket.read_exact(&mut length).unwrap();
            let length = usize::try_from(u32::from_be_bytes(length)).unwrap();
            let mut body = vec![0; length - 4];
            socket.read_exact(&mut body).unwrap();
            body
        };
        read_body(&mut socket);
        // AuthenticationOk, then ReadyForQuery.
        socket
            .write_all(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I")
            .unwrap();
        let mut tag = [0; 1];
        socket.read_exact(&mut tag).unwrap();
        assert_eq!(&tag, b"Q");
        assert_eq!(read_body(&mut socket), b"BEGIN\0");
        began_tx.send(()).unwrap();
        let mut buffer = [0; 1024];
        while socket.read(&mut buffer).is_ok_and(|n| n > 0) {}
        let _ = closed_tx.send(());
    });
    let mut config = Config::new();
    config.host("127.0.0.1").port(port).user("materialize");
    let mut connecting = Box::pin(LiveSubscription::connect(
        &config,
        NoTls,
        Subscribe::query("SELECT 1"),
        LiveOptions::default(),
    ));
    // Waiting on another thread lets this runtime run `connect` and, once it
    // is dropped, the tasks it aborts.
    let began = tokio::task::spawn_blocking(move || began.recv_timeout(STEP));
    tokio::select! {
        started = &mut connecting => panic!("connect returned: {started:?}"),
        began = began => assert!(began.unwrap().is_ok(), "the server received BEGIN"),
    }
    drop(connecting);
    let closed = tokio::task::spawn_blocking(move || closed.recv_timeout(STEP))
        .await
        .unwrap();
    assert!(closed.is_ok(), "dropping connect closed the connection");
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_constant_query_completes() {
    let subscribe = Subscribe::query("SELECT 1 AS one");
    let mut live = LiveSubscription::connect(&config(), NoTls, subscribe, LiveOptions::default())
        .await
        .unwrap();
    let Some(Ok(Event::Batch(batch))) = next(&mut live).await else {
        panic!("expected a batch");
    };
    assert_eq!(live.columns(), Some(&["one".to_string()][..]));
    assert!(batch.is_snapshot());
    assert_eq!(batch.upper(), Frontier::after(u64::MAX));
    assert_eq!(batch.updates().len(), 1);
    assert!(next(&mut live).await.is_none(), "the stream ends");
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_snapshot_over_the_timestamp_limit_fails_the_subscription() {
    let subscribe = Subscribe::query("SELECT generate_series(1, 100) AS n");
    let options = LiveOptions::default()
        .max_pending_bytes(None)
        .max_timestamp_bytes(Some(1000));
    let mut live = LiveSubscription::connect(&config(), NoTls, subscribe, options)
        .await
        .unwrap();
    let failure = next(&mut live).await;
    assert!(
        matches!(&failure, Some(Err(Error::TimestampTooLarge(error))) if error.limit == 1000),
        "{failure:?}"
    );
    assert!(next(&mut live).await.is_none());
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_table_streams_its_snapshot_then_its_changes() {
    let admin = admin().await;
    let object = table(&admin, "live_stream", "(id int, v text)").await;
    execute(
        &admin,
        "INSERT INTO materialize.public.live_stream VALUES (1, 'a')",
    )
    .await;
    let mut live = LiveSubscription::connect(
        &config(),
        NoTls,
        Subscribe::object(object.object),
        LiveOptions::default(),
    )
    .await
    .unwrap();
    let mut seen = Vec::new();
    let mut inserted = false;
    while seen.len() < 2 {
        let Some(Ok(Event::Batch(batch))) = next(&mut live).await else {
            panic!("expected a batch");
        };
        for update in batch.into_updates() {
            if let Change::Diff { row, diff } = update.change {
                seen.push((row, diff));
            }
        }
        if !inserted {
            execute(
                &admin,
                "INSERT INTO materialize.public.live_stream VALUES (2, 'b')",
            )
            .await;
            inserted = true;
        }
    }
    assert_eq!(
        live.columns(),
        Some(&["id".to_string(), "v".to_string()][..])
    );
    let row = |id: &str, v: &str| vec![Some(id.to_string()), Some(v.to_string())];
    assert_eq!(seen, [(row("1", "a"), 1), (row("2", "b"), 1)]);
    drop(live);
    execute(&admin, "DROP TABLE materialize.public.live_stream").await;
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_missing_table_fails_at_start() {
    let subscribe = Subscribe::query("SELECT * FROM no_such_table");
    let Err(Error::Server {
        sqlstate,
        message,
        classification,
        ..
    }) = LiveSubscription::connect(&config(), NoTls, subscribe, LiveOptions::default()).await
    else {
        panic!("expected a server error");
    };
    // Materialize reports an unknown name as an internal error, which before
    // the first progress row may come from the object changing.
    assert_eq!(sqlstate.as_deref(), Some("XX000"), "{message}");
    assert!(message.contains("unknown catalog item"), "{message}");
    assert_eq!(
        classification,
        Classification::CheckObject {
            otherwise: ErrorKind::Fatal
        }
    );
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_dataflow_error_after_the_snapshot_fails_the_subscription() {
    let admin = admin().await;
    table(&admin, "live_poisoned", "(id int)").await;
    execute(
        &admin,
        "INSERT INTO materialize.public.live_poisoned VALUES (1)",
    )
    .await;
    let subscribe = Subscribe::query("SELECT 1 / id AS v FROM materialize.public.live_poisoned");
    let mut live = LiveSubscription::connect(&config(), NoTls, subscribe, LiveOptions::default())
        .await
        .unwrap();
    let Some(Ok(Event::Batch(snapshot))) = next(&mut live).await else {
        panic!("expected the snapshot");
    };
    assert!(snapshot.is_snapshot());
    execute(
        &admin,
        "INSERT INTO materialize.public.live_poisoned VALUES (0)",
    )
    .await;
    let failure = loop {
        match next(&mut live).await {
            Some(Ok(_)) => {}
            Some(Err(error)) => break error,
            None => panic!("the stream ended without failing"),
        }
    };
    let Error::Server {
        sqlstate,
        message,
        classification,
        ..
    } = &failure
    else {
        panic!("{failure:?}");
    };
    assert_eq!(sqlstate.as_deref(), Some("XX000"), "{message}");
    assert!(message.contains("division by zero"), "{message}");
    assert_eq!(
        *classification,
        Classification::Kind {
            kind: ErrorKind::StreamPoisoned
        }
    );
    assert!(next(&mut live).await.is_none());
    drop(live);
    execute(&admin, "DROP TABLE materialize.public.live_poisoned").await;
}

/// A table whose snapshot of `rows` ids arrives as one chunk per id, all from
/// one `FETCH`, because the memory limit of zero releases each update alone.
async fn chunked_snapshot(admin: &Client, name: &str, rows: u32) -> (Subscribe, LiveOptions) {
    let object = table(admin, name, "(id int)").await;
    execute(
        admin,
        &format!("INSERT INTO materialize.public.{name} SELECT generate_series(1, {rows})"),
    )
    .await;
    let options = LiveOptions::default()
        .buffer(1)
        .max_pending_bytes(Some(0))
        .max_rows(u64::from(rows) + 10);
    (Subscribe::object(object.object), options)
}

fn ids(event: Event) -> Vec<String> {
    let updates = match event {
        Event::SnapshotChunk { updates } => updates,
        Event::Batch(batch) => batch.into_updates(),
    };
    updates
        .into_iter()
        .filter_map(|update| match update.change {
            Change::Diff { mut row, .. } => row.pop().flatten(),
            _ => None,
        })
        .collect()
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_burst_larger_than_the_buffer_reaches_a_prompt_consumer() {
    let admin = admin().await;
    let (subscribe, options) = chunked_snapshot(&admin, "live_burst", 100).await;
    let mut live = LiveSubscription::connect(&config(), NoTls, subscribe, options)
        .await
        .unwrap();
    let mut seen = BTreeSet::new();
    let mut events = 0;
    while seen.len() < 100 {
        let Some(Ok(event)) = next(&mut live).await else {
            panic!("expected an event");
        };
        events += 1;
        for id in ids(event) {
            assert!(seen.insert(id.clone()), "id {id} delivered twice");
        }
    }
    assert!(
        events > 1,
        "the snapshot arrived as more events than the buffer holds"
    );
    drop(live);
    execute(&admin, "DROP TABLE materialize.public.live_burst").await;
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn a_consumer_that_drains_too_slowly_fails_the_subscription() {
    let admin = admin().await;
    let (subscribe, options) = chunked_snapshot(&admin, "live_slow", 20).await;
    // Each event waits less than the limit, but the FETCH's 20 events together
    // wait far longer, which would hold the session idle in its transaction.
    let options = options.max_consumer_wait(Duration::from_secs(1));
    let mut live = LiveSubscription::connect(&config(), NoTls, subscribe, options)
        .await
        .unwrap();
    let mut events = 0;
    let failure = loop {
        match next(&mut live).await {
            Some(Ok(_)) => {
                events += 1;
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Some(Err(error)) => break error,
            None => panic!("the stream ended without failing"),
        }
    };
    assert!(
        matches!(failure, Error::ConsumerFellBehind { capacity: 1, .. }),
        "{failure:?}"
    );
    assert!(
        events < 20,
        "the subscription failed before every event was taken"
    );
    assert!(next(&mut live).await.is_none());
    execute(&admin, "DROP TABLE materialize.public.live_slow").await;
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL"]
async fn dropping_ends_the_subscribe_while_a_fetch_waits() {
    let admin = admin().await;
    let (_, sizes) = rows(
        &admin,
        "SELECT size FROM mz_catalog.mz_cluster_replica_sizes ORDER BY credits_per_hour LIMIT 1",
    )
    .await;
    let size = sizes[0][0].clone().unwrap();
    for cluster in ["live_stall", "live_run"] {
        execute(&admin, &format!("DROP CLUSTER IF EXISTS {cluster} CASCADE")).await;
        execute(
            &admin,
            &format!("CREATE CLUSTER {cluster} (SIZE = '{size}')"),
        )
        .await;
    }
    table(&admin, "live_stall_base", "(id int)").await;
    execute(
        &admin,
        "CREATE MATERIALIZED VIEW materialize.public.live_stalled IN CLUSTER live_stall \
         AS SELECT * FROM materialize.public.live_stall_base",
    )
    .await;
    let object = lookup(&admin, "public", "live_stalled")
        .await
        .unwrap()
        .object;
    // Without replicas the view stops writing, so the subscribe's snapshot
    // never completes: after the first progress row a FETCH waits.
    execute(
        &admin,
        "ALTER CLUSTER live_stall SET (REPLICATION FACTOR = 0)",
    )
    .await;
    let mut config = config();
    config.options("--cluster=live_run");
    // A long timeout, so that only the closed connection can end the FETCH.
    let options = LiveOptions::default().fetch_timeout(Duration::from_secs(120));
    let live = LiveSubscription::connect(&config, NoTls, Subscribe::object(object), options)
        .await
        .unwrap();
    let subscribes = "SELECT count(*) FROM mz_internal.mz_subscriptions AS s \
                      JOIN mz_catalog.mz_clusters AS c ON c.id = s.cluster_id \
                      WHERE c.name = 'live_run'";
    let count = async |expected: &str| {
        for _ in 0..40 {
            if rows(&admin, subscribes).await.1[0][0].as_deref() == Some(expected) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        false
    };
    assert!(count("1").await, "the subscribe started");
    drop(live);
    assert!(count("0").await, "the subscribe ended");
    for cluster in ["live_stall", "live_run"] {
        execute(&admin, &format!("DROP CLUSTER {cluster} CASCADE")).await;
    }
    execute(
        &admin,
        "DROP TABLE materialize.public.live_stall_base CASCADE",
    )
    .await;
}
