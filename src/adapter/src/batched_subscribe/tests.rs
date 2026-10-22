// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mz_compute_client::controller::{AcceptedBatch, AcceptedShare};
use mz_compute_client::stash::OwnedStash;
use mz_dyncfg::{ConfigUpdates, ConfigVal};
use mz_expr::EvalError;
use mz_ore::cast::CastLossy;
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::{PersistClient, PersistLocation, Schemas};
use mz_persist_types::ShardId;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, Row, SqlScalarType, Timestamp};
use mz_sql::plan::SubscribeOutput;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
use tokio::sync::mpsc;

use crate::active_compute_sink::{SubscribeBacklogAccounting, SubscribeRowFormat};
use crate::batched_subscribe::{Forwarder, ForwarderEvent};
use crate::coord::peek::PeekResponseUnary;

const SINK_ID: GlobalId = GlobalId::User(7);

/// How many turns a test gives the forwarder or blob storage before it declares a wait lost.
const TURNS: usize = 1_000;

/// A persist client whose blob traffic is counted through the metrics registry.
struct CountedClient {
    registry: MetricsRegistry,
    client: PersistClient,
}

impl CountedClient {
    async fn new() -> Self {
        let cfg = PersistConfig::new_for_tests();
        let mut updates = ConfigUpdates::default();
        // Without these persist inlines small parts into shard state, where no blob counter
        // sees them and a delete has nothing to remove.
        updates.add_dynamic("persist_blob_target_size", ConfigVal::Usize(0));
        updates.add_dynamic(
            "persist_inline_writes_single_max_bytes",
            ConfigVal::Usize(0),
        );
        // Deletion is a silent no-op when this is off.
        updates.add_dynamic("persist_batch_delete_enabled", ConfigVal::Bool(true));
        updates.apply(&cfg);

        let registry = MetricsRegistry::new();
        let cache = PersistClientCache::new(cfg, &registry, |_, _| PubSubClientConnection::noop());
        let client = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("in-memory location opens");
        Self { registry, client }
    }

    fn succeeded(&self, op: &str) -> u64 {
        let Some(family) = self
            .registry
            .gather()
            .into_iter()
            .find(|m| m.name() == "mz_persist_external_succeeded_count")
        else {
            return 0;
        };
        family
            .get_metric()
            .iter()
            .filter(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|l| l.name() == "op" && l.value() == op)
            })
            .map(|metric| u64::cast_lossy(metric.get_counter().value()))
            .sum()
    }

    /// Writes `updates` as one stashed batch in the stash schema of [`desc`].
    async fn stash(&self, shard_id: ShardId, updates: &[(SourceData, u64, i64)]) -> OwnedStash {
        let schemas: Schemas<SourceData, ()> = Schemas {
            id: None,
            key: Arc::new(desc()),
            val: Arc::new(UnitSchema),
        };
        let mut builder = self.client.batch_builder::<SourceData, (), Timestamp, i64>(
            shard_id,
            "test_forwarder",
            schemas,
            Antichain::from_elem(Timestamp::MIN),
            None,
        );
        for (data, time, diff) in updates {
            builder
                .add(data, &(), &Timestamp::from(*time), diff)
                .await
                .expect("valid update");
        }
        let proto = builder
            .finish(Antichain::from_elem(Timestamp::from(u64::MAX)))
            .await
            .expect("valid bounds")
            .into_transmittable_batch();
        OwnedStash::from_proto(&self.client, shard_id, vec![proto])
    }

    async fn wait_until_nothing_is_left(&self) {
        for _ in 0..TURNS {
            let written = self.succeeded("blob_set");
            if written > 0 && self.succeeded("blob_delete") == written {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!(
            "{} of {} blob keys left behind",
            self.succeeded("blob_set") - self.succeeded("blob_delete"),
            self.succeeded("blob_set")
        );
    }
}

fn desc() -> RelationDesc {
    RelationDesc::builder()
        .with_column("value", SqlScalarType::UInt64.nullable(false))
        .finish()
}

fn ok(value: u64) -> SourceData {
    SourceData(Ok(Row::pack_slice(&[Datum::UInt64(value)])))
}

fn inline(updates: &[(u64, u64, i64)]) -> AcceptedShare {
    let rows: Vec<_> = updates
        .iter()
        .map(|(value, time, diff)| {
            (
                Row::pack_slice(&[Datum::UInt64(*value)]),
                Timestamp::from(*time),
                Diff::from(*diff),
            )
        })
        .collect();
    AcceptedShare::Inline(
        rows.iter()
            .map(|(row, time, diff)| (row.as_row_ref(), time, *diff))
            .collect(),
    )
}

fn stashed(shard_id: ShardId, stash: OwnedStash, from: u64) -> AcceptedShare {
    AcceptedShare::Stashed {
        shard_id,
        stash,
        from: Antichain::from_elem(Timestamp::from(from)),
    }
}

fn batch(lower: u64, upper: Option<u64>, shares: Vec<AcceptedShare>) -> AcceptedBatch {
    AcceptedBatch {
        lower: Antichain::from_elem(Timestamp::from(lower)),
        upper: upper.map_or_else(Antichain::new, |u| Antichain::from_elem(Timestamp::from(u))),
        shares,
        error: None,
    }
}

/// The client row of a data update with PROGRESS output.
fn data_row(value: u64, time: u64, diff: i64) -> Row {
    Row::pack_slice(&[
        Datum::from(Numeric::from(Timestamp::from(time))),
        Datum::False,
        Datum::Int64(diff),
        Datum::UInt64(value),
    ])
}

fn progress_row(upper: u64) -> Row {
    Row::pack_slice(&[
        Datum::from(Numeric::from(Timestamp::from(upper))),
        Datum::True,
        Datum::Null,
        Datum::Null,
    ])
}

/// A forwarder and the client and coordinator ends it talks to.
struct Harness {
    forwarder: Forwarder,
    rx: mpsc::UnboundedReceiver<PeekResponseUnary>,
    backlog: Arc<Mutex<SubscribeBacklogAccounting>>,
    events: mpsc::UnboundedReceiver<ForwarderEvent>,
}

impl Harness {
    fn new(persist: &CountedClient, read_batch_bytes: usize) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (events_tx, events) = mpsc::unbounded_channel();
        let backlog = Arc::new(Mutex::new(SubscribeBacklogAccounting::default()));
        let forwarder = Forwarder::spawn(
            SINK_ID,
            SubscribeRowFormat {
                emit_progress: true,
                arity: 1,
                output: SubscribeOutput::Diffs,
            },
            desc(),
            persist.client.clone(),
            tx,
            Arc::clone(&backlog),
            read_batch_bytes,
            1024 * 1024,
            events_tx,
        );
        Harness {
            forwarder,
            rx,
            backlog,
            events,
        }
    }

    /// Receives the next message without draining it from the backlog.
    async fn recv_undrained(&mut self) -> Option<PeekResponseUnary> {
        tokio::time::timeout(Duration::from_secs(30), self.rx.recv())
            .await
            .expect("the forwarder sends a message")
    }

    /// Receives and drains the next message, as the client writer does.
    async fn recv(&mut self) -> Option<PeekResponseUnary> {
        let message = self.recv_undrained().await;
        if message.is_some() {
            self.drain();
        }
        message
    }

    fn drain(&self) {
        self.backlog.lock().expect("not poisoned").pop();
    }

    /// Receives the next message, which must hold rows.
    async fn rows(&mut self) -> Vec<Row> {
        match self.recv().await {
            Some(message) => rows_of(message),
            None => panic!("the stream ended"),
        }
    }

    /// Receives rows until one message holds the progress row of `upper`, and returns the data
    /// rows before it, sorted.
    async fn rows_until_progress(&mut self, upper: u64) -> Vec<Row> {
        let mut data = Vec::new();
        loop {
            let rows = self.rows().await;
            if rows == [progress_row(upper)] {
                data.sort();
                return data;
            }
            data.extend(rows);
        }
    }

    async fn event(&mut self) -> ForwarderEvent {
        tokio::time::timeout(Duration::from_secs(30), self.events.recv())
            .await
            .expect("the forwarder reports an event")
            .expect("the forwarder holds the event sender")
    }
}

fn rows_of(message: PeekResponseUnary) -> Vec<Row> {
    let PeekResponseUnary::Rows(mut rows) = message else {
        panic!("expected rows, got {message:?}");
    };
    let mut out = Vec::new();
    while let Some(row) = rows.next() {
        out.push(row.to_owned());
    }
    out
}

fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort();
    rows
}

#[mz_ore::test(tokio::test)]
async fn inline_shares_are_formatted_then_followed_by_progress() {
    let persist = CountedClient::new().await;
    let mut harness = Harness::new(&persist, 1024 * 1024);

    harness.forwarder.send(batch(
        0,
        Some(5),
        vec![inline(&[(1, 2, 1)]), inline(&[(2, 3, -1), (3, 4, 2)])],
    ));

    let data = harness.rows().await;
    assert_eq!(
        sorted(data),
        sorted(vec![
            data_row(1, 2, 1),
            data_row(2, 3, -1),
            data_row(3, 4, 2)
        ]),
    );
    assert_eq!(harness.rows().await, [progress_row(5)]);

    // A batch without content still announces its upper.
    harness.forwarder.send(batch(5, Some(9), vec![]));
    assert_eq!(harness.rows().await, [progress_row(9)]);
}

#[mz_ore::test(tokio::test)]
async fn stashed_shares_are_read_back_with_times_and_retractions() {
    let persist = CountedClient::new().await;
    let mut harness = Harness::new(&persist, 1024 * 1024);
    let shard_id = ShardId::new();
    let stash = persist
        .stash(shard_id, &[(ok(1), 1, 1), (ok(1), 3, -1), (ok(2), 2, 3)])
        .await;
    let encoded = stash.encoded_size_bytes();

    harness.forwarder.send(batch(
        0,
        Some(5),
        vec![stashed(shard_id, stash, 0), inline(&[(4, 4, 1)])],
    ));
    assert_eq!(harness.forwarder.unread_stash_bytes(), encoded);

    assert_eq!(
        harness.rows_until_progress(5).await,
        sorted(vec![
            data_row(1, 1, 1),
            data_row(1, 3, -1),
            data_row(2, 2, 3),
            data_row(4, 4, 1),
        ]),
    );
    assert_eq!(harness.forwarder.unread_stash_bytes(), 0);
    persist.wait_until_nothing_is_left().await;
}

#[mz_ore::test(tokio::test)]
async fn a_trimmed_stashed_share_omits_updates_before_the_trim() {
    let persist = CountedClient::new().await;
    let mut harness = Harness::new(&persist, 1024 * 1024);
    let shard_id = ShardId::new();
    let stash = persist
        .stash(shard_id, &[(ok(1), 1, 1), (ok(2), 3, 1), (ok(3), 4, -1)])
        .await;

    harness
        .forwarder
        .send(batch(3, Some(5), vec![stashed(shard_id, stash, 3)]));

    assert_eq!(
        harness.rows_until_progress(5).await,
        sorted(vec![data_row(2, 3, 1), data_row(3, 4, -1)]),
    );
    persist.wait_until_nothing_is_left().await;
}

#[mz_ore::test(tokio::test)]
async fn the_forwarder_waits_for_the_client_to_drain() {
    let persist = CountedClient::new().await;
    // Every formatted row exceeds one byte, so every row is its own message, and any message
    // queued behind the one the client drains exceeds the backlog bound.
    let mut harness = Harness::new(&persist, 1);
    let updates: Vec<_> = (0..10).map(|value| (value, 1, 1)).collect();
    harness
        .forwarder
        .send(batch(0, Some(2), vec![inline(&updates)]));

    // The message at the front is tolerated and one more may queue behind it.
    let mut data = Vec::new();
    for _ in 0..2 {
        data.extend(rows_of(harness.recv_undrained().await.expect("rows")));
    }
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        harness.rx.try_recv().is_err(),
        "the forwarder sends past a full backlog"
    );

    // Draining one message admits exactly one more.
    harness.drain();
    data.extend(rows_of(harness.recv_undrained().await.expect("rows")));
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        harness.rx.try_recv().is_err(),
        "the forwarder sends past a full backlog"
    );

    harness.drain();
    harness.drain();
    data.extend(harness.rows_until_progress(2).await);
    let expected: Vec<_> = (0..10).map(|value| data_row(value, 1, 1)).collect();
    assert_eq!(sorted(data), sorted(expected));
}

#[mz_ore::test(tokio::test)]
async fn dropping_the_forwarder_deletes_unread_stashed_shares() {
    let persist = CountedClient::new().await;
    let mut harness = Harness::new(&persist, 1);
    let shard_id = ShardId::new();
    let updates: Vec<_> = (0..100).map(|value| (ok(value), 1, 1)).collect();
    let reading = persist.stash(shard_id, &updates).await;
    let queued = persist.stash(shard_id, &updates).await;
    harness
        .forwarder
        .send(batch(0, Some(2), vec![stashed(shard_id, reading, 0)]));
    harness
        .forwarder
        .send(batch(2, Some(3), vec![stashed(shard_id, queued, 0)]));

    // The client stops draining, so the forwarder stalls in the middle of the first read.
    for _ in 0..2 {
        harness.recv_undrained().await.expect("rows");
    }
    assert!(harness.forwarder.unread_stash_bytes() > 0);
    assert_eq!(persist.succeeded("blob_delete"), 0);

    drop(harness.forwarder);
    persist.wait_until_nothing_is_left().await;
}

#[mz_ore::test(tokio::test)]
async fn an_error_ends_the_stream() {
    let persist = CountedClient::new().await;
    let shard_id = ShardId::new();

    // A poisoned batch reports its error and nothing else, and later batches are not forwarded.
    let mut harness = Harness::new(&persist, 1024 * 1024);
    let poisoned_stash = persist.stash(shard_id, &[(ok(1), 1, 1)]).await;
    let mut poisoned = batch(
        0,
        Some(2),
        vec![inline(&[(2, 1, 1)]), stashed(shard_id, poisoned_stash, 0)],
    );
    poisoned.error = Some("division by zero".into());
    harness.forwarder.send(poisoned);
    let later_stash = persist.stash(shard_id, &[(ok(3), 2, 1)]).await;
    harness
        .forwarder
        .send(batch(2, Some(3), vec![stashed(shard_id, later_stash, 2)]));

    let Some(PeekResponseUnary::Error(error)) = harness.recv().await else {
        panic!("expected the batch error");
    };
    assert!(error.to_string().contains("division by zero"), "{error}");
    assert_eq!(harness.event().await, ForwarderEvent::Errored(SINK_ID));
    assert!(harness.recv().await.is_none(), "the stream continues");

    // An error update in a stashed share is the subscribe's error.
    let mut harness = Harness::new(&persist, 1024 * 1024);
    let error = SourceData(Err(DataflowError::from(EvalError::DivisionByZero)));
    let erroring = persist.stash(shard_id, &[(error, 1, 1)]).await;
    harness
        .forwarder
        .send(batch(0, Some(2), vec![stashed(shard_id, erroring, 0)]));

    let message = loop {
        match harness.recv().await {
            Some(PeekResponseUnary::Rows(_)) => {}
            message => break message,
        }
    };
    let Some(PeekResponseUnary::Error(error)) = message else {
        panic!("expected the stashed error, got {message:?}");
    };
    assert!(error.to_string().contains("division by zero"), "{error}");
    assert_eq!(harness.event().await, ForwarderEvent::Errored(SINK_ID));
    assert!(harness.recv().await.is_none(), "the stream continues");

    persist.wait_until_nothing_is_left().await;
}

#[mz_ore::test(tokio::test)]
async fn an_empty_upper_finishes_the_stream() {
    let persist = CountedClient::new().await;
    let mut harness = Harness::new(&persist, 1024 * 1024);

    harness
        .forwarder
        .send(batch(0, None, vec![inline(&[(1, 0, 1)])]));

    assert_eq!(harness.rows().await, [data_row(1, 0, 1)]);
    assert_eq!(harness.event().await, ForwarderEvent::Finished(SINK_ID));
    assert!(
        harness.recv().await.is_none(),
        "the stream continues past an empty upper"
    );
}

fn inline_bytes(share: &AcceptedShare) -> usize {
    match share {
        AcceptedShare::Inline(updates) => updates.byte_len(),
        AcceptedShare::Stashed { .. } => panic!("not an inline share"),
    }
}

#[mz_ore::test(tokio::test)]
async fn queued_inline_shares_count_until_formatted() {
    let persist = CountedClient::new().await;
    let mut harness = Harness::new(&persist, 1);
    let updates: Vec<_> = (0..10).map(|value| (value, 1, 1)).collect();
    let first = inline(&updates);
    let second = inline(&[(10, 2, 1)]);
    let (first_bytes, second_bytes) = (inline_bytes(&first), inline_bytes(&second));
    assert!(first_bytes > 0 && second_bytes > 0);

    harness.forwarder.send(batch(0, Some(2), vec![first]));
    harness.forwarder.send(batch(2, Some(3), vec![second]));
    assert_eq!(
        harness.forwarder.unread_inline_bytes(),
        first_bytes + second_bytes
    );

    // The client stops draining, so the forwarder stalls while formatting the first share.
    for _ in 0..2 {
        harness.recv_undrained().await.expect("rows");
    }
    assert_eq!(
        harness.forwarder.unread_inline_bytes(),
        first_bytes + second_bytes
    );

    harness.drain();
    harness.drain();
    harness.rows_until_progress(2).await;
    harness.rows_until_progress(3).await;
    assert_eq!(harness.forwarder.unread_inline_bytes(), 0);
}

#[mz_ore::test(tokio::test)]
async fn dropped_inline_shares_release_their_count() {
    let persist = CountedClient::new().await;
    let mut harness = Harness::new(&persist, 1024 * 1024);

    // A poisoned batch discards its own shares and every batch queued behind it.
    let mut poisoned = batch(0, Some(2), vec![inline(&[(1, 1, 1)])]);
    poisoned.error = Some("division by zero".into());
    harness.forwarder.send(poisoned);
    harness
        .forwarder
        .send(batch(2, Some(3), vec![inline(&[(2, 2, 1)])]));
    assert!(harness.forwarder.unread_inline_bytes() > 0);

    assert_eq!(harness.event().await, ForwarderEvent::Errored(SINK_ID));
    assert!(harness.recv().await.is_some());
    assert!(harness.recv().await.is_none(), "the stream continues");
    assert_eq!(harness.forwarder.unread_inline_bytes(), 0);

    // A batch sent after the stream ended is dropped at once.
    harness
        .forwarder
        .send(batch(3, Some(4), vec![inline(&[(3, 3, 1)])]));
    assert_eq!(harness.forwarder.unread_inline_bytes(), 0);
}
