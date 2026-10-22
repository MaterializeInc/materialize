// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mz_adapter_types::connection::ConnectionId;

use mz_compute_client::controller::{AcceptedBatch, AcceptedShare};
use mz_compute_client::stash::OwnedStash;
use mz_compute_types::errors::DataflowErrorSer;
use mz_controller_types::ClusterId;
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

use crate::AdapterError;
use crate::active_compute_sink::{
    ActiveBatchedSubscribe, ActiveComputeSinkRetireReason, SUBSCRIBE_MESSAGE_OVERHEAD_BYTES,
    SubscribeBacklogAccounting, SubscribeRowFormat,
};
use crate::batched_subscribe::{Forwarder, ForwarderEvent};
use crate::coord::peek::PeekResponseUnary;
use crate::error::SubscribeBudget;

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
    AcceptedShare::Inline {
        rows: rows
            .iter()
            .map(|(row, time, diff)| (row.as_row_ref(), time, *diff))
            .collect(),
        errors: Vec::new(),
    }
}

/// The error `internal error: <name>`.
fn named_error(name: &str) -> DataflowError {
    DataflowError::from(EvalError::Internal(name.into()))
}

/// An inline share with the row updates `updates` and the error updates `errors`, each an
/// error's name, time, and diff.
fn inline_with_errors(updates: &[(u64, u64, i64)], errors: &[(&str, u64, i64)]) -> AcceptedShare {
    let AcceptedShare::Inline { rows, .. } = inline(updates) else {
        unreachable!("an inline share");
    };
    let errors = errors
        .iter()
        .map(|(name, time, diff)| {
            (
                DataflowErrorSer::from(named_error(name)),
                Timestamp::from(*time),
                Diff::from(*diff),
            )
        })
        .collect();
    AcceptedShare::Inline { rows, errors }
}

fn stashed(shard_id: ShardId, stash: OwnedStash, from: u64) -> AcceptedShare {
    stashed_with_errors(shard_id, stash, from, None)
}

/// A stashed share whose error updates end at `last_error_time`.
fn stashed_with_errors(
    shard_id: ShardId,
    stash: OwnedStash,
    from: u64,
    last_error_time: Option<u64>,
) -> AcceptedShare {
    AcceptedShare::Stashed {
        shard_id,
        stash,
        from: Antichain::from_elem(Timestamp::from(from)),
        last_error_time: last_error_time.map(Timestamp::from),
    }
}

fn batch(lower: u64, upper: Option<u64>, shares: Vec<AcceptedShare>) -> AcceptedBatch {
    AcceptedBatch {
        lower: Antichain::from_elem(Timestamp::from(lower)),
        upper: upper.map_or_else(Antichain::new, |u| Antichain::from_elem(Timestamp::from(u))),
        shares,
        fatal: None,
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
        Self::with_max_buffered_bytes(persist, read_batch_bytes, usize::MAX)
    }

    fn with_max_buffered_bytes(
        persist: &CountedClient,
        read_batch_bytes: usize,
        max_buffered_bytes: usize,
    ) -> Self {
        Self::with_client_channel(persist, read_batch_bytes, max_buffered_bytes).0
    }

    /// The same, with a sender of the client channel, which keeps the client's stream from
    /// ending while it is held.
    fn with_client_channel(
        persist: &CountedClient,
        read_batch_bytes: usize,
        max_buffered_bytes: usize,
    ) -> (Self, mpsc::UnboundedSender<PeekResponseUnary>) {
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
            tx.clone(),
            Arc::clone(&backlog),
            read_batch_bytes,
            1024 * 1024,
            max_buffered_bytes,
            events_tx,
        );
        let harness = Harness {
            forwarder,
            rx,
            backlog,
            events,
        };
        (harness, tx)
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
async fn a_fatal_error_ends_the_stream() {
    let persist = CountedClient::new().await;
    let shard_id = ShardId::new();

    // A batch with a fatal error reports it and nothing else, and later batches are not
    // forwarded.
    let mut harness = Harness::new(&persist, 1024 * 1024);
    let failed_stash = persist.stash(shard_id, &[(ok(1), 1, 1)]).await;
    let mut failed = batch(
        0,
        Some(2),
        vec![inline(&[(2, 1, 1)]), stashed(shard_id, failed_stash, 0)],
    );
    failed.fatal = Some("stash write failed".into());
    harness.forwarder.send(failed);
    let later_stash = persist.stash(shard_id, &[(ok(3), 2, 1)]).await;
    harness
        .forwarder
        .send(batch(2, Some(3), vec![stashed(shard_id, later_stash, 2)]));

    let Some(PeekResponseUnary::Error(error)) = harness.recv().await else {
        panic!("expected the fatal error");
    };
    assert!(error.to_string().contains("stash write failed"), "{error}");
    assert_eq!(harness.event().await, ForwarderEvent::Errored(SINK_ID));
    assert!(harness.recv().await.is_none(), "the stream continues");

    persist.wait_until_nothing_is_left().await;
}

/// Receives the next message, which must be the subscribe's error, and its text.
async fn expect_error(harness: &mut Harness) -> String {
    let message = harness.recv().await;
    let Some(PeekResponseUnary::Error(error)) = message else {
        panic!("expected the subscribe's error before any row, got {message:?}");
    };
    assert_eq!(harness.event().await, ForwarderEvent::Errored(SINK_ID));
    assert!(harness.recv().await.is_none(), "the stream continues");
    error.to_string()
}

/// No row of a batch that holds an inline error update reaches the client, whatever the order
/// of its shares, and the error is reported as a plain subscribe reports it.
#[mz_ore::test(tokio::test)]
async fn no_row_of_a_batch_with_an_inline_error_reaches_the_client() {
    let persist = CountedClient::new().await;
    let shard_id = ShardId::new();
    let mut harness = Harness::new(&persist, 1);
    let rows = persist.stash(shard_id, &[(ok(1), 1, 1)]).await;
    harness.forwarder.send(batch(
        0,
        Some(2),
        vec![
            inline(&[(2, 1, 1)]),
            stashed(shard_id, rows, 0),
            inline_with_errors(&[(3, 1, 1)], &[("e", 1, -1)]),
        ],
    ));
    assert_eq!(
        expect_error(&mut harness).await,
        AdapterError::Unstructured(anyhow::Error::msg(named_error("e").to_string())).to_string(),
        "a retraction fails the subscribe as an insertion does"
    );
    persist.wait_until_nothing_is_left().await;
}

/// No row of a batch whose stashed share holds an error update reaches the client, and only the
/// stashed shares that hold an error update are read.
#[mz_ore::test(tokio::test)]
async fn no_row_of_a_batch_with_a_stashed_error_reaches_the_client() {
    let persist = CountedClient::new().await;
    let shard_id = ShardId::new();
    let mut harness = Harness::new(&persist, 1);
    let error = SourceData(Err(named_error("e")));
    let erroring = persist
        .stash(shard_id, &[(ok(1), 1, 1), (error, 1, 1), (ok(2), 1, 1)])
        .await;
    let clean = persist.stash(shard_id, &[(ok(3), 1, 1)]).await;
    harness.forwarder.send(batch(
        0,
        Some(2),
        vec![
            inline(&[(4, 1, 1)]),
            stashed(shard_id, clean, 0),
            stashed_with_errors(shard_id, erroring, 0, Some(1)),
        ],
    ));
    assert!(
        expect_error(&mut harness)
            .await
            .contains("internal error: e"),
        "the stashed error is the subscribe's error"
    );
    persist.wait_until_nothing_is_left().await;
}

/// The subscribe fails on the error update with the least `(time, error)` across the batch's
/// shares, as a plain subscribe reports the first of its consolidated error updates.
#[mz_ore::test(tokio::test)]
async fn the_first_error_is_the_least_time_and_error() {
    let persist = CountedClient::new().await;
    let shard_id = ShardId::new();
    let mut harness = Harness::new(&persist, 1024 * 1024);
    let stash = persist
        .stash(
            shard_id,
            &[
                (SourceData(Err(named_error("c"))), 2, 1),
                (SourceData(Err(named_error("a"))), 3, 1),
            ],
        )
        .await;
    harness.forwarder.send(batch(
        0,
        Some(4),
        vec![
            inline_with_errors(&[], &[("b", 3, 1), ("d", 2, -1)]),
            stashed_with_errors(shard_id, stash, 0, Some(3)),
        ],
    ));
    let mut candidates: Vec<_> = [("c", 2), ("d", 2)]
        .into_iter()
        .map(|(name, time)| (time, DataflowErrorSer::from(named_error(name))))
        .collect();
    candidates.sort();
    let expected = candidates[0].1.to_string();
    let error = expect_error(&mut harness).await;
    assert!(error.contains(&expected), "{error} is not {expected}");
    persist.wait_until_nothing_is_left().await;
}

/// A stashed share may announce an error update that cancels with another one written in a
/// different hand-out. Read and found clean, its rows are forwarded and its batches deleted.
#[mz_ore::test(tokio::test)]
async fn a_share_whose_errors_cancel_is_forwarded() {
    let persist = CountedClient::new().await;
    let shard_id = ShardId::new();
    let mut harness = Harness::new(&persist, 1024 * 1024);
    let stash = persist
        .stash(
            shard_id,
            &[
                (SourceData(Err(named_error("e"))), 2, 1),
                (ok(1), 2, 1),
                (SourceData(Err(named_error("e"))), 2, -1),
            ],
        )
        .await;
    harness.forwarder.send(batch(
        0,
        Some(4),
        vec![
            stashed_with_errors(shard_id, stash, 0, Some(2)),
            inline(&[(2, 3, 1)]),
        ],
    ));
    assert_eq!(
        harness.rows_until_progress(4).await,
        sorted(vec![data_row(1, 2, 1), data_row(2, 3, 1)]),
    );
    assert_eq!(harness.forwarder.unread_stash_bytes(), 0);
    persist.wait_until_nothing_is_left().await;
}

/// The batches of the subscribe as the controller accepts them, trimmed at the start 5, for an
/// error inserted at 6 and retracted at 8, and a row at 5. `split` cuts the range at 7, between
/// the insertion and the retraction.
async fn insert_and_retract(
    persist: &CountedClient,
    shard_id: ShardId,
    split: bool,
) -> Vec<AcceptedBatch> {
    let inserted = (SourceData(Err(named_error("e"))), 6, 1);
    let retracted = (SourceData(Err(named_error("e"))), 8, -1);
    if split {
        let first = persist.stash(shard_id, &[(ok(1), 5, 1), inserted]).await;
        let second = persist.stash(shard_id, &[retracted]).await;
        vec![
            batch(
                5,
                Some(7),
                vec![stashed_with_errors(shard_id, first, 5, Some(6))],
            ),
            batch(
                7,
                Some(10),
                vec![stashed_with_errors(shard_id, second, 7, Some(8))],
            ),
        ]
    } else {
        let only = persist
            .stash(shard_id, &[(ok(1), 5, 1), inserted, retracted])
            .await;
        vec![batch(
            5,
            Some(10),
            vec![stashed_with_errors(shard_id, only, 5, Some(8))],
        )]
    }
}

/// An error inserted and retracted after the start fails the subscribe on its insertion,
/// whether both land in one batch or are split across two, and no row of the failing batch
/// reaches the client.
#[mz_ore::test(tokio::test)]
async fn the_failure_does_not_depend_on_batch_boundaries() {
    let persist = CountedClient::new().await;
    let shard_id = ShardId::new();
    let mut failures = Vec::new();
    for split in [false, true] {
        let mut harness = Harness::new(&persist, 1024 * 1024);
        for batch in insert_and_retract(&persist, shard_id, split).await {
            harness.forwarder.send(batch);
        }
        failures.push(expect_error(&mut harness).await);
    }
    assert_eq!(failures[0], failures[1]);
    assert!(failures[0].contains("internal error: e"), "{}", failures[0]);
    persist.wait_until_nothing_is_left().await;
}

/// A retraction alone fails the subscribe once it lies at or after the start, as a plain
/// subscribe fails on an error update of either sign. The batches before it are forwarded.
#[mz_ore::test(tokio::test)]
async fn a_retraction_after_the_start_fails_the_subscribe() {
    let persist = CountedClient::new().await;
    let mut harness = Harness::new(&persist, 1024 * 1024);
    // The error stands before the start 5, so the trim removed its insertion.
    harness
        .forwarder
        .send(batch(5, Some(7), vec![inline(&[(1, 5, 1)])]));
    harness.forwarder.send(batch(
        7,
        Some(10),
        vec![inline_with_errors(&[(2, 8, 1)], &[("e", 8, -1)])],
    ));
    assert_eq!(
        harness.rows_until_progress(7).await,
        vec![data_row(1, 5, 1)]
    );
    assert!(
        expect_error(&mut harness)
            .await
            .contains("internal error: e")
    );
}

/// A stashed share whose error updates all lie before the trim is forwarded, and the read
/// filters those errors out with the rows before the trim.
#[mz_ore::test(tokio::test)]
async fn errors_before_the_trim_do_not_fail_the_batch() {
    let persist = CountedClient::new().await;
    let shard_id = ShardId::new();
    let mut harness = Harness::new(&persist, 1024 * 1024);
    let stash = persist
        .stash(
            shard_id,
            &[
                (SourceData(Err(named_error("e"))), 1, 1),
                (ok(1), 1, 1),
                (ok(2), 3, 1),
            ],
        )
        .await;
    harness.forwarder.send(batch(
        3,
        Some(5),
        vec![stashed_with_errors(shard_id, stash, 3, Some(1))],
    ));
    assert_eq!(
        harness.rows_until_progress(5).await,
        vec![data_row(2, 3, 1)]
    );
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
        AcceptedShare::Inline { rows, errors } => {
            assert!(errors.is_empty(), "rows only");
            rows.byte_len()
        }
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
    let third = inline(&[(11, 3, 1), (12, 3, 1)]);
    let (second_bytes, third_bytes) = (inline_bytes(&second), inline_bytes(&third));
    assert!(second_bytes > 0 && third_bytes > 0);

    harness.forwarder.send(batch(0, Some(2), vec![first]));
    harness.forwarder.send(batch(2, Some(3), vec![second]));
    harness.forwarder.send(batch(3, Some(4), vec![third]));
    assert_eq!(
        harness.forwarder.unread_inline_bytes(),
        second_bytes + third_bytes + 2 * SUBSCRIBE_MESSAGE_OVERHEAD_BYTES
    );

    // The client stops draining, so the forwarder stalls while formatting the first batch.
    for _ in 0..2 {
        harness.recv_undrained().await.expect("rows");
    }
    assert_eq!(
        harness.forwarder.unread_inline_bytes(),
        second_bytes + third_bytes + 2 * SUBSCRIBE_MESSAGE_OVERHEAD_BYTES
    );

    harness.drain();
    harness.drain();
    harness.rows_until_progress(2).await;
    harness.rows_until_progress(3).await;
    harness.rows_until_progress(4).await;
    assert_eq!(harness.forwarder.unread_inline_bytes(), 0);
}

#[mz_ore::test(tokio::test)]
async fn a_large_head_batch_alone_does_not_count_against_the_memory_bound() {
    let persist = CountedClient::new().await;
    let mut harness = Harness::new(&persist, 1);
    let updates: Vec<_> = (0..1000).map(|value| (value, 1, 1)).collect();
    let head = inline(&updates);
    assert!(inline_bytes(&head) > 1000);

    harness.forwarder.send(batch(0, Some(2), vec![head]));
    assert_eq!(harness.forwarder.unread_inline_bytes(), 0);

    // The client stops draining, so the forwarder stalls while formatting the batch.
    for _ in 0..2 {
        harness.recv_undrained().await.expect("rows");
    }
    assert_eq!(harness.forwarder.unread_inline_bytes(), 0);

    // A batch queued behind it counts.
    let queued = inline(&[(1000, 2, 1)]);
    let queued_bytes = inline_bytes(&queued);
    harness.forwarder.send(batch(2, Some(3), vec![queued]));
    assert_eq!(
        harness.forwarder.unread_inline_bytes(),
        queued_bytes + SUBSCRIBE_MESSAGE_OVERHEAD_BYTES
    );
}

#[mz_ore::test(tokio::test)]
async fn batches_without_shares_retire_a_client_that_does_not_read() {
    const MAX_BUFFERED_BYTES: usize = 64 * 1024;
    let persist = CountedClient::new().await;
    let (mut harness, channel) =
        Harness::with_client_channel(&persist, 1024 * 1024, MAX_BUFFERED_BYTES);
    let subscribe = ActiveBatchedSubscribe {
        conn_id: ConnectionId::Static(1),
        session_uuid: uuid::Uuid::nil(),
        cluster_id: ClusterId::user(1).expect("a valid id"),
        depends_on: BTreeSet::new(),
        as_of: Timestamp::MIN,
        start_time: 0,
        channel,
        backlog_accounting: Arc::clone(&harness.backlog),
        max_buffered_bytes: MAX_BUFFERED_BYTES,
        max_unread_stash_bytes: usize::MAX,
        forwarder: harness.forwarder,
    };

    // Each batch only advances the frontier, as over a quiet input.
    let mut retired = None;
    for upper in 1..=1_000 {
        if let Some(reason) = subscribe.send(batch(upper - 1, Some(upper), Vec::new())) {
            retired = Some(reason);
            break;
        }
    }
    let reason = retired.expect("the subscribe retires");
    assert!(
        matches!(
            reason,
            ActiveComputeSinkRetireReason::BufferExceeded {
                budget: SubscribeBudget::BufferedBytes,
                ..
            }
        ),
        "{reason:?}"
    );

    subscribe.retire(reason);
    let mut last = None;
    while let Ok(message) = harness.rx.try_recv() {
        last = Some(message);
    }
    assert!(
        matches!(
            last,
            Some(PeekResponseUnary::Error(
                AdapterError::SubscribeFellBehind { .. }
            ))
        ),
        "{last:?}"
    );
}

/// Returns an inline share of at least `bytes` bytes, at time `time`, with values from `first`.
fn inline_of_bytes(bytes: usize, time: u64, first: u64) -> AcceptedShare {
    let mut count = 1;
    loop {
        let updates: Vec<_> = (first..first + count)
            .map(|value| (value, time, 1))
            .collect();
        let share = inline(&updates);
        if inline_bytes(&share) >= bytes {
            return share;
        }
        count *= 2;
    }
}

/// A client that drains one message at a time, scaled from the defaults: a read batch size of
/// half the memory bound models 100 MiB of 128 MiB. With a chunk the size of the read batch,
/// the message queued behind the drained one and a batch of inline shares queued behind the head
/// batch would exceed the bound.
#[mz_ore::test(tokio::test)]
async fn a_draining_client_is_not_retired_with_a_full_chunk_queued() {
    const MAX_BUFFERED_BYTES: usize = 64 * 1024;
    let persist = CountedClient::new().await;
    let mut harness =
        Harness::with_max_buffered_bytes(&persist, MAX_BUFFERED_BYTES / 2, MAX_BUFFERED_BYTES);

    let head = inline_of_bytes(4 * MAX_BUFFERED_BYTES, 1, 0);
    let queued = inline_of_bytes(MAX_BUFFERED_BYTES / 2, 2, 1_000_000);
    harness.forwarder.send(batch(0, Some(2), vec![head]));
    harness.forwarder.send(batch(2, Some(3), vec![queued]));

    let mut messages = 0;
    loop {
        // Gives the forwarder time to fill the backlog up to where it waits.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let backlog = harness.backlog.lock().expect("not poisoned").backlog_size();
        let buffered = backlog + harness.forwarder.unread_inline_bytes();
        assert!(
            buffered <= MAX_BUFFERED_BYTES,
            "{buffered} bytes buffered after {messages} messages, over {MAX_BUFFERED_BYTES}",
        );
        messages += 1;
        if harness.rows().await == [progress_row(2)] {
            break;
        }
    }
    assert!(
        messages > 4,
        "the head batch was not chunked: {messages} messages"
    );
}

#[mz_ore::test(tokio::test)]
async fn dropped_inline_shares_release_their_count() {
    let persist = CountedClient::new().await;
    let mut harness = Harness::new(&persist, 1024 * 1024);

    // A failing batch discards its own shares and every batch queued behind it.
    let failing = batch(
        0,
        Some(2),
        vec![inline_with_errors(&[(1, 1, 1)], &[("e", 1, 1)])],
    );
    harness.forwarder.send(failing);
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
