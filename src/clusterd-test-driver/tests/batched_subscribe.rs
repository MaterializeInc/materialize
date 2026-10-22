// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! End-to-end tests of the batched subscribe sink against a real clusterd.
//!
//! These tests only run when `CLUSTERD_COMPUTE_ADDR` is set (see
//! [`mz_clusterd_test_driver::target::e2e_enabled`]), and otherwise pass by skipping. Each test
//! opens its own controller connection, which replaces the previous one, and hosts persist PubSub
//! on the fixed `DRIVER_PUBSUB_BIND`, so they must run one at a time (`--test-threads=1`). They
//! are meant for a clusterd with several workers, so that the replica merges the responses of
//! several parts.

use std::net::SocketAddr;
use std::time::Duration;

use mz_clusterd_test_driver::data::write_updates_single_ts;
use mz_clusterd_test_driver::dataflow::{DataflowBuilder, PersistSource};
use mz_clusterd_test_driver::driver::Driver;
use mz_clusterd_test_driver::persist_host::PersistHost;
use mz_clusterd_test_driver::responses::BatchedSubscribeEvent;
use mz_clusterd_test_driver::target;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_expr::{MirRelationExpr, MirScalarExpr, func};
use mz_persist_client::PersistClient;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{
    Datum, GlobalId, RelationDesc, ReprScalarType, Row, SqlColumnType, SqlScalarType, Timestamp,
};
use timely::PartialOrder;
use timely::progress::Antichain;

const TIMEOUT: Duration = Duration::from_secs(60);

/// A connected driver with an initialized compute instance.
struct Harness {
    driver: Driver,
    client: PersistClient,
    location: PersistLocation,
    /// Keeps a temporary blob directory alive, if the test made one.
    _blob_dir: Option<tempfile::TempDir>,
}

/// Connects to the clusterd under test, or returns `None` when no clusterd is configured.
async fn harness() -> Option<Harness> {
    if !target::e2e_enabled() {
        return None;
    }
    let consensus = std::env::var("COCKROACH_URL").expect("COCKROACH_URL for e2e");
    let (blob, blob_dir) = match std::env::var("PERSIST_BLOB_URL") {
        Ok(blob) => (blob, None),
        Err(_) => {
            let dir = tempfile::tempdir().expect("tempdir");
            (format!("file://{}", dir.path().display()), Some(dir))
        }
    };
    let location = PersistLocation {
        blob_uri: blob.parse().expect("blob uri"),
        consensus_uri: consensus.parse().expect("consensus uri"),
    };
    let bind: SocketAddr = std::env::var("DRIVER_PUBSUB_BIND")
        .unwrap_or_else(|_| "0.0.0.0:6879".to_string())
        .parse()
        .expect("pubsub bind");
    let host = PersistHost::start_on(bind, location.clone())
        .await
        .expect("host");
    let client = host.client().await.expect("client");
    let driver = Driver::connect(host, &target::compute_addr())
        .await
        .expect("connect");
    driver
        .create_instance(None, false, Default::default())
        .expect("create instance");
    driver
        .send(ComputeCommand::InitializationComplete)
        .expect("init complete");
    Some(Harness {
        driver,
        client,
        location,
        _blob_dir: blob_dir,
    })
}

/// A one-column `bigint` schema.
fn key_desc() -> RelationDesc {
    RelationDesc::builder()
        .with_column("k", SqlScalarType::Int64.nullable(false))
        .finish()
}

fn row(k: i64) -> Row {
    Row::pack_slice(&[Datum::Int64(k)])
}

fn ts(t: u64) -> Timestamp {
    Timestamp::from(t)
}

fn frontier(t: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(ts(t))
}

impl Harness {
    /// Writes `updates` at `t`, advancing the shard's upper to `t + 1`.
    async fn write(&self, shard: ShardId, desc: &RelationDesc, t: u64, updates: &[(Row, i64)]) {
        write_updates_single_ts(&self.client, shard, desc, updates, ts(t))
            .await
            .expect("write");
    }

    /// Submits and schedules a dataflow that imports `shard` as `source_id`, builds
    /// `build(Get source_id)` as `view_id`, and exports it as the batched subscribe `sink_id`.
    #[allow(clippy::too_many_arguments)]
    fn subscribe(
        &self,
        source_id: GlobalId,
        sink_id: GlobalId,
        shard: ShardId,
        source_desc: RelationDesc,
        output_desc: RelationDesc,
        build: impl FnOnce(MirRelationExpr) -> MirRelationExpr,
        as_of: u64,
        up_to: Antichain<Timestamp>,
    ) {
        let GlobalId::User(sink) = sink_id else {
            panic!("sink ids are user ids");
        };
        let view_id = GlobalId::User(sink + 1_000_000);
        let mut builder = DataflowBuilder::new(format!("batched-subscribe-{sink_id}"));
        let input = builder.import_persist(
            source_id,
            PersistSource {
                shard,
                location: self.location.clone(),
                desc: source_desc,
                upper: ts(as_of + 1),
            },
        );
        builder.build(view_id, build(input.get()));
        builder.export_batched_subscribe(sink_id, view_id, output_desc, ts(as_of), up_to);
        builder.as_of(ts(as_of));
        let df = builder.finish().expect("dataflow");
        self.driver.register_batched_subscribe(sink_id);
        self.driver.submit_dataflow(df).expect("submit");
        self.driver.schedule(sink_id).expect("schedule");
    }

    /// Waits for `sink_id` to report an upper at or beyond `upper`, and returns its responses.
    async fn await_upper(
        &self,
        sink_id: GlobalId,
        upper: Antichain<Timestamp>,
    ) -> Vec<BatchedSubscribeEvent> {
        self.driver
            .await_batched_subscribe(sink_id, upper, TIMEOUT)
            .await
            .expect("batched subscribe progress")
    }
}

/// A batch's description and content, for asserting on it.
#[derive(Debug, PartialEq)]
struct Batch {
    lower: Antichain<Timestamp>,
    upper: Antichain<Timestamp>,
    updates: Vec<(Row, Timestamp, i64)>,
    error: Option<String>,
}

/// Unpacks `events` into batches, failing on a `DroppedAt`.
fn batches(events: Vec<BatchedSubscribeEvent>) -> Vec<Batch> {
    events
        .into_iter()
        .map(|event| match event {
            BatchedSubscribeEvent::Batch {
                lower,
                upper,
                updates,
                error,
            } => Batch {
                lower,
                upper,
                updates,
                error,
            },
            BatchedSubscribeEvent::DroppedAt { frontier, .. } => {
                panic!("unexpected drop at {frontier:?}")
            }
        })
        .collect()
}

/// Asserts that `batches` continue a chain that ends at `lower`, and returns the chain's new end.
fn assert_chain(lower: &Antichain<Timestamp>, batches: &[Batch]) -> Antichain<Timestamp> {
    let mut end = lower.clone();
    for batch in batches {
        assert_eq!(
            batch.lower, end,
            "each lower is the previous upper: {batches:?}"
        );
        assert!(
            PartialOrder::less_than(&batch.lower, &batch.upper),
            "each upper is beyond its lower: {batches:?}",
        );
        end = batch.upper.clone();
    }
    end
}

/// The updates of `batches`, sorted.
fn all_updates(batches: &[Batch]) -> Vec<(Row, Timestamp, i64)> {
    let mut updates: Vec<_> = batches
        .iter()
        .flat_map(|batch| batch.updates.iter().cloned())
        .collect();
    updates.sort();
    updates
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn a_batched_subscribe_delivers_its_snapshot_then_deltas() {
    let Some(h) = harness().await else {
        return;
    };
    let (source_id, sink_id) = (GlobalId::User(1100), GlobalId::User(1101));
    let shard = ShardId::new();
    let desc = key_desc();
    h.write(shard, &desc, 0, &[(row(1), 1), (row(2), 1)]).await;
    h.write(shard, &desc, 1, &[(row(3), 1)]).await;
    h.subscribe(
        source_id,
        sink_id,
        shard,
        desc.clone(),
        desc.clone(),
        |get| get,
        1,
        Antichain::new(),
    );

    // The shard's upper rests at 2 until the next write, so the first description is `[1, 2)`
    // and holds the snapshot at the as-of 1.
    let snapshot = batches(h.await_upper(sink_id, frontier(2)).await);
    assert_eq!(
        snapshot,
        vec![Batch {
            lower: frontier(1),
            upper: frontier(2),
            updates: vec![(row(1), ts(1), 1), (row(2), ts(1), 1), (row(3), ts(1), 1)],
            error: None,
        }],
    );

    h.write(shard, &desc, 2, &[(row(4), 1)]).await;
    h.write(shard, &desc, 3, &[(row(5), 1)]).await;
    let deltas = batches(h.await_upper(sink_id, frontier(4)).await);
    assert_eq!(assert_chain(&frontier(2), &deltas), frontier(4));
    assert_eq!(
        all_updates(&deltas),
        vec![(row(4), ts(2), 1), (row(5), ts(3), 1)],
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn retractions_reach_the_client() {
    let Some(h) = harness().await else {
        return;
    };
    let (source_id, sink_id) = (GlobalId::User(1200), GlobalId::User(1201));
    let shard = ShardId::new();
    let desc = key_desc();
    h.write(shard, &desc, 0, &[(row(1), 1), (row(2), 1)]).await;
    h.subscribe(
        source_id,
        sink_id,
        shard,
        desc.clone(),
        desc.clone(),
        |get| get,
        0,
        Antichain::new(),
    );
    let snapshot = batches(h.await_upper(sink_id, frontier(1)).await);
    assert_eq!(
        all_updates(&snapshot),
        vec![(row(1), ts(0), 1), (row(2), ts(0), 1)],
    );

    h.write(shard, &desc, 1, &[(row(1), -1)]).await;
    let deltas = batches(h.await_upper(sink_id, frontier(2)).await);
    assert_eq!(assert_chain(&frontier(1), &deltas), frontier(2));
    assert_eq!(all_updates(&deltas), vec![(row(1), ts(1), -1)]);
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn an_empty_description_still_reports_progress() {
    let Some(h) = harness().await else {
        return;
    };
    let (source_id, sink_id) = (GlobalId::User(1300), GlobalId::User(1301));
    let shard = ShardId::new();
    let desc = key_desc();
    h.write(shard, &desc, 0, &[(row(1), 1)]).await;
    h.subscribe(
        source_id,
        sink_id,
        shard,
        desc.clone(),
        desc.clone(),
        |get| get,
        0,
        Antichain::new(),
    );
    let snapshot = batches(h.await_upper(sink_id, frontier(1)).await);
    assert_eq!(assert_chain(&frontier(0), &snapshot), frontier(1));

    // Advance the shard's upper without writing an update.
    h.write(shard, &desc, 1, &[]).await;
    let progress = batches(h.await_upper(sink_id, frontier(2)).await);
    assert_eq!(
        progress,
        vec![Batch {
            lower: frontier(1),
            upper: frontier(2),
            updates: vec![],
            error: None,
        }],
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn up_to_completes_the_subscribe() {
    let Some(h) = harness().await else {
        return;
    };
    let (source_id, sink_id) = (GlobalId::User(1400), GlobalId::User(1401));
    let shard = ShardId::new();
    let desc = key_desc();
    for t in 0..4 {
        h.write(shard, &desc, t, &[(row(i64::try_from(t).unwrap()), 1)])
            .await;
    }
    h.subscribe(
        source_id,
        sink_id,
        shard,
        desc.clone(),
        desc.clone(),
        |get| get,
        0,
        frontier(2),
    );
    let events = batches(h.await_upper(sink_id, Antichain::new()).await);
    assert_eq!(assert_chain(&frontier(0), &events), Antichain::new());
    let (completion, data) = events.split_last().expect("a completion");
    assert_eq!(
        *completion,
        Batch {
            lower: frontier(2),
            upper: Antichain::new(),
            updates: vec![],
            error: None,
        },
        "completion is the description `[up_to, ∅)` without updates",
    );
    assert_eq!(
        all_updates(data),
        vec![(row(0), ts(0), 1), (row(1), ts(1), 1)],
        "no update at or after `up_to` is delivered",
    );

    // An `up_to` at the as-of completes before any update.
    let (source_id, sink_id) = (GlobalId::User(1410), GlobalId::User(1411));
    h.subscribe(
        source_id,
        sink_id,
        shard,
        desc.clone(),
        desc.clone(),
        |get| get,
        3,
        frontier(3),
    );
    let events = batches(h.await_upper(sink_id, Antichain::new()).await);
    assert_eq!(
        events,
        vec![Batch {
            lower: frontier(3),
            upper: Antichain::new(),
            updates: vec![],
            error: None,
        }],
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn an_error_poisons_the_subscribe_and_stops_it() {
    let Some(h) = harness().await else {
        return;
    };
    let (source_id, sink_id) = (GlobalId::User(1500), GlobalId::User(1501));
    let shard = ShardId::new();
    let desc = key_desc();
    // `(k, k / (k - 3))`, which fails for `k = 3`.
    let output_desc = RelationDesc::builder()
        .with_column("k", SqlScalarType::Int64.nullable(false))
        .with_column(
            "q",
            SqlColumnType {
                scalar_type: SqlScalarType::Int64,
                nullable: false,
            },
        )
        .finish();
    let quotient = |get: MirRelationExpr| {
        let three = MirScalarExpr::literal_ok(Datum::Int64(3), ReprScalarType::Int64);
        get.map(vec![MirScalarExpr::column(0).call_binary(
            MirScalarExpr::column(0).call_binary(three, func::SubInt64),
            func::DivInt64,
        )])
    };
    h.write(shard, &desc, 0, &[(row(1), 1), (row(2), 1)]).await;
    h.subscribe(
        source_id,
        sink_id,
        shard,
        desc.clone(),
        output_desc,
        quotient,
        0,
        Antichain::new(),
    );
    let clean = batches(h.await_upper(sink_id, frontier(1)).await);
    assert_eq!(assert_chain(&frontier(0), &clean), frontier(1));
    assert!(clean.iter().all(|batch| batch.error.is_none()));

    h.write(shard, &desc, 1, &[(row(3), 1)]).await;
    let poisoned = batches(h.await_upper(sink_id, frontier(2)).await);
    assert_eq!(poisoned.len(), 1, "{poisoned:?}");
    assert_eq!(
        (&poisoned[0].lower, &poisoned[0].upper),
        (&frontier(1), &frontier(2))
    );
    let error = poisoned[0]
        .error
        .as_deref()
        .expect("a poisoned description");
    assert!(error.contains("division by zero"), "{error}");

    // Neither later updates nor later progress produce a description.
    h.write(shard, &desc, 2, &[(row(4), 1)]).await;
    h.write(shard, &desc, 3, &[]).await;
    let ok = h
        .driver
        .await_batched_subscribe(sink_id, frontier(3), Duration::from_secs(5))
        .await;
    assert!(ok.is_err(), "no description follows a poisoned one: {ok:?}");
    let after = h.driver.take_batched_subscribe(sink_id).expect("events");
    assert_eq!(after, vec![]);
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn descriptions_form_a_gapless_chain_from_the_as_of() {
    let Some(h) = harness().await else {
        return;
    };
    let (source_id, sink_id) = (GlobalId::User(1600), GlobalId::User(1601));
    let shard = ShardId::new();
    let desc = key_desc();
    for t in 0..3 {
        let k = i64::try_from(t).unwrap();
        h.write(shard, &desc, t, &[(row(k), 1), (row(100 + k), 1)])
            .await;
    }
    let as_of = 2;
    h.subscribe(
        source_id,
        sink_id,
        shard,
        desc.clone(),
        desc.clone(),
        |get| get,
        as_of,
        Antichain::new(),
    );

    // Writes race the sink, so description boundaries vary between runs. The chain and the
    // placement of every update in it do not.
    let mut expected = Vec::new();
    for t in 0..3 {
        let k = i64::try_from(t).unwrap();
        expected.push((row(k), ts(as_of), 1));
        expected.push((row(100 + k), ts(as_of), 1));
    }
    for t in 3..20 {
        let k = i64::try_from(t).unwrap();
        let updates = [(row(k), 1), (row(k - 1), -1), (row(100 + k), 1)];
        h.write(shard, &desc, t, &updates).await;
        expected.extend(updates.into_iter().map(|(row, diff)| (row, ts(t), diff)));
    }
    let events = batches(h.await_upper(sink_id, frontier(20)).await);
    assert_eq!(assert_chain(&frontier(as_of), &events), frontier(20));
    for (index, batch) in events.iter().enumerate() {
        assert_eq!(batch.error, None);
        for (_, time, _) in &batch.updates {
            assert!(
                batch.lower.less_equal(time) && !batch.upper.less_equal(time),
                "update at {time} lies outside its description: {batch:?}",
            );
            if index > 0 {
                assert_ne!(
                    *time,
                    ts(as_of),
                    "only the first description holds the snapshot"
                );
            }
        }
    }
    expected.sort();
    assert_eq!(all_updates(&events), expected);
}
