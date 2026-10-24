// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the compute state's own helpers, as opposed to the peek sweep it drives.

use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::sinks::{
    BatchedSubscribeSinkConnection, ComputeSinkConnection, ComputeSinkDesc,
};
use mz_dyncfg::ConfigUpdates;
use mz_repr::{Datum, Diff, RelationDesc, SqlScalarType};

use super::peek_stash::tests::CountedBlob;
use super::peek_stash::{DeliveredBatch, StashUpload, stash_shard_id};
use super::peek_sweep_tests::Harness;
use super::*;
use crate::server::{WorkerResponse, forward_response};
use crate::sink::batched_subscribe::OwnedShare;

/// The sink the batched subscribe fixtures export.
const BATCHED_SINK: GlobalId = GlobalId::User(1);

/// A dataflow that exports a batched subscribe as [`BATCHED_SINK`].
fn batched_subscribe_dataflow() -> DataflowDescription<(), ()> {
    let mut dataflow = DataflowDescription::new("batched".to_string());
    dataflow.sink_exports.insert(
        BATCHED_SINK,
        ComputeSinkDesc {
            from: GlobalId::User(0),
            from_desc: RelationDesc::empty(),
            connection: ComputeSinkConnection::BatchedSubscribe(BatchedSubscribeSinkConnection {
                stash_id: Uuid::nil(),
                with_snapshot: true,
                output_desc: RelationDesc::empty(),
                mfp: None,
            }),
            with_snapshot: true,
            up_to: Antichain::new(),
            non_null_assertions: Vec::new(),
            refresh_schedule: None,
            from_arrangement: None,
        },
    );
    dataflow
}

fn dropped_at(frontier: Timestamp) -> QueuedResponse {
    QueuedResponse::DroppedAt {
        frontier: Antichain::from_elem(frontier),
    }
}

/// Reconciliation retains no dataflow that exports a sink in `subscribe_ids`, because the
/// subscribe response buffers it discards are shared.
#[mz_ore::test]
fn a_batched_subscribe_is_not_retained_by_reconciliation() {
    assert!(
        batched_subscribe_dataflow()
            .subscribe_ids()
            .any(|id| id == BATCHED_SINK),
        "a dataflow exporting a batched subscribe is rebuilt by reconciliation",
    );
}

#[mz_ore::test]
fn batched_subscribes_report_no_frontiers() {
    assert!(
        subscribe_or_copy_ids(&batched_subscribe_dataflow()).contains(&BATCHED_SINK),
        "a batched subscribe's collection is created with frontier reporting suppressed",
    );
}

#[mz_ore::test(tokio::test)]
async fn reconciliation_discards_queued_batched_subscribe_responses() {
    let mut harness = Harness::new(|_| {});
    // A sink rendered before the reconcile holds the buffer it was rendered with.
    let old_buffer = Rc::clone(&harness.state.batched_subscribe_response_buffer);
    old_buffer
        .borrow_mut()
        .push((BATCHED_SINK, dropped_at(Timestamp::from(1))));

    harness.state.discard_queued_sink_responses();
    // Dropping the sink's token during reconciliation queues a `DroppedAt` on the old buffer.
    old_buffer
        .borrow_mut()
        .push((BATCHED_SINK, dropped_at(Timestamp::from(2))));
    harness.active().process_batched_subscribes();
    assert!(
        harness.responses.try_recv().is_err(),
        "no response queued before the reconcile is sent",
    );

    let new_buffer = Rc::clone(&harness.state.batched_subscribe_response_buffer);
    new_buffer
        .borrow_mut()
        .push((BATCHED_SINK, dropped_at(Timestamp::from(3))));
    harness.active().process_batched_subscribes();
    let (response, _nonce) = harness
        .responses
        .try_recv()
        .expect("a response queued after the reconcile is sent");
    assert_eq!(
        response.into_compute(),
        ComputeResponse::BatchedSubscribeResponse(
            BATCHED_SINK,
            Box::new(dropped_at(Timestamp::from(3)).into_response()),
        ),
    );
}

/// A stashed share of four updates, written to `blob`.
async fn stashed_share(blob: &CountedBlob) -> OwnedShare {
    let desc = RelationDesc::builder()
        .with_column("value", SqlScalarType::Int64.nullable(false))
        .finish();
    let mut upload = StashUpload::open_range(
        blob.clients(),
        PersistLocation::new_in_mem(),
        // Above the runs this upload produces, so no merge writes parts of its own.
        1024,
        stash_shard_id(Uuid::new_v4()),
        desc,
        Antichain::from_elem(Timestamp::from(0)),
        Antichain::from_elem(Timestamp::from(5)),
    )
    .await
    .expect("the in-memory location opens");
    for value in 0..4 {
        upload
            .push_update(
                &Row::pack_slice(&[Datum::Int64(value)]),
                Timestamp::from(1),
                Diff::ONE,
            )
            .await
            .expect("persist takes the update");
    }
    let batch = upload
        .finish_stashed()
        .await
        .expect("persist finishes the batch")
        .expect("updates were pushed");
    let shard_id = batch.shard_id();
    OwnedShare::Stashed {
        delivered: DeliveredBatch::new(batch, tokio::runtime::Handle::current(), shard_id),
        last_error_time: None,
    }
}

/// A stashed share queued on a buffer that a reconcile discards is deleted, since no response
/// will ever name it.
#[mz_ore::test(tokio::test)]
async fn discarding_queued_batched_subscribe_responses_deletes_their_stash_batches() {
    let mut harness = Harness::new(|_| {});
    let blob = CountedBlob::new();
    let share = stashed_share(&blob).await;

    let old_buffer = Rc::clone(&harness.state.batched_subscribe_response_buffer);
    old_buffer.borrow_mut().push((
        BATCHED_SINK,
        QueuedResponse::Batch {
            lower: Antichain::from_elem(Timestamp::from(0)),
            upper: Antichain::from_elem(Timestamp::from(5)),
            shares: vec![share],
            fatal: None,
        },
    ));
    harness.state.discard_queued_sink_responses();
    // The sink that held the old buffer goes with the dataflow the reconcile drops.
    drop(old_buffer);

    blob.wait_until_nothing_is_left("a discarded queued response")
        .await;
    assert_eq!(blob.deletes_of_nothing(), 0);
    harness.active().process_batched_subscribes();
    assert!(harness.responses.try_recv().is_err(), "nothing is sent");
}

#[mz_ore::test(tokio::test)]
async fn a_response_for_a_departed_client_deletes_its_stash_batches() {
    let blob = CountedBlob::new();
    let share = stashed_share(&blob).await;
    let (response_tx, response_rx) = tokio::sync::mpsc::unbounded_channel();
    drop(response_rx);

    let forwarded = forward_response(
        &response_tx,
        WorkerResponse::BatchedSubscribe(
            BATCHED_SINK,
            QueuedResponse::Batch {
                lower: Antichain::from_elem(Timestamp::from(0)),
                upper: Antichain::from_elem(Timestamp::from(5)),
                shares: vec![share],
                fatal: None,
            },
        ),
    );
    assert!(!forwarded, "a closed client takes no response");
    blob.wait_until_nothing_is_left("a response for a departed client")
        .await;
    assert_eq!(blob.deletes_of_nothing(), 0);
}

#[mz_ore::test]
fn row_iteration_limit_observes_updates_and_disabled_rows() {
    let config = mz_dyncfgs::all_dyncfgs();
    let row_iteration_config = PeekRowIterationConfig::new(&config);
    let mut tracker = PeekRowIterationTracker::new(row_iteration_config.current_limit(), 0);

    tracker.track_next().unwrap();
    tracker.track_next().unwrap();

    let mut updates = ConfigUpdates::default();
    updates.add(&PEEK_ROW_ITERATION_LIMIT, 3);
    updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, true);
    updates.apply(&config);
    tracker.set_limit(row_iteration_config.current_limit());
    tracker.track_next().unwrap();

    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, false);
    updates.apply(&config);
    tracker.set_limit(row_iteration_config.current_limit());
    tracker.track_next().unwrap();

    let mut updates = ConfigUpdates::default();
    updates.add(&PEEK_ROW_ITERATION_LIMIT, 5);
    updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, true);
    updates.apply(&config);
    tracker.set_limit(row_iteration_config.current_limit());
    tracker.track_next().unwrap();
    assert_eq!(
        tracker.track_next(),
        Err(PeekError::RowIterationLimitExceeded { limit: 5 })
    );
}
