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
use mz_repr::RelationDesc;

use super::peek_sweep_tests::Harness;
use super::*;

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
                start: Timestamp::MIN,
                output_desc: RelationDesc::empty(),
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

fn dropped_at(frontier: Timestamp) -> BatchedSubscribeResponse {
    BatchedSubscribeResponse::DroppedAt {
        frontier: Antichain::from_elem(frontier),
        orphans: Vec::new(),
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
        response,
        ComputeResponse::BatchedSubscribeResponse(
            BATCHED_SINK,
            Box::new(dropped_at(Timestamp::from(3))),
        ),
    );
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
