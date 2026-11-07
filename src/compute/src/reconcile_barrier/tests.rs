// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::mpsc;
use std::time::Duration;

use super::*;

#[mz_ore::test]
fn a_side_passes_once_the_other_finished_the_same_connections_drops() {
    let barrier = ReconcileBarrier::default();
    barrier.arrive(BarrierSide::Interactive, 0);
    assert!(
        !barrier.passable(BarrierSide::Interactive, 0),
        "the interactive side waits while maintenance has not dropped what the connection replaces"
    );
    barrier.arrive(BarrierSide::Maintenance, 0);
    assert!(barrier.passable(BarrierSide::Maintenance, 0));
    assert!(barrier.passable(BarrierSide::Interactive, 0));
}

/// A runtime that never finishes reconciling a connection lets the other one pass it once it
/// finished the drops of a later connection.
#[mz_ore::test]
fn a_later_arrival_releases_a_side_whose_connection_the_other_skipped() {
    let barrier = ReconcileBarrier::default();
    barrier.arrive(BarrierSide::Maintenance, 0);
    assert!(!barrier.passable(BarrierSide::Maintenance, 0));
    // The interactive runtime never received the first connection's `InitializationComplete`.
    barrier.arrive(BarrierSide::Interactive, 1);
    assert!(
        barrier.passable(BarrierSide::Maintenance, 0),
        "an arrival at a later connection releases the earlier one"
    );
    assert!(
        !barrier.passable(BarrierSide::Interactive, 1),
        "an arrival at an earlier connection does not release the later one"
    );
    barrier.arrive(BarrierSide::Maintenance, 1);
    assert!(barrier.passable(BarrierSide::Interactive, 1));
}

#[mz_ore::test]
fn a_waiter_passes_once_the_other_side_arrived_past_its_connection_before_it_rechecked() {
    let barrier = ReconcileBarrier::default();
    barrier.arrive(BarrierSide::Interactive, 0);
    assert!(!barrier.passable(BarrierSide::Interactive, 0));
    // Maintenance passes the connection, serves it, and finishes the next connection's drops
    // before the interactive worker rechecks, which overwrites its arrival at the first.
    barrier.arrive(BarrierSide::Maintenance, 0);
    assert!(barrier.passable(BarrierSide::Maintenance, 0));
    barrier.arrive(BarrierSide::Maintenance, 1);
    assert!(
        barrier.passable(BarrierSide::Interactive, 0),
        "the waiter must pass, or both sides wait forever"
    );
}

#[mz_ore::test]
fn record_keeps_the_first_position_of_a_nonce() {
    let order = ConnectionOrder::default();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    order.record(first);
    order.record(second);
    order.record(first);
    assert_eq!(order.position(first), Some(0));
    assert_eq!(order.position(second), Some(1));
    assert_eq!(order.position(Uuid::new_v4()), None);
}

/// A waiting side parks after `passable` returns `false`, and the other side's arrival unparks
/// it, so no arrival is lost between the check and the park.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn an_arrival_wakes_the_waiting_side() {
    let barrier = ReconcileBarrier::default();
    let (passed_tx, passed_rx) = mpsc::channel();
    let waiter = {
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.arrive(BarrierSide::Interactive, 0);
            while !barrier.passable(BarrierSide::Interactive, 0) {
                // No timeout: only the arrival's unpark ends this park.
                std::thread::park();
            }
            passed_tx.send(()).expect("receiver alive");
        })
    };
    assert!(
        passed_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "the interactive side passed before maintenance arrived"
    );
    barrier.arrive(BarrierSide::Maintenance, 0);
    passed_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("the arrival wakes the waiting side");
    waiter.join().expect("waiter panicked");
}
