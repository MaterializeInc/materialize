// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

fn observation(replica: u64, since: u64, upper: u64) -> (GlobalId, ReplicaId, FrontiersResponse) {
    (
        GlobalId::User(1),
        ReplicaId::User(replica),
        FrontiersResponse {
            read_frontier: Some(Antichain::from_elem(Timestamp::from(since))),
            write_frontier: Some(Antichain::from_elem(Timestamp::from(upper))),
            ..Default::default()
        },
    )
}

fn row(since: Option<u64>, upper: Option<u64>) -> Row {
    Row::pack_slice(&[
        Datum::String("u1"),
        since.map_or(Datum::Null, |t| Timestamp::from(t).into()),
        upper.map_or(Datum::Null, |t| Timestamp::from(t).into()),
    ])
}

#[mz_ore::test]
fn native_frontiers_distinguish_unknown_from_completed() {
    let mut reporter = NativeFrontiers::default();
    let (id, replica, mut partial) = observation(1, 10, 20);
    partial.read_frontier = None;
    let updates = reporter.update([(id, replica, partial)]);
    assert!(
        updates[0].1.is_empty(),
        "no global row without a read observation"
    );
    assert_eq!(
        updates[1].1.len(),
        1,
        "the observed replica upper is reportable"
    );

    let updates = reporter.update([observation(1, 10, 20), observation(2, 15, 30)]);
    assert_eq!(updates[0].1, vec![(row(Some(10), Some(30)), Diff::ONE)]);
    let (id, replica, mut completed) = observation(2, 15, 30);
    completed.write_frontier = Some(Antichain::new());
    let updates = reporter.update([observation(1, 10, 20), (id, replica, completed)]);
    assert_eq!(
        updates[0].1,
        vec![
            (row(Some(10), Some(30)), Diff::MINUS_ONE),
            (row(Some(10), None), Diff::ONE),
        ]
    );
}

#[mz_ore::test]
fn native_frontiers_retract_disconnected_and_dropped_observations() {
    let mut reporter = NativeFrontiers::default();
    reporter.update([observation(1, 10, 20), observation(2, 15, 30)]);
    assert!(
        reporter
            .update([observation(1, 10, 20), observation(2, 15, 30)])
            .iter()
            .all(|(_, updates)| updates.is_empty())
    );

    let updates = reporter.update([observation(1, 10, 20)]);
    assert_eq!(
        updates[0].1,
        vec![
            (row(Some(10), Some(30)), Diff::MINUS_ONE),
            (row(Some(10), Some(20)), Diff::ONE),
        ]
    );
    assert_eq!(updates[1].1.len(), 1);
    assert_eq!(updates[1].1[0].1, Diff::MINUS_ONE);

    // Reconnection can reveal a different actual since, not the stale cache.
    let updates = reporter.update([observation(1, 18, 25)]);
    assert_eq!(
        updates[0].1,
        vec![
            (row(Some(10), Some(20)), Diff::MINUS_ONE),
            (row(Some(18), Some(25)), Diff::ONE),
        ]
    );
    let updates = reporter.update([]);
    assert_eq!(
        updates[0].1,
        vec![(row(Some(18), Some(25)), Diff::MINUS_ONE)]
    );
    assert_eq!(updates[1].1.len(), 1);
    assert_eq!(updates[1].1[0].1, Diff::MINUS_ONE);
    assert!(
        reporter
            .update([])
            .iter()
            .all(|(_, updates)| updates.is_empty())
    );
}
