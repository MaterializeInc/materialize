// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

fn observation(
    replica: u64,
    since: u64,
    upper: u64,
) -> (GlobalId, ReplicaId, bool, FrontiersResponse) {
    (
        GlobalId::User(1),
        ReplicaId::User(replica),
        false,
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
    let (id, replica, storage_sink, mut partial) = observation(1, 10, 20);
    partial.read_frontier = None;
    let updates = reporter.update([(id, replica, storage_sink, partial)]);
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
    let (id, replica, storage_sink, mut completed) = observation(2, 15, 30);
    completed.write_frontier = Some(Antichain::new());
    let updates = reporter.update([
        observation(1, 10, 20),
        (id, replica, storage_sink, completed),
    ]);
    assert_eq!(
        updates[0].1,
        vec![
            (row(Some(10), Some(30)), Diff::MINUS_ONE),
            (row(Some(10), None), Diff::ONE),
        ]
    );
}

#[mz_ore::test]
fn native_storage_sinks_report_replica_but_not_global_frontiers() {
    let mut reporter = NativeFrontiers::default();
    let (id, replica, _, frontiers) = observation(1, 10, 20);
    let updates = reporter.update([(id, replica, true, frontiers)]);
    assert!(updates[0].1.is_empty(), "storage owns the global row");
    let replica_row = Row::pack_slice(&[
        Datum::String("u1"),
        Datum::String("u1"),
        Timestamp::from(20).into(),
    ]);
    assert_eq!(updates[1].1, vec![(replica_row.clone(), Diff::ONE)]);
    let updates = reporter.update([]);
    assert!(updates[0].1.is_empty());
    assert_eq!(updates[1].1, vec![(replica_row, Diff::MINUS_ONE)]);
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

#[mz_ore::test]
fn native_refresh_rows_follow_observed_storage_progress() {
    let schedule = RefreshSchedule {
        everies: vec![],
        ats: vec![5.into(), 10.into(), 20.into()],
    };
    let initial_as_of = Antichain::from_elem(10.into());
    let snapshot = |upper: Option<&Antichain<Timestamp>>| {
        refresh_row(GlobalId::User(1), &schedule, &initial_as_of, upper)
            .into_iter()
            .collect()
    };
    let mut reporter = NativeFrontiers::default();
    assert!(replace_rows(&mut reporter.refreshes, snapshot(None)).is_empty());

    // No replica observations are needed. The committed first refresh, not an
    // earlier time in the schedule, is pending until storage passes it.
    let pending = row(None, Some(10));
    assert_eq!(
        replace_rows(
            &mut reporter.refreshes,
            snapshot(Some(&Antichain::from_elem(0.into())))
        ),
        vec![(pending.clone(), Diff::ONE)]
    );
    assert!(replace_rows(&mut reporter.refreshes, snapshot(Some(&initial_as_of))).is_empty());

    let mut previous = pending;
    for (upper, expected) in [
        // Preserve the observed upper even when it is between scheduled times.
        (Antichain::from_elem(11.into()), row(Some(10), Some(11))),
        (Antichain::from_elem(20.into()), row(Some(10), Some(20))),
        (Antichain::new(), row(Some(20), None)),
    ] {
        assert_eq!(
            replace_rows(&mut reporter.refreshes, snapshot(Some(&upper))),
            vec![(previous, Diff::MINUS_ONE), (expected.clone(), Diff::ONE)]
        );
        previous = expected;
    }
    assert!(
        replace_rows(&mut reporter.refreshes, snapshot(Some(&Antichain::new()))).is_empty(),
        "completed rows remain until the MV is dropped"
    );
    assert_eq!(
        replace_rows(&mut reporter.refreshes, BTreeSet::new()),
        vec![(previous, Diff::MINUS_ONE)]
    );
}

#[mz_ore::test]
fn native_periodic_refresh_distinguishes_max_from_completion_and_unknown() {
    use std::time::Duration;

    use mz_repr::refresh_schedule::RefreshEvery;

    let schedule = RefreshSchedule {
        everies: vec![RefreshEvery {
            interval: Duration::from_millis(10),
            aligned_to: 0.into(),
        }],
        ats: vec![],
    };
    let initial_as_of = Antichain::from_elem(10.into());
    let report = |upper: Option<&Antichain<Timestamp>>| {
        refresh_row(GlobalId::User(1), &schedule, &initial_as_of, upper)
    };
    assert_eq!(report(None), None);
    assert_eq!(
        report(Some(&Antichain::from_elem(Timestamp::MAX))),
        Some(row(Some(u64::MAX - 5), Some(u64::MAX)))
    );
    assert_eq!(
        report(Some(&Antichain::new())),
        Some(row(Some(u64::MAX), None))
    );
}
