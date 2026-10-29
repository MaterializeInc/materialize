// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

#[mz_ore::test]
fn native_lag_metrics_sample_and_retire_replica_collections() {
    use mz_cluster_client::metrics::ControllerMetrics;
    use mz_controller_types::ClusterId;
    use mz_ore::metrics::MetricsRegistry;

    let registry = MetricsRegistry::new();
    let metrics = ControllerMetrics::new(&registry);
    let mut lag = lag::NativeWallclockLag::default();
    let sample = |value| {
        (
            ClusterId::User(2),
            GlobalId::User(3),
            ReplicaId::User(4),
            value,
        )
    };
    lag.update_metrics(&metrics, [sample(WallclockLag::Seconds(7))]);
    lag.update_metrics(&metrics, [sample(WallclockLag::Undefined)]);
    let families = registry.gather();
    let count = families
        .iter()
        .find(|family| family.name() == "mz_dataflow_wallclock_lag_seconds_count")
        .expect("native lag samples produce a public metric series");
    assert_eq!(count.get_metric().len(), 1);
    let count = &count.get_metric()[0];
    assert_eq!(count.get_counter().as_ref().expect("counter").value(), 2.0);
    let labels: BTreeMap<_, _> = count
        .get_label()
        .iter()
        .map(|label| (label.name(), label.value()))
        .collect();
    assert_eq!(
        labels,
        BTreeMap::from([
            ("collection_id", "u3"),
            ("instance_id", "u2"),
            ("replica_id", "u4"),
        ])
    );

    lag.update_metrics(&metrics, []);
    assert!(registry.gather().iter().all(|family| {
        !family
            .name()
            .starts_with("mz_dataflow_wallclock_lag_seconds")
            || family.get_metric().is_empty()
    }));
}

fn row(since: Option<u64>, upper: Option<u64>) -> Row {
    Row::pack_slice(&[
        Datum::String("u1"),
        since.map_or(Datum::Null, |t| Timestamp::from(t).into()),
        upper.map_or(Datum::Null, |t| Timestamp::from(t).into()),
    ])
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
