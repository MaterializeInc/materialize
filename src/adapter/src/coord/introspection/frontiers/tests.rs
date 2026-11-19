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
