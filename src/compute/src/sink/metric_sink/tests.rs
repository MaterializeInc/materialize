// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::metrics::ComputeMetrics;
use crate::server::ComputeRuntimeRole;

/// A frontier that has closed every timestamp strictly below `bound`.
fn frontier(bound: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(Timestamp::from(bound))
}

fn label_a() -> Vec<(String, String)> {
    vec![("a".into(), "1".into())]
}

/// `label_a()`, borrowed: what `stage_ok` now takes (see `extract_row`).
const LABEL_A: &[(&str, Option<&str>)] = &[("a", Some("1"))];

fn key_m() -> PublishedKey {
    ("m".into(), label_a())
}

/// Mirrors the shaped relation `shape_metric_sink_source` builds: `labels`/`help` are
/// non-null by construction, `metric_name`/`value` stay nullable, and `metric_kind`/
/// `name_valid` are the planner's computed classification columns.
fn shaped_desc() -> RelationDesc {
    use mz_repr::SqlScalarType;

    RelationDesc::builder()
        .with_column("metric_name", SqlScalarType::String.nullable(true))
        .with_column(
            "labels",
            SqlScalarType::Map {
                value_type: Box::new(SqlScalarType::String),
                custom_id: None,
            }
            .nullable(false),
        )
        .with_column("value", SqlScalarType::Float64.nullable(true))
        .with_column("help", SqlScalarType::String.nullable(false))
        .with_column("metric_kind", SqlScalarType::Int32.nullable(true))
        .with_column("name_valid", SqlScalarType::Bool.nullable(true))
        .finish()
}

/// Stages one gauge update for the `(m, {a:1})` series.
fn stage_m(st: &mut SinkState, value: f64, time: u64, diff: i64) {
    st.stage_ok(
        "m",
        Some(MetricKind::Gauge),
        true,
        LABEL_A,
        Some(value),
        "h",
        Timestamp::from(time),
        diff,
    );
}

/// Stages one gauge update with a null `value` for the `(m, {a:1})` series.
fn stage_m_null(st: &mut SinkState, time: u64, diff: i64) {
    st.stage_ok(
        "m",
        Some(MetricKind::Gauge),
        true,
        LABEL_A,
        None,
        "h",
        Timestamp::from(time),
        diff,
    );
}

#[mz_ore::test]
fn fold_and_publish() {
    let mut st = SinkState::default();
    stage_m(&mut st, 2.0, 0, 1);
    st.integrate(&frontier(1));
    st.publish_if_healthy();
    assert_eq!(st.published.len(), 1);
    assert_eq!(st.published[&key_m()].0, 2.0);

    // Unsupported type (metric_kind = None) is skipped and counted.
    st.stage_ok("h1", None, true, &[], Some(1.0), "h", Timestamp::from(1), 1);
    st.integrate(&frontier(2));
    assert_eq!(st.skipped, 1);

    // Error freezes publication. The update at time 2 retracts the old value and inserts the
    // new one, both fold into `working` while frozen.
    st.errors = 1;
    stage_m(&mut st, 2.0, 2, -1);
    stage_m(&mut st, 9.0, 2, 1);
    st.integrate(&frontier(3));
    st.publish_if_healthy();
    assert_eq!(st.published[&key_m()].0, 2.0);

    // Recovery republishes the integrated value.
    st.errors = 0;
    st.publish_if_healthy();
    assert_eq!(st.published[&key_m()].0, 9.0);
    assert_eq!(st.collisions, 0);
}

#[mz_ore::test]
fn value_update_split_across_activations_no_collision() {
    let mut st = SinkState::default();
    // Establish the series at value 5.
    stage_m(&mut st, 5.0, 0, 1);
    st.integrate(&frontier(1));
    st.publish_if_healthy();
    assert_eq!(st.published[&key_m()].0, 5.0);

    // A value update 5 -> 9 at time 1 arrives insert-first, split across two activations. The
    // timestamp stays open until both diffs are buffered.
    stage_m(&mut st, 9.0, 1, 1);
    st.integrate(&frontier(1));
    st.publish_if_healthy();
    assert_eq!(st.collisions, 0);
    stage_m(&mut st, 5.0, 1, -1);

    // Close the timestamp: the series is present at value 9 with no collision.
    st.integrate(&frontier(2));
    st.publish_if_healthy();
    assert_eq!(st.published[&key_m()].0, 9.0);
    assert_eq!(st.collisions, 0);
}

#[mz_ore::test]
fn duplicate_multiplicity_consolidates() {
    let mut st = SinkState::default();
    // The same identity at multiplicity 2 consolidates to a single live row.
    stage_m(&mut st, 5.0, 0, 1);
    stage_m(&mut st, 5.0, 0, 1);
    st.integrate(&frontier(1));
    st.publish_if_healthy();
    assert_eq!(st.published[&key_m()].0, 5.0);
    assert_eq!(st.collisions, 0);

    // A second, distinct live value for the same series is a genuine collision.
    stage_m(&mut st, 7.0, 1, 1);
    st.integrate(&frontier(2));
    st.publish_if_healthy();
    assert_eq!(st.collisions, 1);
    // The smallest value wins deterministically.
    assert_eq!(st.published[&key_m()].0, 5.0);
}

#[mz_ore::test]
fn no_publish_before_time_closed() {
    let mut st = SinkState::default();
    // An update at time 5 must not appear while the frontier still allows data at time 5.
    stage_m(&mut st, 2.0, 5, 1);
    st.integrate(&frontier(5));
    st.publish_if_healthy();
    assert!(st.published.is_empty());

    // Once the frontier advances past time 5, the update publishes.
    st.integrate(&frontier(6));
    st.publish_if_healthy();
    assert_eq!(st.published[&key_m()].0, 2.0);
}

#[mz_ore::test]
fn null_value_gaps_series() {
    let mut st = SinkState::default();
    // A null-valued row for (m,{a}) at a closed time: no series, counted in null_values.
    stage_m_null(&mut st, 1, 1);
    st.integrate(&frontier(2));
    st.publish_if_healthy();
    assert!(!st.published.contains_key(&key_m()));
    assert_eq!(st.null_values, 1);

    // A later non-null value republishes the series (gap closes) and clears the count, even
    // though the null-valued row is still live alongside it.
    stage_m(&mut st, 5.0, 3, 1);
    st.integrate(&frontier(4));
    st.publish_if_healthy();
    assert_eq!(st.published[&key_m()].0, 5.0);
    assert_eq!(st.null_values, 0);
}

#[mz_ore::test]
fn null_labels_become_empty() {
    let mut st = SinkState::default();
    // An empty label vector (the shaped relation's `{}` for a source row with no labels)
    // keys and publishes correctly.
    st.stage_ok(
        "m",
        Some(MetricKind::Gauge),
        true,
        &[],
        Some(1.0),
        "h",
        Timestamp::from(1),
        1,
    );
    st.integrate(&frontier(2));
    st.publish_if_healthy();
    assert_eq!(st.published[&("m".into(), vec![])].0, 1.0);
}

#[mz_ore::test]
fn extract_row_normalizes_null_datums() {
    let desc = shaped_desc();
    let cols = ColumnIndices::resolve(&desc);

    let mut row = Row::default();
    {
        let mut packer = row.packer();
        packer.push(Datum::Null); // metric_name
        packer.push_dict_with(|_| {}); // labels: always non-null by construction
        packer.push(Datum::Null); // value
        packer.push(Datum::String("")); // help: always non-null by construction
        packer.push(Datum::Null); // metric_kind: defensively treated as unsupported
        packer.push(Datum::Null); // name_valid: defensively treated as invalid
    }

    let datums: Vec<Datum> = row.iter().collect();
    let (name, metric_kind, name_valid, labels, value, help) = extract_row(&cols, &datums);
    assert_eq!(name, "");
    assert_eq!(metric_kind, None);
    assert!(!name_valid);
    assert_eq!(labels, Vec::new());
    assert_eq!(value, None);
    assert_eq!(help, "");
}

/// A null map value must survive extraction as `None`; unwrapping it as a string panicked the
/// worker and took down the replica.
#[mz_ore::test]
fn extract_row_keeps_null_label_values() {
    let desc = shaped_desc();
    let cols = ColumnIndices::resolve(&desc);

    let mut row = Row::default();
    {
        let mut packer = row.packer();
        packer.push(Datum::String("m"));
        packer.push_dict_with(|row| {
            row.push(Datum::String("bad"));
            row.push(Datum::Null);
            row.push(Datum::String("good"));
            row.push(Datum::String("1"));
        });
        packer.push(Datum::Float64(1.0.into()));
        packer.push(Datum::String("h"));
        packer.push(Datum::Int32(0));
        packer.push(Datum::True);
    }

    let datums: Vec<Datum> = row.iter().collect();
    let (_name, _metric_kind, _name_valid, labels, _value, _help) = extract_row(&cols, &datums);
    assert_eq!(labels, vec![("bad", None), ("good", Some("1"))]);
}

#[mz_ore::test]
fn null_or_empty_label_value_skips_row() {
    let mut st = SinkState::default();
    // A null label value has no representable label set: publishes nothing, counts as skipped.
    st.stage_ok(
        "m",
        Some(MetricKind::Gauge),
        true,
        &[("a", None)],
        Some(1.0),
        "h",
        Timestamp::from(1),
        1,
    );
    st.integrate(&frontier(2));
    st.publish_if_healthy();
    assert!(st.published.is_empty());
    assert_eq!(st.skipped, 1);

    // An empty label value folds to `{}` in Prometheus, so it is skipped too rather than
    // published as a bare `{}` series. It is a distinct identity from the null row, so both
    // stay live and count: `skipped` reaches 2, not 1.
    st.stage_ok(
        "m",
        Some(MetricKind::Gauge),
        true,
        &[("a", Some(""))],
        Some(1.0),
        "h",
        Timestamp::from(3),
        1,
    );
    st.integrate(&frontier(4));
    st.publish_if_healthy();
    assert!(st.published.is_empty());
    assert_eq!(st.skipped, 2);
}

fn pkey(name: &str, labels: &[(&str, &str)]) -> PublishedKey {
    (
        name.to_string(),
        labels
            .iter()
            .map(|&(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    )
}

#[mz_ore::test]
fn build_families_groups_by_name_and_kind() {
    let published: BTreeMap<PublishedKey, PublishedValue> = BTreeMap::from([
        (
            pkey("http_requests", &[("code", "200")]),
            (5.0, MetricKind::Counter, "requests".to_string()),
        ),
        (
            pkey("http_requests", &[("code", "500")]),
            (2.0, MetricKind::Counter, "requests".to_string()),
        ),
        (
            pkey("temp_celsius", &[]),
            (21.5, MetricKind::Gauge, "temperature".to_string()),
        ),
    ]);

    let families = build_families(&published);

    // One family per metric name, in `BTreeMap` (name) order.
    assert_eq!(families.len(), 2);

    let requests = &families[0];
    assert_eq!(requests.name(), "http_requests");
    assert_eq!(requests.help(), "requests");
    let metrics = requests.get_metric();
    assert_eq!(metrics.len(), 2);
    // Metrics keep the `BTreeMap` label order, and land in the counter oneof.
    assert_eq!(metrics[0].get_label()[0].value(), "200");
    assert_eq!(metrics[0].get_counter().value(), 5.0);
    assert_eq!(metrics[1].get_label()[0].value(), "500");
    assert_eq!(metrics[1].get_counter().value(), 2.0);

    let temp = &families[1];
    assert_eq!(temp.name(), "temp_celsius");
    let temp_metrics = temp.get_metric();
    assert_eq!(temp_metrics.len(), 1);
    assert_eq!(temp_metrics[0].get_gauge().value(), 21.5);
}

#[mz_ore::test]
fn count_conflicts_flags_type_and_help_disagreement() {
    // The family winner is the smallest-label entry. Here `m`'s winner is the `[a=1]` gauge with
    // help `h1`; the other two disagree on kind, then help.
    let published: BTreeMap<PublishedKey, PublishedValue> = BTreeMap::from([
        (
            pkey("m", &[("a", "1")]),
            (1.0, MetricKind::Gauge, "h1".to_string()),
        ),
        (
            pkey("m", &[("b", "2")]),
            (2.0, MetricKind::Counter, "h1".to_string()),
        ),
        (
            pkey("m", &[("c", "3")]),
            (3.0, MetricKind::Gauge, "h2".to_string()),
        ),
        (
            pkey("other", &[]),
            (1.0, MetricKind::Gauge, "h".to_string()),
        ),
    ]);
    assert_eq!(count_conflicts(&published), 2);

    // A family whose entries all agree has no conflicts.
    let consistent: BTreeMap<PublishedKey, PublishedValue> = BTreeMap::from([
        (
            pkey("m", &[("a", "1")]),
            (1.0, MetricKind::Gauge, "h".to_string()),
        ),
        (
            pkey("m", &[("b", "2")]),
            (2.0, MetricKind::Gauge, "h".to_string()),
        ),
    ]);
    assert_eq!(count_conflicts(&consistent), 0);
}

#[mz_ore::test]
fn err_stream_freezes_and_recovers() {
    let mut st = SinkState::default();
    // Establish a healthy value.
    stage_m(&mut st, 5.0, 0, 1);
    st.integrate(&frontier(1));
    st.publish_if_healthy();
    assert_eq!(st.published[&key_m()].0, 5.0);

    // An error appears, buffered through `stage_err` (not by setting `errors` directly). A value
    // update lands in the same window.
    st.stage_err(Timestamp::from(1), 1);
    stage_m(&mut st, 5.0, 1, -1);
    stage_m(&mut st, 9.0, 1, 1);
    st.integrate(&frontier(2));
    assert_eq!(st.errors, 1);
    st.publish_if_healthy();
    // Publication frozen at the last healthy value while erroring.
    assert_eq!(st.published[&key_m()].0, 5.0);

    // The error is retracted; net errors returns to 0 and publication recovers to the value
    // integrated during the freeze.
    st.stage_err(Timestamp::from(2), -1);
    st.integrate(&frontier(3));
    assert_eq!(st.errors, 0);
    st.publish_if_healthy();
    assert_eq!(st.published[&key_m()].0, 9.0);
}

/// Sums a counter family's samples across the registry's scrape output.
fn counter_total(registry: &MetricsRegistry, name: &str) -> f64 {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == name)
        .flat_map(|family| family.get_metric())
        .map(|metric| metric.get_counter().value())
        .sum()
}

/// Counts the samples of family `metric` whose `label_key` label equals `label`.
fn companion_gauge_count(
    registry: &MetricsRegistry,
    metric: &str,
    label_key: &str,
    label: &str,
) -> usize {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == metric)
        .flat_map(|family| family.get_metric())
        .filter(|metric| {
            metric
                .get_label()
                .iter()
                .any(|l| l.name() == label_key && l.value() == label)
        })
        .count()
}

#[mz_ore::test]
fn independent_sink_registrations_do_not_alias() {
    let registry = MetricsRegistry::new();
    let metrics = ComputeMetrics::register_with(&registry, ComputeRuntimeRole::Solo).for_worker(0);
    // These distinct labels alias when all six companion descriptor IDs are summed.
    let mut first = PendingRegistration::new(
        "u350".into(),
        SinkCollector::new("u350", Arc::new(Mutex::new(SinkState::default()))),
    );
    let mut second = PendingRegistration::new(
        "u518".into(),
        SinkCollector::new("u518", Arc::new(Mutex::new(SinkState::default()))),
    );
    assert_eq!(first.try_register(&registry, &metrics), Retry::Skip);
    assert_eq!(second.try_register(&registry, &metrics), Retry::Skip);
    assert_eq!(
        counter_total(
            &registry,
            "mz_compute_metric_sink_registration_retries_total"
        ),
        0.0
    );

    let assert_companions = |first_count, second_count| {
        for suffix in [
            "frontier_ms",
            "errors",
            "skipped",
            "conflicts",
            "collisions",
            "null_values",
        ] {
            let family = format!("mz_compute_metric_sink_{suffix}");
            for (label, count) in [("u350", first_count), ("u518", second_count)] {
                assert_eq!(
                    companion_gauge_count(&registry, &family, "sink", label),
                    count,
                    "{family}: {label}"
                );
            }
        }
    };
    assert_companions(1, 1);
    drop(first);
    assert_companions(0, 1);
}

/// The register/retry/unregister ordering the operator runs on every activation, with a stand-in
/// for the previous incarnation of the same curated sink. Two incarnations share a `Desc` id
/// because a curated sink's companion gauges are keyed on its stable name, so the new one can
/// only register once the old one's handle drops.
#[mz_ore::test]
fn pending_registration_retries_until_predecessor_drops() {
    const LABEL: &str = "mz_curated_example";
    const RETRIES: &str = "mz_compute_metric_sink_registration_retries_total";
    // A live collector emits exactly one frontier gauge, so its presence stands in for
    // "collector registered" across the register/drop lifecycle below.
    const FRONTIER: &str = "mz_compute_metric_sink_frontier_ms";
    const SINK_LABEL: &str = "sink";

    let registry = MetricsRegistry::new();
    let metrics = ComputeMetrics::register_with(&registry, ComputeRuntimeRole::Solo).for_worker(0);

    // The predecessor: an incarnation of the same sink still registered.
    let incumbent = registry.register_collector_with_dropper(SinkCollector::new(
        LABEL,
        Arc::new(Mutex::new(SinkState::default())),
    ));
    assert_eq!(
        companion_gauge_count(&registry, FRONTIER, SINK_LABEL, LABEL),
        1
    );

    let mut registration = PendingRegistration::new(
        LABEL.to_string(),
        SinkCollector::new(LABEL, Arc::new(Mutex::new(SinkState::default()))),
    );

    assert_eq!(registration.try_register(&registry, &metrics), Retry::Arm);
    assert_eq!(counter_total(&registry, RETRIES), 1.0);
    // The failed attempt registered nothing, so the incumbent is still the only series.
    assert_eq!(
        companion_gauge_count(&registry, FRONTIER, SINK_LABEL, LABEL),
        1
    );

    // A retry is armed, so an activation before the interval elapses attempts nothing.
    assert_eq!(registration.try_register(&registry, &metrics), Retry::Skip);
    assert_eq!(counter_total(&registry, RETRIES), 1.0);

    // Clearing `next_attempt` stands in for the retry interval elapsing.
    registration.next_attempt = None;
    assert_eq!(registration.try_register(&registry, &metrics), Retry::Arm);
    assert_eq!(counter_total(&registry, RETRIES), 2.0);

    drop(incumbent);
    registration.next_attempt = None;
    assert_eq!(registration.try_register(&registry, &metrics), Retry::Skip);
    assert_eq!(counter_total(&registry, RETRIES), 2.0);
    assert_eq!(
        companion_gauge_count(&registry, FRONTIER, SINK_LABEL, LABEL),
        1
    );

    // Already registered: a further activation is a no-op, not a second registration.
    assert_eq!(registration.try_register(&registry, &metrics), Retry::Skip);
    assert_eq!(counter_total(&registry, RETRIES), 2.0);

    // Dropping the registration is what frees the `Desc` id for the next incarnation.
    drop(registration);
    assert_eq!(
        companion_gauge_count(&registry, FRONTIER, SINK_LABEL, LABEL),
        0
    );
}

/// The registration follows the sink token, not the operator closure. Builds a real timely
/// operator with `render_sink`'s wiring (register at build time, keep only a clone of the
/// registration in the operator closure, return the original as the sink token) around a
/// constant input that closes at once. When timely retires the operator and drops its closure
/// the collector's series must survive, because the token still holds the registration; only
/// dropping the token unregisters. Building a real operator, rather than dropping an `Rc` by
/// hand, is what proves the closure does not carry the registering handle.
///
/// It stops short of driving `render_sink` itself, which would need a full `ComputeState`.
#[mz_ore::test]
fn registration_survives_input_close_in_dataflow() {
    use differential_dataflow::input::Input;
    use timely::WorkerConfig;
    use timely::communication::Allocator;
    use timely::dataflow::channels::pact::Pipeline;
    use timely::worker::Worker as TimelyWorker;

    const LABEL: &str = "mz_curated_example";
    const FRONTIER: &str = "mz_compute_metric_sink_frontier_ms";
    const SINK_LABEL: &str = "sink";

    let registry = MetricsRegistry::new();
    let metrics = ComputeMetrics::register_with(&registry, ComputeRuntimeRole::Solo).for_worker(0);

    let mut worker = TimelyWorker::new(
        WorkerConfig::default(),
        Allocator::Thread(Default::default()),
        Some(Instant::now()),
    );

    // `MetricsRegistry` and `WorkerMetrics` are cheap shared handles, so the clones the operator
    // registers through and the originals the assertions scrape are the same registry.
    let registry_op = registry.clone();
    let metrics_op = metrics.clone();
    let registration = worker.dataflow::<Timestamp, _, _>(move |scope| {
        // Dropping the input handle immediately closes the input, standing in for a
        // constant-folded source whose frontier closes as soon as the dataflow starts.
        let (_input, collection) = scope.new_collection::<Row, Diff>();
        let registration = Rc::new(RefCell::new(Some(PendingRegistration::new(
            LABEL.to_string(),
            SinkCollector::new(LABEL, Arc::new(Mutex::new(SinkState::default()))),
        ))));
        let registration_op = Rc::clone(&registration);

        let scope_for_activator = scope.clone();
        let mut op = OperatorBuilder::new("MetricSinkTest".to_string(), scope.clone());
        let mut input = op.new_input(collection.inner, Pipeline);
        let operator_info = op.operator_info();
        op.build(move |_caps| {
            let activator = scope_for_activator.activator_for(operator_info.address);
            if let Some(reg) = registration_op.borrow_mut().as_mut() {
                if let Retry::Arm = reg.try_register(&registry_op, &metrics_op) {
                    activator.activate_after(REGISTRATION_RETRY_INTERVAL);
                }
            }
            move |_frontiers| {
                input.for_each(|_, _| {});
            }
        });

        registration
    });

    // The build-time attempt registered the collector.
    assert_eq!(
        companion_gauge_count(&registry, FRONTIER, SINK_LABEL, LABEL),
        1
    );

    // Step until timely retires the operator and drops its closure, releasing its `Rc` clone.
    for _ in 0..100 {
        if Rc::strong_count(&registration) == 1 {
            break;
        }
        worker.step();
    }
    assert_eq!(
        Rc::strong_count(&registration),
        1,
        "the operator closure should have dropped once its input closed"
    );

    // The registration rode on the token, not the closure, so the series is still live.
    assert_eq!(
        companion_gauge_count(&registry, FRONTIER, SINK_LABEL, LABEL),
        1
    );

    // Dropping the token is what unregisters.
    drop(registration);
    assert_eq!(
        companion_gauge_count(&registry, FRONTIER, SINK_LABEL, LABEL),
        0
    );
}

/// Every companion gauge carries the label the collector was built with. A curated sink passes
/// its stable name here, so its health series stay identifiable across boots even though the
/// sink's `GlobalId` is transient.
#[mz_ore::test]
fn collector_labels_gauges_with_sink_label() {
    let state = Arc::new(Mutex::new(SinkState::default()));
    let collector = SinkCollector::new("mz_curated_example", state);

    let families = collector.collect();
    // The point is that every emitted family carries the label, so guard only that there is
    // something to check. Pinning the exact count churns on every added gauge without telling
    // the next reader whether they broke labelling or just added a family.
    assert!(!families.is_empty());
    for family in &families {
        for metric in family.get_metric() {
            let sink = metric
                .get_label()
                .iter()
                .find(|l| l.name() == "sink")
                .expect("sink label present");
            assert_eq!(sink.value(), "mz_curated_example");
        }
    }
}
