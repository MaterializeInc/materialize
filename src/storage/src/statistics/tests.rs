// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::metrics::MetricsRegistry;
use mz_storage_client::statistics::Counter;

use super::*;

/// Returns the id among the first user ids whose statistics `worker` of `worker_count`
/// aggregates.
fn id_aggregated_by(worker: usize, worker_count: usize) -> GlobalId {
    (0..)
        .map(GlobalId::User)
        .find(|id| aggregating_worker(*id, worker_count) == worker)
        .unwrap()
}

#[mz_ore::test]
fn sink_statistics_aggregate_on_one_worker() {
    let defs = SinkStatisticsMetricDefs::register_with(&MetricsRegistry::new());
    let worker_count = 2;

    // Cover a sink aggregated by each worker, so both routing directions are exercised.
    for aggregator in 0..worker_count {
        let mut workers: Vec<_> = (0..worker_count)
            .map(|worker| AggregatedStatistics::new(worker, worker_count))
            .collect();
        let id = id_aggregated_by(aggregator, worker_count);
        for (worker, statistics) in workers.iter_mut().enumerate() {
            statistics.initialize_sink(id, || SinkStatistics::new(id, worker, &defs));
            statistics
                .get_sink(&id)
                .unwrap()
                .inc_messages_committed_by(u64::cast_from(worker) + 1);
        }

        // Route every worker's local data to the aggregating worker, as the statistics
        // dataflow does.
        for worker in 0..worker_count {
            let (sources, sinks) = workers[worker].emit_local();
            assert!(sources.is_empty());
            if worker == aggregator {
                assert!(
                    sinks.is_empty(),
                    "the aggregator must not route its own data"
                );
            } else {
                assert_eq!(sinks.len(), 1);
            }
            workers[aggregator].ingest(sources, sinks);
        }

        for (worker, statistics) in workers.iter_mut().enumerate() {
            let (_sources, sinks) = statistics.snapshot();
            if worker == aggregator {
                assert_eq!(sinks.len(), 1);
                assert_eq!(sinks[0].id, id);
                // 1 + 2 messages, one increment per worker.
                assert_eq!(sinks[0].messages_committed, Counter::from(3));
            } else {
                assert!(sinks.is_empty(), "only the aggregator reports {id}");
            }
        }
    }
}

fn source_statistics(
    id: GlobalId,
    worker: usize,
    defs: &SourceStatisticsMetricDefs,
) -> SourceStatistics {
    let stats = SourceStatistics::new(
        id,
        worker,
        defs,
        id,
        &mz_persist_client::ShardId::new(),
        SourceEnvelope::None(mz_storage_types::sources::envelope::NoneEnvelope {
            key_envelope: mz_storage_types::sources::envelope::KeyEnvelope::None,
            key_arity: 0,
        }),
        Antichain::from_elem(Timestamp::MIN),
    );
    initialize_gauges(&stats);
    stats
}

/// Initializes the gauges that start out uninitialized, so the statistics get reported.
fn initialize_gauges(stats: &SourceStatistics) {
    stats.initialize_snapshot_committed(&Antichain::from_elem(Timestamp::MIN));
    stats.initialize_rehydration_latency_ms();
    stats.update_rehydration_latency_ms(&Antichain::new());
}

#[mz_ore::test]
fn source_gauges_reset_on_restart_on_the_aggregator() {
    let defs = SourceStatisticsMetricDefs::register_with(&MetricsRegistry::new());
    let worker_count = 2;
    let aggregator = 1;
    let id = id_aggregated_by(aggregator, worker_count);
    let mut workers: Vec<_> = (0..worker_count)
        .map(|worker| AggregatedStatistics::new(worker, worker_count))
        .collect();

    let initialize = |workers: &mut Vec<AggregatedStatistics>| {
        for (worker, statistics) in workers.iter_mut().enumerate() {
            statistics.initialize_source(id, id, Antichain::from_elem(Timestamp::MIN), || {
                source_statistics(id, worker, &defs)
            });
        }
    };
    let route = |workers: &mut Vec<AggregatedStatistics>| {
        let (sources, sinks) = workers[0].emit_local();
        workers[aggregator].ingest(sources, sinks);
    };

    initialize(&mut workers);
    for statistics in &workers {
        statistics.get_source(&id).unwrap().set_records_indexed(5);
    }
    route(&mut workers);
    let (sources, _sinks) = workers[aggregator].snapshot();
    assert_eq!(sources.len(), 1);
    let mut reset = sources[0].clone();
    reset.reset_gauges();
    assert_ne!(sources[0].records_indexed, reset.records_indexed);

    // A suspend-and-restart advances the epoch on every worker, then reinitializes.
    for statistics in &mut workers {
        statistics.advance_global_epoch(id);
    }
    initialize(&mut workers);
    let (sources, _sinks) = workers[aggregator].snapshot();
    assert_eq!(sources.len(), 1);
    assert_eq!(
        sources[0].records_indexed, reset.records_indexed,
        "the aggregator must not report gauges of the previous incarnation"
    );
}

#[mz_ore::test]
fn statistics_arriving_before_initialization_are_kept() {
    let defs = SinkStatisticsMetricDefs::register_with(&MetricsRegistry::new());
    let worker_count = 2;
    let aggregator = 1;
    let id = id_aggregated_by(aggregator, worker_count);
    let mut workers: Vec<_> = (0..worker_count)
        .map(|worker| AggregatedStatistics::new(worker, worker_count))
        .collect();

    // Worker 0 initializes and reports before the aggregator initialized the sink.
    workers[0].initialize_sink(id, || SinkStatistics::new(id, 0, &defs));
    workers[0]
        .get_sink(&id)
        .unwrap()
        .inc_messages_committed_by(7);
    let (sources, sinks) = workers[0].emit_local();
    workers[aggregator].ingest(sources, sinks);

    workers[aggregator].initialize_sink(id, || SinkStatistics::new(id, aggregator, &defs));
    let (_sources, sinks) = workers[aggregator].snapshot();
    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0].messages_committed, Counter::from(7));
}

#[mz_ore::test]
fn statistics_arriving_after_deinitialization_are_dropped() {
    let defs = SinkStatisticsMetricDefs::register_with(&MetricsRegistry::new());
    let worker_count = 2;
    let aggregator = 1;
    let id = id_aggregated_by(aggregator, worker_count);
    let mut workers: Vec<_> = (0..worker_count)
        .map(|worker| AggregatedStatistics::new(worker, worker_count))
        .collect();
    for (worker, statistics) in workers.iter_mut().enumerate() {
        statistics.initialize_sink(id, || SinkStatistics::new(id, worker, &defs));
    }
    workers[0]
        .get_sink(&id)
        .unwrap()
        .inc_messages_committed_by(7);
    let (sources, sinks) = workers[0].emit_local();

    let StatisticsEvent::Deinitialized { .. } = workers[aggregator].deinitialize(id) else {
        panic!("expected a deinitialized event");
    };
    workers[aggregator].ingest(sources, sinks);
    assert!(workers[aggregator].pending_sink_statistics.is_empty());
    let (_sources, sinks) = workers[aggregator].snapshot();
    assert!(sinks.is_empty());

    // The tombstone stays until every worker deinitialized the sink.
    workers[aggregator].ingest_deinitialized(vec![(id, aggregator)]);
    assert!(workers[aggregator].deinitialized.contains_key(&id));
    let _ = workers[0].deinitialize(id);
    workers[aggregator].ingest_deinitialized(vec![(id, 0)]);
    assert!(workers[aggregator].deinitialized.is_empty());
}

#[mz_ore::test]
fn deinitialization_by_other_workers_first_is_tracked() {
    let worker_count = 2;
    let aggregator = 1;
    let id = id_aggregated_by(aggregator, worker_count);
    let mut statistics = AggregatedStatistics::new(aggregator, worker_count);

    // Worker 0 dropped the object before the aggregator processed the drop.
    statistics.ingest_deinitialized(vec![(id, 0)]);
    let _ = statistics.deinitialize(id);
    assert!(statistics.deinitialized.contains_key(&id));
    statistics.ingest_deinitialized(vec![(id, aggregator)]);
    assert!(statistics.deinitialized.is_empty());
}

#[mz_ore::test]
fn source_statistics_arriving_before_initialization_are_kept() {
    let defs = SourceStatisticsMetricDefs::register_with(&MetricsRegistry::new());
    let worker_count = 2;
    let aggregator = 1;
    let id = id_aggregated_by(aggregator, worker_count);
    let mut workers: Vec<_> = (0..worker_count)
        .map(|worker| AggregatedStatistics::new(worker, worker_count))
        .collect();

    workers[0].initialize_source(id, id, Antichain::from_elem(Timestamp::MIN), || {
        source_statistics(id, 0, &defs)
    });
    workers[0]
        .get_source(&id)
        .unwrap()
        .inc_messages_received_by(7);
    let (sources, sinks) = workers[0].emit_local();
    workers[aggregator].ingest(sources, sinks);
    assert!(
        workers[aggregator]
            .pending_source_statistics
            .contains_key(&id)
    );

    workers[aggregator].initialize_source(id, id, Antichain::from_elem(Timestamp::MIN), || {
        source_statistics(id, aggregator, &defs)
    });
    let (sources, _sinks) = workers[aggregator].snapshot();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].messages_received, Counter::from(7));
}

#[mz_ore::test]
fn previous_epoch_statistics_keep_counters_and_drop_gauges() {
    let defs = SourceStatisticsMetricDefs::register_with(&MetricsRegistry::new());
    let worker_count = 2;
    let aggregator = 1;
    let id = id_aggregated_by(aggregator, worker_count);
    let mut workers: Vec<_> = (0..worker_count)
        .map(|worker| AggregatedStatistics::new(worker, worker_count))
        .collect();
    for (worker, statistics) in workers.iter_mut().enumerate() {
        statistics.initialize_source(id, id, Antichain::from_elem(Timestamp::MIN), || {
            source_statistics(id, worker, &defs)
        });
    }

    // The aggregator restarts the source, while worker 0 still reports about the previous
    // incarnation.
    workers[aggregator].advance_global_epoch(id);
    workers[aggregator].initialize_source(id, id, Antichain::from_elem(Timestamp::MIN), || {
        source_statistics(id, aggregator, &defs)
    });
    initialize_gauges(workers[aggregator].get_source(&id).unwrap());
    let stale = workers[0].get_source(&id).unwrap();
    stale.inc_messages_received_by(7);
    stale.set_records_indexed(5);
    let (sources, sinks) = workers[0].emit_local();
    workers[aggregator].ingest(sources, sinks);

    let (sources, _sinks) = workers[aggregator].snapshot();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].messages_received, Counter::from(7));
    let mut reset = sources[0].clone();
    reset.reset_gauges();
    assert_eq!(
        sources[0].records_indexed, reset.records_indexed,
        "gauges of the previous incarnation must be dropped"
    );
}
