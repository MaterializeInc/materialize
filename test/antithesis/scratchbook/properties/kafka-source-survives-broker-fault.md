# kafka-source-survives-broker-fault

## Summary

After a network partition or Kafka outage that prevents the source from making progress, once connectivity is restored the source resumes ingestion and eventually visits every message produced during the outage.

## Code paths

- `src/storage/src/source/kafka.rs` — `render_reader` polls per-partition `PartitionQueue`s. rdkafka's internal reconnect logic handles broker reconnect; the storage reader must not enter a permanent stall state when the consumer errors out.
- `src/storage/src/healthcheck.rs` — the source's `HealthStatusUpdate` transitions: `Running` → `Stalled { hint }` during the outage → back to `Running` after recovery. `Ceased` would be a violation (terminal failure for a transient fault).
- `src/storage/src/statistics.rs` — `offset_known` and `offset_committed` resume advancing post-recovery. The rehydration-latency reset (commit `0a34b6c79d`) is relevant if the reconnect goes through a dataflow restart.

## How to check it

Workload procedure:
1. Produce N messages.
2. Inject a network partition between the `materialized` container and the Kafka container. The partition isolates only that pair; persist/metadata remain reachable.
3. Produce N more messages while the partition is active.
4. Heal the partition (Antithesis fault scheduler) and call `ANTITHESIS_STOP_FAULTS`.
5. Poll `mz_internal.mz_source_statistics_per_worker.offset_committed` until it advances past `max_produced_offset`. Bound the poll loop with a generous timeout.
6. `assert_sometimes!(source_resumed_after_broker_fault, "kafka source resumed after Kafka container partition")`.

## What goes wrong on violation

The source enters a permanent stall: rdkafka thinks it's reconnected but the reader never re-reads; or the operator transitions to `Ceased` and the source must be manually dropped/recreated.

## Antithesis angle

- Bidirectional network partition: `materialized` ↔ Kafka.
- Asymmetric partition: outbound packets to Kafka dropped but inbound responses allowed (or vice versa). rdkafka may not detect this and may sit waiting for a response forever.
- Repeated short partitions: stress reconnect cadence.
- Kafka container hang (CPU throttling to zero rather than network partition).

## Existing instrumentation

None. Workload-level `assert_sometimes!` is the entry point. Optional SUT-side: `assert_sometimes!(kafka_consumer_reconnected, ...)` inside the reader after rdkafka reports a successful reconnect.

## Provenance

Surfaced by: Failure Recovery, External Dependencies.
