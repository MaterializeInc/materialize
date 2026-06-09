# kafka-source-no-internal-panic

## Summary

The explicit panics and `assert!`s in the Kafka source reader never fire under any Antithesis-injected fault sequence. Each site is converted to a uniquely-messaged Antithesis assertion so a firing is a reportable property failure rather than a clusterd crash.

## Targeted sites

`src/storage/src/source/kafka.rs`:

| Line | Site | Antithesis form |
|------|------|------------------|
| 276 | `panic!("unexpected source export details: {:?}", details)` | `assert_unreachable!("kafka: unexpected source export details")` |
| 891 | `assert!(reader.partition_consumers.is_empty())` | `assert_always!(reader.partition_consumers.is_empty(), "kafka: partition_consumers not drained at shutdown")` |
| 1142 | `assert!(self.last_offsets.get(output_index).unwrap().contains_key(&partition))` | `assert_always!(…, "kafka: partition missing from last_offsets")` |
| 1193 | `panic!("got negative offset ({}) from otherwise non-error'd kafka message", msg.offset())` | `assert_unreachable!("kafka: negative offset from non-error message")` |
| 1457 | `assert!(…)` (debug-mode payload validation) | `assert_always!(…, "kafka: payload check")` |

Plus the cluster of `expect()` sites that are structurally similar — resume-upper missing (265), statistics not initialized (282), restored offset out of `i64` range (345), `position()` failure (606), `partition_known` lookup (853, 855), offset arithmetic (997, 1055, 1060, 1063, 1072, 1082), watermark not negative (1492). These are lower-priority but mass-conversion to `assert_always!(false, ...)` is cheap.

## Why these sites matter

- The "negative offset" panic at 1193 is the most interesting: rdkafka has shipped negative offsets in the past under certain protocol bugs, and an `i64` cast that wraps silently would be worse than the panic. Antithesis can reach this through manual broker-state manipulation in the workload.
- The capability-downgrade assertion family (relevant to commit `99ad668af5`'s topic-recreation panic) — currently that code path *logs and continues* rather than panicking, but if a future refactor reintroduces a `panic!` on offset regression, this property catches it.
- The `partition_consumers.is_empty()` assertion at 891 catches a shutdown-ordering bug that would manifest as a clusterd crash on source drop.

## Antithesis angle

- Topic deletion + recreation on the Kafka container. Specifically: drop a topic with offsets `[0..1000]`, recreate it with offsets `[0..100]` (lower watermark). The source's resume frontier sees `last_offset = 1000` and rdkafka delivers offset `100`. The dedup at kafka.rs:1158 handles this; the assertion at 1142 catches the case where the *partition itself* is missing from the dedup table.
- Partition rebalance: increase Kafka topic partition count from the broker side mid-run. The metadata fetcher must discover and assign the new partitions correctly.
- Manual offset reset: most relevant for the negative-offset panic at 1193.
- Clock jumps: Kafka's internal timestamp arithmetic uses millisecond offsets; clock jitter has historically interacted poorly with the `expect("kafka sources always have upstream_time")` at line 1209.

## Existing instrumentation

The panics and asserts already exist. They currently abort clusterd. The work is wrapping each site with the Antithesis SDK so the abort becomes a reportable, replayable property failure. Each site uses a distinct message naming exactly the invariant violated.

## Relationship to other properties

This is the SUT-side counterpart to the workload-level `kafka-source-no-data-loss` and `kafka-source-no-data-duplication`. A workload-level row-count mismatch tells you data is wrong; a fired SUT-side assertion tells you *where* it went wrong.

## Provenance

Surfaced by: Failure Recovery, External Dependencies. Regression targets: commits `99ad668af5`, `3e32df1f69`.
