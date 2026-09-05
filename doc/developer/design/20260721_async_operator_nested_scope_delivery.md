# Async operator input starvation in nested timely scopes

## Status

Root cause located and measured on staging.
The fix is not yet designed.
This document records the findings so the fix for the async operator bridge can be designed from a clean context.

## Symptom

Compute hydration reading a large persist snapshot from S3 runs at about 90 MB/s.
Both CPU (about 4 percent) and network sit far below saturation, so the process is mostly parked, not compute or bandwidth bound.
The effect only appears against real S3.
Reproduction attempts against the file backend and against minio both failed to show it.

## Conclusion first

A `builder_async` async operator that runs inside a nested timely scope receives its input in a dribble of about one to three records per operator schedule.
The identical async operator running in the outer (root) scope receives the whole upstream flood at once.
This starves the persist fetch operator, which is one such nested async operator, so it issues about one blob fetch at a time instead of filling its concurrency budget.

The throttle is the interaction between the async operator bridge (`src/timely-util/src/builder_async.rs`) and nested scope scheduling.
It is not persist, not the fetch loop, not the byte semaphore, not backpressure, and not the timely exchange.

## Why S3 only

The nested scope delivers input at roughly the downstream retirement rate, which during hydration equals the fetch completion rate.
Against minio or the file backend each blob get completes in well under a millisecond, so a one to three record per schedule intake still keeps up and hydration looks fast.
Against S3 each part is about 45 MB fetched as a single stream at about 100 MB/s, so a fetch takes about 0.45 s.
At that latency the one to three record per schedule intake caps the pipeline at about two parts per second, about 90 MB/s.

## The pipeline

The persist source wraps its operators like this.

```mermaid
flowchart LR
  subgraph outer["outer (root) scope"]
    descs["shard_source_descs (async)\nchosen worker emits all snapshot parts"]
  end
  subgraph nested["nested scope: outer.scoped(granular_backpressure)"]
    enter["Enter (region ingress)"]
    fetch["shard_source_fetch (async)\nExchange by worker_idx"]
  end
  descs -->|descs.enter(scope)| enter --> fetch
  fetch -->|completed_fetches feedback| descs
```

The nested scope is created unconditionally in `src/storage-operators/src/persist_source.rs` by `scope.scoped("granular_backpressure", ...)`.
The backpressure operator is only inserted inside it when `max_inflight_bytes` is `Some`.
On the measured hydration `max_inflight_bytes` is `None`, so the backpressure operator is absent and the nested scope carries no flow control.
The fetch operator still runs inside that nested scope because `descs.enter(scope)` moves the descs output into it.

## Measurements

All numbers are from staging, an isolated single dataflow: a default index over `materialize.sf100.lineitem`, a single 59 GB persist shard, on a fresh `M.1-8xlarge` replica, replication factor 0 then 1.
Instrumentation lives on branch `mh/hydration-instr-main`, PR 37771.

Effective concurrent blob get, measured as `rate(mz_persist_read_batch_part_seconds)`, sits at about 0.9 the whole time.
Throughput is about 90 MB/s.
Hydration of the shard takes about 13 to 14 minutes.

The `descs` operator emits the whole snapshot at once.
The chosen worker logs a single emit of about 1333 parts, batched into 38 containers of about 35 parts each, in one operator schedule, then never emits the snapshot again.
So the source floods, it does not dribble.

Three probe operators, all reading the same `descs` output for the same shard on the chosen worker, isolate the throttle.

* Outer scope, `Pipeline` pact, no exchange: pulls 38 in one go, `accepted_total` reaches 38 by schedule 3.
  This is bulk.
* Nested scope reached through `descs.enter(scope)`, `Pipeline` pact, no exchange: pulls one to three per schedule, `accepted_total` crawls from 3 to 15 over about six seconds while schedules climb from 3 to 20.
  This is a dribble.
* Nested scope, real `Exchange` pact, the actual fetch input: pulls one per schedule across all workers.
  This is a dribble.

The nested `Pipeline` probe has no exchange and still dribbles.
The outer `Pipeline` probe uses the same async bridge and is bulk.
The single variable that changes the outcome is outer versus nested scope.

The fetch operator itself is not scheduling starved in the sense of a busy loop.
The `accept_input` probe shows it scheduled a few times per second, and on almost every schedule the timely input channel holds only one to two messages.
`accept_input` drains everything available, so the channel genuinely holds one to two.
The fetch loop drains its whole backlog per activation, so admitting more per activation does not help: there is nothing queued to admit.

## What was ruled out, with evidence

* The column pager. Hydration is neutral to it and CPU is low with it both on and off.
* The fetch concurrency knob `persist_source_fetch_concurrency`. Set to 32, later 64, no change.
* The fetch byte semaphore. `mz_persist_semaphore_blocking_seconds` is 0 throughout. Raising `persist_fetch_semaphore_permit_adjustment` from 0.1 to 1.0 left L at 0.9 and hydration still about 14 minutes.
* The backpressure operator. `mz_persist_backpressure_emitted_bytes` is 0 for the shard, so it is not in this dataflow's path.
* The arrangement. A full read that pulls every column is no faster.
* Thread starvation. Ambient and isolated runtimes both have about 62 threads.
* The timely exchange. The nested `Pipeline` probe has no exchange and dribbles anyway.
* The async bridge in general. The outer `Pipeline` probe uses the bridge and delivers bulk.
* Message granularity, payload size, scope feedback, and connected inputs. Five in process microrepros in `src/timely-util/src/builder_async.rs` tests reproduce the one part per message symptom and various topologies, and all of them ramp to the concurrency cap. They do not reproduce the dribble, because an in process two worker exchange delivers a one schedule flood in bulk.

## The open question the fix must answer

Why does an async operator in a nested scope observe only one to three input records per schedule when the upstream pushed the whole batch at once, while the same operator in the outer scope observes the whole batch.

Candidate mechanisms, to be confirmed while designing the fix.

* The region ingress operator that `enter` inserts may forward only a bounded amount per schedule into the nested subgraph.
* The nested subgraph may be stepped by the parent only a bounded number of times per parent schedule, so the async operator inside runs rarely.
* The async bridge activation may not propagate out of the nested subgraph to the parent, so the operator is only re-scheduled when something else steps the subgraph, and each step delivers whatever arrived in the meantime.

The relevant code is `src/timely-util/src/builder_async.rs`, specifically `build` and `build_reschedule`, `accept_input`, and the `TimelyWaker` activation path, together with how timely steps a nested `Subgraph` and how `Enter` delivers into it.

## Fix direction

The real fix is at the async bridge level, so that async operators in a nested scope receive their input in bulk, the same as in the outer scope.
This benefits every nested async operator, not only persist.
The design needs to identify which of the candidate mechanisms above is responsible and correct it without breaking progress tracking or the activation contract.

A narrower alternative exists but is not the chosen fix.
When `max_inflight_bytes` is `None` the persist source could skip the `granular_backpressure` nested scope and run in the outer scope.
That removes the dribble for this path only and leaves the general bridge bug in place, so it is recorded here only as a fallback.

## Reproduction and instrumentation

Branch `mh/hydration-instr-main`, PR 37771.
The instrumentation logs under target `mz_persist_client::operators::shard_source` at info, greppable by `INSTR`.

* `INSTR descs emit`: parts emitted per descs batch.
* `INSTR fetch recv` and `INSTR fetch loop`: fetch intake and in flight depth.
* `INSTR accept`: messages an async operator pulls from its timely channel per schedule, tagged with the operator name.
* `INSTR send`: messages an async operator pushes to its output per schedule.

The probe operators `shard_source_fetch_probe` (outer, `Pipeline`) and `shard_source_fetch_nprobe` (nested, `Pipeline`) are the decisive comparison and live in the `shard_source` wrapper.
All instrumentation is diagnostic and must be removed or gated before any fix lands.

The staging harness is `.experiment/fetch_concurrency_remeasure.py`, subcommand `run`.
Loki datasource uid is `c90a4f6e-0536-4f9a-aa1c-92d9aaeefa10`, Prometheus uid is `Ks85Oh14z`.
