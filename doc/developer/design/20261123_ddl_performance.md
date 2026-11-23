# DDL performance: incremental transaction state

## Outcome

Make small catalog mutations avoid rebuilding unchanged durable catalog state.
Improve single-statement DDL latency and CPU cost at growing catalog sizes,
without changing durability, visibility, fencing, or transaction semantics.

## Evidence

The focused baseline measured CREATE/DROP tables, views, and indexes at 5,000,
10,000, and 30,000 mixed catalog objects with one and eight clients.
[Build 237](https://buildkite.com/materialize/spec-sheet/builds/237) passed.
The runtime was `e2a571a03`, with harness revision `b8b1bcf8d7`.
The report is `tools/ddl-performance/RESULTS.md` in materialize-spec-sheet,
commit `96c5bc2`. Local raw evidence and analysis are preserved at
`/home/ubuntu/ddl-performance-evidence/build-237/`.

Snapshot reconstruction grows from about 3.8 ms at 5k objects to 20.5 ms at
30k. Transaction initialization grows from about 2.1 to 11.2 ms. Eight-client
profiles attribute about 22–25% of inclusive CPU samples at 30k to durable
transaction startup. Speculative apply averages only 1.4–1.7 ms at 30k.
Timers nest and have distinct denominators. They must not be summed into
client latency.

Single-client view/index throughput falls about 42–45% from 5k to 30k.
Eight clients mostly add queueing. Builtin acknowledgement takes roughly
30 ms per observation across sizes. Sampled environmentd RSS remains below
2.8 GiB against a 16 GiB limit. Logical consensus and oracle counters include
substantial background work and are not exact per-DDL CRDB request counts.

This is one increasing mixed-catalog sweep, with live dataflows and accumulated
churn. Some telemetry windows are sparse. Profiles occupy the middle repetition,
while client baseline tables use the other two repetitions. Measurement overhead
has not been isolated with a scrape-free control.

## Architectural direction

The concurrent-DDL command framework already serves some autocommit statements.
It retains intent, replays against the current catalog, and validates external
preparation inputs. It is not a generic catalog read-set validation engine.
Inspection of upstream/main `d9c983d539` confirms that ordinary table/view/index
creation and drops still largely use serialized raw-operation paths. The
concurrent-DDL changes are already ancestors of the measured runtime.

The desired pipeline is concurrent preparation, ordered validation and durable
commit, ordered publication and controller updates, then asynchronous completion
before client acknowledgement. Keep a clear commit order. Do not start by adding
parallel durable writers or removing the global DDL lock from paths that depend
on it to protect prepared plans.

Completion must preserve table registration/write/forget ordering, generation
fencing, catalog revision publication, and builtin visibility. Moving waits off
the coordinator must not acknowledge success early. This pipeline work is a
separate follow-up, not part of the immediate implementation.

## Immediate implementation

Target durable snapshot reconstruction and transaction initialization. Reuse
synchronized base state with transaction-local changes so unchanged collections
are neither rebuilt nor decoded for every DDL. Choose the concrete representation
after tracing update application, transaction rollback, and captured snapshots.
A cache that still copies the whole catalog into every transaction is insufficient.

Preserve synchronization and fencing on transaction entry, exact updates and
retractions, uniqueness enforcement, abort isolation, dry-run snapshots,
bootstrap, read-only, and savepoint behavior. Publish cached state only at the
same authoritative update boundary used for durable state. No format migration
or SQL behavior change is intended.

## Verification and measurement

Exercise durable catalog transactions, aborts, captured snapshots, fencing,
progress, reopen, and consolidation with existing catalog tests and targeted
regressions. Check adapter callers and DDL SQL behavior. Obtain independent
review of the completed change, particularly ownership and failure recovery.

Compare the same focused 5k/10k/30k sweep with one and eight clients, matching
runtime settings, fixtures, instrumentation, and collection protocol. If the
implementation base changes, establish a matching baseline rather than attribute
unrelated upstream changes to this optimization. Report raw repetitions,
latencies, throughput, CPU, sampled memory, and logical backend counters.
Check that work was eliminated rather than moved to commit or cache maintenance.

## Follow-up candidates

- Extend semantic command replay to ordinary CREATE/DROP operations.
- Separate ordered publication from asynchronous completion waits.
- Replace catalog-wide quota scans with incremental accounting if measured.
- Avoid repeated full-state work during preparation retries and collision probes.
- Break down table collection creation and registration costs separately.
- Investigate batching backend work after preserving per-request failure and
  publication semantics. No backend-call reduction is promised by this local
  reconstruction optimization.
