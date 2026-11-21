# A state machine driver for the Antithesis harness

- Associated: `test/antithesis/`, `misc/python/materialize/antithesis_driver/`,
  `misc/python/materialize/antithesis/`

## The Problem

The Antithesis harness in `test/antithesis/` was a Test Composer workload:
about 60 short-lived test commands, generated into five templates, called a
Python library of about 44,000 lines (plus 20,000 lines of tests). Antithesis
scheduled the commands. Each command started with no memory of the others, so
the library kept its models in SQLite journals in a shared state directory,
and grew leases, resume logic, quiet-period ledgers, heartbeat audits, a
local scheduler that emulated Test Composer, and telemetry of command costs.
The inventory behind this document puts about 2,100 to 2,400 lines of the
rollout, restart and recovery code alone down to that, plus most of
`state.py`, `quiet.py`, `ddl_hold.py`, `exit_audit.py`, `telemetry*.py` and
`local_*.py`.

Two of its conventions also weakened what a run reported:

- Every module imported `always_or_unreachable as always` (26 modules). A
  safety check that a run never evaluated passed silently. The harness
  compensated with `findings.py`, which classified a run's exports.
- Exploration milestones were guidance events from `observations.py`, about
  280 call sites, not SDK `sometimes`/`reachable` assertions. An unreached
  milestone was never a property, so a run's property list did not show what
  it failed to exercise. The SUT-side `mz_ore::antithesis_sometimes!` and
  `antithesis_reachable!` follow the same convention.

Antithesis now recommends a different shape (the `antithesis` Claude plugin,
version 0.2.0, module `workload`): one driver program, a
[hegel](https://hegel.dev) concurrent state machine, in a container excluded
from faults. Its single test command, a `first_` command, creates
`/workload-active`. The driver reports setup complete, waits for that file,
then runs forever. Rules draw inputs and send requests, the model lives in
memory, guarantees are `always` assertions, situations worth reaching are
`sometimes` and `reachable` assertions, and an eventually rule stops faults,
checks what must hold afterwards and ends the timeline.

## Success Criteria

- One process drives the whole workload, with its models in memory and no
  cross-process coordination.
- Every guarantee is an `always` assertion, which fails when never reached.
  Every situation worth reaching is a `sometimes` or `reachable` assertion,
  which shows up as a property whether or not a run reaches it.
- No rule blocks a round of the state machine, whatever faults do to the
  system.
- A local loop runs the same driver against a standalone container and
  against the Kubernetes deployment, and lists the assertions a run would
  fail.
- The driver keeps the messages of the legacy assertions it ports, so their
  properties keep their history.
- Once its families are ported, the driver's runs reach every property the
  legacy workload's recent runs reached, and fail none they passed unless the
  failure is a product bug. This is the plugin's criterion for converting a
  workload.

## Out of Scope

- Changing the deployment: the driver uses the same manifests, images,
  orchestratord configuration and upgrade-base images.
- Changing what the `mz_ore::antithesis` macros report (see the open
  questions). New call sites of them are in scope.

## Solution Proposal

### Comparison

| | Legacy workload | Driver |
|---|---|---|
| Shape | ~60 commands in 5 templates, scheduled by Test Composer | one hegel state machine, its rules on up to 6 worker threads |
| State | SQLite journals, leases and resume steps per family | in memory |
| Randomness | `antithesis.random` through `rng.py`, per-timeline parameters stored in SQLite | hegel draws, which Antithesis controls, per-timeline parameters drawn when the machine starts, swarm testing of rules |
| Stalls | per-command watchdog that exits the process | each SQL session on its own thread, a rule waits at most 1 s, a pending operation is checked when it completes, a watchdog cancels statements after 60 s |
| Guarantees | `always_or_unreachable`, `findings.py` classifies | `always` |
| Exploration | guidance events, never failing | `sometimes`, `reachable` |
| After faults | `eventually_`/`finally_` commands, Test Composer stops faults | an eventually rule runs `$ANTITHESIS_STOP_FAULTS`, checks, exits 0 |
| setup complete | `setup.py` in the workload pod | the driver, before `/workload-active` |
| Cataloged code | the whole library | the driver package |
| Local loop | kind cluster plus a local scheduler | a standalone container, or kind with published release images, plus a would-fail report |
| Size | ~44,000 lines | ~4,300 lines including tests, for the families below |

### Architecture

`misc/python/materialize/antithesis_driver/`:

- `client.py`: sessions that never block a rule. A rule hands an operation to
  an idle session and waits for it up to a second. The operation's completion
  callback records its outcome in the model whenever it arrives, with a real
  time interval that is sound under any network delay. Errors go through
  `sql.classify`, widened for the two internal errors that mean "outcome
  unknown", and a `COMMIT` counts as acknowledged only when its tag says so.
- `machine.py`: the state machine, one rule per component action, an always-run
  invariant for the periodic checks, and the eventually rule.
- Components, each with rules, a model and assertions: `history.py` (client
  histories checked by the existing Elle-style `serializability.py`),
  `counters.py` (read-then-write bounds), `atomic.py` (multi-table write
  transactions), `views.py` (24 maintained views of the existing
  `ivm_model.py`), `disrupt.py` (replica changes, pod deletions,
  rollouts), `health.py` (recovery checks).
- `main.py`: startup: on Kubernetes the persist bucket, the Materialize CR and
  its first generation, then the schema.
- `local.py`: the local loop.

Seven new `mz_ore::antithesis_reachable!` markers in the SUT mark rare
branches the driver's faults should reach:

- persist `compare_and_append` retrying after an indeterminate consensus
  result, and finding that such an attempt committed,
- a durable catalog write losing its `compare_and_append`,
- a table write conflicting in the txns shard,
- the three ways a read-only generation's catch-up ends: caught up, skipped,
  or the deadline.

The internals research behind this document lists about thirty more
candidates.

The image is `test/antithesis/driver/` (mzbuild `antithesis-driver`), the pod
is `test/antithesis/manifests/30-driver.yaml`.

### Removing the legacy workload

The legacy workload is gone: its commands, image, templates, drivers, SQLite
state, telemetry, findings classifier and local scheduler. Its pure models
stay in `misc/python/materialize/antithesis/` with their tests, because the
driver's components and the families still to port use them (`sink_oracle.py`, `upsert_recovery_model.py`,
`spec_model.py`, `isolation.py`, `schema_model.py`, the retraction and
session rebinding models, `halt_proof.py`, `exit_evidence.py`,
`kube_watch.py`). The legacy implementation of each family, named by its
`drivers/` path below, is at commit 93a74b6551, the base of this change.

### Ported families

| Family | Legacy source | What carries over |
|---|---|---|
| Client history | `drivers/history.py` | the checker unchanged, the read paths, the assertion messages, epoch rotation |
| Contended counters | `drivers/counters.py` | the bounds and log checks and their messages, plus a post-fault read |
| Atomic commits | `drivers/atomic_commits.py` | `atomic_commits_model.py` unchanged, each read judged against what was settled before it began |
| IVM | `drivers/ivm.py` | the views and the reference of `ivm_model.py`, checked on the consistent cut of one `SELECT` and at every closed timestamp of a `SUBSCRIBE`, with the legacy messages of the latter |
| Postgres sources | `drivers/postgres_sources.py` | the ledger, LSN and export checks and their messages, in the bucket without a terminal upstream op. A single upstream writer resolves unknown commits with `txid_status`, so the upstream model is exact. Real-time recency reads, and a post-fault catch-up check, which the legacy workload lacked |
| Kafka sources | `drivers/kafka_sources.py` | the progress, offset and export checks and their messages against a continuously tailed topic, and a post-fault catch-up check |
| Kafka sinks | `drivers/kafka_sinks.py` | `sink_oracle.py` unchanged, the stream, view and model checks and their messages, against continuously tailed topics |
| MySQL, SQL Server sources, webhooks | none | new. MySQL and SQL Server share the Postgres source's model, judgment and write protocol (`cdc.py`) and differ only in the dialect. Webhooks are checked for phantoms, duplicates, bodies, append order and post-fault visibility, but not for visibility right after a 200, since appends take the storage controller's wall clock rather than an oracle timestamp |
| Source status | `drivers/source_status.py` | export frontiers advance after faults stop, and every source runs |
| Rollouts | `drivers/rollouts.py` | requests with both strategies, manual promotion, pod deletion in a drawn phase, `activeGeneration` monotonicity, settling after faults |
| Pod restarts | `drivers/pod_restarts.py` | deletion of environmentd, clusterd and orchestratord pods with a UID precondition and the same grace menu |
| Recovery | `drivers/recovery.py` | canary, replicas online, CR up to date, crash loops (`crash_loops.py`), plus every session draining and a `SUBSCRIBE` advancing |
| Status claims and spec changes | `drivers/rollouts.py`, `drivers/spec_phase.py` | `spec_model.py` unchanged: the status claim judged against the active StatefulSet with the legacy messages and settle time, and the spec-field-by-phase cells as a patch the rollout rule makes when it observes the phase. Polled, not watched |
| Read-only isolation | `drivers/read_only_isolation.py` | `isolation.py` unchanged: direct writes to a candidate, upstream replication connections and the timestamp oracle samples, with the legacy messages. Not the persist handle audit, which needs `persistcli` in the image |
| Upsert decode-error recovery | `drivers/upsert_recovery.py` | `upsert_recovery_model.py`, judged against the topic prefix below the mapped offsets, plus an export with inline errors |
| Window and aggregate retractions | `drivers/window_retractions.py`, `drivers/aggregate_retractions.py` | the models unchanged, on the consistent cut of one read as for IVM. Not their `SUBSCRIBE` checks |
| Schema evolution, session rebinding | `drivers/schema_evolution.py`, `drivers/session_rebinding.py` | `schema_model.py` unchanged and the `Parameters` and `Cursor` of `session_rebinding_model.py`, with unknown outcomes allowed either way. Not the explicitly versioned dependents, nor catalog transactions with rename conflicts |
| COPY | `copy_recovery.py` | batch atomicity of `COPY FROM STDIN` under client failures, plus `COPY TO STDOUT` against the same query |
| Data types, CDC types, Kafka formats | none | new. Values that follow from a seed stored in the row, so every visible row is judged on its own: 49 column types in tables and COPY, the upstream types of the three CDC sources, and an Avro record. JSON and Avro sources and Avro sinks reuse the Kafka source and sink checks |
| Cancellation | none | new. One cancel per operation, by cancel request, `pg_cancel_backend` or `statement_timeout`, a drawn delay after the statement was sent. Batches of rows with a generation, so a statement answered as cancelled must have taken no effect, also for `CREATE` and `DROP` of indexes and materialized views, which a plain `DROP` afterwards decides |
| Fixed timestamps, bounded `SUBSCRIBE` | none | new. Reads recorded with their `mz_now()` are repeated `AS OF` it from a table, an indexed view and a materialized view, on each replica, and replayed by `SUBSCRIBE ... AS OF ... UP TO`. The model is built from reads only |
| Blue/green deployments | none | new. Staging schema and cluster, swapped in one transaction and dropped, under reads and subscriptions through the live names. After a swap whose COMMIT got no answer the order of generations is no longer judged, because the server can still apply it |
| Exports added while a source ingests | none | new, part of `cdc.py`. The added export is read in the same `SELECT` as the others and judged at their ledger prefix. An export that equals its table at a later prefix has a property of its own |
| System parameter changes | none | new. No check of its own |

### Families still to port

In the order the inventory suggests, by how central the guarantee is and how
much of the port is mechanical:

1. Rollout observer oracles: protected StatefulSets, leader fencing, the
   SQL-serving version never going backwards. A background CR watch thread
   should fire the phase-targeted patches, which the rollout rule makes on
   its next poll today, so a zero-delay patch still lands in a short
   Promoting window.
2. Exit evidence (`halt_proof.py` and `exit_evidence.py` are pure): every
   environmentd and clusterd exit explained by a designed halt.
3. Upstream TRUNCATE, DROP and re-export for the Postgres source, with the
   legacy buckets for the known bugs after an ingestion restart, which need
   restart detection. The persistcli shard audit, and with it the persist
   handle audit of the read-only isolation checks.
4. The IVM subscriber's legacy `AS OF` resume and peek comparisons.
5. Access control, the shard audit, and `COPY` to and from object storage.

## Minimal Viable Prototype

The families in "Ported families", validated by:

- unit tests of the components' failure paths
  (`antithesis_driver/test_components.py`), each breaking the condition its
  assertion guards;
- `bin/pyactivate -m materialize.antithesis_driver.local --output DIR
  standalone`, against `materialize/materialized:v26.51.0`: every run ends
  with the eventually and exit 0, and the only unmet assertions are
  `sometimes` on situations that need faults (indeterminate outcomes, an
  environmentd restart);
- `... local --output DIR kind`, the Antithesis manifests on kind with the
  v26.51.0 release images and a locally built driver image. Pod deletions
  and a rollout ran during the workload, the rollout reached Applied in the
  eventually, and the pod exited 0;
- `snouty validate` of the rendered driver config;
- a review by a second agent with fresh context. It found two false
  positives that local runs could not show, one in the `SUBSCRIBE` oracle and
  one in the eventually, and several rarer races, all fixed.

No Antithesis run has been launched for it yet.

## Alternatives

- **Keep Test Composer and simplify the library.** It would keep the
  cross-process state that the inventory attributes most of the complexity
  to, and Antithesis's tooling now assumes a hegel driver.
- **Write the driver in Rust.** The plugin prefers the language of the
  system's client library, else Rust. Materialize's clients speak pgwire from
  any language, the Rust frontend of hegel is more mature, and Antithesis
  instruments Rust natively. Python reuses the existing pure models
  (`serializability.py`, `ivm_model.py`, `spec_model.py`, `sink_oracle.py`,
  ...) as they are, and keeps the harness in one language. The cost is the
  GIL: a model check holds every worker, so checks must stay cheap.
- **Keep the legacy workload until the driver reaches parity.** Both would
  need maintaining, against one deployment whose RBAC, images and CI they
  share, while the families move over one at a time. Parity is judged
  against the legacy runs' recorded properties instead.

## Open questions

- Should `mz_ore::antithesis_sometimes!` and `antithesis_reachable!` become
  real SDK assertions, as the driver's are, so unreached SUT branches show up
  as properties?
- Paced DDL needs about an hour of continuous quiet and a ban on other DDL.
  It fits the driver only as an exclusive mode that pauses the other rules,
  or as part of the eventually.
- The timing bounds (statement cancel after 60 s, the 45 minute budget of
  the eventually, the disruption cooldowns) are guesses until the first runs
  on the simulated core.
