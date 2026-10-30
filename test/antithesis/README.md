# Antithesis harness

Runs self-managed Materialize, deployed by the real orchestratord on
Kubernetes, under [Antithesis](https://antithesis.com). The target is the class
of bugs that only appear at a specific restart point, under a concurrent-writer
race, or in a timing window: 0dt rollouts, persist leases and finalization,
controller and catalog state rebuilt at boot, and source ingestion across
restarts.

The workload commands under `workload/test/main/` select independent drivers
and checks from `misc/python/materialize/antithesis/drivers/`.

## Coverage

| Scenario | Drivers and checks |
|---|---|
| Concurrent reads and writes | Client histories check isolation and read-modify-write counts. Contended counters check conservation and duplicate writes. |
| Queries racing DDL and replica replacement | Session mix exercises peeks, COPY, SUBSCRIBE, real-time recency, and replica targeting while changing indexes and replicas. |
| Generation handoff | Rollouts and spec-phase changes overlap writes, source creation, catalog changes, and targeted pod restarts. Isolation checks observe candidate generations directly. |
| Temporary and permanent hangs | Coordinator canaries probe independent paths concurrently, record recovery beyond the tight deadline, and fail unanswered probes at the wedge bound. Recovery checks require progress after faults stop. |
| Wrong results after faults | Read paths compare indexes, persist reads, recomputation, replicas, and SUBSCRIBE at a common timestamp. IVM checks maintained views against Python reference results, including current reads after faults stop for completed and interrupted workloads. |
| External data | Postgres transaction ledgers, Kafka source histories, and sink progress and envelopes check ingestion, replay, atomicity, and output contents. |
| Panics and persistent damage | Process instrumentation reports panics and unexpected halts. Pod history detects crash loops. Shard audits inspect persist state and remap frontiers. |

Harness pod kills respect quiet periods and record the targeted pod UID, so a
restart can be distinguished from a spontaneous crash. Paced-DDL and canary
quiet periods renew in short chunks, allowing faults to resume soon after an
interrupted command. Read-path rounds limit issued work, and shard audits share
a time budget across persistcli commands. Final checks report incomplete work.

Timing bounds still need calibration on Antithesis's simulated core. Local
unit tests validate the oracles and driver control flow, including their
failure paths. They do not establish fault coverage or throughput under the
simulator. The IVM reference derives views from the observed base tables.
Consistent corruption of both base rows and views requires the independent
client-history and external-source oracles.

## Workload templates

The image generates four templates from the authoritative commands in
`workload/test/main/`:

| Template | Workloads | Initial bulk setup |
|---|---|---|
| `main` | Full mixed workload, including interactions between subsystems | Postgres, Kafka sources, Kafka sinks, IVM |
| `transactions` | Client histories, contended counters, session mix, read paths, access control | None. Session mix creates its own RTR source when needed. |
| `cdc` | Postgres and Kafka sources, Kafka sinks, source progress, shard audits | Postgres, Kafka sources, Kafka sinks |
| `rollouts` | Deployment scenarios, spec-phase changes, histories, counters, read paths, access control, Postgres, IVM, generation isolation | Postgres, IVM |

Health checks, resource checks, and pod restarts remain available in each
template. Restart triggers are restricted to workloads that template can
schedule. For example, the transaction template does not wait for a bulk
source creation or select a candidate generation that no driver creates.

`templates.py` discovers executable commands and derives profile membership
from their driver imports. Adding a command for an existing driver includes
it in the corresponding focused templates. The full `main` template keeps
every command. Generated wrappers call the original source through the
measurement runner, and the original sources live outside the runtime test
directory. To inspect the generated templates locally:

```
bin/pyactivate -m materialize.antithesis.templates \
    --source test/antithesis/workload/test/main --output /tmp/mz-antithesis-templates
```

## Resumable rollouts

Ordinary rollout invocations advance one persisted scenario step. Request
intent, phase waits, delays, absolute deadlines, targeted kills, checks, and
cleanup survive command interruption. Lost responses retry the same request.
The CR lease is released between invocations. A scenario's DDL hold survives
scheduler gaps and expires at the scenario deadline if it is never resumed.

Spec-phase cells share that journal ownership. Their watch resource version,
cell choice, targeted patch, promotion, claim recheck, and cleanup also resume
in bounded steps. A zero-delay patch still runs immediately when the watched
phase arrives, preserving short promotion windows.

The paced-DDL scenario needs a continuous quiet interval to measure its
timing property. The ordinary driver selects it through the same swarm
weights as the other scenarios, then a dedicated command claims and runs
that trial. Its quiet period still renews in short chunks.

## Efficiency and interleaving measurements

Every generated command records wall time, workload process CPU, reaped child
CPU, completion status, and comparison outcomes in `telemetry.sqlite`.
Checks distinguish passed, failed, skipped, and uninstrumented invocations.
Dead command processes, including watchdog exits, are counted as interrupted.
Interrupted CPU and Materialize CPU are outside these measurements.
Comparison totals are batched until command exit, so abruptly killed commands
can leave comparison work unreported. The summary marks this limitation.

`anytime_workload_efficiency` and `finally_workload_efficiency` print a JSON
summary with command costs, comparison units per timeline hour, quiet-time
fraction, and a verified exposure matrix. The quiet fraction measures the
union of requested intervals, so overlapping requests are counted once.
Comparison units differ by oracle. Compare costs within the same workload
and read path, and inspect coverage combinations alongside throughput.

A history exposure is credited only when a later checked read contains the
write's id and the history checks pass. SUBSCRIBE witnesses require a correct
closed timestamp from a fetch requested after the observed exposure. The full
phase-GET or acknowledged pod-delete interval must fit within the write's
unresolved interval. The matrix records observed phase, kill role, write
family, read path, and outcome.
A phase and a kill can occur at different points of that same interval.
Replica exposure requires the deleted pod's cluster id to match the checked
workload. Missing cluster identity receives no replica credit.
Standalone guidance events expose witnessed combinations to Antithesis.
Coverage observations and the generated inventory distinguish observed,
not-yet-observed, and never-evaluated targets without creating product failures.

History and IVM interleaving commands stage generated writes, trigger a pod
delete or promote an observed ready rollout, then use the existing correctness
oracles to check later reads. Append transactions can remain open across the
disruption. Atomic read-modify-write statements race it in autocommit mode,
because Materialize does not support `INSERT ... SELECT` in an explicit
transaction. Ordinary writers and fault scheduling continue alongside these
commands.
The `antithesis_interleaving` events identify round modes, SQL outcomes, and
disruption outcomes.

Raw observations are capped at 4096 events and one hour of history. The
summary reports pruned events, making missing measurement history visible.
Aggregate costs and coverage counts survive pruning. These limits affect
measurement witnesses, while the correctness oracles retain their own data.

## Incremental Postgres oracle

The Postgres checker replays a bounded transaction suffix from an independent
reference checkpoint. Checkpoints are built from recorded upstream plans
after validating the visible ledger. Multiple exports retain their own
pre-terminal reference state, including across TRUNCATE, DROP, and schema
changes. Expected rows are never adopted from Materialize query results.

Before retiring a batch, the checker performs a full ledger comparison and
requires independent upstream commit evidence. The exact retiring batch
survives until a full read confirms its atomic upstream deletion against
persisted LSN bounds. Additional periodic full reads detect corruption below a checkpoint,
including reappearing retired rows. Terminal and restart attribution remains
in indexed cold evidence, outside the hot replay suffix.
Live-export terminal summaries and active-export indexes keep that cold
evidence out of repeated hot lookups. Durable transaction and export counters
prevent identifier reuse after compaction.

Normal writers defer when retained transactions and admitted writers reach
1024 per slot. `MZ_ANTITHESIS_PG_HISTORY_TXNS_CAP` overrides that budget. One
additional slot is reserved for a progress heartbeat. Admission is atomic
across concurrent commands. Uncertain transactions are retained until ledger
evidence or upstream counter-lock fencing resolves them. This applies
backpressure during long ingestion stalls while preserving the evidence the
oracles need.

## Layout

| Path | What |
|---|---|
| `manifests/` | Kubernetes manifest templates and the orchestratord Helm values |
| `workload/` | mzbuild image `antithesis-workload`: the Python workload, its entrypoint, and authoritative commands under `test/main/` |
| `config/manifests/` | Rendered manifests (gitignored: they embed the license key) |
| `misc/python/materialize/antithesis/` | Workload library and drivers (importable outside Antithesis) |
| `misc/python/materialize/orchestratord/` | Kubernetes client for an orchestratord-managed environment |

The Materialize CR is not in the manifests. orchestratord registers its CRD at
startup, so the workload's setup step (`materialize.antithesis.setup`) creates
the CR, waits for the first generation to serve SQL, and emits
`setup_complete`.

## Instrumentation

### Product findings and harness health

The raw report includes platform checks for command scheduling, instrumentation,
and other harness health. These are separate from product correctness. A command
that was not selected or completed during a finite run does not demonstrate a
product failure. The platform still reports these default properties.

Export all property results and complete harness health, exit-evidence, and
coverage events across the run's histories, then classify them with:

```shell
bin/pyactivate -m materialize.antithesis.findings \
    --properties properties.ndjson --events events.ndjson --output findings.json
```

The command returns 1 for product violations with assessed harness evidence, 2 for
inconclusive evidence or harness health failures, and 0 when evaluated checks found
no product violations. Missing coverage remains in `incomplete_coverage` and does
not become a product failure. Unknown failed properties remain unresolved.
Concrete counterexamples remain in `product_candidates` when invalid or missing
harness evidence prevents product attribution. Without an event export the verdict
is inconclusive, because harness errors are SDK events rather than product
assertions. A truncated export or a filter that omits relevant health events
cannot establish a complete run assessment.

Native halt exits are accepted only when complete, termination-correlated evidence
explains every reported occurrence using the reviewed product policy. The workload
emits this evidence as `antithesis_exit_evidence` events when it can capture it.
After setup, a passive observer samples both namespaces every two seconds.
It reports API and capture costs through `antithesis_exit_observer`, and
per-incarnation capture failures through `antithesis_exit_evidence_missing`.
Missing diagnostics do not invalidate a later successful capture. An anytime
command audits the observer and samples only when its heartbeat is stale.
Capture uses at most three attempts and ten seconds per 30-second observer
window. Truncated logs, changed incarnations, and ambiguous termination
timestamps remain unresolved.
An optional `--exit-evidence` JSON file can supply the same occurrence records,
keyed by native property ID. An unexplained exit remains unresolved.
Native aborts with potentially matching designed-panic observations remain
candidates until incident attribution is established. Independent unexpected
panic assertions retain their product verdict.

Coverage milestones use guidance events and counters rather than failure-producing
reachability assertions. The image generates `/opt/antithesis/coverage-catalog.json`
from literal observation callsites so efficiency summaries include targets that
have not been hit. Dynamically named targets remain explicitly unresolved in that
inventory. The SDK source catalog continues to include conditional product
invariants, which report actual counterexamples without requiring a visit to every
assertion site.

The Antithesis build flavor (`bin/mzimage ... --antithesis`, or
`CI_ANTITHESIS=1`) builds with sancov coverage flags and the
`mz-ore/antithesis` feature, which links the Antithesis SDK and reports:

- every process panic as the failed property `Materialize process panicked`,
  except panics marked with `mz_ore::antithesis::expect_panic()`;
- every `halt!` as a per-site `process halted at <file>:<line>` event, plus a
  verdict from `mz_ore::antithesis::classify_halt`: a structured observation
  for messages on the reviewed `DESIGNED_HALTS` list, a dedicated failed
  property for halts of filed bugs, and `Materialize process
  halted for an unexpected reason` for everything else;
- every soft assertion that logs instead of panicking as
  `Materialize soft assertion failed`;
- optional-hit safety assertions placed with `mz_ore::antithesis_always!` and
  `mz_ore::antithesis_unreachable!`, and standalone coverage observations from
  `mz_ore::antithesis_{sometimes,reachable}!`.

Without the feature all of these compile to nothing.

## Memory and resource kills

The Antithesis VM has about 10 GiB, and every pod shares it, including two
environmentd generations and two sets of clusterd replicas during a rollout.
environmentd is limited to 2 GiB (`environmentdResourceRequirements` in the
CR), clusterd to 1 GiB per container (the `clusterd-memory` LimitRange, since
replica sizes carry no memory limit), orchestratord to 512 MiB. The
dependencies and the workload have no limit.

A kill for memory exits like a crash, so tell them apart before reading a
dead pod as a Materialize bug:

- A panic reports `Materialize process panicked` from inside the process. An
  OOM kill or an eviction is a SIGKILL (137) with no panic message. The
  platform's `No unexpected crashes` and `No unexpected container exits` skip
  137, and `anytime_pod_restarts` counts it as an explained exit and leaves
  the verdict to the resource kill check below.
- `anytime_resource_kills` and `finally_resource_kills` read every pod, the
  eviction and OOM events, and the node conditions. `No Materialize container
  is OOM-killed at its own memory limit` means environmentd, clusterd or
  orchestratord outgrew its own limit while the node had memory: a
  Materialize finding, with the limit in the details. Node-memory eviction
  and pressure reports use `workload_harness_error`: they invalidate the
  harness observations without accusing the product. Missing attribution
  remains unresolved. Restarts and failures after node pressure need that
  context when interpreting their cause.
- The platform's `Peak memory usage` fails above 95% of the VM's memory. Its
  examples list the largest processes, which shows whose growth to look at.

## Running locally

Run the harness unit tests without a Kubernetes cluster:

```
bin/pyactivate -m pip install -r misc/python/materialize/antithesis/requirements.txt
bin/pyactivate -m pytest -q misc/python/materialize/antithesis
```

The SDK pin is shared with the workload image. Driver tests skip when this
optional SDK is absent. Pure model tests remain available without it.

```
bin/pyactivate -m materialize.antithesis.local up --build-dir <git checkout>
bin/pyactivate -m materialize.antithesis.local down
```

`up` builds the Antithesis flavor of environmentd, clusterd, orchestratord,
and the workload; loads them into a kind cluster; renders and applies the
manifests; and waits for `setup_complete`. mzbuild needs a git work tree, so
from a jj secondary workspace pass a git worktree checked out at the same
commit. The license key comes from `MZ_CI_LICENSE_KEY` or
`~/.config/materialize/antithesis-license-key`.

`--upgrade-from v26.45.0` starts the environment on that release's published
images instead, for the upgrade scenario below. The published images work on
kind; only Antithesis needs them rebuilt.

## Cross-version upgrades

Rendered with `--upgrade-from-environmentd-image` and
`--upgrade-from-clusterd-image`, the environment starts on an older release and
the rollouts driver's `upgrade` action rolls out the image under test while the
other drivers keep running. The action's weight is never zero, so every timeline
can leave the old release, and once the spec names the new image every other
action keeps it. Some upgrades start under `ManuallyPromote` and are cancelled
before promotion, after which the old release must still serve.

The observer checks that the version serving SQL never goes backwards, and
convergence checks that an upgraded environment serves a newer version than the
one it started on.

The base release is the newest release of the previous minor version
(`images.upgrade_base_version`). The published release images seed AWS-LC from
CPU jitter, which aborts on Antithesis's deterministic CPU, so CI rebuilds
environmentd and clusterd from the release tag without jitter entropy and pushes
them as `<release>--antithesis.upgrade-base`. They have no coverage
instrumentation. Every image tag starts with its version
(`images.image_tag`), because orchestratord reads it to gate environmentd
arguments and to refuse rollbacks.

## Submitting to Antithesis

CI builds the images on PRs labeled `ci-antithesis` (see `ci/README.md`) and
pushes them to the Antithesis registry. The step requires `MZ_CI_LICENSE_KEY`
to render and publish the config image against those exact references.
The step's `antithesis-images.json` artifact lists the images, upgrade base
images, and a `config` reference pinned to its pushed digest. Download the
artifact and launch without a local registry login:

```
snouty launch --launcher basic_k8s_test \
    --config-image "$(jq -r '.config' antithesis-images.json)" \
    --param "antithesis.images=$(jq -r 'del(.config, ."upgrade-base-version") | [.[]] | join(";")' antithesis-images.json)" \
    --duration <minutes> ...
```

To render and validate the manifests locally:

```
bin/pyactivate -m materialize.antithesis.render --output test/antithesis/config/manifests \
    --environmentd-image <environmentd> --clusterd-image <clusterd> \
    --orchestratord-image <orchestratord> --workload-image <antithesis-workload> \
    [--upgrade-from-environmentd-image <upgrade-base-environmentd> \
     --upgrade-from-clusterd-image <upgrade-base-clusterd>]
snouty validate test/antithesis/config
snouty launch --launcher basic_k8s_test --config test/antithesis/config --duration <minutes> ...
```

snouty builds the config image (`FROM scratch` with the rendered manifests under
`/manifests`) and pushes it to `ANTITHESIS_REPOSITORY`. environmentd will not
start under Kubernetes without a license key, and Antithesis runs air-gapped,
so the key is baked into the rendered `materialize-backend` Secret. The config
image must therefore only ever go to the tenant's private Antithesis registry,
never to GHCR.
