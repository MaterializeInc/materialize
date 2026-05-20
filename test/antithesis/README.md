# Antithesis POC

This directory contains everything needed to run Materialize under
[Antithesis](https://antithesis.com/) — a deterministic-hypervisor fuzzer that
injects container-level faults (kill / pause / partition / drop / dup) at
randomized cadence and replays interesting histories.

If you're just here to **make something happen locally**:

```sh
make up-local GROUP=kafka          # build + bring up the Kafka workload group
make smoke    GROUP=kafka          # build + up + a one-shot psql smoke test
make down     GROUP=kafka          # tear down (preserves volumes)
make clean    GROUP=kafka          # tear down + remove volumes
make build-all                     # regenerate compose YAML for every group
```

Valid `GROUP` values: `kafka`, `pg-cdc`, `mysql-cdc`, `sql-server-cdc`,
`parallel-workload`, `upsert-stress`, `combined` (kitchen-sink, default).

If you're here to **understand how this is wired up**, read on.

---

## Table of contents

1. [What Antithesis is and what we use it for](#what-antithesis-is)
2. [Architecture at a glance](#architecture-at-a-glance)
3. [Workload groups](#workload-groups)
4. [Directory layout](#directory-layout)
5. [Test command conventions](#test-command-conventions)
6. [Local dev loop](#local-dev-loop)
7. [CI / push to the Antithesis registry](#ci--push-to-the-antithesis-registry)
8. [Authoring a new property / driver](#authoring-a-new-property--driver)
9. [Authoring a new workload group](#authoring-a-new-workload-group)
10. [Conventions that are easy to violate](#conventions-that-are-easy-to-violate)
11. [Next test frameworks to port](#next-test-frameworks-to-port)

---

## What Antithesis is

Antithesis runs the system under test (SUT) on a deterministic hypervisor that
records every nondeterministic decision (network, scheduling, IO) so a failure
can be replayed bit-for-bit. On top of that it provides:

- **Fault injection** — random kill / pause / partition / packet drop / dup at
  the container layer. We don't need any code in the SUT to participate.
- **Test Composer** — a directory of executable files (`/opt/antithesis/test/v1/<template>/*`)
  whose filename prefix tells Antithesis how often to launch each one.
- **An assertion SDK** (`antithesis.assertions`) — `always(...)`,
  `sometimes(...)`, `unreachable(...)`, plus utilities for sourcing entropy
  through Antithesis so retries and reorderings get explored.

The "test program" is therefore not a single test — it's a fleet of small,
independent **drivers** that each exercise one property. Antithesis launches
them concurrently, faults the SUT around them, and reports which `always`
checks ever failed and which `sometimes` checks never fired.

The properties we care about are mostly the headline Materialize guarantees:
strict-serializable reads, MV-reflects-source, CDC source no-data-loss /
no-data-duplication, catalog recovery after restart, upsert-state
rehydration, GTID monotonicity, frontier monotonicity, etc.

---

## Architecture at a glance

```
                  test/antithesis/mzcompose.py             (kitchen-sink topology, Python)
                  test/antithesis/groups.yaml              (manifest: 7 workload groups)
                              │
                              │  export-compose.py --group=NAME
                              ▼
       test/antithesis/configs/<group>/docker-compose.yaml (one per group, generated)
       test/antithesis/configs/<group>/.env                (image fingerprints, generated)
                              │
                              │  packaged as `antithesis-config-<group>` (FROM scratch)
                              ▼
                       Antithesis platform
                              │
                              │  brings up the compose, then scans every
                              │  container for /opt/antithesis/test/v1/<template>/
                              ▼
              ┌──────────────────────────────────────┐
              │  workload container                  │
              │  image: antithesis-workload-<group>  │
              │  built from test/antithesis/workload │
              │                                      │
              │  /opt/antithesis/test/v1/            │
              │    kafka/                            │
              │      first_select_upsert…py          │  ← runs once at start
              │      parallel_driver_…py             │  ← launched concurrently, many times
              │      singleton_driver_…py            │  ← launched once per timeline
              │      anytime_kafka_…py               │  ← runs continuously
              │    defaults/        (merged in)      │
              └──────────────────────────────────────┘
```

The committed `docker-compose.yaml` per group uses
`${MATERIALIZED_IMAGE}` / `${ANTITHESIS_WORKLOAD_IMAGE}` placeholders, so the
file is stable across SUT changes — only the `.env` shifts per fingerprint.
CI lints any drift via `ci/test/lint-main/checks/check-antithesis-compose.sh`.

### What `export-compose.py` actually does

`mzcompose.py` is the canonical kitchen-sink topology — every service every
group might want. `export-compose.py` then:

1. Filters services down to the active group (universal + group-specific from
   `groups.yaml`).
2. Prunes `depends_on` edges to dropped services.
3. Resolves `mzbuild:` to `${PLACEHOLDER}` (Materialize-built) or upstream
   image refs (e.g. `postgres:17.7`). The plain mzbuild flavor of Postgres
   bakes in `libeatmydata` and other test patches that subvert fault
   injection — Antithesis runs against vanilla upstream images.
4. Strips `ports:` (Antithesis is container-to-container; bare ports collide
   on ephemeral host bindings under podman), host bind-mounts (no host FS),
   unsafe env vars (`MZ_EAT_MY_DATA`, host-path configs), and mzcompose-only
   keys.
5. Inlines the postgres-metadata bootstrap SQL into an entrypoint heredoc.
6. Sets `container_name == hostname == service_key` (Antithesis triage reports
   attribute log lines by hostname).
7. Joins every service to a single explicit `antithesis-net` bridge network
   (auto-generated networks have flaky DNS for early service-up races).
8. Upgrades `condition: service_started` to `service_healthy` wherever the
   dependency declares a healthcheck (avoids DNS races on initial bring-up).
9. Applies per-group last-mile rewrites — e.g. `upsert-stress` drops both
   clusterd1 timely configs and the workload service's `CLUSTERD_WORKERS` to
   `1`.

### What `export-env.py` does

Writes the `.env` for the per-group `docker-compose.yaml`:
```
MATERIALIZED_IMAGE=…/materialized:mzbuild-<fingerprint>
ANTITHESIS_WORKLOAD_IMAGE=…/antithesis-workload-<group>:mzbuild-<fingerprint>
ANTITHESIS_UPSERT_HAMMER_IMAGE=…/antithesis-upsert-hammer:mzbuild-<fingerprint>
```

The `.env` is then COPYed into the `antithesis-config-<group>` mzbuild image
so its fingerprint tracks materialized + workload transitively.

### What `push-antithesis.py` does

CI-only. After `ci.test.build` produces antithesis-flavored Rust binaries +
images on GHCR, this script retags `materialized`, every
`antithesis-workload-<group>`, every `antithesis-config-<group>`, and
`antithesis-upsert-hammer` to the Antithesis GCP Artifact Registry
(`us-central1-docker.pkg.dev/molten-verve-216720/materialize-repository`)
that Antithesis's sandbox has read IAM on. GHCR packages default private →
Antithesis hits 4001 image-not-reachable.

---

## Workload groups

Each group is one Antithesis job: one compose, one workload image, one
focused topology. Antithesis picks **exactly one template per execution
history** so drivers across different focused groups never interleave within
the same timeline.

| Group               | Focus                                                                                          | Extra services beyond universal                                                       |
| ------------------- | ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `kafka`             | Kafka source / upsert envelope correctness under broker + clusterd faults                      | zookeeper, kafka, schema-registry, clusterd2                                          |
| `pg-cdc`            | PG CDC source correctness under upstream-PG / clusterd / envd faults                           | postgres-source, clusterd2                                                            |
| `mysql-cdc`         | MySQL CDC + MyISAM no-data-loss + GTID monotonicity, multithreaded replica                     | mysql, mysql-replica, clusterd2                                                       |
| `sql-server-cdc`    | SQL Server CDC source correctness                                                              | sql-server, clusterd2                                                                 |
| `parallel-workload` | Randomized concurrent SQL stress + SinceViolation drivers — broadest                           | kafka stack, postgres-source, mysql primary+replica, polaris, clusterd2, clusterd-pool-0/1 |
| `upsert-stress`     | Focused stress for INC-936 (single 1-worker clusterd1 + 6 long-lived `upsert-hammer-{i}` pods) | zookeeper, kafka, schema-registry, upsert-hammer-0..5                                  |
| `combined`          | Kitchen-sink: every service + every driver from every focused group                            | all of the above                                                                      |

**Universal services** (added to every group automatically by `groups.yaml`):
`postgres-metadata`, `minio`, `materialized`, `clusterd1`, `fault-orchestrator`,
`workload`.

**Default drivers** (auto-merged into every real template at image build time
via the `defaults/` pseudo-template):
- `singleton_driver_catalog_recovery_consistency`
- `parallel_driver_strict_serializable_reads`
- `parallel_driver_mv_reflects_table_updates`
- `anytime_health_check`

These cover the SUT-anchored properties (catalog, persist, basic
serializability) regardless of which focused-group template Antithesis picks.

---

## Directory layout

```
test/antithesis/
├── AGENTS.md                       # quick orientation for skills agents
├── README.md                       # this file
├── Makefile                        # local-dev entry point
├── mzcompose.py                    # source of truth: kitchen-sink topology
├── groups.yaml                     # workload-group manifest
├── groups.py                       # manifest loader (shared by export + workload runtime)
├── export-compose.py               # mzcompose.py + groups.yaml → per-group docker-compose.yaml
├── export-env.py                   # → per-group .env with mzbuild fingerprints
├── push-antithesis.py              # CI: retag + push to Antithesis GCP registry
│
├── configs/<group>/                # GENERATED outputs, committed
│   ├── docker-compose.yaml         # the file Antithesis ingests
│   ├── .env                        # placeholder in tree; CI overwrites at build
│   ├── Dockerfile                  # FROM scratch — bundles the two files above
│   └── mzbuild.yml                 # antithesis-config-<group> (publish: false)
│
├── workload/                       # shared source tree for every per-group workload image
│   ├── Dockerfile                  # symlinked to from workloads/<group>/
│   ├── workload-entrypoint.sh      # provisions antithesis_cluster, fires setup_complete
│   ├── setup-complete.sh           # writes the setup_complete JSONL event
│   ├── bake-test-templates.sh      # image-build-time: copies scripts into /opt/antithesis/test/v1/
│   ├── scripts-for-group.py        # tiny manifest-walker for the bake script
│   ├── test/
│   │   ├── defaults/               # default_drivers + default_anytime → merged into every template
│   │   ├── kafka/                  # kafka group's setup/drivers/anytime scripts
│   │   ├── pg-cdc/                 # pg-cdc group …
│   │   ├── mysql-cdc/              # mysql-cdc group …
│   │   ├── sql-server-cdc/         # sql-server-cdc group …
│   │   ├── parallel-workload/      # parallel-workload group …
│   │   └── helpers/                # helper_*.py — shared utilities; ignored by Test Composer
│   └── stubs/materialize/mzcompose/   # stubs satisfying parallel_workload's module-load imports
│
├── workloads/<group>/              # per-group image dirs
│   ├── mzbuild.yml                 # antithesis-workload-<group>, ANTITHESIS_WORKLOAD_GROUP build arg
│   ├── Dockerfile -> ../../workload/Dockerfile
│   └── (gitignored materialize/, testdrive-files/, groups.yaml — populated by pre-image: copy)
│
├── upsert-hammer/                  # standalone antithesis-upsert-hammer image (INC-936 stress)
│   ├── Dockerfile, mzbuild.yml
│   └── upsert_hammer_loop.py
│
└── fault-orchestrator/             # single bash container coordinating global fault windows
    └── pause_faults.sh             # inlined into the compose `command:` (no mzbuild image)
```

### Why per-group `workloads/<group>/` dirs exist

Each group needs its own `antithesis-workload-<group>` mzbuild image so that
`bake-test-templates.sh` runs at **image build time** with the right
`ANTITHESIS_WORKLOAD_GROUP` arg and only that group's scripts land in
`/opt/antithesis/test/v1/`. The Dockerfile is shared via a symlink; per-group
`mzbuild.yml` files differ only in `name:` and `build-args:`.

We can't filter at container startup: Antithesis scans the **image layout**
for the `/opt/antithesis/test/v1/` directory before any process runs.

---

## Test command conventions

Antithesis routes commands by filename prefix. We use these:

| Prefix              | When it runs                                    | Notes                                                                                  |
| ------------------- | ----------------------------------------------- | -------------------------------------------------------------------------------------- |
| `first_`            | Once at start, before any driver                | One-shot setup (replication config, source creation, ALTER SYSTEM SET).                |
| `singleton_driver_` | At most once per timeline                       | For long-lived stateful tests (cross-restart catalog model, upsert state rehydration). |
| `parallel_driver_`  | Concurrently, many invocations per timeline     | The workhorse — most properties.                                                       |
| `anytime_`          | Continuously, in parallel with everything       | Quick safety probes (frontier monotonic, health check).                                |
| `helper_`           | **Ignored** by Test Composer                    | Importable Python modules. Always-bake (the bake script copies all `helper_*` files in regardless of manifest entries).  |
| `serial_driver_`    | Reserved; not used today                        | One-at-a-time per timeline.                                                            |
| `eventually_` / `finally_` | Reserved; not used today                 | End-of-timeline finalizers.                                                            |

**Drivers must be short and independent.** Antithesis re-launches them
freely; cumulative coverage comes from many invocations, not one big batch.
Per-invocation prefix-scoping (e.g. `prefix = f"p{helper_random.random_u64():016x}"`)
is the standard pattern for keeping concurrent invocations from colliding.

**Singleton drivers** are the exception: they own their timeline and run long
enough to span multiple Antithesis-injected restarts. They keep the
authoritative model **in memory** across cycles so a post-recovery read can be
compared against the pre-fault expectation.

### Property-assertion patterns

Every driver follows the same shape:

```python
from antithesis.assertions import always, sometimes

always(
    invariant_holds(observed),
    "human-readable property name (kept stable across runs)",
    {"prefix": prefix, "observed": observed, "expected": expected},
)
sometimes(
    liveness_event_happened,
    "liveness anchor — without this, the always check could be vacuous",
    {...},
)
```

- `always(...)` — **safety** — must never be False. The triage report flags it
  if a single timeline ever observes it False.
- `sometimes(...)` — **liveness / coverage anchor** — must be True at least
  once across all timelines. Reports the *absence* (e.g. "the catchup never
  completed within budget across any timeline").

Pair every safety check with a liveness anchor or the safety check can be
vacuously satisfied (a driver that immediately bails on fault would pass
`always(invariant)` trivially).

### Fault-tolerance helpers

- `helper_pg.py` — pgwire connect/query helpers with bounded retry across
  Antithesis fault windows. `CONNECT_TIMEOUT_S` and `_RETRY_BUDGET_S` are
  sized against the global fault-orchestrator cadence (defaults 20-40s
  on/off — see below).
- `helper_fault_tolerance.py` — single source of truth for the
  "looks like fault injection" string list. Used by every driver to demote
  `always(False)` to `sometimes(False)` on connection-shaped errors.
- `helper_random.py` — `AntithesisRandom` routes entropy through the SDK so
  every randomized choice in a driver explores fresh paths per timeline
  instead of locking in after one seed.

### The fault-orchestrator

`fault-orchestrator/pause_faults.sh` runs in its own bash container and
alternates Antithesis fault-ON / fault-OFF windows globally
(`MIN_ON=20`, `MAX_ON=40`, `MIN_OFF=20`, `MAX_OFF=40` by default), calling
`${ANTITHESIS_STOP_FAULTS} <seconds>` to open quiet windows. Per-driver
quiet-window requests are an anti-pattern: many concurrent drivers asking
for quiet windows keeps the SUT mostly un-faulted. Centralising the cadence
gives one coordinated rhythm and lets every driver tune its catchup timeout
against a single known cycle (`CATCHUP_TIMEOUT_S = 90.0` ≈ at least one full
fault-ON + fault-OFF window plus margin).

Outside Antithesis `ANTITHESIS_STOP_FAULTS` is unset, the script exits
immediately, and the service is a no-op — `make up-local` still brings it up
because compose dependencies reference it.

---

## Local dev loop

Two parallel target families exist:

### Antithesis-flavored (matches CI)

```
make build GROUP=<name>     # export-compose + export-env + acquire x86_64 antithesis images
make up    GROUP=<name>     # build, then `docker compose up -d`
make smoke GROUP=<name>     # build + up + a one-shot psql write/read smoke test
make test  GROUP=<name>     # same smoke test against an already-running stack
```

These build with `--antithesis --arch x86_64`, matching what the .env / compose
expect on the Antithesis platform. On Apple Silicon they run under
Rosetta/qemu, and testdrive **does not work** in that mode (segfaults inside
foundationdb init).

### Local-dev (host arch, plain images)

```
make build-local GROUP=<name>   # --no-antithesis + host arch
make up-local    GROUP=<name>   # build-local + up
```

`-local` targets:
- Use the host arch (native aarch64 on Apple Silicon).
- Use **non-antithesis-flavored** images — they don't need `libvoidstar.so`
  installed locally, and crucially the full transitive image set (`testdrive`,
  etc.) is only published in the plain flavor.
- Are the only thing that exercises the workload image's testdrive-runner
  drivers natively.

Default GROUP if unset is `combined` (kitchen-sink, slowest to build).

`RUNTIME=podman` (auto-detected) or `RUNTIME=docker` toggles the container
runtime. Antithesis itself uses neither — its sandbox parses the compose
file with its own orchestrator.

---

## CI / push to the Antithesis registry

The Buildkite pipeline runs `ci/test/build-antithesis.sh`, which:

1. Loops over `load_manifest().groups` and writes `configs/<group>/.env`
   with refs pointing at the Antithesis GCP Artifact Registry.
2. Runs `bin/pyactivate -m ci.test.build` — the standard mzbuild build
   that produces antithesis-flavored images on GHCR.
3. `docker login`s the Antithesis registry with
   `ANTITHESIS_GCP_SERVICE_ACCOUNT_JSON` (a Buildkite-agent env var, distinct
   from `GCP_SERVICE_ACCOUNT_JSON`).
4. Runs `push-antithesis.py`, which retags and pushes every Materialize-built
   image (`materialized`, every `antithesis-workload-<group>`, every
   `antithesis-config-<group>`, `antithesis-upsert-hammer`) to the Antithesis
   registry. Public images (postgres, minio, kafka stack, mssql-server) stay
   on upstream registries — Antithesis can reach those directly.

The actual Antithesis run is then triggered via the
[`antithesis-launch` skill](./AGENTS.md), which uses `snouty submit`.
`snouty validate` is the local-validation step that brings the compose up,
emits `setup_complete`, and runs the test commands one round-trip — without
fault injection.

Lint job `ci/test/lint-main/checks/check-antithesis-compose.sh` re-runs
`export-compose.py` for every group and diffs against the committed YAMLs.
If you touch `mzcompose.py` or `groups.yaml`, you also need to rerun:

```sh
for g in kafka pg-cdc mysql-cdc sql-server-cdc parallel-workload upsert-stress combined; do
    bin/pyactivate test/antithesis/export-compose.py --group=$g \
        > test/antithesis/configs/$g/docker-compose.yaml
done
```

or `make build-all`.

---

## Authoring a new property / driver

1. Pick the right template directory under `workload/test/`. If the property
   is SUT-anchored (catalog, persist, MV/strict-serializable) it goes under
   `defaults/`; otherwise under the focused-group dir that owns the source it
   targets.
2. Pick a filename prefix (`parallel_driver_…`, `singleton_driver_…`, …).
3. Use existing helpers: `helper_pg`, `helper_kafka`, `helper_random`,
   `helper_logging`, `helper_fault_tolerance.looks_like_fault`.
4. Per-invocation scope by a u64 prefix so concurrent invocations don't
   collide on the same rows / keys / role names.
5. Pair every `always(...)` safety check with a `sometimes(...)` liveness
   anchor. Skip the safety check entirely on a fault-shaped failure
   (`looks_like_fault(str(exc))`) — that's not a property violation, and a
   false positive on transient unavailability would obscure real bugs.
6. Add the entry to `groups.yaml` under the right group (template entries use
   `<template>/<basename>` paths). For SUT-anchored drivers add to
   `default_drivers:` instead.
7. Build locally and verify:
   ```sh
   make up-local GROUP=<your-group>
   docker compose -p materialize-antithesis-<group> \
       --env-file test/antithesis/configs/<group>/.env \
       -f test/antithesis/configs/<group>/docker-compose.yaml \
       exec workload bash -lc 'ls /opt/antithesis/test/v1/'
   docker compose … exec workload python3 /opt/antithesis/test/v1/<template>/<your_file>.py
   ```
8. Use the `antithesis-research`, `antithesis-workload`, and
   `antithesis-launch` skills for systematic guidance.

---

## Authoring a new workload group

A new group means a new Antithesis job; the existing topology covers most
common cases via `combined`, so do this only when the group needs a
materially different topology or worker-count rewrite.

1. Add the group to `groups.yaml` with `services:` (extras beyond universal),
   `setup:`, `drivers:`, `anytime:`, and a `description:` that captures the
   *why*.
2. Make sure every script under `<template>/...` referenced exists at
   `workload/test/<template>/<basename>.py` (or `.sh` for
   `anytime_health_check`).
3. Create the per-group image dir:
   ```
   workloads/<new-group>/
       mzbuild.yml          # name: antithesis-workload-<new-group>
                            # build-args: ANTITHESIS_WORKLOAD_GROUP: <new-group>
       Dockerfile -> ../../workload/Dockerfile
       .gitignore           # /materialize/, /testdrive-files/, /groups.yaml
   ```
   Easiest: copy from `workloads/kafka/` and tweak names.
4. Create the per-group config dir:
   ```
   configs/<new-group>/
       Dockerfile           # FROM scratch + COPY docker-compose.yaml .env /
       mzbuild.yml          # name: antithesis-config-<new-group>, publish: false
       docker-compose.yaml  # placeholder — regenerated by export-compose
       .env                 # placeholder — CI overwrites
   ```
5. Wire the image into `export-compose.py` `MATERIALIZE_IMAGES` if the name
   isn't already covered by the per-group placeholder logic.
6. Run `make build-all` to regenerate all per-group `docker-compose.yaml`
   files, and commit the generated YAML.
7. Add the group name to `Makefile`'s `ALL_GROUPS`.

Lint will catch missing groups via
`ci/test/lint-main/checks/check-antithesis-compose.sh`.

---

## Conventions that are easy to violate

- **No underscores in service names.** Docker DNS treats them as malformed;
  `export-compose.py` enforces this. Use hyphens.
- **`MZ_EAT_MY_DATA` / fsync-noop / `libeatmydata` are stripped** at export
  time — they would defeat crash-recovery fault injection. Don't add them
  back via service config.
- **Bare `ports:` are stripped.** Antithesis is container-to-container; bare
  ports collide on ephemeral host bindings under podman during compose
  startup. Anything you need to reach during local dev gets reached by
  `docker compose exec`, not by host port-forward.
- **`depends_on: service_started` → upgraded to `service_healthy`** wherever
  the dependency declares a healthcheck. Don't rely on `service_started`;
  DNS isn't ready when it fires.
- **Auto-generated docker networks → replaced with explicit
  `antithesis-net` bridge.** Auto networks had flaky early-bringup DNS.
- **`set_explicit_names` forces `container_name == hostname == service_key`.**
  Antithesis triage reports attribute log lines and assertion failures by
  hostname.
- **`restart: no` on every long-lived service.** Antithesis's
  kill/pause/restart directives fight docker-compose auto-restart. Process
  exits should be visible to triage, not papered over.
- **Don't open per-driver quiet windows.** The global fault-orchestrator
  owns the cadence; per-driver requests union into a near-permanent quiet
  state and we never actually exercise faults.
- **Don't introduce new mzbuild images without a public-image policy.**
  `export-compose.py` raises if it can't classify a mzbuild ref. Either add
  the name to `MATERIALIZE_IMAGES` (Materialize-published) or to
  `PUBLIC_FALLBACKS` (swap to upstream).
- **Sized retry budgets, not unbounded retries.** Connection helpers retry
  for `_RETRY_BUDGET_S` (~180s) — comfortably longer than one full
  fault-ON+OFF cycle, comfortably shorter than a forever-loop that hides a
  real stall. Tune to match the orchestrator cadence if you change defaults.
- **The `.env` in `configs/<group>/.env` committed in tree is a placeholder.**
  CI overwrites it with real fingerprints + the Antithesis registry. Don't
  rely on it for `make up-local` — that path uses the host-arch
  `--no-antithesis` flow and writes its own.
- **`AntithesisRandom`, not `random.Random(seed)`**, for any decision you
  want the fuzzer to explore. A stdlib RNG seeded once locks in the timeline
  after one draw.
- **`scratch_directory=None` on clusterd matches production.** Adding a
  scratch dir puts you on a code path production doesn't exercise and can
  race on `LOCK` files between replicas in the same compose.

---

## Next test frameworks to port

Today the POC covers (per workload group):
- CDC source correctness — `pg-cdc`, `mysql-cdc`, `sql-server-cdc`, `kafka`
- Catalog recovery & persist invariants — `defaults`
- Randomized concurrent SQL stress + SinceViolation — `parallel-workload`
- Focused upsert stress (INC-936) — `upsert-stress`
- Strict-serializable reads & MV correctness — `defaults`

There's a long tail of in-tree test frameworks under `test/` that exercise
correctness properties Antithesis is unusually well-suited to amplify. Below
are the strongest candidates, roughly in order of return-on-effort.

### 1. `test/platform-checks` — highest leverage

**Why it's a fit.** Platform-checks already encodes properties as
`Check` classes with `initialize` / `manipulate` / `validate` phases,
exercised across upgrade / restart / backup-restore / zero-downtime-deploy
**scenarios**. That structure is a near-1:1 mapping to Antithesis's
`first_` + `parallel_driver_` + `singleton_driver_` shape — and the
"scenario" abstraction is exactly the kind of restart/upgrade interleaving
Antithesis can fault-inject randomly rather than enumerate by hand.

**Properties to land.** "Every materialized DDL persists across an
environmentd restart" (already partially covered by
`catalog_recovery_consistency`), "every source resumes ingestion after
clusterd restart", "schema migrations are atomic across upgrade", and the
full battery of `Check` subclasses in `misc/python/materialize/checks/`.

**Effort.** Medium. Each Check class needs an adapter that drives its
`manipulate` phase from a `parallel_driver_` and its `validate` phase from
either an `anytime_` probe or a `singleton_driver_` post-fault check.
Scenarios become "this driver should still pass when Antithesis kills
environmentd/clusterd between cycle N and cycle N+1," which is mostly free.

**Group.** A new `platform-checks` group, or merge into `defaults/` for the
SUT-anchored ones.

### 2. `test/0dt` — zero-downtime deploy

**Why it's a fit.** The `0dt` framework drives an explicit
"second-environmentd starts, leases transition, first one steps down"
sequence and asserts continuous read availability + clean fence handoff.
Antithesis can randomly inject faults *during* the transition window — the
exact moments hardest to test by hand — and assert the same continuous-read
property as a `parallel_driver_`. We have **zero deploy coverage today**, so
this is high-value greenfield.

**Properties.** "Reads succeed continuously across deploy boundary",
"writes either fence cleanly or the new leader picks up uncommitted state
correctly", "no torn catalog writes spanning the leadership transition".

**Effort.** Medium-high. Requires a second `materialized` service in the
compose plus orchestration to swap leader state — not faultable purely by
container kills today. May be feasible as a `singleton_driver_` that
shells out to `mz` admin commands.

**Group.** New `0dt` group with a second materialized container.

### 3. `test/txn-wal-fencing` — fencing under concurrent workload

**Why it's a fit.** This framework introduces a second Materialize instance
while a concurrent workload is running and asserts the older instance fences
correctly. The race window is small and sensitive — perfect for Antithesis's
deterministic-replay debugger to hammer at. The "second-environmentd" shape
overlaps `0dt` but the property is narrower (fencing only, not full deploy).

**Properties.** "Old environmentd's writes never land after the new
environmentd has started", "no dual-leader writes to persist", "fencing
errors surface clearly to clients holding open sessions on the old
instance".

**Effort.** Low-medium. The framework is small and self-contained; the main
work is wiring a second `materialized` and writing the dual-instance
property as a singleton driver.

**Group.** Could fold into the `0dt` group above — same topology.

### 4. `test/sqlsmith` — random query generation

**Why it's a fit.** SQLsmith generates random ASTs and runs them against
Materialize; today it just checks "no panic / known errors only". Under
Antithesis, panics become one of *many* properties — also: "no internal
error escapes to the client", "no plan hangs past N seconds", "no query
crashes a single replica without the cluster reissuing on the other". SQL
generation is naturally seeded through `AntithesisRandom`, so the fuzzer
explores fresh ASTs per timeline.

**Properties.** "No SUT panic on any well-formed query",
"`materialized` never logs `internal error` for queries we generate",
"clusterd recovers cleanly from any query that kills a replica".

**Effort.** Low. SQLsmith already runs in a sidecar container; reuse the
binary and drive it from a `parallel_driver_`.

**Group.** Either fold into `parallel-workload` (it already exercises
randomized SQL) or stand up a dedicated `sqlsmith` group.

### 5. `test/source-sink-errors` + `test/pubsub-disruption`

**Why it's a fit.** Both frameworks inject specific disruptions (toxiproxy /
service kill) and assert on `mz_internal.mz_*_statuses` tables. Antithesis
already injects the disruptions for free; the *assertion* — "after a Kafka
broker goes down, `mz_source_statuses.status` reflects 'stalled' within
X seconds; after it comes back, status returns to 'running'" — is exactly
a `parallel_driver_` shape. We have no equivalent today.

**Properties.** "Source/sink status tables eventually converge to ground
truth", "no source stays 'stalled' after the upstream recovers", "persist
pubsub disruption is invisible to user-visible read availability".

**Effort.** Low. The properties are clean status-table queries; the
disruption side comes from Antithesis already.

**Group.** Fold into the existing `kafka`, `pg-cdc`, etc. groups as
additional drivers.

### 6. `test/zippy` — generative model-based checking

**Why it's a fit.** Zippy generates pseudo-random DDL/DML sequences while
keeping an authoritative expected-state model. Translating that to
Antithesis means: drive the model from a `singleton_driver_`, drive the SUT
from `parallel_driver_` workers, and assert model-vs-SUT equality after
each settle. The model becomes the recovery checker — exactly what
`upsert_state_rehydration` does today, but generalized.

**Properties.** "After any sequence of DDL/DML and any subset of injected
faults, materialized's view of the world matches the model's."

**Effort.** High. Zippy has a lot of generation logic; this is more a
rewrite than a port. Probably the right move is to **steal the model and
the action vocabulary** rather than the orchestration.

**Group.** Would need its own dedicated group.

### What we explicitly should *not* port

- **`test/feature-benchmark`, `test/parallel-benchmark`, `test/scalability`.**
  Performance measurement. Antithesis's deterministic hypervisor distorts
  wall-clock timing — perf numbers are unreliable under it.
- **`test/sqllogictest`, `test/pgtest`, `test/testdrive`.** Pure
  correctness/conformance. They run against a single-process Materialize
  with no fault injection target. We already pull `test/pg-cdc/*.td` (and
  mysql-cdc, sql-server-cdc) into the workload image and run them from
  `singleton_driver_*_testdrive.py`, which is the right pattern when we
  want a specific .td file to run under faults.
- **`test/chbench`, `test/lang`, `test/limits`.** Scaling /
  language-conformance — high-throughput single-shot loads, not interleaved
  property checks.
