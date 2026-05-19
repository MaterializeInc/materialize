---
commit: e7ac38b338
updated: 2026-05-06
scope: existing-test-drivers-to-antithesis-test-composer
status: research
---

# Test Driver Integration

## Antithesis driver model

Antithesis Test Composer discovers executable commands under
`/opt/antithesis/test/v1/<test_dir>/<prefix>_<command>`. The filename prefix
controls scheduling:

- `first_`: one-time per-timeline setup, no faults, no concurrency.
- `singleton_driver_`: run one existing monolithic test or workload once per
  timeline under fault injection.
- `serial_driver_`: repeat operations that need exclusive driver access.
- `parallel_driver_`: repeat operations that can overlap with other drivers.
- `anytime_`: invariant or availability checks that may run alongside drivers.
- `eventually_`: terminal recovery check after faults and drivers stop.
- `finally_`: terminal final-state check after drivers complete naturally.

The official porting path for existing tests is: start with a
`singleton_driver_` wrapper, then split the workload into `first_`,
`parallel_driver_`, `serial_driver_`, `anytime_`, `eventually_`, and
`finally_` commands as the workload matures.

Sources:

- https://antithesis.com/docs/test_templates/first_test/
- https://antithesis.com/docs/test_templates/test_composer_reference/
- https://antithesis.com/docs/test_templates/composer_example/
- https://antithesis.com/docs/environment/fault_injection/

## Materialize driver fit

| Driver | Current shape | Antithesis fit | Integration work |
| --- | --- | --- | --- |
| `testdrive` | Rust binary plus `.td` corpus. `src/testdrive/src/lib.rs` exposes `run_file`, `run_stdin`, and `run_string`; `src/testdrive/ci/Dockerfile` already packages the binary. | Best first target. Use as `singleton_driver_` or `serial_driver_` around selected `.td` files. | Build a client image that includes `testdrive` and selected `.td` files. Pass service hostnames directly instead of relying on `mzcompose`. Disable reset for commands that share state. |
| `parallel-workload` | Python randomized concurrent SQL workload. `test/parallel-workload/mzcompose.py` starts many services and calls `materialize.parallel_workload.parallel_workload.run`. | Best existing stress workload for Antithesis once decoupled from `mzcompose`. Initially use `singleton_driver_parallel_workload_regression` with a short runtime. Later split action groups into `parallel_driver_` commands. | Refactor the standalone path: `run()` currently requires a `Composition` during database setup and several scenarios require `composition` for kill, 0dt, backup/restore, Minio, and Polaris actions. Start with `regression`, `rename`, or `repeat-row` scenarios and let Antithesis provide node/network faults. |
| `zippy` | Sequential randomized, model-like workload that tracks expected state and validates correctness. | Strong correctness driver as a `singleton_driver_`; longer-term, map bootstrap to `first_`, actions to `serial_driver_`, and validation to `anytime_`/`finally_`. | It is tightly coupled to `mzcompose` for service start/stop, testdrive calls, and failure actions. Needs an Antithesis client adapter with topology already running and service-control actions either removed or translated. |
| `SQLancer` | External generator with Materialize wrapper in `misc/python/materialize/sqlancer.py`. | Good `singleton_driver_` or `parallel_driver_` for SQL logic bugs. | Existing wrapper starts `materialized` through `mzcompose`; create a direct command wrapper targeting the already-running `materialized` service. |
| `SQLsmith` | Query generator against one or more Materialize instances. | Good `parallel_driver_` query-fuzzing command after basic setup. | Existing mzcompose workflow seeds data and runs a service; turn the seed-data step into `first_` and the generator invocation into `parallel_driver_`. |
| `RQG` | Grammar-based query generator with optional reference comparison. | Good later-stage `singleton_driver_` or `parallel_driver_`, especially for reference-comparison workloads. | Needs a direct wrapper and a clear decision about whether the reference DB belongs in the topology. |
| `sqllogictest` | Embedded-Mz SQL correctness runner. | Lower initial value for Antithesis distributed fault testing because it does not exercise the normal multi-container system path. | Can be used for SQL-engine smoke coverage, but prioritize networked drivers first. |
| `mzcompose` | Python orchestration over Docker Compose. | Use as source material for topology and command arguments, not as an Antithesis test command. | Do not invoke from Test Composer commands, because commands run inside containers while Antithesis owns orchestration. |

## Recommended integration sequence

### Phase 1: thin singleton wrappers

Goal: get existing coverage running under Antithesis faults with minimal
refactoring.

Create one template, for example `/opt/antithesis/test/v1/materialize_smoke/`,
in the `test-driver` container:

```text
first_bootstrap_sql
singleton_driver_testdrive_smoke
singleton_driver_sqlancer_norec
eventually_materialized_recovers
```

Start with a small `.td` selection that only needs `materialized`, then add
Kafka/Schema Registry/Minio/Postgres dependencies as soon as the base wiring is
stable. This proves image layout, service discovery, readiness signaling,
nonzero-exit failure reporting, and local dry-run mechanics.

### Phase 1b: broad topology, narrow commands

Goal: cover many Materialize configurations, sources, and sinks without turning
the first Antithesis run into an unstructured full-CI clone.

Use one "broad integration" topology that starts the common dependency set:

```text
materialized
test-driver
redpanda-or-kafka
schema-registry
postgres
mysql
sql-server
minio
azurite
cockroach-or-postgres-metadata
```

Within that topology, split incompatible workload families into separate test
directories. Antithesis runs commands from one test directory per execution
history, so this gives each history a coherent setup while the overall run
covers many families.

Suggested directories:

```text
/opt/antithesis/test/v1/base_sql/
/opt/antithesis/test/v1/kafka_sources/
/opt/antithesis/test/v1/kafka_sinks/
/opt/antithesis/test/v1/postgres_cdc/
/opt/antithesis/test/v1/mysql_cdc/
/opt/antithesis/test/v1/sql_server_cdc/
/opt/antithesis/test/v1/s3_copy/
/opt/antithesis/test/v1/persist_durability/
/opt/antithesis/test/v1/catalog_ddl/
```

Each directory should have its own `first_` setup command that creates only the
objects that its driver commands need. This avoids having every timeline create
every source and sink type before useful faulted execution begins.

Do not make each existing `.td` file its own test directory. That would preserve
the old example-based shape and waste Antithesis' scheduling. Prefer command
families such as `parallel_driver_ingest`, `parallel_driver_verify`,
`serial_driver_alter_source`, `anytime_health`, and `eventually_catch_up`.

### Configuration model

Treat configuration variation as three different kinds:

| Variation type | Examples | Where it belongs |
| --- | --- | --- |
| Per-command/session | session variables, transaction isolation, target cluster, SQL API vs pgwire, retry/timeout tuning | Randomize inside driver commands. |
| Per-timeline SQL setup | cluster size/replica count, feature flags via `ALTER SYSTEM`, source/sink options, envelope/format choices, indexes, materialized vs non-materialized views | Choose in `first_` setup commands, record it in a config table, then have drivers read it. |
| Per-run topology/startup | persist backend, metadata store, embedded vs external clusterd, Kafka vs Redpanda, Minio vs Azurite, single vs multi-`materialized`, boot flags/environment variables | Use separate compose/config images or separate Antithesis launches. |

This prevents one run from needing mutually incompatible startup settings while
still letting Antithesis explore a large state space inside each run.

For the source/sink matrix, the first broad run should prefer representative
combinations over exhaustive Cartesian product coverage:

| Axis | Initial representative values |
| --- | --- |
| Kafka source envelope | append-only, upsert, Debezium-style where cheap |
| Kafka format | Avro/CSR and JSON first; add Protobuf later if needed |
| Kafka sink | exactly-once sink plus simple sink smoke |
| CDC source | one Postgres, one MySQL, one SQL Server setup path |
| Object storage | S3-compatible Minio first; Azurite in a separate run or separate directory if both stay cheap |
| Persist/metadata | file/local for smoke; external metadata/blob for durability runs |
| Cluster topology | one multi-replica cluster in broad run; separate run for external clusterd/topology-specific faults |

### Phase 2: dedicated randomized workload wrappers

Goal: reuse Materialize's existing randomized drivers while avoiding
orchestration conflicts.

- Add a composition-free `parallel-workload` command for non-orchestration
  scenarios. It should target service hostnames, take bounded `--runtime` and
  `--threads`, and use workload-side Antithesis assertions for query failures,
  unexpected panics, and meaningful reachability.
- Add a `zippy` singleton command for a narrow scenario that does not start or
  stop containers itself.
- Add `eventually_` or guarded `ANTITHESIS_STOP_FAULTS` recovery checks that
  poll SQL/HTTP availability and then validate durable state.

### Phase 3: full Test Composer decomposition

Goal: let Antithesis choose operation ordering and concurrency instead of
running a monolith.

Suggested mapping:

| Test Composer command | Materialize behavior |
| --- | --- |
| `first_schema` | Create baseline databases, roles, clusters, connections, seed tables, and optional source topics. |
| `parallel_driver_insert` | Insert/COPY/Kafka ingest actions from `parallel-workload` and `zippy`. |
| `parallel_driver_select` | Peeks, subscribes, HTTP SQL, prepared statements, and query fuzzing. |
| `parallel_driver_ddl` | DDL that is safe to run concurrently with expected-error handling. |
| `serial_driver_admin` | Mutations that need exclusivity: disruptive DDL, large source/sink setup, backup/restore-like phases. |
| `anytime_availability` | Low-cost SQL/HTTP health and "read eventually succeeds" checks under active faults. |
| `eventually_recovery` | Stop faults, poll for recovery, verify sources/sinks/dataflows catch up. |
| `finally_consistency` | Validate final row counts, watermarks, sink contents, and catalog consistency when drivers completed naturally. |

## Key engineering constraints

- Test commands must eventually exit. Long-running drivers need explicit small
  runtimes.
- Commands should tolerate transient network and process failures during driver
  phases. Reserve hard failures for property violations, unrecoverable errors,
  or failure to recover after a quiet period.
- Do not duplicate fault systems at first. Antithesis already injects network,
  throttling, hang, restart, and clock faults. Materialize actions like kill,
  0dt, backup/restore, and toxiproxy are useful later, but they should be
  reintroduced intentionally.
- Use Antithesis SDK randomness for new wrappers. Existing `random.seed(...)`
  paths are reproducible locally, but Antithesis-guided randomness gives better
  replay and search.
- Prefer workload-side Python assertions first. Add Rust SDK assertions in
  `materialized` only where internal state is necessary to express the
  property.
- Avoid duplicating Materialize test infrastructure. Antithesis commands should
  be thin adapters over shared helper modules, existing driver entry points, and
  reusable source/sink setup code. If a wrapper starts accumulating generic
  Kafka, CDC, SQL retry, expected-error, or object-model logic, move that logic
  into a reusable `misc/python/materialize/...` helper that both mzcompose tests
  and Antithesis commands can call.

## Thin-adapter design

The intended layering is:

```text
Antithesis command file
  -> antithesis-specific adapter: lifecycle/assertions/random/quiet-periods
    -> shared Materialize workload helper
      -> existing driver/library/client code
        -> Materialize and dependencies
```

For example, an upsert-source command should not reimplement all testdrive Kafka
or SQL behavior. It should either:

- invoke `testdrive` with generated or templated `.td` fragments for setup and
  straightforward checks; or
- call a shared Python helper that owns topic creation, message production,
  expected-state tracking, and SQL polling.

The Antithesis-specific layer should stay small:

- choose randomized parameters using Antithesis randomness;
- call the shared helper;
- translate helper results into Antithesis assertions;
- request quiet periods or run recovery checks when appropriate;
- exit cleanly.

This gives Antithesis a native driver shape without creating a second
Materialize test framework.

## Reuse plan for existing infrastructure

| Existing infrastructure | Reuse strategy |
| --- | --- |
| `testdrive` binary and `.td` DSL | Use directly for scripted setup and checks. Generate small `.td` fragments where needed instead of duplicating SQL/Kafka command behavior in Python. |
| `Composition.testdrive()` / `run_testdrive_files()` argument knowledge | Extract the command-line argument construction into a reusable helper if Antithesis and mzcompose need the same defaults. Do not call Docker orchestration from Antithesis commands. |
| `parallel-workload` action/object model | Refactor only the orchestration boundary so actions can run against an already-running topology. Keep action definitions, expected errors, and object tracking in the existing modules. |
| `zippy` action/state model | Reuse scenario/action/state code, but introduce an execution adapter for "services already exist" rather than forking zippy logic. |
| Source/sink setup patterns in `.td` files | Promote repeated patterns into templates or helper functions used by both current tests and Antithesis commands. |
| SQL retry/polling logic | Centralize in shared helper code, especially for eventual catch-up checks. |

The first implementation should probably add an `antithesis`-aware adapter
module under `misc/python/materialize/` rather than building substantial logic
under `antithesis/test/`. The files under `antithesis/test/` should mostly be
small executable entry points and `helper_` imports/templates.

## Implemented prototype: upsert sources

The first prototype follows that thin-adapter design:

```text
antithesis/test/v1/upsert_sources/*
  -> materialize.antithesis.upsert_sources
    -> testdrive binary
      -> Kafka + Materialize
```

Files:

- `misc/python/materialize/antithesis/sdk.py`: small wrapper around the
  Antithesis Python SDK with local fallbacks for import-time and local smoke
  testing.
- `misc/python/materialize/antithesis/upsert_sources.py`: reusable upsert-source
  workload helper. It generates small testdrive fragments for setup, writes,
  reads, health checks, and final consistency checks.
- `antithesis/test/v1/upsert_sources/first_create_upsert_source`: creates a
  text/text Kafka upsert source and an expected-state table.
- `antithesis/test/v1/upsert_sources/parallel_driver_write_upserts`: generates
  random keyed upsert/tombstone records and records the latest expected value
  for keys whose Kafka write command completed.
- `antithesis/test/v1/upsert_sources/parallel_driver_read_stale_safe`: runs
  low-cost reads that are safe during active faults.
- `antithesis/test/v1/upsert_sources/anytime_upsert_source_health`: checks
  source status without requiring exact catch-up under active faults.
- `antithesis/test/v1/upsert_sources/eventually_upsert_source_catches_up`:
  requests a quiet period, writes a sentinel, waits for it, and verifies all
  expected rows are visible.
- `antithesis/test/v1/upsert_sources/finally_upsert_expected_rows_visible`:
  verifies the expected-state table is represented in the upsert source table
  when drivers complete naturally.

This is intentionally not a full testdrive replacement. It delegates Kafka
topic creation, Kafka ingest, SQL execution, retries, and SQL result checking to
the existing `testdrive` binary.

## Immediate next implementation slice

1. Create `antithesis/config/docker-compose.yaml` with `materialized` and a
   `test-driver` client.
2. Build a `test-driver` image containing the Antithesis Python SDK,
   `testdrive`, selected `.td` files, and small Python wrappers.
3. Add a readiness entrypoint that waits for Materialize, emits
   `setup_complete`, and sleeps.
4. Add `singleton_driver_testdrive_smoke` for one or two stable SQL-only
   `.td` files.
5. Add `eventually_materialized_recovers` that requests or relies on a quiet
   period, polls Materialize, and checks a small durable invariant.
6. Validate locally with `docker compose exec` and then with `snouty validate`
   once `snouty` is installed.
