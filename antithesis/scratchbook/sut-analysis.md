---
commit: e7ac38b338
updated: 2026-05-06
scope: existing-test-driver-integration
---

# SUT Analysis

This is a targeted analysis for integrating existing Materialize test drivers
with Antithesis. It is not a full product-wide property catalog.

## System shape

Materialize is exercised in existing system tests as a networked database
process (`materialized`/`environmentd`) plus optional dependencies such as
Kafka, Schema Registry, Postgres, MySQL, SQL Server, Minio/Azurite, CockroachDB,
and Polaris. The existing `src/materialized/ci/Dockerfile` packages the
`materialized` binary and symlinks it as `environmentd` and `clusterd`, which
is a useful base for an Antithesis SUT image.

Existing tests are normally orchestrated by `mzcompose`, which starts services,
sets ports, applies Materialize system parameters, and then runs one or more
drivers. In Antithesis, Docker Compose owns orchestration, so `mzcompose` should
be treated as a source of service topology and command arguments rather than as
a scheduled test command.

## Existing drivers

- `testdrive`: networked integration DSL for Materialize and external systems.
  The Rust crate exposes `run_file`, `run_stdin`, and `run_string`; the CI image
  installs the `testdrive` binary. It is the best first target because it can
  run as a normal executable client against service hostnames.
- `sqllogictest`: SQL correctness corpus. It is valuable, but less directly
  useful for distributed Antithesis faults because the current runner embeds an
  Mz instance rather than exercising the normal multi-container deployment.
- `parallel-workload`: randomized concurrent SQL stress workload. It is a good
  Antithesis fit conceptually, but today its `mzcompose` workflow starts many
  dependencies and its core `run()` path requires a `Composition` for setup and
  several scenarios.
- `zippy`: pseudo-random sequential action generator with expected-state
  validation. It is strong for correctness, but currently uses `mzcompose` for
  service lifecycle actions and testdrive invocations.
- `SQLancer`, `SQLsmith`, and `RQG`: query generators/fuzzers. These are good
  candidates for direct command wrappers after a base client image exists.

## State under test

Relevant durable and semi-durable state includes:

- SQL catalog objects: databases, schemas, roles, clusters, replicas, tables,
  views, materialized views, sources, sinks, secrets, and connections.
- Persist state: blob storage and consensus metadata, optionally backed by file
  storage, Minio/Azurite, CockroachDB, Postgres metadata, or FoundationDB.
- External data systems: Kafka topics, schema registry subjects, upstream CDC
  tables, object storage contents, and sink outputs.
- Workload-local expected state: `parallel-workload`'s shared `Database` object
  and `zippy`'s generated capabilities/state.

## Concurrency model

Materialize itself has multiple asynchronous subsystems: SQL/session handling,
catalog/coordinator work, storage ingestion, compute dataflows, persist reads
and writes, and external source/sink clients. The existing Python
`parallel-workload` driver adds client-side concurrency by spawning worker
threads that randomly choose action lists and share a thread-safe model of
created objects. Zippy instead generates a single sequential action stream and
validates expected state.

Antithesis can add process, network, timing, and clock faults around these
drivers. The first integration should avoid duplicating fault systems until the
base harness is stable.

## Failure-prone integration areas

- Driver commands must eventually exit; several Materialize workloads are
  designed as long-running CI or longevity runs and need bounded runtimes.
- Existing workflows often assume `mzcompose` can start, stop, or kill services.
  Those actions need translation or temporary exclusion under Antithesis.
- Reset semantics matter. `testdrive` defaults to cleaning Materialize state
  before scripts, while Antithesis driver commands may intentionally share state
  within a timeline.
- External dependencies should be introduced incrementally. Starting with the
  full `testdrive` or `parallel-workload` service set would add avoidable state
  space and make early failures harder to classify.
- Expected transient failures under active fault injection need to be retried or
  classified. Hard failures should represent property violations or failure to
  recover after faults are quiet.
- The desired coverage spans many configurations, sources, and sinks. A single
  minimal topology will not be enough. The harness needs a tiered strategy:
  randomize cheap per-timeline configuration inside Test Composer commands, and
  split startup/topology variations into separate Antithesis launches.

## Integration conclusion

The least risky path is:

1. Package a small client container and run selected networked `testdrive`
   scripts as singleton or serial driver commands.
2. Add a broad integration topology with common source/sink dependencies and
   separate test directories for base SQL, Kafka, CDC, object storage, persist,
   and catalog/DDL families.
3. Add direct wrappers for query fuzzers and composition-free
   `parallel-workload` scenarios.
4. Decompose the strongest randomized workloads into first, parallel, serial,
   anytime, eventually, and finally commands after the base harness has proven
   service discovery, readiness, logging, and recovery checks.
