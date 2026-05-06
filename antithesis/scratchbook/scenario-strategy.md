---
commit: HEAD
updated: 2026-05-06
scope: how-to-add-additional-antithesis-scenarios
---

# Scenario Strategy

Decision: Antithesis runs are organized as **N narrow scenarios**, each with its
own compose file and config directory, rather than one wide union topology.

## Why

The Antithesis docs are explicit:

- A test launch consumes exactly one config image (`antithesis.config_image`)
  and a list of service images (`antithesis.images`). Multiple launches per
  commit are normal — see the GitHub Action `test_name` field documented at
  `https://antithesis.com/docs/using_antithesis/ci/`.
- Within one config, Antithesis picks **exactly one** test template per
  execution history (`first_test`). Workloads that share a topology can ship as
  multiple templates; topologies that conflict belong in separate configs.
- The "Optimizing" page rewards small focused environments. The "virtual time
  per wall time" metric punishes oversized topologies — the recommendation is
  that `vt/wt × n_containers > 1`, which one large compose makes hard.
- The "Sizing" page treats parallelism as the lever for finding bugs faster,
  not topology breadth.

## Repo layout

```
antithesis/
  configs/
    base/                          # SQL + Kafka/SR + driver
      docker-compose.yaml
    mysql_mt_replicas/             # MySQL CDC w/ replica_parallel_workers > 1
      docker-compose.yaml
    pg_cdc/
      docker-compose.yaml
    ...
  test/v1/                         # shared template tree
    upsert_sources/
    mysql_mts/
    pg_cdc/
  test-driver/                     # one mzbuild image; ships the template tree
  bin/render-compose-yaml.py       # `--scenario <name>` selects which compose to render
  mzcompose.py                     # one SERVICES list per scenario, exposed via workflows
```

The driver image stays shared across scenarios. Each scenario's compose
references it by the same image tag; the cost of carrying templates the
scenario doesn't need is small relative to building and pushing N driver
variants.

## When to split vs share

- Different *workload* on the same topology → multiple test templates inside
  the same config (Antithesis selects one per timeline).
- Different *topology* (extra/different services, different startup args) →
  separate config directory, separate `snouty launch`.
- Different *startup parameter on the same service* (e.g. file-backed persist
  vs Minio-backed persist; `replica_parallel_workers=1` vs `=8`) → separate
  scenario. Per-timeline config tunables go inside `first_` setup commands.

## Per-launch identity

Use `snouty launch --source <scenario_name>` so each scenario keeps its own
property history line in reports. The webhook reference notes property history
is "generated from all previous runs with the same `antithesis.source`
parameter."

## Implementation evolution

The branch starts with a single scenario at `antithesis/config/` (legacy
singular). As soon as the second scenario lands, rename to
`antithesis/configs/base/` and update `render-compose-yaml.py` to take a
`--scenario` argument. Don't pre-emptively migrate before there's a second
consumer.

## Examples to plan around

- **base**: materialized + redpanda + driver; covers Tier 1 SQL + Kafka work.
- **pg_cdc**: + Postgres; reuses `test/pg-cdc/*.td`.
- **mysql_cdc**: + MySQL primary; reuses `test/mysql-cdc/*.td`.
- **mysql_mt_replicas (SS-95)**: + MySQL primary + MySQL replica configured
  with `replica_parallel_workers=N`. Tests the contract enforced at
  `src/storage/src/source/mysql/replication.rs:508` under fault injection.
- **persist_durability**: + cockroach (or postgres metadata) + minio; covers
  external metadata + external blob; later.
- **s3_copy**: + minio; reuses `test/testdrive/copy-*-s3-minio*.td`.

## Sources

- `https://antithesis.com/docs/test_templates/first_test/`
- `https://antithesis.com/docs/test_templates/test_best_practices/`
- `https://antithesis.com/docs/webhook/test_webhook/`
- `https://antithesis.com/docs/webhook/webhook_reference/`
- `https://antithesis.com/docs/getting_started/setup/`
- `https://antithesis.com/docs/best_practices/sizing/`
- `https://antithesis.com/docs/best_practices/optimizing/`
- `https://antithesis.com/docs/using_antithesis/ci/`
