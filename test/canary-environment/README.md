# Introduction

The canary environment workflow creates various database objects against the
Materialize Production Sandbox.

The goals of this framework are:
- create long-lived database objects in the Materialize Production Sandbox whose
  operation is then monitored periodically going forward.
- dog-food [`mz-deploy`](../../src/mz-deploy) — Materialize's project tooling —
  against a real, continuously-loaded environment.

The objects are declared as an `mz-deploy` project (`project.toml` + `models/`)
and reconciled with `mz-deploy`. A small set of objects that `mz-deploy` cannot
manage, plus the upstream RDS/MySQL load, are created with `testdrive` (see
[What `mz-deploy` does not manage](#what-mz-deploy-does-not-manage)).

# Workload

## TPC-H Generator (`models/qa_canary_environment/public_tpch*`)

Two materialized views based on TPC-H queries (`tpch_q01`, `tpch_q18`) run
against a `LOAD GENERATOR TPCH` source. The queries were chosen to include both
outer joins and aggregations.

## Postgres source (`models/qa_canary_environment/public_pg_cdc*`)

The Postgres source is an RDS instance running in AWS that replicates two
tables. `pg_cron` jobs update the tables every second to keep emitting changes.

## MySQL source (`models/qa_canary_environment/public_mysql_cdc*`)

The MySQL source is an RDS instance running in AWS that replicates two tables.
MySQL events update the tables every second.

The RDS MySQL instance's parameter group must enable CDC, including the
`binlog_row_metadata = FULL` setting that `CREATE TABLE FROM SOURCE` requires
(RDS MySQL 8.0 defaults it to `MINIMAL`). The full set Materialize checks is
`binlog_format = ROW`, `binlog_row_image = FULL`, `gtid_mode = ON`,
`enforce_gtid_consistency = ON`, and `binlog_row_metadata = FULL`. These are
server-level settings configured on the parameter group (dynamic, no reboot),
not by this workflow — the RDS master user can't set them via `SET GLOBAL`.

## Kafka sources (`models/qa_canary_environment/public_loadgen*`)

A Kafka broker running on Confluent Cloud is used to ingest topics fed by a
`loadgen` that runs independently (see [`../canary-load`](../canary-load)).

# Project layout

`mz-deploy` forbids *storage* objects (sources, source-tables, sinks, tables,
secrets, connections) from sharing a schema with *computation* objects (views,
materialized views). The materialized views also have consumer-facing names that
`test/canary-load` and `test/parallel-benchmark` depend on, so they keep their
original schema and the storage objects feeding them move to a parallel
`_sources` schema:

| Schema | Contents |
|--------|----------|
| `public` | secrets + connections (Kafka, CSR, AWS, S3-Tables/GCS Iceberg catalogs, GCP, Postgres, MySQL) |
| `public_tpch` | MVs `tpch_q01`, `tpch_q18` |
| `public_tpch_sources` | TPC-H source + source-tables + the `tpch_q18` sinks |
| `public_pg_cdc` | MV `pg_wmr` |
| `public_pg_cdc_sources` | Postgres source + source-tables + the `pg_relationships` sinks |
| `public_mysql_cdc` | MV `mysql_wmr` |
| `public_mysql_cdc_sources` | MySQL source + source-tables + the `mysql_wmr` sinks |
| `public_loadgen` | MV `sales_product_product_category` |
| `public_loadgen_sources` | Kafka sources + source-tables + product/category seed tables + the loadgen Iceberg sinks |

Each materialized view carries a default index on
`qa_canary_environment_compute` and `GRANT ALL PRIVILEGES` to the canary roles.
The dbt source-level indexes were dropped: an apply-managed index on the
blue/green compute cluster would be destroyed by the cluster swap at promote.

The `CREATE TABLE ... FROM SOURCE` source-tables carry no `GRANT`: `apply`
groups a source's tables into one transaction, and Materialize forbids mixing a
`GRANT` with object creation in a single transaction
(`transactions which modify objects are restricted to just modifying objects`).
No consumer reads the source-tables (`test/canary-load` reads the MVs, and CI
connects as the superuser admin), so the grant is simply omitted rather than
applied out of band. Sources, plain tables, and MVs are not grouped, so their
grants apply fine.

Object grants alone aren't enough: to read an object, the `test/canary-load`
role (`infra+qacanaryload@materialize.io`) also needs `USAGE` on the object's
schema, on the database, and on the cluster its query runs on. dbt only ever
granted the object level — the rest was a one-time manual grant that survived
only because dbt never dropped the schemas. `reset` recreates the schemas (and
the split clusters are new), so the `USAGE` grants are reapplied on every
`create`:

* **Database + the four MV schemas** (`public_tpch`, `public_pg_cdc`,
  `public_mysql_cdc`, `public_loadgen`) are granted by mz-deploy *modifier*
  files: `models/qa_canary_environment.sql` (database modifier) and
  `models/qa_canary_environment/<schema>.sql` (schema modifiers, sitting
  alongside each schema's object directory). A schema-modifier `GRANT` must
  target its own schema or `compile` rejects it.
* **The cluster** (`qa_canary_environment_compute`, not an mz-deploy object) and
  the **two testdrive-managed schemas** (`public_table`, `public_webhook`) are
  granted in the testdrive block of `workflow_create`, since mz-deploy has no
  cluster-grant modifier and doesn't manage those schemas.

**Iceberg sinks: AWS/S3-Tables enabled, GCS still disabled.** The five
S3-Tables model sinks and the `public_table.table_mv_iceberg_sink` testdrive
sink are active. The five `*_gcs_iceberg_sink` model sinks remain
`*.sql.disabled` (mz-deploy only reads `*.sql`, so it skips them) and the
`table_mv_gcs_iceberg_sink` testdrive sink is left out of `mzcompose.py`. To
re-enable a GCS sink, rename its `.sql.disabled` file back to `.sql` (and add
the testdrive sink back to `mzcompose.py`).

## What `mz-deploy` does not manage

Two object groups are created by `mzcompose.py` (testdrive) instead, because
`mz-deploy` cannot represent them:

- **`public_webhook.webhook_source`** — `CREATE SOURCE ... FROM WEBHOOK` is a
  distinct statement type that `mz-deploy` does not accept.
- **the `public_table` domain** (`public_table.table`, `public_table.table_mv`,
  and its three sinks) — `table` is a reserved word, so it cannot be matched to
  an object file path, and the `table_mv` MV reads it.

The upstream RDS Postgres / MySQL setup (replicated tables, publication,
`pg_cron` jobs, MySQL events) and the BigLake namespace bootstrap also live in
`mzcompose.py` — they are outside Materialize.

# Setup

## Environment variables

Authentication and host information is passed in via environment variables.
`mzcompose.py` writes them into the git-ignored `profiles.toml` (credentials)
and renders the git-ignored `project.toml` from the committed
`project.toml.example` template, filling `[prod.variables]` with the connection
endpoints (Confluent broker, RDS hosts, GCS bucket); it forwards the secret
values into the `mz-deploy` container, where `CREATE SECRET ... env_var(...)`
reads them. **Neither the credentials nor the connection endpoints are written
to a checked-in file** — `project.toml` is git-ignored, and only the
placeholder `project.toml.example` is committed. (mz-deploy has no env/CLI
override for profile variables, so the endpoints have to be rendered into
`project.toml`; keeping it out of git is what stops them leaking.)

```
export MATERIALIZE_PROD_SANDBOX_HOSTNAME=...us-east-1.aws.materialize.cloud
export MATERIALIZE_PROD_SANDBOX_USERNAME=...@materialize.com
export MATERIALIZE_PROD_SANDBOX_APP_PASSWORD=mzp_...

export MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME=...eu-west-1.rds.amazonaws.com
export MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD=...
export MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME=...eu-west-1.rds.amazonaws.com
export MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD=...

export CONFLUENT_CLOUD_QA_CANARY_KAFKA_BROKER=...confluent.cloud:9092
export CONFLUENT_CLOUD_QA_CANARY_CSR_URL=https://...aws.confluent.cloud
export CONFLUENT_CLOUD_QA_CANARY_CSR_USERNAME=...
export CONFLUENT_CLOUD_QA_CANARY_CSR_PASSWORD=...
export CONFLUENT_CLOUD_QA_CANARY_KAFKA_USERNAME=...
export CONFLUENT_CLOUD_QA_CANARY_KAFKA_PASSWORD=...

# Used by the GCS Iceberg sinks, defined in the i2 repository (i2/buildkite.py):
# the base64-encoded GCP service account key JSON and the canary-dedicated GCS
# bucket (no age-based object expiry).
export QA_CANARY_ICEBERG_GCP_SA_JSON_B64=...
export QA_CANARY_ICEBERG_GCS_BUCKET=...
```

## Clusters

Four clusters must exist in the sandbox environment. Storage objects are split
across three so the cold-hydration memory spike (which is far larger than
steady state) doesn't land on one replica:

| Cluster | Hosts | Notes |
|---------|-------|-------|
| `qa_canary_environment_storage` | tpch + pg_cdc + mysql_cdc sources and their source-tables | |
| `qa_canary_environment_upsert` | the Kafka loadgen `customer`/`sales` sources (and the `customer_tbl`/`sales_tbl` source-tables, which ingest on their source's cluster) | **disk-backed** — the UPSERT key→value state spills to RocksDB on local disk, which is what keeps the cold backlog re-read from OOMing |
| `qa_canary_environment_sinks` | all sinks | isolates sink backfill from source snapshots |
| `qa_canary_environment_compute` | materialized views + indexes | |

They are pre-created manually (with environment-appropriate sizes) and not
declared in the project, so `mz-deploy` never resizes them; `stage` clones the
compute cluster for the blue/green swap. Source-tables have no `IN CLUSTER` of
their own — they ingest on their source's cluster — so a source's placement
determines its tables' placement. The use of the `quickstart` cluster is
strongly discouraged.

# Creating the objects

To create or re-create the database objects:

1. Make sure the required environment variables are set.
2. Run:

```
./mzcompose run create
```

This runs, in order: the upstream RDS/MySQL setup + BigLake namespace bootstrap,
`mz-deploy setup` (creates the `_mz_deploy` metadata database, cluster, and
roles — the sandbox user is a superuser, which `setup` requires under RBAC),
`mz-deploy apply` (reconciles secrets/connections/sources/source-tables/tables
and refreshes `types.lock`), the testdrive-managed objects, and finally
`mz-deploy stage` / `wait` / `promote` to deploy the materialized views
blue/green.

The `wait` step is **best-effort**: a from-scratch hydration of this
environment (TPC-H scale factor 1, plus re-reading the full Kafka loadgen topic
backlog through the UPSERT source-tables) can take far longer than any practical
timeout — like the old dbt canary, which never waited (`dbt run` created the
objects and let them hydrate in production), `create` watches hydration for a
bounded window, then promotes regardless. The MVs finish hydrating in production
afterwards, so don't expect the `test` workflow to pass until hydration
completes. Tune the window with `--wait-timeout <seconds>` (default 1800; `0`
skips waiting). Watch progress with `mz-deploy list`, or query
`mz_internal.mz_source_statistics` (snapshot progress) and
`mz_internal.mz_compute_hydration_statuses` (per-MV).

## Switching an existing (dbt-built) environment over

`create` does not drop anything, and the `mz-deploy` schema layout differs from
the old dbt layout (storage objects moved to the `_sources` schemas), so running
`create` against an environment that still holds the dbt-built objects would
create a *parallel* set — including a second Postgres/MySQL source, i.e. a second
replication slot — rather than adopt them. `mz-deploy` also has no command to
take ownership of pre-existing objects.

To switch over, drop the per-domain objects first and let `create` rebuild them:

```
./mzcompose run reset
./mzcompose run create
```

`reset` is **destructive**: it drops every per-domain schema of
`qa_canary_environment` (the sources, source-tables, MVs, sinks, and the
testdrive-managed `public_table` / `public_webhook` objects), the `_mz_deploy`
deployment metadata, and any orphaned `*_<deploy-id>` staging clusters from a
failed deploy. It deliberately **keeps** the `public` schema (connections and
secrets) and the four base clusters.

Keeping `public` is load-bearing for the AWS connection. Materialize derives an
AWS connection's external ID from the connection's id
(`mz_<prefix>_<connection_id>`), so dropping and recreating
`qa_canary_aws_connection` rotates the external ID and breaks the
`qa-canary-environment-iceberg-role` IAM trust policy, which is pinned to a
single external ID. By keeping `public`, `apply` *adopts* the existing
connections (UpToDate/ALTER, never DROP+CREATE), so the external ID — and the
S3-Tables Iceberg catalog validation — keep working.

If the AWS connection has already been recreated (its external ID rotated), the
IAM role trust must be updated once to match the current value:

```sql
SELECT id, external_id, example_trust_policy FROM mz_internal.mz_aws_connections;
```

Apply the printed `example_trust_policy` to `qa-canary-environment-iceberg-role`,
then re-run `create`. After that the external ID is stable (`apply` never drops,
and `reset` keeps `public`).

To validate the port without disturbing the live canary, point the profile at a
scratch sandbox instead — but note a brand-new environment needs the Iceberg IAM
role trust configured for that connection's external ID before the S3-Tables
catalog will validate.

# Validating the workloads

The `test` workflow replaces the former `dbt test` data tests. It checks that:
- the RDS Postgres replication-slot count stays low (so Postgres does not run
  out of disk from stalled Materialize connections);
- every canary source is running (`mz_source_statuses`);
- every cluster replica is online (`mz_cluster_replica_statuses`);
- compute frontiers and their imports are within a minute of `mz_now()`;
- every monitored source-table and materialized view emits new rows on
  `SUBSCRIBE` (i.e. it is still making progress).

To run the validation:

1. Make sure the required environment variables are set correctly.
2. Run:

```
./mzcompose run test
```

# Developing the project

`mz-deploy compile` type-checks the whole project. It needs `project.toml` (which
is git-ignored — `mzcompose.py` generates it), so for an offline compile on a
fresh checkout, seed it from the committed template first:

```
cp test/canary-environment/project.toml.example test/canary-environment/project.toml
```

It also reads `types.lock` for the columns of the `CREATE TABLE ... FROM SOURCE`
tables, whose schemas come from the live upstreams (RDS / Kafka Avro).

`types.lock` is a **lock file and is committed** (like `Cargo.lock`): it lets
`compile`/CI type-check deterministically with no live connection. When an
upstream schema changes, refresh it with `mz-deploy lock` (or `mz-deploy apply`,
which refreshes it as a side effect) and commit the result — the diff shows
which columns changed, the same way `cargo update` does for dependencies.

```
mz-deploy -d test/canary-environment --profile prod compile
mz-deploy -d test/canary-environment --profile prod lock     # refresh after an upstream schema change
```
