---
title: "Guide: Upgrade the major version of your PostgreSQL source"
description: "How to upgrade the major version of the PostgreSQL database behind a Materialize source with zero downtime."
menu:
  main:
    parent: "postgresql"
    name: "Upgrade the major version"
    weight: 90
---

This guide shows you how to upgrade the **major version** (for example,
PostgreSQL 15 to 16) of the upstream database behind a Materialize
[PostgreSQL source](/sql/create-source/postgres/) while keeping Materialize
serving fresh results.

The challenge specific to Materialize is that the source holds an active
[logical replication slot](https://www.postgresql.org/docs/current/logical-replication.html)
on the upstream primary. A major-version upgrade replaces the primary with a
new instance running the new version, and the replication slot does **not**
carry over automatically. How you handle the slot determines whether reads stay
fresh through the upgrade.

The approach in this guide keeps Materialize continuously fresh. You keep the
existing source running against the old primary the entire time, build the
new-version primary in parallel, and hydrate a second source against it before
cutting consumers over.

{{< note >}}
This procedure involves a brief **write** freeze on the application at the
moment of cutover, which is inherent to any major-version upgrade. Materialize
keeps serving fresh **reads** throughout.
{{< /note >}}

{{< warning >}}
Managed upgrade services that swap the primary out from under Materialize — such
as [Amazon RDS Blue/Green
deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments.html)
— cannot be used while a Materialize source is attached. See [Why managed
Blue/Green deployments don't work](#why-managed-bluegreen-deployments-dont-work).
{{< /warning >}}

## Upgrade with a parallel source

```text
app writes ──▶ PG (old major) ──native logical replication──▶ PG (new major)
                    │                                              │
                    ▼                                              ▼
              existing source                                new source
              (serving fresh)                          (snapshotting + catching up)
```

### 1. Stand up the new-version primary

Provision a new PostgreSQL instance running the target major version, with
logical replication enabled (for RDS, `rds.logical_replication = 1`; see
[enable logical replication](/ingest-data/postgres/amazon-rds/#1-enable-logical-replication)).

Because logical replication does **not** replicate DDL, pre-create the schema of
the replicated tables on the new instance to match the old one, including
`REPLICA IDENTITY FULL`:

```sql
-- On the new-version primary, recreate the table definitions.
ALTER TABLE my_table REPLICA IDENTITY FULL;
```

### 2. Replicate old ▶ new with native logical replication

On the **old** primary, create (or reuse) a publication for the tables you
replicate to Materialize:

```sql
-- On the old primary.
CREATE PUBLICATION upgrade_pub FOR TABLE my_table;
```

On the **new** primary, subscribe to it:

```sql
-- On the new primary.
CREATE SUBSCRIPTION upgrade_sub
    CONNECTION 'host=OLD_PRIMARY_HOST port=5432 dbname=DB user=REPL_USER password=...'
    PUBLICATION upgrade_pub;
```

A single publication can feed **multiple** subscribers, so the old primary now
has two logical consumers at once — Materialize's slot and this subscription's
slot — coexisting without conflict. Confirm both are active:

```sql
-- On the old primary.
SELECT slot_name, plugin, active FROM pg_replication_slots;
```

{{< warning >}}
Ensure the new primary can reach the old primary on the PostgreSQL port. In a
cloud VPC, the upstream endpoint typically resolves to a **private** IP, so the
security group must allow instance-to-instance traffic — not just the client's
IP.
{{< /warning >}}

### 3. Create a parallel source in Materialize

Leave your existing source untouched and serving. Create a **second**
connection and source pointed at the new primary, in its own schema so the
subsources and downstream objects don't collide:

```mzsql
CREATE SCHEMA upgrade;

CREATE SOURCE upgrade.my_source
    FROM POSTGRES CONNECTION pg_new (PUBLICATION 'upgrade_pub')
    FOR ALL TABLES;
```

The new source begins [snapshotting](/ingest-data/#snapshotting) and then
catches up. Snapshot time scales with data volume and is the long pole of the
upgrade — but it happens **in the background** while the existing source keeps
serving fresh. Recreate your downstream views and materialized views in the
`upgrade` schema on top of the new source.

### 4. Cut over

Once the new source has caught up, perform a coordinated cutover:

1. **Freeze application writes** to the old primary.
1. **Drain replication:** wait until the new primary matches the old primary
   exactly. Comparing an order-independent fingerprint across both databases is
   a reliable check:

    ```sql
    SELECT count(*), sum(id), min(id), max(id), count(DISTINCT id) FROM my_table;
    ```

1. **Synchronize sequences.** Native logical replication does **not** advance
   sequences on the target. Copy each sequence's value forward, or post-cutover
   inserts will collide on the primary key:

    ```sql
    -- Read on the old primary...
    SELECT last_value FROM my_table_id_seq;
    -- ...then set on the new primary.
    SELECT setval('my_table_id_seq', <last_value>);
    ```

1. **Drain both sources** in Materialize so they reflect the frozen state, then
   verify the fingerprint matches across the old primary, new primary, existing
   source, and new source.
1. **Swap the schemas.** Rather than repointing each consumer, swap the
   production schema with the `upgrade` schema. The swap is atomic, so
   consumers keep referencing the same schema-qualified names and move together
   at a single instant:

    ```mzsql
    ALTER SCHEMA public SWAP WITH upgrade;
    ```

    To roll back, run the same statement again.

1. **Resume application writes**, now pointed at the new primary.

Throughout the cutover, reads against Materialize stay live and fresh — the
existing source serves until the swap, and the new source is already caught up
at the swap.

{{< warning >}}
If you have [sinks](/sql/create-sink/), create them in a **dedicated schema and
cluster** that is excluded from the swap. Sinks must not be recreated as part of
a blue/green cutover; instead, cut them over to the new definition of their
upstream dependencies after the swap. This mirrors the guidance in [blue/green
deployments with dbt](/manage/dbt/blue-green-deployments/).
{{< /warning >}}

{{< note >}}
Any active [`SUBSCRIBE`](/sql/subscribe/) commands attached to the swapped
cluster(s) will break at the swap. On retry, the client automatically connects
to the newly deployed cluster.
{{< /note >}}

### 5. Decommission

Drop the subscription on the new primary, drop the old source in Materialize,
and decommission the old primary.

## Why managed Blue/Green deployments don't work

[Amazon RDS Blue/Green deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments.html)
let AWS build the upgraded "green" instance, keep it in sync, and swap endpoints
at switchover. This is an appealing shortcut, but it is **not compatible with an
attached Materialize source**.

Creating the deployment fails at `CREATING_READ_REPLICA_OF_SOURCE` with *"external
replication on the blue primary instance"*. One of the documented prerequisites
is that the DB instance is not the source or target of external replication, and
Materialize's logical replication slot is external replication. No RDS setting
bypasses this.

Releasing the slot — by [dropping the source](/sql/drop-source/) — does let the
deployment be created, but that is not a viable production step:

- `DROP SOURCE` defaults to `RESTRICT` and fails when the source has dependents.
  Forcing it with `CASCADE` drops the entire downstream dataflow — tables, views,
  materialized views, indexes, and sinks — which means rebuilding and rehydrating
  your environment, not a brief pause.
- Even after the switchover, Materialize's original slot on blue is unusable, so
  the new source must snapshot from scratch against green.

The result is a staleness window that begins when you detach from blue and lasts
through provisioning and a full re-snapshot. Use the [parallel
source](#upgrade-with-a-parallel-source) procedure above instead: it keeps the
existing source serving fresh for the entire upgrade, and the only coordinated
pause is the write freeze at cutover.

## Considerations

- **DDL is not replicated.** Pre-create the target schema on the new instance
  before subscribing.
- **Sequences are not advanced on the target** by native logical replication.
  Synchronize them at cutover.
- **`REPLICA IDENTITY FULL`** must be set on replicated tables so Materialize
  captures all column values on updates and deletes.
- **A publication can feed multiple subscribers**, so Materialize's slot and the
  upgrade subscription's slot coexist on the old primary without conflict.

## Related pages

- [Ingest data from Amazon RDS](/ingest-data/postgres/amazon-rds/)
- [Guide: Ingest from a dedicated PostgreSQL replica](/ingest-data/postgres/logical-replica/)
- [Guide: Handle upstream schema changes with zero downtime](/ingest-data/postgres/source-versioning/)
- [Blue/green deployments with dbt](/manage/dbt/blue-green-deployments/)
- [`ALTER SCHEMA`](/sql/alter-schema/)
