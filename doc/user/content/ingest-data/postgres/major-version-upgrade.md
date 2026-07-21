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

This guide covers two approaches:

| Approach | Reads stay fresh? | Who moves the data? | Use when |
|----------|-------------------|---------------------|----------|
| [Parallel source (self-managed)](#approach-a-parallel-source-self-managed) | **Yes, continuously** | You (native logical replication) | Zero read downtime is a hard requirement. |
| [Amazon RDS Blue/Green](#approach-b-amazon-rds-bluegreen-managed) | No — bounded freshness gap | AWS | A brief staleness window is acceptable and you want AWS to manage the cutover. |

{{< note >}}
Both approaches involve a brief **write** freeze on the application at the
moment of cutover, which is inherent to any major-version upgrade. The
difference is whether Materialize keeps serving fresh **reads** throughout.
{{< /note >}}

## Approach A: Parallel source (self-managed)

This is the only approach that keeps Materialize continuously fresh. You keep
the existing source running against the old primary the entire time, build the
new-version primary in parallel, and hydrate a second source against it before
cutting consumers over.

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
1. **Repoint consumers** from the existing source's objects to the `upgrade`
   schema's objects.
1. **Resume application writes**, now pointed at the new primary.

Throughout the cutover, reads against Materialize stay live and fresh — the
existing source serves until the flip, and the new source is already caught up
at the flip.

### 5. Decommission

Drop the subscription on the new primary, drop the old source in Materialize,
and decommission the old primary.

## Approach B: Amazon RDS Blue/Green (managed)

[Amazon RDS Blue/Green deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments.html)
let AWS build the upgraded "green" instance, keep it in sync, and swap
endpoints at switchover. This is operationally simpler, but it **cannot keep
Materialize continuously fresh** because of the constraint below.

{{< warning >}}
**A Blue/Green deployment cannot be created while a Materialize source is
attached to the blue instance.** Materialize's logical replication slot counts
as *external replication*, and RDS refuses to create the deployment
(`CREATING_READ_REPLICA_OF_SOURCE` fails with *"external replication on the blue
primary instance"*). You must [drop the
source](/sql/drop-source/) first, which starts an unavoidable **freshness
gap**. No RDS flag bypasses this.
{{< /warning >}}

### 1. Detach Materialize and create the deployment

1. `DROP SOURCE` in Materialize to release the replication slot. Confirm
   `pg_replication_slots` no longer lists Materialize's slot. **The freshness
   gap starts here.**
1. Create the Blue/Green deployment, targeting the new major version with a
   parameter group that has `rds.logical_replication = 1`. Wait for the green
   instance to become **Available** (a major-version upgrade can take tens of
   minutes; it scales with database size).

The publication and `REPLICA IDENTITY` settings are copied from blue into green.

### 2. Attach Materialize to green

The green instance is a logical replica that is itself a primary
(`pg_is_in_recovery()` returns `false`), so it can host Materialize's slot.
Create a source against **green's temporary endpoint** and let it snapshot and
catch up:

```mzsql
CREATE SOURCE my_source
    FROM POSTGRES CONNECTION pg_green (PUBLICATION 'mz_source')
    FOR ALL TABLES;
```

**The freshness gap ends when this source has caught up.** As in Approach A,
snapshot time scales with data volume.

### 3. Switch over and repoint the source

Trigger the Blue/Green switchover. AWS promotes green **in place** — it takes
over the production endpoint, while the old blue is demoted and renamed. The
switchover completes in tens of seconds, during which the application has a
brief write pause.

Because green is promoted in place (same instance, same LSN timeline), its
replication slot is preserved. Repoint the source's connection at the
production endpoint so Materialize resumes from the **same slot with no
re-snapshot**:

```mzsql
ALTER CONNECTION pg_green SET (HOST = 'PRODUCTION_ENDPOINT') WITH (VALIDATE = true);
```

After the switchover, green's temporary endpoint is removed; a source restart
would otherwise fail to resolve it, so run the `ALTER CONNECTION` promptly.

{{< note >}}
This seamless resume works **only** because a Blue/Green switchover promotes
green in place, preserving the slot and LSN timeline. Repointing a source at an
unrelated instance (one that doesn't carry the same slot) forces a fresh
snapshot instead.
{{< /note >}}

### 4. Clean up

Delete the Blue/Green deployment and the demoted old instance.

## Considerations

- **DDL is not replicated.** For Approach A, pre-create the target schema on the
  new instance before subscribing.
- **Sequences are not advanced on the target** by native logical replication.
  Synchronize them at cutover (Approach A) or RDS handles it at switchover
  (Approach B).
- **`REPLICA IDENTITY FULL`** must be set on replicated tables so Materialize
  captures all column values on updates and deletes.
- **`ALTER CONNECTION ... SET (HOST = ...)`** resumes from the existing slot
  only when the target instance preserves that slot (an in-place promotion).
  Otherwise Materialize re-snapshots.

## Related pages

- [Ingest data from Amazon RDS](/ingest-data/postgres/amazon-rds/)
- [Guide: Ingest from a dedicated PostgreSQL replica](/ingest-data/postgres/logical-replica/)
- [Guide: Handle upstream schema changes with zero downtime](/ingest-data/postgres/source-versioning/)
- [`ALTER CONNECTION`](/sql/alter-connection/)
