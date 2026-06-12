---
title: "Guide: Ingest from a dedicated PostgreSQL replica"
description: "How to point a Materialize PostgreSQL source at a dedicated logical replica instead of your production primary."
aliases:
  - /ingest-data/postgres/sidecar/
menu:
  main:
    parent: "postgresql"
    name: "Ingest from a dedicated replica"
    weight: 55
---

{{< note >}}
We **nearly always recommend** connecting Materialize directly to your primary
(see [self-hosted PostgreSQL](/ingest-data/postgres/self-hosted/) or the guide
for your [hosted service](/ingest-data/postgres/)). The dedicated-replica setup
on this page is an exception path: reach for it only when you have a **serious,
specific concern** that direct replication can't address. It adds an extra
system to operate and an extra hop of replication lag.
{{< /note >}}

This guide shows you how to stand up a dedicated PostgreSQL **replica** using
native [PostgreSQL logical
replication](https://www.postgresql.org/docs/current/logical-replication.html),
and then point a Materialize [PostgreSQL source](/sql/create-source/postgres/)
at that replica instead of your production primary. This is sometimes referred
to as the **sidecar pattern**.

## When to use this

Materialize connects to PostgreSQL using the replication protocol, which holds a
[replication slot](/ingest-data/postgres/replication-slot-active/) open on the
upstream database. For the vast majority of deployments, connecting directly to
the primary is the right choice. Consider a dedicated replica only when you have
a concrete concern such as:

- **You need to isolate WAL-retention risk from the primary.** A replication
  slot pins WAL on whichever database it lives on, so a paused or lagging
  Materialize source can retain WAL and risk filling that database's disk.
  Pointing Materialize at a replica keeps this risk off your production primary.

- **You can't reconfigure the primary.** Enabling `wal_level = logical` requires
  a restart, and you may not be able to restart or modify the primary. You can
  instead enable it on a replica you control.

- **You can't set `REPLICA IDENTITY FULL` on the primary's tables.** Materialize
  requires `REPLICA IDENTITY FULL` to capture all column values in change
  events. If you can't alter the primary's tables, you can set it on the
  replica's copies instead. See also the [PostgreSQL source
  FAQ](/ingest-data/postgres/faq/).

If none of these apply, prefer connecting directly to the primary — the replica
adds an extra system to operate and its own replication lag on top of the
primary.

## How it works

There are **two** logical-replication hops to set up:

```
  primary  ──(native PG logical replication)──▶  replica  ──(Materialize source)──▶  Materialize
```

1. **primary → replica:** native PostgreSQL logical replication keeps the
   replica's tables in sync with the primary. The primary is the *publisher*;
   the replica is the *subscriber*.
1. **replica → Materialize:** the replica acts as a *publisher* for Materialize.
   This means the **replica** also needs `wal_level = logical`, its own
   publication, and `REPLICA IDENTITY FULL` on the tables you replicate.

## Prerequisites

- A PostgreSQL primary on **PostgreSQL 11+**, with the tables you want to
  replicate.
- A separate PostgreSQL instance (**11+**) to act as the replica, with network
  connectivity from the replica to the primary.
- A superuser (or a role with `REPLICATION` and the relevant table privileges)
  on each instance.

## A. Set up native logical replication (primary → replica)

### 1. Enable logical replication on both instances

On **both** the primary and the replica, set `wal_level = logical` in
`postgresql.conf`. The replica needs it too because it will, in turn, publish to
Materialize.

```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

This requires a **restart** (not just a reload). On managed services the
parameter name differs — for example, Amazon RDS uses
`rds.logical_replication = 1` in the parameter group.

After restarting, verify on each instance:

```postgres
SHOW wal_level;   -- should return: logical
```

### 2. Create the tables on the replica

Logical replication does **not** copy DDL, so create the table structure
yourself on the replica. Columns are matched by name; the replica table may have
extra columns or a different column order, but replicated columns must have
compatible types.

On the **replica**, recreate each table you want to replicate. For example:

```postgres
CREATE TABLE orders (
    id         bigint PRIMARY KEY,
    customer   text,
    amount     numeric,
    created_at timestamptz
);
```

### 3. Set `REPLICA IDENTITY FULL` on the replica's tables

So that Materialize can capture all column values, set `REPLICA IDENTITY FULL`
on each replicated table **on the replica**:

```postgres
ALTER TABLE orders REPLICA IDENTITY FULL;
```

{{< note >}}
A table with a primary key already has a usable replica identity for
`UPDATE`/`DELETE`. However, Materialize requires `REPLICA IDENTITY FULL` to
ingest unchanged
[TOASTed](https://www.postgresql.org/docs/current/storage-toast.html) values.
{{< /note >}}

### 4. Create a publication and replication user on the primary

On the **primary**, create a publication for the tables to replicate:

```postgres
CREATE PUBLICATION repl_to_replica FOR TABLE orders;
```

To add more tables later: `ALTER PUBLICATION repl_to_replica ADD TABLE
other_table;`

Create a role for the replica to connect as. It needs `REPLICATION` and `SELECT`
on the tables:

```postgres
CREATE ROLE repuser WITH REPLICATION LOGIN PASSWORD '<strong_password>';
GRANT SELECT ON orders TO repuser;
```

Allow the connection in the primary's `pg_hba.conf` (adjust the replica's
IP/CIDR), then reload:

```
host    all    repuser    <replica_ip>/32    scram-sha-256
```

```postgres
SELECT pg_reload_conf();
```

### 5. Create the subscription on the replica

On the **replica**, create a subscription to the primary's publication:

```postgres
CREATE SUBSCRIPTION orders_sub
    CONNECTION 'host=<primary_host> port=5432 dbname=<db> user=repuser password=<strong_password>'
    PUBLICATION repl_to_replica;
```

By default this immediately creates a replication slot on the primary, copies
the existing table data, and begins streaming ongoing changes.

### 6. Verify the primary → replica hop

On the **replica**:

```postgres
SELECT subname, subenabled FROM pg_subscription;
SELECT * FROM pg_stat_subscription;   -- last_msg_receipt_time should advance
```

On the **primary**:

```postgres
SELECT slot_name, active, restart_lsn FROM pg_replication_slots;
SELECT * FROM pg_stat_replication;
```

Then test end to end: insert a row on the primary and confirm it appears on the
replica.

```postgres
-- primary
INSERT INTO orders VALUES (1, 'acme', 99.50, now());
-- replica (a moment later)
SELECT * FROM orders WHERE id = 1;
```

## B. Create a publication for Materialize (on the replica)

Materialize ingests from a publication on the **replica**. Create a separate
publication for it (keep it distinct from the `primary → replica` publication):

```postgres
CREATE PUBLICATION mz_source FOR TABLE orders;
```

## C. Connect Materialize to the replica

From here, treat the replica as you would any self-hosted PostgreSQL source.
Follow the [self-hosted PostgreSQL guide](/ingest-data/postgres/self-hosted/) to
configure network security, create a connection, and create the source —
pointing the connection at your **replica** and using the `mz_source`
publication you just created:

```mzsql
CREATE SOURCE mz_source
    FROM POSTGRES CONNECTION pg_replica (PUBLICATION 'mz_source')
    FOR ALL TABLES;
```

## Things to watch out for

- **Don't leave orphaned replication slots.** Both hops use replication slots
  that pin WAL while inactive. Drop subscriptions cleanly (`DROP SUBSCRIPTION`,
  which also drops the remote slot) rather than just disabling them, and
  [drop the Materialize source](/sql/drop-source/) when you no longer need it.
  See [Replication slot is
  active](/ingest-data/postgres/replication-slot-active/).
- **DDL is not replicated.** When you add a column, apply it on the replica
  first, then the primary, to avoid replication errors. For handling schema
  changes in Materialize, see [Handle upstream schema
  changes](/ingest-data/postgres/source-versioning/).
- **Sequences are not replicated** by native logical replication — only table
  data.
- **The replica adds lag.** Changes reach Materialize only after they've been
  applied on the replica, so end-to-end latency is the primary → replica lag
  plus the replica → Materialize lag.
