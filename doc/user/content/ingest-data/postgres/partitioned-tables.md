---
title: "Guide: Ingest from partitioned tables"
description: "How to ingest data from a declaratively partitioned PostgreSQL table into Materialize."
menu:
  main:
    parent: "postgresql"
    name: "Ingest from partitioned tables"
    weight: 60
---

This guide shows you how to ingest data from a [declaratively partitioned
PostgreSQL table](https://www.postgresql.org/docs/current/ddl-partitioning.html)
into Materialize.

## How PostgreSQL publishes a partitioned table

When you add a partitioned table to a publication, PostgreSQL expands it to the
table's **leaf partitions**. The parent table is not itself replicated:

```sql
-- On PostgreSQL, where orders is partitioned by range
CREATE PUBLICATION mz_source FOR TABLE orders;

SELECT tablename FROM pg_publication_tables WHERE pubname = 'mz_source';
--   tablename
-- ----------------
--  orders_2026_01
--  orders_2026_02
```

Materialize therefore ingests one table per partition, and you reassemble the
parent table with a `UNION ALL` over them.

{{< warning >}}

Do **not** create a publication with [`publish_via_partition_root =
true`](https://www.postgresql.org/docs/current/sql-createpublication.html) for a
Materialize source. Materialize does not support ingesting from a publication
that uses this option, and doing so can produce incorrect results.

The configuration is accepted rather than rejected: the source is created, the
initial snapshot is correct, and inserts, updates, and deletes appear to
replicate normally. Problems surface only once partitions are added, attached,
or detached. Use one of the approaches on this page instead.

{{< /warning >}}

## Approach 1: Ingest the leaf partitions

Ingest each partition as its own table, then union them into a **materialized
view**. Use a materialized view rather than a view so that you can add and remove
partitions later by [replacing the materialized
view](/transform-data/updating-materialized-views/replace-materialized-view/),
without recreating the objects that depend on it.

{{% include-headless "/headless/replacement-views/public-preview-annotation" %}}

### 1. Set `REPLICA IDENTITY FULL` on each partition

Materialize requires `REPLICA IDENTITY FULL` on every table it ingests. Setting
it on the parent table does **not** cascade to the partitions, so set it on each
leaf partition:

```sql
-- On PostgreSQL
ALTER TABLE orders_2026_01 REPLICA IDENTITY FULL;
ALTER TABLE orders_2026_02 REPLICA IDENTITY FULL;
```

### 2. Create the source and one table per partition

Using an existing [PostgreSQL connection](/sql/create-connection/#postgresql),
create the source, then create one table per partition:

```mzsql
CREATE SOURCE pg_src
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source');

CREATE TABLE orders_2026_01 FROM SOURCE pg_src (REFERENCE orders_2026_01);
CREATE TABLE orders_2026_02 FROM SOURCE pg_src (REFERENCE orders_2026_02);
```

### 3. Union the partitions into a materialized view

```mzsql
CREATE MATERIALIZED VIEW orders AS
  SELECT * FROM orders_2026_01
  UNION ALL
  SELECT * FROM orders_2026_02;
```

Build your downstream objects on `orders`, not on the individual partitions.

### Add a partition

New partitions are **not** ingested automatically. When a partition is created
upstream, add it to the publication, create a table for it, and then replace the
materialized view to include it.

1. On PostgreSQL, prepare the new partition and add it to the publication:

   ```sql
   ALTER TABLE orders_2026_03 REPLICA IDENTITY FULL;
   ALTER PUBLICATION mz_source ADD TABLE orders_2026_03;
   ```

1. In Materialize, create a table for the new partition:

   ```mzsql
   CREATE TABLE orders_2026_03 FROM SOURCE pg_src (REFERENCE orders_2026_03);
   ```

1. Create a replacement view that includes the new partition:

   ```mzsql
   CREATE REPLACEMENT MATERIALIZED VIEW orders_v2 FOR orders AS
     SELECT * FROM orders_2026_01
     UNION ALL
     SELECT * FROM orders_2026_02
     UNION ALL
     SELECT * FROM orders_2026_03;
   ```

1. Before applying the replacement view, verify that it is hydrated, to avoid
   downtime:

   ```mzsql
   SELECT mv.name, h.hydrated
   FROM mz_catalog.mz_materialized_views AS mv
   JOIN mz_internal.mz_hydration_statuses AS h ON (mv.id = h.object_id)
   WHERE mv.name = 'orders_v2';
   ```

1. Apply the replacement:

   ```mzsql
   ALTER MATERIALIZED VIEW orders APPLY REPLACEMENT orders_v2;
   ```

1. Verify that rows from the new partition are now served by `orders`:

   ```mzsql
   SELECT count(*) FROM orders WHERE order_date >= '2026-03-01';
   ```

Objects that depend on `orders` do not need to be recreated. They do have to
process the diff emitted by the replacement:

{{% include-from-yaml data="examples/alter_materialized_view"
name="cpu-memory-considerations" %}}

### Remove a partition

Dropping or detaching a partition upstream removes it from the publication. If
Materialize is still ingesting that partition when this happens, the
corresponding table becomes inaccessible, and the materialized view that unions
it stops answering queries until you rebuild it.

To avoid this, retire the partition in Materialize **first**, then upstream:

1. Create a replacement view that excludes the partition, and apply it:

   ```mzsql
   CREATE REPLACEMENT MATERIALIZED VIEW orders_v3 FOR orders AS
     SELECT * FROM orders_2026_02
     UNION ALL
     SELECT * FROM orders_2026_03;

   ALTER MATERIALIZED VIEW orders APPLY REPLACEMENT orders_v3;
   ```

1. Drop the table in Materialize, which now has no dependents:

   ```mzsql
   DROP TABLE orders_2026_01;
   ```

1. On PostgreSQL, remove the partition from the publication and retire it:

   ```sql
   ALTER PUBLICATION mz_source DROP TABLE orders_2026_01;
   ALTER TABLE orders DETACH PARTITION orders_2026_01;
   DROP TABLE orders_2026_01;
   ```

Because the partition was removed from the materialized view before it was
retired upstream, its rows are retracted rather than left behind. To confirm,
compare the row count against the parent table upstream:

```mzsql
SELECT count(*) FROM orders;
```

## Approach 2: Flatten the table on a dedicated replica

If you would rather not perform the steps above on every partition rollover, you
can replicate the partitioned table into a **dedicated PostgreSQL replica** where
it is an ordinary, non-partitioned table, and point Materialize at that replica.
New partitions then flow through without any changes in Materialize.

This is an extension of the [dedicated replica
guide](/ingest-data/postgres/logical-replica/), and carries the same trade-offs:
an extra system to operate, and an extra hop of replication lag.

### How the flattening works

PostgreSQL logical replication matches the publisher's relation to the
subscriber's relation **by schema-qualified name**. The
`publish_via_partition_root` option changes which name the publisher advertises:
with the option on, changes written to any partition are published as though
they came from the parent table.

So if `public.orders` is partitioned on the primary and the publication uses
`publish_via_partition_root = true`, the subscriber receives changes for
`public.orders` and applies them to *its* `public.orders` — which you create as
an ordinary, non-partitioned table. The subscriber is never told that
partitioning exists, and the partitions themselves are never named on the wire:

```sql
-- On the primary, with publish_via_partition_root = true
SELECT tablename FROM pg_publication_tables WHERE pubname = 'repl_to_replica';
--  tablename
-- -----------
--  orders       -- the parent table, not orders_2026_01, orders_2026_02, ...
```

This is also why new partitions need no action: a partition added upstream
publishes under the same parent name, so it lands in the same table on the
replica.

### Set up the primary → replica hop

Follow the [dedicated replica guide](/ingest-data/postgres/logical-replica/) for
the base setup on both instances (`wal_level = logical`, a replication user, and
network access), with the following differences:

1. On the **primary**, create the publication with `publish_via_partition_root =
   true`:

   ```sql
   CREATE PUBLICATION repl_to_replica FOR TABLE orders
       WITH (publish_via_partition_root = true);
   ```

   List the **parent** table, not the individual partitions.

   {{< note >}} This option is safe on the `primary → replica` hop, which is
   native PostgreSQL logical replication. Do not use it on the publication that
   Materialize reads from. {{< /note >}}

1. On the **replica**, create `orders` as a plain table with the same name and
   columns as the parent table upstream, but without `PARTITION BY`:

   ```sql
   CREATE TABLE orders (
       id          bigint NOT NULL,
       order_date  date   NOT NULL,
       -- ...
       PRIMARY KEY (id, order_date)
   );

   ALTER TABLE orders REPLICA IDENTITY FULL;
   ```

   Recreate the parent table's primary key, or another unique index, on the
   replica. The subscriber uses it to locate rows for `UPDATE` and `DELETE`;
   without a suitable index, each replicated change requires a sequential scan
   of the table.

1. On the **replica**, subscribe to the primary's publication. This creates a
   replication slot on the primary, copies the existing rows out of all the
   partitions, and then streams ongoing changes:

   ```sql
   CREATE SUBSCRIPTION orders_sub
       CONNECTION 'host=<primary_host> port=5432 dbname=<db> user=repuser password=<password>'
       PUBLICATION repl_to_replica;
   ```

1. Verify that the flattening worked. The replica should have a single, ordinary
   table holding the rows from every partition:

   ```sql
   -- On the replica
   SELECT relname, relkind FROM pg_class WHERE relname = 'orders';
   --  relname | relkind
   -- ---------+---------
   --  orders  | r          -- an ordinary table, not 'p' for partitioned

   SELECT count(*) FROM orders;   -- compare against the parent table upstream
   ```

### Connect Materialize to the replica

From here the replica is an ordinary, non-partitioned PostgreSQL database.
Follow [Connect Materialize to the
replica](/ingest-data/postgres/logical-replica/#c-connect-materialize-to-the-replica)
to create the publication that Materialize reads and the source itself. That
publication is a plain one:

```sql
-- On the replica
CREATE PUBLICATION mz_source FOR TABLE orders;
```

New partitions created upstream are replicated to the replica automatically, as
long as the partitioned table has a primary key. If it does not, set `REPLICA
IDENTITY FULL` on each new partition as it is created; otherwise PostgreSQL
rejects `UPDATE` and `DELETE` against that partition on the primary.

Partition maintenance is still not replicated. When you drop or detach a
partition upstream, mirror it on the replica with a matching `DELETE` so that
Materialize retracts the rows:

```sql
-- On the replica
DELETE FROM orders WHERE order_date >= '2026-01-01' AND order_date < '2026-02-01';
```

## Things to watch out for

- **`REPLICA IDENTITY FULL` does not cascade.** Setting it on the parent table
  leaves the partitions unchanged. Set it on each partition, including new ones.

- **Attaching a populated partition.** `ATTACH PARTITION` does not replicate the
  rows the table already contains, because they were never written to the WAL.
  Load rows through the parent table, or resynchronize afterwards.

- **Don't `TRUNCATE`.** Truncating an upstream table that Materialize reads makes
  the corresponding table in Materialize inaccessible until it is recreated.
  Note that `TRUNCATE` of the parent table propagates to a dedicated replica, so
  it affects both approaches. Use an unqualified `DELETE` instead, including when
  resynchronizing a replica:

  ```sql
  DELETE FROM orders;
  ```

- **Reconcile periodically.** Because partition maintenance is invisible to
  logical replication, a scheduled row count or checksum comparison between the
  upstream parent table and Materialize is the most reliable way to catch drift.
