---
title: "PostgreSQL"
description: "How to sync results from Materialize to PostgreSQL using SUBSCRIBE and mz-catalog-sync."
menu:
  main:
    parent: sink
    name: "PostgreSQL"
    weight: 44
---

This guide shows how to continuously sync a table, view, or materialized view
from Materialize into PostgreSQL.
[`mz-catalog-sync`](https://github.com/MaterializeInc/mz-catalog-sync) is a
reference implementation that reads a [`SUBSCRIBE`](/sql/subscribe/) stream
with `ENVELOPE UPSERT` and applies each change to a destination table in
PostgreSQL. Despite its name, the sync logic isn't specific to `mz_catalog`
objects: it only needs a relation name, a destination table, and a key
column, so the same program syncs any relation you own alongside the catalog
objects it ships with by default.

Use this pattern when you need your data queryable from PostgreSQL itself,
for example to hand a servicing layer to a team that only speaks PostgreSQL,
or to plug into tooling that expects a PostgreSQL source.

## Before you begin

- [Python](https://www.python.org/) 3.13 or later and
  [`uv`](https://docs.astral.sh/uv/) on the host that runs the sync.
- A PostgreSQL database to sync into.
- Materialize SQL credentials to run `SUBSCRIBE`.

## Step 1. Get the sync program

```sh
git clone https://github.com/MaterializeInc/mz-catalog-sync.git
cd mz-catalog-sync
uv sync
```

## Step 2. Create the destination schema

The sync tracks per-object progress in a `catalog_freshness` table, and
writes each synced relation to its own destination table. Create both in
PostgreSQL:

```sql
CREATE TABLE catalog_freshness (
    mz_environment_id TEXT,
    catalog_object    TEXT,
    freshness         BIGINT,
    PRIMARY KEY (mz_environment_id, catalog_object)
);
```

For each relation you want to sync, add a destination table. Give its
columns the same native types as the Materialize columns you're syncing
(don't default to `TEXT` for everything), plus `mz_environment_id` and
`deleted_at` bookkeeping columns:

```sql
CREATE TABLE mz_environment_items (
    mz_environment_id TEXT,
    id                INTEGER,
    name              TEXT,
    price             NUMERIC,
    deleted_at        BIGINT,
    PRIMARY KEY (mz_environment_id, id)
);
```

{{< warning >}}
A destination column type that doesn't match its Materialize source column
can crash the whole sync process, not just the one relation. Deletes bind
the key column's value with no target column to cast against, so PostgreSQL
falls back to the parameter's native type; a `TEXT` destination column
against an `INTEGER` source raises `operator does not exist: text =
integer` and takes down every relation the process was syncing, catalog
objects included.
{{< /warning >}}

`mz_environment_id` lets you sync the same relation from more than one
Materialize environment into a shared table without collisions. Deletes are
soft: a deleted row keeps its columns and gets `deleted_at` set to the
Materialize timestamp of the delete, rather than being removed. Filter
`WHERE deleted_at IS NULL` when you query the destination table directly.

## Step 3. Configure the relations to sync

Open `main.py` and add an entry to `CATALOG_OBJECTS` for each relation, next
to the catalog objects it ships with by default:

```python
CatalogObject(
    mz_name="public.items",
    pg_name="mz_environment_items",
    columns=["id", "name", "price"],
    keys=["id"],
),
```

- `mz_name` is the fully qualified Materialize relation to read from: a
  table, view, or materialized view.
- `pg_name` is the destination table from step 2.
- `columns` are the Materialize columns to sync, in the order the sync
  writes them.
- `keys` are the columns that make a row unique in `mz_name`; the sync uses
  them for the `ENVELOPE UPSERT` key and the `ON CONFLICT` clause on the
  PostgreSQL side.

## Step 4. Run the sync

```sh
export MZ_DSN="postgres://<user>@<mz-host>:6875/materialize?sslmode=require"
export PG_DSN="postgres://<user>:<password>@<pg-host>:5432/<database>?sslmode=require"

uv run python main.py
```

The process runs until stopped. It reads a snapshot of every configured
relation first, then applies inserts, updates, and deletes as they happen in
Materialize. On restart, it reconciles: the snapshot marks every existing row
for that environment as deleted before re-upserting what's actually there, so
a row removed from `mz_name` while the sync was down still ends up marked
`deleted_at`.

## Step 5. Validate

With the sync running, change data in the source relation and confirm it
lands in PostgreSQL:

```mzsql
INSERT INTO items VALUES (3, 'gizmo', 4.99);
UPDATE items SET price = 24.99 WHERE id = 2;
DELETE FROM items WHERE id = 1;
```

```sql
SELECT id, name, price, deleted_at FROM mz_environment_items ORDER BY id;
```

Each change should be visible in PostgreSQL in well under a second.

## Related pages

- [`SUBSCRIBE`](/sql/subscribe/)
- [Sinks](/fundamentals/concepts/sinks/)
- [Troubleshooting sinks](/export-data/sink-troubleshooting/)
