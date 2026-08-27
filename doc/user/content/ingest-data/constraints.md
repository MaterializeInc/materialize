---
title: "Constraints"
description: "How Materialize incorporates upstream constraints, which upstream constraint changes are safe, and how to change constraints without downtime."
menu:
  main:
    identifier: ingest-constraints
    parent: ingest-data
    weight: 38
---

When Materialize creates a table from a source, it captures the upstream
table's constraints and incorporates some of them into the table's schema.
Those constraints become part of the contract between your upstream database
and Materialize: Materialize relies on them for correctness, so dropping one
upstream can put the affected table into an error state.

This guide explains what Materialize does with each constraint type, which
upstream changes are safe, and how to change constraints without downtime.

## How Materialize incorporates constraints

| Upstream constraint | What Materialize does with it |
|---------------------|-------------------------------|
| `PRIMARY KEY` | Recorded as a unique key of the table. Keys power optimizations such as `DISTINCT` and `GROUP BY` elimination and join simplification, and satisfy key validation for [upsert sinks](/sql/create-sink/kafka/#upsert-key-selection). The key's columns are also marked non-nullable. |
| `UNIQUE` | Recorded as a unique key of the table, with the same optimizer and sink benefits, if all of its columns are `NOT NULL` or the constraint was created with `NULLS NOT DISTINCT`. |
| `NOT NULL` | Recorded as the column's nullability. The optimizer uses it, for example to prove that an `IS NULL` filter returns no rows. |
| `CHECK` | Ignored. |
| `FOREIGN KEY` | Ignored. |
| `EXCLUSION` (PostgreSQL) | Ignored. |
| `DEFAULT` | Ignored. |

Constraints that Materialize ignores are enforced upstream at write time, so
you can add or drop them freely without affecting ingestion.

Constraints added upstream **after** the Materialize table is created are also
not incorporated. Adding a constraint, and later dropping it again, does not
affect ingestion. To incorporate a newly added constraint, create a new table
from the source.

## Which upstream changes are safe

Whether an upstream constraint change is safe depends on the source type.
The authoritative per-source matrices live in each source's "Handling
upstream operations" section:

- [PostgreSQL](/sql/create-table/postgres/#handling-upstream-operations)
- [MySQL](/sql/create-table/mysql/#handling-upstream-operations)
- [SQL Server](/sql/create-table/sql-server/#handling-upstream-operations)

Per-source gotchas worth calling out:

- In **MySQL**, any unique index acts as a `UNIQUE` constraint, and the
  primary key is the index literally named `PRIMARY`.
- In **PostgreSQL**, plain (non-unique) indexes are invisible to Materialize;
  only `PRIMARY KEY` and `UNIQUE` constraints are captured.

In short, for a constraint that existed when the Materialize table was
created:

- **Adding** a constraint upstream is always a non-event.
- **Dropping** an ignored constraint (`CHECK`, `FOREIGN KEY`, `EXCLUSION`,
  `DEFAULT`) is a non-event.
- **Dropping** an incorporated constraint (`PRIMARY KEY`, `UNIQUE`,
  `NOT NULL`) puts the affected table into an error state, unless you exclude
  the constraint first (see below).

## Exclude constraints you plan to drop

For **PostgreSQL**, **MySQL**, and **SQL Server** sources, `CREATE TABLE ...
FROM SOURCE` supports excluding constraints so that their upstream drop
becomes a non-event:

```mzsql
-- Do not record the named PRIMARY KEY/UNIQUE constraints as keys.
CREATE TABLE users
FROM SOURCE pg_source (REFERENCE public.users)
WITH (EXCLUDE CONSTRAINTS ('users_wallet_id_key'));

-- Record no constraints at all: no keys, every column nullable.
CREATE TABLE users
FROM SOURCE pg_source (REFERENCE public.users)
WITH (EXCLUDE ALL CONSTRAINTS);
```

- `EXCLUDE CONSTRAINTS ('<name>' [, ...])` takes upstream constraint names as
  string literals, matched exactly (including case) against the `PRIMARY KEY`
  and `UNIQUE` constraint names on the referenced table. An excluded
  constraint is not recorded as a key, so dropping (or dropping and
  recreating) it upstream does not affect ingestion.
- `EXCLUDE ALL CONSTRAINTS` records no keys and ingests every column as
  nullable, so dropping any `PRIMARY KEY`, `UNIQUE`, or `NOT NULL` constraint
  upstream does not affect ingestion. This suits environments where constraint
  churn comes from migrations you do not control.

The two options cannot be combined, and naming a constraint that does not
exist on the referenced table is an error.

Per-source notes:

- **MySQL**: constraints are unique indexes, so the names to exclude are index
  names, and the primary key's index is literally named `PRIMARY`.
- **SQL Server**: exclusion controls only which keys Materialize records. SQL
  Server does not allow dropping a `PRIMARY KEY` while change data capture is
  enabled, and any upstream `ALTER COLUMN` (including `NOT NULL` changes)
  still puts the table into an error state regardless of these options.

### Planned constraint drop, without downtime

Suppose a `ALTER TABLE public.users DROP CONSTRAINT users_wallet_id_key;` is
scheduled upstream. Before it runs, recreate the table in a new versioned
schema, excluding the doomed constraint, and swap:

```mzsql
-- Step 1: Recreate the table in a new versioned schema, excluding the doomed
-- constraint. This starts a snapshot of v2.users while v1.users keeps serving.
CREATE SCHEMA v2;

CREATE TABLE v2.users
FROM SOURCE pg_source (REFERENCE public.users)
WITH (EXCLUDE CONSTRAINTS ('users_wallet_id_key'));

-- Step 2: Once v2.users finishes snapshotting, recreate your views in v2:
CREATE MATERIALIZED VIEW v2.active_users AS
    SELECT id, email, wallet_id FROM v2.users WHERE deleted_at IS NULL;

-- Step 3: Swap, then clean up the old objects.
ALTER SCHEMA v1 SWAP WITH v2;
```

The upstream constraint can now be dropped, and nothing in Materialize
notices. This is the same choreography as the [schema change
workflows](/ingest-data/postgres/source-versioning/); the new table
re-snapshots the upstream table (per table, not per source), while the old
table keeps serving until the swap.

### Recover a table that is already in an error state

If a constraint was dropped upstream before you excluded it, the affected
table is permanently in an error state, and the error names the dropped
constraint. Recovery is the same versioned-schema workflow, except that no
exclusion is needed: a replacement table snapshots the **current** upstream
schema, which no longer contains the dropped constraint. Create the
replacement table in a new versioned schema, wait for it to snapshot, swap,
and drop the errored table.

Note that `EXCLUDE CONSTRAINTS` names must exist on the upstream table at
creation time, so the option is the tool for **planned** drops; after the
drop, a plain recreate captures the new schema.

## What exclusion costs

Excluding a constraint means Materialize no longer knows about the uniqueness
or nullability it declared:

- The optimizer can no longer use the key, so queries that benefited from
  `DISTINCT`/`GROUP BY` elimination or join simplification on those columns
  may become more expensive.
- Sinks that need a key must either use a different unique key or declare one
  with [`KEY ... NOT ENFORCED`](/sql/create-sink/kafka/#upsert-key-selection).
- With `EXCLUDE ALL CONSTRAINTS`, every column is nullable and the table has
  no keys at all, comparable to ingesting from a keyless Kafka topic. This is
  supported and correct, but it is the performance floor: expect none of the
  key-driven optimizations.
