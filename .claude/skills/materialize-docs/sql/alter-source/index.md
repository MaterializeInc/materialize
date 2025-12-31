---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-source/
complexity: intermediate
description: '`ALTER SOURCE` changes certain characteristics of a source.'
doc_type: reference
keywords:
- ALTER SOURCE
- 'Important:'
- 'Note:'
product_area: Sources
status: stable
title: ALTER SOURCE
---

# ALTER SOURCE

## Purpose
`ALTER SOURCE` changes certain characteristics of a source.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER SOURCE` changes certain characteristics of a source.



Use `ALTER SOURCE` to:

- Add a subsource to a source.
- Rename a source.
- Change owner of a source.
- Change retain history configuration for the source.

## Syntax

This section covers syntax.

#### Add subsource

### Add subsource

To add the specified upstream table(s) to the specified PostgreSQL/MySQL/SQL Server source:

<!-- Syntax example: examples/alter_source / syntax-add-subsource -->

> **Note:** 
When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.



#### Rename

### Rename

To rename a source:

<!-- Syntax example: examples/alter_source / syntax-rename -->

#### Change owner

### Change owner

To change the owner of a source:

<!-- Syntax example: examples/alter_source / syntax-change-owner -->

#### (Re)Set retain history config

### (Re)Set retain history config

To set the retention history for a source:

<!-- Syntax example: examples/alter_source / syntax-set-retain-history -->

To reset the retention history to the default for a source:

<!-- Syntax example: examples/alter_source / syntax-reset-retain-history -->


## Context

This section covers context.

### Adding subsources to a PostgreSQL/MySQL/SQL Server source

Note that using a combination of dropping and adding subsources lets you change
the schema of the PostgreSQL/MySQL/SQL Server tables that are ingested.

> **Important:** 
When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.



### Dropping subsources from a PostgreSQL/MySQL/SQL Server source

Dropping a subsource prevents Materialize from ingesting any data from it, in
addition to dropping any state that Materialize previously had for the table
(such as its contents).

If a subsource encounters a deterministic error, such as an incompatible schema
change (e.g. dropping an ingested column), you can drop the subsource. If you
want to ingest it with its new schema, you can then add it as a new subsource.

You cannot drop the "progress subsource".

## Examples

This section covers examples.

### Adding subsources

```mzsql
ALTER SOURCE pg_src ADD SUBSOURCE tbl_a, tbl_b AS b WITH (TEXT COLUMNS [tbl_a.col]);
```text

> **Important:** 
When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.



### Dropping subsources

To drop a subsource, use the [`DROP SOURCE`](/sql/drop-source/) command:

```mzsql
DROP SOURCE tbl_a, b CASCADE;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the source being altered.
- In addition, to change owners:
   - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the source is namespaced
  by a schema.


## See also

- [`CREATE SOURCE`](/sql/create-source/)
- [`DROP SOURCE`](/sql/drop-source/)
- [`SHOW SOURCES`](/sql/show-sources)

