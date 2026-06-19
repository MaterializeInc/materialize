---
title: "CREATE BRANCH"
description: "`CREATE BRANCH` forks every object in a schema into a writable, isolated namespace."
menu:
  main:
    parent: 'commands'
---

{{< warning >}}
`CREATE BRANCH` is an experimental feature gated behind the
`enable_branching` flag, which is off by default. Behavior, syntax, and
durability guarantees may change without notice.
{{< /warning >}}

Use `CREATE BRANCH` to fork every object in a schema at a single, coordinated
point in time. The branch is a real namespace: tables, materialized views,
indexes, sources, and sinks all have their own branch-local identity. Reads
on a branched object return the source's data at the fork time plus any
branch-local writes. Writes affect only the branch. The source is untouched.

A branch is intended for short-lived, throwaway use: agent iteration, full-app
dev environments, cross-object refactors that need a sandbox. There is no
`MERGE BRANCH`; the production path back from a branch is dbt / mz-deploy /
CI/CD.

## Syntax

```mzsql
CREATE BRANCH branch_name FROM SCHEMA schema_name;
```

`branch_name` is a single identifier. `schema_name` is the source schema; it
must already exist.

`FROM BRANCH` (sub-branches) and `FROM DATABASE` (whole-database branching)
are not parsed in this release.

## Details

### Identity

Each branched object gets its own catalog item id and global id. Reads of
branch items resolve through the branch's own identity, never through the
source. The source can be `ALTER`-ed once the branch is dropped; while a
branch is live, source-side `ALTER`, `DROP`, `RENAME`, and `DROP SCHEMA`
on objects the branch references are blocked with a clear error naming
the blocking branch.

### Storage

A branched object's persist shard initially references the source's blobs
directly. A new write to the branched object lands in the branch's own blob
namespace. Compaction on the branch trends storage cost toward
write-proportional rather than source-size-proportional over time.

The source's blobs are pinned against garbage collection while any branch
references them. Dropping the branch releases those references; the next GC
cycle reclaims any blobs that no other branch references.

### Sources and sinks

Sources and sinks in a branched schema are inert by default:

* A branched **source** does not run an ingestion against the external
  system. Reads of the branched source return the data the source had at
  the branch timestamp, and nothing later.
* A branched **sink** does not emit. Reads of the branched sink's output
  shard still work.

This is the safety story: branching a schema that contains a production
sink can never accidentally double-emit to the source's external
destination.

### Compute

Branched MVs and indexes render on the session cluster.

## Examples

```mzsql
-- Off by default; an operator opts in at the system level.
ALTER SYSTEM SET enable_branching = on;

CREATE BRANCH nightly FROM SCHEMA public;

-- Reads observe source data at branch time.
SELECT count(*) FROM nightly.orders;

-- Writes only affect the branch.
INSERT INTO nightly.orders VALUES (...);
SELECT count(*) FROM nightly.orders;   -- one more than public.orders

-- Source-side DDL is blocked while the branch is live.
ALTER TABLE public.orders ADD COLUMN c text;
-- ERROR: cannot alter "public.orders": branch "nightly" references it

-- Teardown releases the source blobs pinned by the branch.
DROP BRANCH nightly;
```

## Restrictions

* `enable_branching` is off by default.
* Only `FROM SCHEMA` is parsed; `FROM BRANCH` and `FROM DATABASE` are not.
* Sources and sinks in a branched schema are inert and cannot be activated
  through SQL in this release.
* Branched MVs and indexes render on the session cluster (no per-branch
  isolated cluster yet).
* No `MERGE BRANCH`. Use dbt / mz-deploy / CI/CD to ship changes to
  production.

## Related pages

* [`DROP BRANCH`](/sql/drop-branch)
* [`SHOW BRANCHES`](/sql/show-branches)
