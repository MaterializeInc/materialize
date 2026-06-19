---
title: "DROP BRANCH"
description: "`DROP BRANCH` tears down a schema branch and releases its references on source blobs."
menu:
  main:
    parent: 'commands'
---

{{< warning >}}
`DROP BRANCH` is an experimental feature gated behind the
`enable_branching` flag, which is off by default.
{{< /warning >}}

Use `DROP BRANCH` to tear down a schema branch created with
[`CREATE BRANCH`](/sql/create-branch). The drop:

* Removes the branch's catalog items.
* Releases every reference the branch holds on the source's persist blobs.
* Marks the branch's own fork shards for deletion; persist's GC reclaims
  them on the next cycle.

## Syntax

```mzsql
DROP BRANCH [IF EXISTS] branch_name;
```

`IF EXISTS` makes `DROP BRANCH` a no-op if the branch does not exist;
without it, dropping a non-existent branch is an error.

## Details

Once a branch is dropped, any source-side DDL the branch was blocking (see
[`CREATE BRANCH`](/sql/create-branch#identity)) becomes possible again. The
source blobs the branch was pinning are eligible for GC on the next pass;
they may persist briefly until the GC cycle runs.

## Examples

```mzsql
ALTER SYSTEM SET enable_branching = on;
CREATE BRANCH nightly FROM SCHEMA public;
-- ... use the branch ...
DROP BRANCH nightly;

-- Idempotent teardown.
DROP BRANCH IF EXISTS nightly;
```

## Related pages

* [`CREATE BRANCH`](/sql/create-branch)
* [`SHOW BRANCHES`](/sql/show-branches)
