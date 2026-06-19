---
title: "SHOW BRANCHES"
description: "`SHOW BRANCHES` lists the live schema branches; `SHOW BRANCH STATUS` reports per-branch state."
menu:
  main:
    parent: 'commands'
---

{{< warning >}}
`SHOW BRANCHES` and `SHOW BRANCH STATUS` are experimental, gated behind
the `enable_branching` flag.
{{< /warning >}}

`SHOW BRANCHES` lists every schema branch the current role can see, with
its owner and creation time.

`SHOW BRANCH STATUS <branch_name>` reports a single row of runtime state for
one branch: how many source blobs it currently pins (`blob_ref_count`),
which cluster its dataflows render on, and whether its indexes have
hydrated.

## Syntax

```mzsql
SHOW BRANCHES;
SHOW BRANCH STATUS branch_name;
```

## Examples

```mzsql
ALTER SYSTEM SET enable_branching = on;

CREATE BRANCH nightly FROM SCHEMA public;
CREATE BRANCH experiment FROM SCHEMA public;

SHOW BRANCHES;
-- branch_name  | owner | created_at
-- nightly      | alice | 2026-06-18 ...
-- experiment   | alice | 2026-06-18 ...

SHOW BRANCH STATUS nightly;
-- branch_name | owner | created_at      | blob_ref_count | cluster | indexes_ready_at
-- nightly     | alice | 2026-06-18 ...  | 42             | default | 2026-06-18 ...
```

## Related pages

* [`CREATE BRANCH`](/sql/create-branch)
* [`DROP BRANCH`](/sql/drop-branch)
