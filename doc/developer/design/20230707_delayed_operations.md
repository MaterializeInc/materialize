# Delayed operations

Epic: [#19547](https://github.com/MaterializeInc/materialize/issues/19547)

## Context

Materialize's API to alter managed clusters apply changes immediately.
This causes clusters to be unavailable until replicas are rehydrated.

As a work-around, managed clusters need to be converted to unmanaged, reconfigured, and converted back to managed clusters:

```sql
-- Set unmanaged
ALTER CLUSTER c SET (MANAGED false);
-- Rename old replicas to `_pending`
ALTER CLUSTER REPLICA c.r1 RENAME r1_pending;
-- Create new replicas
CREATE CLUSTER REPLICA c.r1 SIZE 'xsmall';
-- Wait until `c.r1` is hydrated; then drop pending replicas
DROP CLUSTER REPLICA c.r1_pending;
-- Set managed
ALTER CLUSTER c SET (MANAGED);
```

## Goals

* Resizing clusters without forced downtime, but fixed costs. (If rehydration finishes within allocated time.)

## Non-Goals

* Resizing clusters with guaranteed absence of downtime.
* Resizing clusters based on hydration status.

## Overview

We'll add the following declarative cluster management commands to Materialize:

```sql
-- Change the SIZE of c1 to 3xsmall, keep existing instances for 1h.
ALTER CLUSTER c1 SET (SIZE '3xsmall') WITH TEARDOWN (AFTER now() + INTERVAL '1 hour');

-- Change the SIZE of c1 to 3xsmall, keep existing instances until midnight 2023-07-08.
ALTER CLUSTER c1 SET (SIZE '3xsmall') WITH TEARDOWN (AFTER TIMESTAMP '2023-07-08 00:00');

-- IN THE FUTURE, we can support different signals, mixed with each other
ALTER CLUSTER c1 SET (SIZE '3xsmall') WITH TEARDOWN (SIGNAL 'hydration');
ALTER CLUSTER c1 SET (SIZE '3xsmall') WITH TEARDOWN (SIGNAL 'hydration', AFTER now() + INTERVAL '10 minutes');
```

A pending action can be aborted while its teardown actions have not been applied:

```sql
-- Abort the resizing operation
ALTER CLUSTER c1 RESET (SIZE)
```

## Detailed description

### `ALTER CLUSTER SET`

The `ALTER CLUSTER SET` statement will learn a `WITH TEARDOWN` modifier:

* `AFTER timestamp`, an expression evaluating to a timestamp. Materialize can should the old replicas once this time has passed.

Open question: Should we allow the `WITH TEARDOWN` modifier for all changes, or only for changes that cannot be applied gracefully?

There can be at most one pending action per cluster, which means the following must fail:

```sql
ALTER CLUSTER c SET (SIZE 'xsmall') WITH TEARDOWN (AFTER now() + INTERVAL '1 hour');
ALTER CLUSTER c SET (SIZE '2xsmall') WITH TEARDOWN (AFTER now() + INTERVAL '1 hour');
ERROR: Cluster c has a pending action.
DETAIL: Wait for the pending action to complete, or abort the pending action.
```

### `ALTER CLUSTER RESET`

The `ALTER CLUSTER RESET` statement will learn to support restoring a pending action.

Open question: This changes the semantics of `RESET` to restore to the last known state instead of the system default state.
We could add a `PENDING` modifier to restore to the last set state.

## Alternatives

We could wait to improve the API until we've built [dynamic cluster scheduling].

## Open questions

* Pending actions must be limited to actions that cannot error.

[dynamic cluster scheduling]: https://github.com/MaterializeInc/materialize/issues/13870
