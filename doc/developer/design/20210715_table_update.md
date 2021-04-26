# UPDATE and DELETE for tables

## Summary

Support UPDATE and DELETE for tables.
These cannot be used in transactions and are serialized along with INSERTs.

## Goals

Add a framework for supporting transactional table operations that need to do writes based on the result of a read.
This framework is designed to be serializable, and so is safe when used by multiple concurrent SQL sessions.

## Non-Goals

General transactions with interleaved reads and writes are not a goal.
High performance under heavy concurrent load is not a goal.

## Description

UPDATE, DELETE, UPSERT, INSERT ON CONFLICT, and unique key constraints are similar operations: a read followed by a write depending on the results of the read:
- DELETE will remove all read rows
- UPDATE will remove all read rows and add a new row for each removed
- unique key constraints will (during UPDATE and INSERT) do a read looking for any rows that might already exist, and do nothing (or error) if there are any result rows
- UPSERT and INSERT ON CONFLICT will do the same as unique key constraints above, but will instead not error and either do nothing for each conflict row or remove it and add a new one

We can add basic support for UPDATE and DELETE with a new plan node called ReadThenWrite that can be configured to do a read and then arbitrary behavior on its results.
We can add support for the others at a later date.

ReadThenWrite is a node that performs a Peek at some time `t1`.
Once the Peek has results ready, they are read and a set of diffs is produced which are sent at some time `t2` where `t1 > t2`.

In order to support our goal of strict serializability, we need to ensure that no writes to tables affected by ReadThenWrite happen between `t1` and `t2`.
Do this by teaching Insert and ReadThenWrite to serialize themselves by checking if another write has started and adding themselves to a queue if so, otherwise marking themselves as the in-progress write and continuing.
At transaction end, if the transaction's session is the in-progress write and there's a queued write, move the queued write to the coordinator's command queue, scheduling it for execution.
It is safe to use `t2` as the write time since the serialization prevents writes to the read table after `t1`, so the read is still correct as of `t2`.

We can optimize performance Insert by having it delay checking for an in-progress write until its commit time.
This allows for INSERT-only workloads to serialize by nature of the single-threaded coordinator, just like they do before this design document.

We want to prevent the case where one user opens a write transaction and forces all other write transactions for the first user to issue a `COMMIT`.
The design above achieves this because INSERT only needs to serialize itself after the user has issued a `COMMIT` statement, and ReadThenWrite has no user interaction between its read and write.
Additionally, ReadThenWrite must be excluded from (multi-statement) transactions.

## Alternatives

Instead of maintaining a global write lock for all tables, it could be per table or perhaps per row.
This is an improvement that could be made if we need more throughput under concurrent workloads.

## Open questions

Is optimistic locking possible and better?
This is another alternative that we can explore if needed.
