# Single-Statement Explicit Transactions

- Associated: https://github.com/MaterializeInc/database-issues/issues/6143

## Context

Many kinds of statements (importantly all DDL `CREATE` statements) can only be executed in an implied single-statement transaction.
These are transactions without a `BEGIN` (hence implied) and known to be exactly one statement long.
DDLs are disallowed because we cannot prospectively execute them until a commit.

Many GUI and other tools however always wrap their statements with `BEGIN` and `COMMIT`,
thus creating an explicit transaction, and are thus prevented from ever executing DDL or other prohibited statements.

## Goals

- Allow many more kinds of statements to execute in explicit transactions,
  as long as there is exactly one statement in the transaction: `BEGIN; <stmt>; COMMIT`.

## Non-Goals

- Multiple DDLs in a single transaction.
- Supporting all statements in this form.

## Overview

Add a new transaction op that holds onto a single `Statement` but does not execute it.
The statements tag is generated and returned as if it did execute.
We can thus only execute statements that can generate their tag from a `Statement` or `Plan` without execution.
If any statement attempts to execute after that, the op will fail to add it, thus enforcing a single statement in the transaction.
On `COMMIT`, the statement executes, returning any errors as normal.

## Detailed description

Many statements cannot be executed without observable side effects to other sessions, for example all DDLs.
However, these and many other statements do have a known `ExecuteResponse` that can be determined without execution (as opposed to statements that return a row count).
We will add a new transaction operation mode called `SingleStatement`.
Currently those modes are `Peeks`, `Writes`, and `Subscribe`.
`SingleStatement` will record the statement, then generate and return to the user an `ExecuteResponse` that assummes success.
The `SingleStatement` operation can only be entered in explicit transactions (must start with `BEGIN`).
On `COMMIT`, the coordinator clears the current transaction, fetching and processing its inner operation (this already occurs before this document).
If the inner operation is `SingleStatement`, the transaction status is set to `Started` (a status that can execute any statement), the recorded statement is executed, and an implicit `COMMIT` is executed.
The implicit `COMMIT` puts the transaction into the same final state (`Default`) as the explicit `COMMIT`.

### Session Transaction State Machine

Here we describe the state machine and invariants for a single session's transaction before this design was implemented.
We will then describe how this design modifies that state machine but still meets all invariants.

Each `Session` has a `tranasction` property of type `enum TransactionStatus`.
It is the responsibility of the various protocol handlers (pgwire, http, ws) to correctly manage this.
A `TransactionStatus` is an enum with variants:

- `Default`
- `Started`
- `InTransaction`
- `InTransactionImplicit`
- `Failed`

`Default` is the initial state, and also the state when there is no in-progress transaction.
In order for a session to execute a statement, the protocol handler must set this to one of the other states then end the transaction with a commit or rollback.
`Started` is used for a single statement, so an immediate and implied commit follows.
`InTransaction` is used when a user explicitly types `BEGIN`, and can only be exited when a user sends `COMMIT` or `ROLLBACK`.
`InTransactionImplicit` is used when a user sends multiple statements in the same query (`SELECT 1; SELECT 2`), and an implicit `BEGIN` and `COMMIT` wrap it.
`Failed` can only be entered when a statement in a transaction has error'd from the `InTransaction` state.
If failures occur in the other in-progress states, an implicit `ROLLBACK` must be sent by the handler.

All status except `Default` have an inner `Transaction` object that tracks what operations have happened in a transaction determined by the first statement.
These operations are: `Peeks`, `Subscribe`, `Writes`, `None`.
`None` is the default and can transition to any other operation.
The other operations can be merged with like operations, except `Subscribe` which supports only a single `SUBSCRIBE`.
When a transaction is being `COMMIT`ed, the transaction is cleared (set to `Default`) and its inner operation is able to do arbitrary post processing.
`Peeks` can enqueue the session back into the Coordinator in order to wait for a strict serializable verification.
`Writes` enqueues the writes along with their session in order to wait for writing to the storage controller.

The `ReadyForQuery` message is sent by the server when it is ready to receive new queries, and it includes the current transaction status code: `Idle`, `InTransaction`, or `Failed`.
`InTransaction` and `Failed` correspond to the similarly named variants above and can only appear in explicit transactions.
`Idle` is return in the `Default` state, because all other transaction states implicitly close and should never be active when deciding the status code.

The pgwire, http, and ws handlers enforce these requirements and state changes.

### Changes from this design

This design adds a new operation to the inner transaction: `SingleStatement`.
This operation records a single statement.
If a second statement is added to the transaction with this operation, the transaction fails.
The post processing for this operation enqueues to the Coordinator a message containing the statement and session, which is processed by a new `sequence_execute_single_statement_transaction` method.
This method:
1. Asserts the session's transaction is in `Default`, which should be a side effect of running it through `sequence_end_transaction` from the `COMMIT`.
2. Puts the session's transaction into `Started` (single-statement, implicit transaction).
3. Executes the inner statement, without sending the result to the user.
4. Commits the transaction.
5. The transaction should again be in `Default`.

The `sequence_execute_single_statement_transaction` function is acting as a handler and must ensure the correct state transitions and invariants as the other handlers.

### Example with state transitions

1. Session starts in `Default`.
2. User initiates a query, the session sets the transaction to some in-progress status (doesn't matter which).
3. User executes `BEGIN` from the query of the previous step. Transaction status changes to `InTransaction`.
4. User executes some statement. Inner transaction operation set to `SingleStatement`.
5. User executes `COMMIT`. The transaction status is set to `Default` and its inner operation saved.
6. The inner operation side effect is processed: `SingleStatement` enqueues a Coordinator message with the statement and session.
7. The Coordinator begins processing the message, setting the session's transaction status to `Started`.
8. The saved statement is executed, setting the inner transaction operation to whatever the statement needs.
9. An implicit `COMMIT` is executed. The transaction status is set to `Default` and its inner operation saved.
10. The inner operation is processed.
11. A result is sent back to the `COMMIT` statement from step 5, and control of the session is returned to the user.

## Alternatives

The simple query protocol allows passing multiple statements in a single query string.
We could parse that and examine it for the `BEGIN; <stmt>; COMMIT` form.
We could then execute *any* statement instead of only ones whose tag we can generate without execution.
This is limited because it would not work for:
- psql
- drivers or APIs that use the extended protocol
- http or websocket endpoints
However, this alternative is not at odds with the above design, and could still be done if we discover any drivers or tools using this wrapped simple format.

## Open questions

1. Proving this doesn't have correctness errors in the code is difficult.
   There are a lot of fiddly parts of session and transaction handling, and this design re-implements a few parts of them without much help from a compiler or existing handlers to enforce anything.
