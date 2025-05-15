# Extending the Query Lifecycle Tables

- Associated: [#9140](https://github.com/MaterializeInc/database-issues/issues/9140)

## The Problem

To work towards end-to-end visibility into query lifecycle, the most important events to add would be when we receive a statement through pgwire (at the `conn.recv()` in `advance_ready` in `protocol.rs`, roughly corresponding to last byte received), and then when parsing finishes (still in `protocol.rs`). The complication with these is that the current `mz_statement_lifecycle_history` records events related to statement _executions_, not to statements themselves, so the above two events don't really fit there.

## Success Criteria

Users and Materialize engineers can understand where time is spent when executing ad hoc queries. Specifically, we should know:
- how much time is spent in Materialize vs. in external systems;
- the breakdown of the time spent inside Materialize, so that we know where to focus our optimization efforts to bring down end-to-end latency.

## Out of Scope

We are not yet going to measure
- the time from the moment a query lands at the balancer to the moment it lands at envd;
- the time it takes for the tokio event loop in `protocol.rs` to come around to processing the next incoming query and the time of the `recv` call itself.

It would be great to eventually measure the above, but it would greatly complicate the implementation. We can revisit this if the need comes up.

We are also not going to measure
- time between first byte received and last byte received;
- time between first result byte sent and last result byte sent.

These are hard to define exactly, as pointed out by @antiguru, as they can depend on TCP's ACKing behavior. For example, in some cases, the user can already start processing the result even before we get the ACK of the last result byte sent, leading to misleading end-to-end measurements.

## Background

There are various ways users can submit queries and request query executions:
1. Submitting a single query for immediate execution through pgwire's ["Simple Query" flow](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY).
2. Submitting multiple queries through pgwire's "Simple Query" flow (delimited by `;`s).
3. Working with prepared statements through SQL commands `PREPARE` / `EXECUTE`. These commands are submitted (confusingly) through the "Simple Query" flow.
4. Working with prepared statements through pgwire's ["Extended Query" flow](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY): `Parse`, `Bind`, `Execute`. This is what happens when PowerBI is used.

Note that even for 1. and 2., the prepared statement code paths are re-used internally. For example, a portal is created even for a single statement in 1., similarly to a prepared statement. Also, a row is added to `mz_prepared_statement_history` even for 1.

(Confusingly, 3's `PREPARE` is the same as 4's `Parse`. So, when someone says "parsing finished", it's not clear whether we mean the actual parsing (i.e., the call to `adapter_client.parse`) or the processing of the whole `Parse` msg from 4. Fortunately, I think the most interesting and costly part of processing a `Parse` msg is the actual parsing, so hopefully this won't be too misleading. But we'll have to be careful to explain these things well in the docs of the lifecycle events.)

## The current state

- There is `mz_prepared_statement_history`, which records statements submitted through any of 1., 2., 3., or 4.
- There is `mz_statement_execution_history`. For 1. and 2., rows here are in 1-to-1 correspondence with rows in `mz_prepared_statement_history`. For 3. and 4., multiple rows here correspond to 1 row in `mz_prepared_statement_history` when the same prepared statement is executed multiple times.
- There is `mz_statement_lifecycle_history`. Confusingly, this has a `statement_id` column, which joins with `mz_statement_execution_history.id`. So, the name of both the table and the column suggest that rows here correspond with rows in `mz_prepared_statement_history`, but actually they correspond with `mz_statement_execution_history`.

The main problem is that `mz_statement_lifecycle_history` records only events from when the Coordinator starts executing a statement, which means that we don't have visibility to when we receive a statement in `protocol.rs` (which Aljoscha calls the "frontend" part of the Adapter), and perform various preparations, e.g., parsing.

## Solution Proposal

So, what I would propose is:
- For statement _execution_ lifecycle events: We rename the current `mz_statement_lifecycle_history` to `mz_statement_execution_lifecycle_history` (which should have been its original name). Additionally, we should rename its `statement_id` field to `execution_id` (which, again, should have been its original name, because it already joins with `mz_statement_execution_history.id` and _not_ with `mz_prepared_statement_history.id`). This is in `mz_internal`, so it should be ok from a user perspective. However, the Console is using these, so I guess there will need to be an interim period where the Console would have to conditionally use the old or the new names based on the Materialize version. @SangJunBak [says](https://github.com/MaterializeInc/database-issues/issues/9140#issuecomment-2881827627) that this would be ok.
    - We add a new event to `mz_statement_execution_lifecycle_history`: `ExecutionMessageReceived` (at the `conn.recv()` in `advance_ready` in `protocol.rs`, roughly corresponding to last byte received): Either when we get an `Execute` in 3. or 4., or when one or multiple queries are submitted through 1. or 2. In case of 2., the same lifecycle event would be copied to multiple statements.
- For statement lifecycle events: We add a new `mz_statement_lifecycle_history` table, which has the same schema as the current `mz_statement_lifecycle_history`, but its `statement_id` joins with `mz_prepared_statement_history.id`. It will record the following events:
    - `StatementReceived` (at the `conn.recv()` in `advance_ready` in `protocol.rs`): when we get a new statement through any of 1., 2., 3., or 4. More specifically:
        - a single query in 1.
        - multiple queries in 2., in which case, again, the same lifecycle event would be copied to multiple statements.
        - a `PREPARE` in 3.
        - a `Parse` in 4.
    - `ParsingComplete`: when our parsing finishes in `protocol.rs`. (This is in 1-to-1 correspondence with `StatementReceived`.) Note that this is not the entire handling of a `Parse` msg of 4., but only the actual parsing.
    - `PrepareComplete`: when we have finished the preparations for a statement. (This is in 1-to-1 correspondence with `StatementReceived`.) More specifically:
        - For 1. and 2., when `one_query` is about to call `adapter_client.execute`.
        - For 3., when a `PREPARE` completes, i.e., when we are about to send the response to the user in `protocol.rs`.
        - For 4., when we are about to send a `ParseComplete` back to the user in `protocol.rs`.
    - Additionally, we should have ...received and ...finished events for each of the other msgs in the "Extended Query" flow, e.g., `Bind`, `Describe`, `Execute`.

Note that if a user is interested only in queries submitted through 1. or 2., and they are not interested in when the parsing and other preparation ended, it will be enough to look at `mz_statement_execution_lifecycle_history` (and optionally join it with `mz_statement_execution_history` and `mz_prepared_statement_history` to get more info about the execution and/or statement). They will still be able to get the end-to-end time, because `ExecutionMessageReceived` will be part of `mz_statement_execution_lifecycle_history`.

If a user is interested also in prepared statements, i.e., 3. or 4, then they would need to look at also `mz_statement_lifecycle_history. In this case, a 4-way join between `mz_statement_lifecycle_history`, `mz_statement_execution_lifecycle_history`, `mz_statement_execution_history`, and `mz_prepared_statement_history` would be needed.

Also, I'm thinking to add a column to `mz_prepared_statement_history` to indicate which of 1., 2., 3., or 4. was used to submit the statement.

## Examples

For a query submitted through 1.:
- `FrontendMessage::Query` arrives in `protocol.rs`, to which
    - `mz_statement_lifecycle_history` would get the following events:
        - `StatementReceived`
        - `ParsingComplete`
        - `PrepareComplete`
    - `mz_statement_execution_lifecycle_history` would get the following events:
        - `ExecutionMessageReceived` (roughly corresponds to last byte of the query arriving) (with the same timestamp as the above `StatementReceived`)
        - `ExecutionBegan` (already exists, emitted in the Coordinator's `begin_statement_execution`)
        - `OptimizationStarted` (new event, but not mentioned above, because it's orthogonal to the prepared statement issues) (this should be very close in time to `ExecutionBegan` if things are behaving correctly, but I have a suspicion that this is not always the case)
        - `OptimizationFinished` (already exists)
        - `StorageDependenciesFinished`
        - `ComputeDependenciesFinished`
        - `ExecutionFinished` (roughly corresponds to first byte of the result sent to user)

For a query submitted through 4.:
- `FrontendMessage::Parse` arrives in `protocol.rs`, to which
    - `mz_statement_lifecycle_history` would get the following events:
        - `StatementReceived`
        - `ParsingComplete`
        - `PrepareComplete`
- `FrontendMessage::Bind` arrives in `protocol.rs`, to which
    - `mz_statement_lifecycle_history` would get the following events:
        - `BindReceived`
        - `BindComplete` (whose timestamp would probably be very close to `BindReceived`, but later we might do more interesting things at bind time, in which case binding will take more time)
- `FrontendMessage::Execute` arrives in `protocol.rs`, to which
    - `mz_statement_lifecycle_history` would get the following events:
        - `ExecuteReceived`
        - `ExecuteFinished`
    - `mz_statement_execution_lifecycle_history` would get the following events:
        - `ExecutionMessageReceived` (with the same timestamp as the above `ExecuteReceived`)
        - `ExecutionBegan`
        - `OptimizationStarted`
        - `OptimizationFinished`
        - `StorageDependenciesFinished`
        - `ComputeDependenciesFinished`
        - `ExecutionFinished` (time will be the same as the above `ExecuteFinished`)

## Minimal Viable Prototype

I have a limited prototype that doesn't add new builtin tables or change schemas in existing ones, but just adds events to the existing `mz_statement_lifecycle_history` without regard to prepared statements:
https://github.com/MaterializeInc/materialize/pull/32424
The prototype shows how we can transfer information between `protocol.rs` and the already existing lifecycle logging code in the `Coordinator` through portals.

## Alternatives

One could imagine trying to keep things simple by just quickly implementing the events only for statements that are not prepared statements. however, it turns out that [a big user](https://github.com/MaterializeInc/accounts/issues/3) is actually using prepared statements, because PowerBI always uses the "Extended Query" flow of pgwire. And since having visibility into that user's PowerBI query latency is one of the main motivations of this work, we should do the right thing and support prepared statements.

## Open questions

When or whether we will need to also measure the things mentioned in the "Out of Scope" section.
