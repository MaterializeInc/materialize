# Extending the Query Lifecycle Tables

Associated:
- [#9140](https://github.com/MaterializeInc/database-issues/issues/9140)
- [Original statement logging design doc](https://github.com/MaterializeInc/materialize/blob/4487749289f1591e7918ebaa1458e23b5e40633f/doc/developer/design/20230519_statement_logging.md)
- [Original statement lifecycle design doc](https://github.com/MaterializeInc/materialize/blob/4487749289f1591e7918ebaa1458e23b5e40633f/doc/developer/design/20231204_query_lifecycle_events_logging.md)
- [Slack discussion](https://materializeinc.slack.com/archives/C08ACQNGSQK/p1747394353074239?thread_ts=1747394055.561859&cid=C08ACQNGSQK)

This design doc is split into short term and medium/long term parts.

## The Problem

We'd like to have more visibility into query lifecycle, with regards to how much time is spent at each stage. This will be needed both for debugging slow queries, and for being able to confidently tell how much time is spent in Materialize when an external system queries us. In the short term, we concentrate on just the latter.

Currently, there are the `began_at` and `finished_at` fields of `mz_statement_execution_history`, and the difference between these (the "Duration" column in the Console's Query History) is intended to show the total time in Materialize. However, `began_at` is taken after some minor steps of processing a query have already taken place. In particular, it doesn't include parsing time, because we take the `began_at` timestamp in the Coordinator in `begin_statement_execution`, but parsing and other things happen in the Adapter frontend (see `pgwire/src/protocol.rs`).

`finished_at` is taken in the Coordinator's `retire_execution`. This is mostly fine, as it roughly corresponds to the first byte of the result sent back to the user, but we might want to eventually also have visibility into when the last byte of the result is sent to the user.

## Success Criteria -- Short Term

In the short term, we concentrate on just being able to confidently tell how much time is spent in Materialize when an external system queries us.

## Out of Scope -- Short Term

We won't add new lifecycle events or new columns to internal tables due to [concerns about data volume](https://materializeinc.slack.com/archives/C08ACQNGSQK/p1747394353074239?thread_ts=1747394055.561859&cid=C08ACQNGSQK).

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

The `execution-began` and `execution-finished` events in `mz_statement_lifecycle_history` correspond to the `began_at` and `finished_at` columns of `mz_statement_execution_history`. (Well, `began_at` doesn't exactly correspond to it, even though they are taken very close in the code. I'll fix this to make them always exactly match.)

The main problem is that `mz_statement_lifecycle_history` records only events from when the Coordinator starts executing a statement, which means that we don't have visibility to when we receive a statement in `protocol.rs` (the "frontend" part of the Adapter), and perform various preparations, e.g., parsing.

## Solution Proposal -- Short Term

As mentioned above, in the short term, we'd like to avoid adding new lifecycle events or new columns to internal tables due to concerns about data volume. Therefore we'll just do the following.

### Metrics

We'll create metrics for certain query processing stages that we hope to always take little time, e.g., parsing. Metrics won't give us visibility into exactly which queries are taking how much time in these stages, but we hopefully won't need that in the short term. Metrics will just be able to tell us that we are indeed always spending little time in these stages. Specifically, we'll add a metric for the actual parsing, and for the processing time of each of the message types in the Extended Query flow (of whom `Parse` and `Bind` should always take little time).

### Move `began_at` to be taken earlier

We'll take `began_at` (and with it the `execution-began` lifecycle event) not in the Coordinator, but in the Adapter frontend. Specifically, we'll take it when the `conn.recv()` call in `advance_ready` in `protocol.rs` returns. For the Extended Query flow, this means the receiving time of the `Execute` message, and does not include e.g., `Parse` and `Bind`. For the Simple Query flow, this means the receiving time of the single message that specifies the query.

Note that this means `began_at` will include the time from the Adapter frontend sending a message to the coordinator to the Coordinator starting to process that message. This should usually be a short time, unless the Coordinator is very busy.

## Prototype

I have a prototype that doesn't add new builtin tables or change schemas in existing ones, but just adds events to the existing `mz_statement_lifecycle_history` without regard to prepared statements:
https://github.com/MaterializeInc/materialize/pull/32424
The prototype shows how we can transfer information between `protocol.rs` and the already existing lifecycle logging code in the Coordinator through portals. We can reuse this code for the "Move `began_at` to be taken earlier" part above.

## Summary of changes in what is included in query time measurements

Currently, the time we spend in the Adapter frontend is not included in query time measurements.

The most prominent part of the time we spend in the Adapter frontend is query parsing. After the above changes:
- for the "Simple Query" flow, parsing will be included in the `began_at` - `finished_at` interval;
- for the "Extended Query" flow, parsing (and the processing of the bind message) will not be included in the `began_at` - `finished_at` interval. However, we'll create a metric by which we can verify that parsing and binding always take a short time.

Another component of the time we spend in the Adapter frontend the wait for the Coordinator message loop to come around to start processing the `execute` message sent from the Adapter frontend. This is currently not accounted for at all, but will be after the above changes. More specifically:
- For the "Simple Query" flow, it will be included in the `began_at` - `finished_at` interval.
- For the "Extended Query" flow, it will be included in the `began_at` - `finished_at` interval for the `execute` message, but not for the other messages (`Parse`, `Bind`, ...). However, the planned metrics will include these.

## ------ MVP up until here, medium/long term plans below ------

## Success Criteria -- Long Term

In the long term, we'd like as much detailed visibility into the query lifecycle as possible, so that users and Materialize engineers can understand where time is spent when executing ad hoc queries. This will be important for knowing where to focus our optimization efforts to bring down end-to-end latency

Some needed lifecycle events:
- receiving (at the `conn.recv()` in `advance_ready` in `protocol.rs`, roughly corresponding to last byte received) and when completing the processing of each message type of the Extended Query flow of pgwire;
- parsing is finished;
- optimization began (see [pull/32508](https://github.com/MaterializeInc/materialize/pull/32508) for why we need this one in particular).

There is a much bigger list of potentially needed lifecycle events at
[#9140](https://github.com/MaterializeInc/database-issues/issues/9140), which will be updated continuously as we are learning about exactly where we need more details.

## Out of Scope -- Medium Term

We won't yet measure
- the time from the moment a query lands at the balancer to the moment it lands at envd;
- the time it takes for the tokio event loop in `protocol.rs` to come around to processing the next incoming query and the time of the `recv` call itself.

It would be great to eventually measure the above, but it would greatly complicate the implementation. We can revisit this if the need comes up.

We are also not going to measure time between first byte received and last byte received.

## Solution Proposal -- Medium Term

*The following is just a draft for now.*

### Internal Table Reorganization

We'll have to reorganize several internal tables.

- For statement _execution_ lifecycle events: We rename the current `mz_statement_lifecycle_history` to `mz_statement_execution_lifecycle_history` (which should have been its original name). Additionally, we should rename its `statement_id` field to `execution_id` (which, again, should have been its original name, because it already joins with `mz_statement_execution_history.id` and _not_ with `mz_prepared_statement_history.id`). This is in `mz_internal`, so it should be ok from a user perspective. However, the Console is using these, so I guess there will need to be an interim period where the Console would have to conditionally use the old or the new names based on the Materialize version. @SangJunBak [says](https://github.com/MaterializeInc/database-issues/issues/9140#issuecomment-2881827627) that this would be ok.
    - We add a new event to `mz_statement_execution_lifecycle_history`: `ExecutionMessageReceived` (at the `conn.recv()` in `advance_ready` in `protocol.rs`, roughly corresponding to last byte received): Either when we get an `Execute` in 3. or 4., or when one or multiple queries are submitted through 1. or 2. In case of 2., the same lifecycle event would be copied to multiple statements.
- For statement lifecycle events: We add a new `mz_statement_lifecycle_history` table, which has the same schema as the current `mz_statement_lifecycle_history`, but its `statement_id` joins with `mz_prepared_statement_history.id`. It will record the following events:
    - `StatementReceived` (at the `conn.recv()` in `advance_ready` in `protocol.rs`): when we get a new statement through any of 1., 2., 3., or 4. More specifically:
        - a single query in 1.
        - multiple queries in 2., in which case, again, the same lifecycle event would be copied to multiple statements.
        - a `PREPARE` in 3.
        - a `Parse` in 4.
    - `ParsingFinished`: when our parsing finishes in `protocol.rs`. (This is in 1-to-1 correspondence with `StatementReceived`.) Note that this is not the entire handling of a `Parse` msg of 4., but only the actual parsing.
    - `PrepareFinished`: when we have finished the preparations for a statement. (This is in 1-to-1 correspondence with `StatementReceived`.) More specifically:
        - For 1. and 2., when `one_query` is about to call `adapter_client.execute`.
        - For 3., when a `PREPARE` completes, i.e., when we are about to send the response to the user in `protocol.rs`.
        - For 4., when we are about to send a `ParseFinished` back to the user in `protocol.rs`.
    - Additionally, we should have ...received and ...finished events for each of the other msgs in the "Extended Query" flow, e.g., `Bind`, `Describe`, `Execute`.

Note that if a user is interested only in queries submitted through 1. or 2., and they are not interested in when the parsing and other preparation ended, it will be enough to look at `mz_statement_execution_lifecycle_history` (and optionally join it with `mz_statement_execution_history` and `mz_prepared_statement_history` to get more info about the execution and/or statement). They will still be able to get the end-to-end time, because `ExecutionMessageReceived` will be part of `mz_statement_execution_lifecycle_history`.

If a user is interested also in prepared statements, i.e., 3. or 4, then they would need to look at also `mz_statement_lifecycle_history. In this case, a 4-way join between `mz_statement_lifecycle_history`, `mz_statement_execution_lifecycle_history`, `mz_statement_execution_history`, and `mz_prepared_statement_history` would be needed.

Also, I'm thinking to add a column to `mz_prepared_statement_history` to indicate which of 1., 2., 3., or 4. was used to submit the statement.

An important design constraint is that we have to make sure that Filter pushdown into Persist works for showing the lifecycle events of a query execution. With the above reorganization, this would still be true: When showing the details page for a query execution, the Console has an `execution_id` and a `statement_id` in hand, which it can use to do two simple queries:
- query `mz_statement_execution_lifecycle_history` with a filter on `execution_id`, which amenable to Filter pushdown since the uuid_v7 change.
- query `mz_statement_lifecycle_history` with a filter on `statement_id`, which is similarly amenable to Filter pushdown.

TODO: Add "last result byte sent", but be careful with how we interpret this! We don't want to simply move `finished_at` to this, because the time it takes to transfer query results depends on not just Materialize, but also the network and the receiving external system. Also, depending on TCP's ACKing behavior, it might happen that the user can already start processing the result even before we get the ACK of the last result byte sent, leading to misleading `finished_at` measurements.

### Examples

For a query submitted through 1.:
- `FrontendMessage::Query` arrives in `protocol.rs`, to which
    - `mz_statement_lifecycle_history` would get the following events:
        - `StatementReceived`
        - `ParsingFinished`
        - `PrepareFinished`
    - `mz_statement_execution_lifecycle_history` would get the following events:
        - `ExecutionMessageReceived` (roughly corresponds to last byte of the query arriving) (with the same timestamp as the above `StatementReceived`)
        - `ExecutionBegan` (already exists, emitted in the Coordinator's `begin_statement_execution`)
        - `OptimizationBegan` (new event, but not mentioned above, because it's orthogonal to the prepared statement issues) (this should be very close in time to `ExecutionBegan` if things are behaving correctly, but I have a suspicion that this is not always the case)
        - `OptimizationFinished` (already exists)
        - `StorageDependenciesFinished` (already exists)
        - `ComputeDependenciesFinished` (already exists)
        - `ExecutionFinished` (roughly corresponds to first byte of the result sent to user)

For a query submitted through 4.:
- `FrontendMessage::Parse` arrives in `protocol.rs`, to which
    - `mz_statement_lifecycle_history` would get the following events:
        - `StatementReceived`
        - `ParsingFinished`
        - `PrepareFinished`
- `FrontendMessage::Bind` arrives in `protocol.rs`, to which
    - `mz_statement_lifecycle_history` would get the following events:
        - `BindReceived`
        - `BindFinished` (whose timestamp would probably be very close to `BindReceived`, but later we might do more interesting things at bind time, in which case binding will take more time)
- `FrontendMessage::Execute` arrives in `protocol.rs`, to which
    - `mz_statement_lifecycle_history` would get the following events:
        - `ExecuteReceived`
        - `ExecuteFinished`
    - `mz_statement_execution_lifecycle_history` would get the following events:
        - `ExecutionMessageReceived` (with the same timestamp as the above `ExecuteReceived`)
        - `ExecutionBegan`
        - `OptimizationBegan`
        - `OptimizationFinished`
        - `StorageDependenciesFinished`
        - `ComputeDependenciesFinished`
        - `ExecutionFinished` (time will be the same as the above `ExecuteFinished`)

## Alternatives

One could imagine trying to keep things simple by just quickly implementing the events only for statements that are not prepared statements. however, it turns out that [a big user](https://github.com/MaterializeInc/accounts/issues/3) is actually using prepared statements, because PowerBI always uses the "Extended Query" flow of pgwire. And since having visibility into that user's PowerBI query latency is one of the main motivations of this work, we should do the right thing and support prepared statements.

Another simplification would be to denormalize the schema of the above internal tables in some way. For example, for each query execution, we could just copy the statement-level events into each of the query executions.

## Open questions

When or whether we will need to also measure the things mentioned in the "Out of Scope" section.
