# Statement Logging (Chapter One)

<!-- - Associated: (Insert list of associated epics, issues, or PRs) -->

<!-- <\!-- -->
<!-- Note: Feel free to add or remove sections as needed. However, most design -->
<!-- docs should at least keep the suggested sections. -->
<!-- -\-> -->

## Context

There exists no -- or very limited -- visiblity into the body of
statements that users are running against Materialize, either for us
or for the users themselves. This creates several problems. Some
examples, in no particular order:

* We can't allocate engineering resources effectively. For example, we
  don't know which query patterns are particularly common and thus
  that we should put particular effort into optimizing.
* Administrators in large organizations have no way to see what
  queries people (or machines) in their organization are commonly
  issuing
* If a user's cluster is crashing, support (or the users themselves)
  are not able to attempt to debug by checking whether a particularly
  expensive or dangerous query was run recently.

Note that two meaningfully distinct features arise from these. The
first is a user-visible statement log; that feature is the subject of
this design doc ("Chapter One"). An upcoming design doc ("Chapter
Two") will discuss the second feature: logging statements in _our_
infrastructure, for our own benefit. It is thought that implementing
Chapter One is a prerequisite for implementing Chapter Two.

## Goals

* Administrators should be able to access a table that records a
  representative sample of the statements issued in the recent past
  (e.g., last 30 days), along with other relevant information (e.g.,
  timestamp information, time to return, current cluster, current
  user, plan information, etc.)

## Non-Goals

* Chapter Two (getting the queries into our systems for our own
  analytics) is explicitly out of scope of _this_ design, but is
  necessary to unlock its full value, and so should be done as a
  follow-up ASAP.
* 100% durability of queries for all customers is permanently out of
  scope. We have to accept the possibility of losing data under
  adverse conditions. The reason for this design choice is that
  otherwise we would have to persist the text of every statement (in
  either Cockroach or S3), which we consider unacceptable for `SELECT`
  queries.
  In the future, we may allow such durability as a specially feature
  for users who request it, are willing to pay for it, and are also
  willing to accept increased latency on the read path. It is not hard
  to implement, but we would want to be careful about the tradeoffs
  involves. That feature is, however, _temporarily_ out of scope;
  i.e., it is not discussed further in this document.

## Overview

The coordinator maintains a buffer of pending events related to
statement execution. Whenever a prepared statement is created, a
corresponding event is added to the buffer. Whenever a statement is
executed, a system var called `statement_history_sample_rate` is
queried, and a random number between 0 and 1 is generated. If the
random number is greater than the sampling rate, a description of the
statement invocation is written into the buffer.

Every so often (controlled by `statement_log_flush_interval`), the
buffer is converted to updates that are written out to
`mz_statement_execution_history` and `mz_prepared_statement_history`,
with a column in the former referencing the latter. These system
tables are persistent, but entries older than `statement_log_max_age`
are periodically deleted.

We will also create an `mz_session_history` table that will track all
user sessions, so that we can track which statements were issued by
which users.

## Detailed description

The coordinator maintains a buffer of pending events, which can be of
the following variants:

``` rust
enum StatementLogEvent {
    StatementPrepared {
        id: Uuid,
        sql: String,
        name: String, // "" for the default name
        session_id: Uuid,
    },
    StatementExecutionBegan {
        id: Uuid,
        prepared_statement_id: Uuid,
        params: Vec<Datum>,
        occurred_at: DateTime,
        // Additional metadata might eventually
        // go here, e.g. `plan: Option<OptimizedMirRelationExpr>`
    },
    StatementExecutionEnded {
        id: Uuid,
        occurred_at: DateTime,
        was_fast_path: bool,
        result: StatementResult,
    },
}
```

where `StatementResult` is defined as follows:

``` rust
enum StatementResult {
    Canceled,
    Error {
        message: String,
    },
    Success {
        // None for statements that don't return a result
        rows_returned: Option<usize>,
    }
}
```

The `StatementPrepared` and `StatementExecutionBegan` events are
generated and stored in the buffer in `fn handle_execute`, after
checking a random value against `statement_history_sample_rate` . This
requires keeping track of the prepared statement bound to each name,
and whether it has been logged yet (as we don't want to log each
statement several times). The `StatementExecutionFinished` events are
generated in several different places, depending on the type of
statement. For example, the event for `SELECT` queries is generated in
response to the `ControllerResponse::PeekResponse` message.

The coordinator also holds on to an interval object whose tick rate is
controlled by `statement_log_flush_interval`. It listens for ticks
on that interval in its main `serve` loop; when such a tick occurs, it
emits data to the system tables, as follows:

* Each `StatementPrepared` event generates an entry in
  `mz_prepared_statement_history`.
* Each `StatementExecutionBegan` event generates an entry in
  `mz_statement_execution_history`, and records that entry for
  retraction as part of a later update.
* Each `StatementExecutionEnded` event generates an update to the
  entry described above.

These entries are consolidated and then emitted to the builtin
tables, and also to the catalog, ensuring durability.

On system restart, all entries in `mz_statement_execution_history`
older than `statement_log_max_age` are deleted. Then "garbage
collection" is done to delete entries in
`mz_prepared_statement_history` and `mz_session_history` that are no
longer referenced.

The schemas of the tables are as follows. Note that deciding on a
particular schema now doesn't preclude us from adding more fields in
the future, if we decide we want more granular information about
particular queries (for example, whether WMR was used).

**mz_prepared_statement_history**

| Field         | Type                       | Meaning                                            |
|---------------|----------------------------|----------------------------------------------------|
| `id`          | `uuid`                     | The unique identifier of the prepared statement.   |
| `session_id`  | `uuid`                     | The ID of the session that prepared the statement. |
| `name`        | `text`                     | The name of the prepared statement.                |
| `sql`         | `text`                     | The SQL text of the statement.                     |
| `prepared_at` | `timestamp with time zone` | The timestamp at which the statement was prepared. |

**mz_statement_execution_history**

| Field                   | Type                       | Meaning                                                                                                  |
|-------------------------|----------------------------|----------------------------------------------------------------------------------------------------------|
| `id`                    | `uuid`                     | The unique identifier of the statement execution.                                                        |
| `prepared_statement_id` | `uuid`                     | The unique identifier of the corresponding item in `mz_prepared_statement_history`.                      |
| `sample_rate`           | `float8`                   | The value of `statement_history_sample_rate` at the time of execution.                                   |
| `params`                | `text list`                | The textual representations of the supplied arguments.                                                   |
| `began_at`              | `timestamp with time zone` | The time at which the coordinator received the command and began executing the statement.                |
| `finished_at`           | `timestamp with time zone` | The time at which the statement finished, or null if it is still running.                                |
| `was_successful`        | `bool`                     | Whether the statement terminated successfully, or null if it is still running.                           |
| `was_canceled`          | `bool`                     | Whether the statement was canceled, or null if it is still running.                                      |
| `was_aborted`           | `bool`                     | Whether the statement was aborted; that is, `environmentd` crashed before execution finished.            |
| `error_message`         | `text`                     | The error message. Non-null if and only if the statement finished without succeeding or being cancelled. |
| `rows_returned`         | `bigint`                   | How many rows the statement returned, or null if it is still running, errored or was canceled.           |
| `was_fast_path`         | `bool`                     | Whether the statement executed on the fast path, or null if it is still running.                         |


**mz_session_history**

| Field              | Type   | Meaning                                               |
|--------------------|--------|-------------------------------------------------------|
| `id`               | `uuid` | The unique identifier of the session.                 |
| `application_name` | `text` | The name of the application that created the session. |
| `user`        | `text` | The name of the user who owns the session.            |


### Observability and Rollout

If we misjudge the size and volume of our typical users' queries, we
will log much more data than we expect. Thus, rollout should proceed
in two phases. First, we should enable the feature in "dry run" mode,
which does not actually log any queries, but tracks (as a Prometheus
metric) how much data would have been logged had the feature actually
been turned on. Only once we validate that it meets expectations
should we actually enable it. We should do something similar before
adding any new columns to the tables (e.g., the plans).

### Testing

For testing, we will probably need a way to make the random sampling
deterministic; this can be accomplished by adding a
`statement_log_random_seed` system var that we set from within
testdrive or SLTs.

## Alternatives

Log all executed statements, rather than sampling. This approach was
rejected because users might come to rely on all statements being logged,
which would require us making all statements durable, dramatically
increasing the cost of high-QPS usage patterns. Instead, we will not
allow setting the sampling rate above some value slightly less than 1,
e.g. 0.99.

## Open questions

* Should we let the user set their own sampling rate, or does this
  have the potential to put unacceptable load on e.g. Cockroach?
* What should be the default values of the system vars?
