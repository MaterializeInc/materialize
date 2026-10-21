# Report server-side execution time to SQL clients

- Associated: [CNS-170](https://linear.app/materializeinc/issue/CNS-170)
- Based on: [database-time proposal, #38443](https://github.com/MaterializeInc/materialize/pull/38443)
- Implementation: the stack [#39930](https://github.com/MaterializeInc/materialize/pull/39930),
  [#39931](https://github.com/MaterializeInc/materialize/pull/39931), [#39932](https://github.com/MaterializeInc/materialize/pull/39932),
  [#39934](https://github.com/MaterializeInc/materialize/pull/39934) and [#39935](https://github.com/MaterializeInc/materialize/pull/39935), mainly
  [#39931](https://github.com/MaterializeInc/materialize/pull/39931) (results after the
  implicit commit) and [#39934](https://github.com/MaterializeInc/materialize/pull/39934)
  (the notice), and [#39564](https://github.com/MaterializeInc/materialize/pull/39564)
  (the Console)
- Status: proposed, with an implementation in review.

## The problem

The Console SQL Shell shows `Returned in 148.0ms`. This measures elapsed time at
the client, including work in Materialize, result transfer, and client-side
delays. It describes the user's wait, but cannot say how much of that wait
Materialize observed. The same distinction matters to other SQL clients.

We want users to distinguish waiting for results to become available from waiting
for the complete result to arrive. This is a diagnostic aid, not an accounting of
CPU time or an exact separation of database and network cost.

## Success criteria

- SQL clients can request a timing for each supported successful execution,
  without sampling, extra privileges, or a second query.
- Each value has a documented start and end and identifies the execution it
  describes. Missing data is never displayed as zero.
- An autocommit write does not look fast merely because its durable commit was
  omitted. Read readiness must not be presented as read completion.
- The Console keeps showing the user's total wait. Clients that do not opt in
  get no new notices, and the Console remains usable against older servers.

## Proposal

Expose opt-in **server-side execution time** for successful statements through
pgwire and the HTTP/WebSocket SQL APIs. For reads, the interval ends at the first
row: **server-side time to first row (TTFR)**. Writes and other commands report
how long they took to complete, and the value names which end point applies.
Returning rows from a write does not replace its staging or commit measurement
with a first-row measurement.

A session opts in with the session variable `emit_execution_time_notice`. Each
successful statement in scope then gets a notice with SQLSTATE `MZ012`, sent
before the statement's completion message, whose detail carries the timing as
JSON. The Console displays it beside its existing client-observed completion
time:

```text
Returned in 148.0ms · 12.0ms in Materialize
```

This answers when Materialize began making rows available, not when the browser
received its first row or when all server work finished. The tooltip states
where the value ends and that delivering the rows is excluded.

On HTTP, WebSocket, and pgwire's simple query protocol, the statement that ends
an implicit transaction commits before its result is reported, for every client,
whether or not it opts in. A write's value can then include its commit, and a
client never sees success for a write whose commit fails (see
[Transactions and association](#transactions-and-association)).

### Why TTFR fits Materialize reads

For ordinary compute-backed reads, the replica completes its work to produce the
query result before returning it to `environmentd`. TTFR therefore captures that
initial query latency without waiting for the entire result to be delivered to
the client. TTFR and client completion answer two useful, distinct questions
without assuming which cost dominates.

A large peek result can be stashed: its response then starts with an inline
batch, which can be empty, and the remaining rows are read back from blob
storage while they are delivered. TTFR ends at the first batch that holds a row,
so an empty inline batch does not end it early. TTFR then includes reading back
the first stashed batch that holds a row, and any batches before it whose rows
an `OFFSET` skipped. Reading back the rest counts as delivery.

### What the duration means

Start when Materialize begins executing a bound statement. Preparation and binding
are outside this interval, except that a SQL `EXECUTE` binds the prepared
statement it runs inside it. A prepared statement can be executed repeatedly, so
charging its preparation to an arbitrary execution is not a useful default.
This boundary also excludes real server work, not just client/network time.

| Statement | `kind` | End of the measured interval |
| --- | --- | --- |
| Read, or another statement that returns rows, such as `SHOW` or `EXPLAIN` | `first_row` | The server-side result consumer first observes a batch with a row, before protocol encoding |
| The same, with an empty result | `empty_result` | The result ends without rows. Display this as an empty-result duration, not a literal first row |
| Write that ends its implicit transaction | `committed` | The implicit commit has completed successfully. pgwire's extended protocol differs, see below |
| Write that reads a table, such as `UPDATE`, `DELETE`, or `INSERT ... SELECT`, which Materialize applies when it executes | `committed` | The statement has completed |
| Write inside an explicit transaction, or before the last statement of an implicit transaction | `staged` | The statement has finished executing and staging its changes, without waiting for a later commit |
| Write with `RETURNING` | `staged` or `committed`, as for the write | The time to its first row, or to the end of an empty result, plus the time its result waited for the implicit commit |
| `COMMIT` | `completed` | The commit has completed successfully, including DDL that the transaction applies at commit |
| DDL, session commands without rows such as `SET`, other transaction control | `completed` | The command completes according to its SQL semantics |

A statement is a write by its syntax: `INSERT`, `UPDATE`, `DELETE`, or
`COPY ... FROM`, and for a SQL `EXECUTE`, the prepared statement it runs. A write
reports `staged` if its transaction still holds uncommitted writes when the
write's result is reported, and `committed` otherwise.

TTFR includes execution work and waits up to the observation, but excludes
subsequent result processing and delivery. For DDL, completion does not mean
that all ongoing work initiated by the command, such as maintaining a
materialized view, has finished.

The notice's detail is a JSON object:

```text
{"kind": "first_row", "duration_us": 12034}
```

Clients must ignore kinds and fields they do not recognize, so new ones can be
added. The notice's message text is meant for people and may change. It names
the end point only for a write whose commit comes later:
`execution time: 0.412 ms (before commit)`. A number that includes the commit
needs no label, because that is what a reader assumes. A read's "first row"
would read as covering less than it does, because Materialize computes the whole
result before the first row.

### Transactions and association

Statement completion and transaction completion are distinct. A successful
statement inside an open transaction is not a promise that the transaction will
commit.

A statement outside `BEGIN` ... `COMMIT` runs in an implicit transaction, which
Materialize commits without a `COMMIT` statement. The statements of one simple
query, or of one HTTP or WebSocket Simple request, share one implicit
transaction. Each query of an HTTP or WebSocket Extended request runs in its
own. The Console's SQL Shell sends Extended requests, so a write there reports
`committed`, and `staged` appears only inside `BEGIN` ... `COMMIT`.

Inside an explicit transaction, each write reports how long executing and staging
that write took. The final `COMMIT` reports how long applying the commit took,
including waits required to complete it durably. Neither interval includes the
client's pauses between statements. Do not measure from `BEGIN` to `COMMIT`, or
from an individual write until the eventual commit: the client controls how long
the transaction stays open. Commit cost belongs to `COMMIT`, not retroactively to
the writes it commits.

For a bare write in a single-statement implicit transaction, execution through
successful implicit commit is a useful total: it covers completing the write,
not just staging it. A timing that includes an implicit commit must not claim
success before that commit succeeds.

Without this design, Materialize reports a statement's result before the
implicit commit that follows it:

- HTTP puts the statement's success in its response. If the commit then fails,
  the response holds both the success and the commit error, and the rest of the
  request still runs.
- WebSocket sends `CommandComplete`, then commits.
- pgwire's simple query protocol sends the last statement's `CommandComplete`,
  then the commit error.

A client can therefore see success for a write that never became durable, and a
commit-inclusive timing could only follow the completion message, where nothing
ties it to its statement. When several statements run in one implicit
transaction, for example `INSERT ...; INSERT ...; SELECT ...` in one simple
query, one commit applies the writes of all of them, so timing it would also
need a rule for which statement pays.

We remove the problem at its source. The statement that ends an implicit
transaction commits before its completion message, as PostgreSQL's
`exec_simple_query` does for the last statement of a simple query. If the commit
fails, the commit error is that statement's result, and the request stops. This
applies to HTTP, WebSocket, and pgwire's simple query protocol. Consequences:

- That statement's interval contains the commit, so `committed` is a
  measurement, not an accounting convention.
- Every timing is final before its statement's result is reported. On
  WebSocket and pgwire the notice precedes the statement's completion message,
  and on HTTP it is in the statement's own `notices` list. No timing arrives
  after the client has moved on to the next statement.
- Earlier writes of a multi-statement implicit transaction report `staged`.
  When the statement that ends the transaction is a query, its time ends at its
  first row, so no value includes the shared commit. When it is another
  statement without rows, such as `SET`, its `completed` value includes the
  commit.
- A read-only implicit transaction also commits before its result is reported,
  so the same rule holds whether or not the transaction wrote anything. The
  commit's coordinator round trip, which otherwise follows the result, then
  delays the read's completion message, but not the end of the request, which
  waits for the commit either way. For a strict serializable read, the commit
  also includes the linearization wait, which can take longer than a round trip
  when the read's timestamp is ahead of the timestamp oracle.

pgwire's extended protocol commits an implicit transaction after
`CommandComplete`: at `Sync`, as PostgreSQL does, or when the next statement
starts. The client decides when to send either, so a commit-inclusive value for
a statement would include a client-controlled delay. A write that waits for that
commit therefore reports `staged`. Writes that are applied when they execute
report `committed` on every transport. A client that wants a write's time
including its commit can use the simple query protocol, or an explicit
transaction, whose `COMMIT` reports the commit.

### Delivery and compatibility

The adapter owns execution and transaction semantics, and the protocol layers
own delivery: a value's interval is fixed before the protocol layer sends it.

Notices are an existing cross-client mechanism and avoid a Console-only API.
Their cost is additional response data and special handling by interested
clients. The notice precedes the statement's `CommandComplete`, or the
`PortalSuspended` of a portal executed in steps (see
[Transactions and association](#transactions-and-association) for HTTP). Each
supported execution gets at most one. Normal notice filtering applies, so opting
in does not override `client_min_messages`.

The Console opts in with a startup option, but only on environments whose
version knows the variable. It already learns each environment's version from
its health check and gates other features on it. An older server would answer
the unknown option with a startup notice, which the Console prints. The Console
checks the version when it connects, using the version its health check last
saw. A connection made right after an upgrade, before the health check has seen
the new version, goes without the notice until the Console reconnects. That only
delays the opt-in: an upgrade cannot be undone after its cutover, so an
environment's version never decreases.

The Console explains where the server-side value ends: in the label where that
changes what the value covers, which is for a write before its commit, and
otherwise in the tooltip. A read's label does not say "first row", for the
reason given under [What the duration means](#what-the-duration-means). The
prototype Console labels reads and other statements "in Materialize", and a
write whose commit comes later "in Materialize, before commit". It shows values
below 0.1 ms as `<0.1ms`, so a very small value is not displayed as zero.

When timing is unavailable, unsupported, or filtered, the Console displays only
its existing elapsed time.

The Console's existing slow-query/Query Insights trigger remains based on the
user's wait, not on the new duration. These are different questions.

## Scope

Cover ordinary successful reads, writes, DDL, session commands, transaction
control, and portals executed in steps. Include a small, documented timing
response, not a new persistent log.

Defer phase breakdowns, failure/cancellation timings, empty requests,
`SUBSCRIBE`, pgwire's `COPY ... TO STDOUT` and `COPY ... FROM STDIN`, and MCP
integration. Long-lived streams need a different latency question. A `COPY` that
streams over pgwire could have a useful duration, but is a separate extension,
not conceptually unmeasurable. `COPY` to or from an object store returns an
ordinary result and is reported like other statements. MCP is deferred without
assuming agents cannot benefit from timing.

The rows of a portal executed in several steps, or of a cursor, are timed at
most once, by the step or `FETCH` that starts executing the portal, if that step
observes the first row or the end of an empty result. The interval then lies
within that step, so it contains no client pause between fetches. Later steps
report nothing, so a portal whose first step observes neither, such as a
`FETCH 0`, is not timed. `DECLARE` reports its own completion like any other
statement.

## Prior designs and current constraints

- The [compute response merger](../../../src/compute-client/src/service.rs) waits
  for every worker shard before returning the peek result. This supports the
  distinction between producing the result and delivering it to the SQL client.
- [Statement Logging (2023)](20230519_statement_logging.md) deliberately chooses
  sampled, buffered history rather than durable recording on every query's
  critical path. Live timing should not depend on making that history complete.
  Current [statement logging](../../../src/adapter/src/statement_logging.rs)
  still samples and throttles records. Reuse relevant observations where useful,
  without inheriting the persistence or sampling policy.
- [Statement Lifecycle Events (2023)](20231204_query_lifecycle_events_logging.md)
  distinguishes dependency waits, execution completion, and returning the last
  row. It explicitly rejects measuring actual client receipt from the server.
  This supports an elapsed-time contract that includes waits, not a claim to
  measure only computation. Its proposed `last-row-returned` event is absent from
  the current `StatementLifecycleEvent` enum, so the design is precedent, not
  evidence that all required instrumentation already exists.
- The [coordinator interface](../reference/adapter/coord_interface.md) explains
  that an execution response can begin further work rather than finish it.
  Without this design, [pgwire](../../../src/pgwire/src/protocol.rs) and
  [HTTP/WebSocket](../../../src/environmentd/src/http/sql.rs) handling place the
  implicit commit after the statement's result (see
  [Transactions and association](#transactions-and-association)).
- [Plan Insights notices](../../../src/adapter/src/notice.rs) provide a dedicated
  SQLSTATE and structured-content precedent. They are produced before
  execution. The execution time notice is sent after execution but before the
  completion message, which committing before the result makes possible for
  writes.
- The [Console notice handler](../../../console/src/platform/shell/machines/webSocketFsm.ts)
  attaches notices to the latest result. A notice that precedes its statement's
  completion message therefore lands on that statement.
- The [Console timing helper](../../../console/src/platform/shell/timings.ts)
  measures the first result from command submission, and a later result from
  the Console's processing of the previous result's `CommandComplete`. In a batch
  these are client-observed intervals, not independent request round trips. The
  server starts the next statement as soon as it has written the previous
  `CommandComplete`, so while the Console is still processing a large result,
  the next statement's `Returned in` can be shorter than its server-side value
  (see [Decided](#decided)).

## Alternatives and tradeoffs

**Report last-row/total server elapsed time (the original proposal).** This
captures result processing that TTFR omits and is useful when the consumer needs
the entire result. It can expose a slow tail hidden by an early first row. But
pgwire and WebSocket also wait for slow clients while streaming, whereas buffered
HTTP builds its response before
transfer. This makes the number both client-sensitive and transport-dependent.
Prefer TTFR as the added read diagnostic while retaining client completion time.
A first row in 2ms followed by a long transfer is not a misleading TTFR, provided
we do not call it completion time. Last-row timing remains a useful possible
secondary diagnostic, not the proposed headline metric.

**Exclude socket waits from total time.** This would better separate result
processing from client backpressure, but needs more measurement and a precise
definition of which waits to remove. It still does not measure CPU time. It is a
different diagnostic, not a prerequisite for reporting first-row latency.

**Time to the first page.** One row followed by a long stall is not a
responsive result browser. Time to a useful page can better describe that
experience, but requires choosing a page size.

The literature supports choosing a metric for the consumer's goal rather than
treating total time as universally better:

- [Oracle's response-time guidance](https://docs.oracle.com/en/database/oracle/oracle-database/23/ccapp/optimizing-queries-response-time.html)
  recommends first-N optimization for interactive consumption, but warns against
  it when the whole result is needed.
- [Ranked Enumeration for Database Queries (2024, revised 2025)](https://arxiv.org/html/2409.08142v2)
  evaluates time to the first k answers, making first and last results distinct
  points on a performance curve. This motivates responsiveness as a separate
  objective, not a claim about Materialize's costs.
- [SQL Server's backpressure guidance](https://learn.microsoft.com/en-us/troubleshoot/sql/database-engine/performance/troubleshoot-query-async-network-io)
  explains how slow client fetching inflates query duration. Conversely,
  [PostgreSQL's EXPLAIN guidance](https://www.postgresql.org/docs/current/sql-explain.html)
  emphasizes total cost for full-result consumption, but its execution timings
  exclude actual client transmission.

**Charge a shared implicit commit to the last write, or report it separately.**
The original proposal charged the commit of a multi-statement implicit
transaction to its last write. That value is not the duration of that
statement: measured from the write's start until the commit, it also includes
the statements after the write, and it can only be reported after their
completion messages. Reporting the shared commit as a separate, transaction-level
value avoids the misattribution, but needs a new message that clients have to
associate with a batch.

**Commit before the result only for transactions that hold writes.** This would
keep the commit's coordinator round trip out of a read's completion message, at
the cost of a second rule for clients to learn and rely on.

**Include the commit for pgwire's extended protocol.** A value that waits for
the commit includes however long the client takes to send `Sync` or its next
statement, so it would violate the rule that no interval includes a client
pause. The write reports `staged` instead.

**Withhold the value for portals executed in steps.** A value that spans fetches
would include the client's pauses between them. The step that starts executing
the portal contains no such pause, so reporting on that step alone needs no
withholding.

**Measure from request arrival or parse.** Includes more server work for simple
queries, but has no single per-execution meaning for reused prepared statements
or separate protocol messages. The original proposal cites low average parse
and bind times from production. An average cannot establish that preparation is
negligible for the slow statements users investigate. Choose and document the
boundary on semantic grounds.

**Read the activity log afterwards.** Useful for historical diagnosis and richer
phases, but sampled/throttled data cannot guarantee a value for this execution.
It also adds a query and monitoring permissions. Share measurement semantics
where they match, not the requirement to persist before reporting.

**Estimate using a WebSocket ping.** Cheap and Console-local, as suggested in the
PR discussion. Subtracting a ping estimates latency, not the transfer time of a
large result or browser work. It is a different, explicitly estimated diagnostic.

**Replace the client timer, or add a WebSocket-only response type.** Replacing the
timer hides part of the user's experience. A dedicated WebSocket response could
make structured association easier, but leaves other SQL clients without the
capability.

**Return the timing as a field of the result.** Most request/response SQL APIs
that report server time do so in the result itself, usually by default, for
example Trino's `stats`, ClickHouse's `statistics.elapsed`, and Cloudflare D1's
`meta.duration`. A field would match that precedent on HTTP and WebSocket. pgwire
has no place for it, so it would leave pgwire clients without the capability,
and the two JSON APIs would need a format change. A notice works unchanged on all
three transports, and sending it before the completion message gives the same
association as a field.

**Opt in on every environment and hide the old servers' startup notice.**
This needs no version knowledge, but it recognizes the notice by its message
text, and that text is not a stable interface.

**Use `EXPLAIN` timing or display every phase.** Useful for deeper diagnosis,
but not the elapsed time of the ordinary statement the user just ran. Keep this
feature to two numbers rather than building a profiler into the result footer.

## Implementation and validation

The implementation covers all three transports and the Console. Its tests check,
per transport, which `kind` each statement type reports, that the notice
precedes the completion message, that a commit failure replaces the result of
the statement that ends the implicit transaction and stops the request, and that
a held-up commit lands in that statement's `committed` value or in `COMMIT`'s,
and not in an earlier write's `staged` value.

In a headless browser driving the SQL Shell against a local server through a TCP
proxy that injects round-trip latency (the browser's developer-tools throttling
does not apply to WebSockets), the server-side value stays flat as the injected
round trip grows, while `Returned in` grows with it. The measurements are in the
discussion of [#39150](https://github.com/MaterializeInc/materialize/pull/39150).

Still to validate: slow readers, whose backpressure TTFR is meant to exclude.

## Decided

Product approved the Console's current display in the review of the user docs
([#39935](https://github.com/MaterializeInc/materialize/pull/39935)):

1. A later statement of a multi-statement input can have a `Returned in`
   shorter than the server's value for the same statement (see
   [Prior designs](#prior-designs-and-current-constraints)). The Console still
   shows both values for later statements, and the SQL Shell documentation
   explains why `Returned in` can be shorter.
2. The labels and the tooltip stay as they are.

## Known gaps

- Statement history can record a statement as successful although its implicit
  commit failed, while the client receives the commit error as that statement's
  result. [SQL-812](https://linear.app/materializeinc/issue/SQL-812) tracks
  recording the transaction's outcome.
- When a failed or cancelled commit, or a rollback after a failed statement over
  HTTP or WebSocket, restores a `SET` or `SET LOCAL` value, clients are not told
  the restored value. Over HTTP, a statement that returns rows does not report
  the values that its successful commit restores either.
  [SQL-815](https://linear.app/materializeinc/issue/SQL-815) tracks announcing
  them.

## Follow-ups

- Report how a read was executed, such as from an index or by a temporary
  dataflow, once [SQL-804](https://linear.app/materializeinc/issue/SQL-804)
  settles user-facing names for these strategies. The internal names would
  otherwise become part of a client-facing format. A new field in the detail is
  compatible with existing clients.
