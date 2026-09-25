# Report server-observed execution time to SQL clients

- Associated: [CS-218](https://linear.app/materializeinc/issue/CS-218)
- Based on: [database-time proposal, #38443](https://github.com/MaterializeInc/materialize/pull/38443)
- Status: draft. Measurement and delivery direction proposed, batch attribution
  and validation still open. This document does not specify an implementation.

## The problem

The Console SQL Shell shows `Returned in 148ms`. This measures elapsed time at the
client, including work in Materialize, result transfer, and client-side delays.
It describes the user's wait, but cannot say how much of that wait Materialize
observed. The same distinction matters to other SQL clients.

We want users to distinguish a long execution from a long end-to-end wait,
without replacing one ambiguous number with another. This is a diagnostic aid,
not an accounting of CPU time or an exact separation of database and network cost.

## Success criteria

- SQL clients can request a timing for each supported successful execution,
  without sampling, extra privileges, or a second query.
- Each value has a documented start and end and identifies the execution it
  describes. Missing data is never displayed as zero.
- An autocommit write does not look fast merely because its durable commit was
  omitted. A read does not look fast merely because only its first row was timed.
- The Console keeps showing the user's total wait. Clients that do not opt in
  are unaffected, and the Console remains usable against older servers.

## Proposal

Expose an opt-in, server-observed elapsed execution duration through pgwire and
the HTTP/WebSocket SQL APIs. Use the existing notice facility, with a stable
machine-readable discriminator and structured timing data. The Console displays
the duration beside its existing elapsed time:

```text
Returned in 148ms · 12ms server elapsed
```

“Server elapsed” is a proposed label, chosen to avoid implying that this is pure
database processing time. The tooltip explains that it includes waits during
execution and result production, including slow-client backpressure, but excludes
preparation and final client receipt. The difference between the two numbers must
not be labelled “network time”.

### What the duration means

Start when Materialize begins executing a bound statement. Preparation and binding
are outside this interval. A prepared statement can be executed repeatedly, so
charging its preparation to an arbitrary execution is not a useful default.
This boundary also excludes real server work, not just client/network time.

| Operation | End of the measured interval |
| --- | --- |
| Read returning rows, including an empty result | All result rows have been encoded and handed to the response path |
| Single-statement implicit write transaction | The implicit commit has completed successfully |
| Write inside an explicit transaction | The statement has finished executing and staging its changes, without waiting for a later commit |
| Explicit `COMMIT` | The commit has completed successfully, measured from the start of executing `COMMIT` |
| DDL, session commands, other transaction control | The command completes according to its SQL semantics |

For DDL, completion does not mean that all ongoing work initiated by the command,
such as maintaining a materialized view, has finished. For reads, it does not mean
that the client has received or rendered the result.

Result handling is transport-dependent. pgwire and WebSocket stream rows and can
wait for a slow client during this interval. Buffered HTTP builds a response
before sending it, so its interval does not include the subsequent response
transfer. These are server-observed durations with the same diagnostic purpose,
not transport-independent measurements of query computation.

### Transactions and association

Statement completion and transaction completion are distinct. A successful
statement inside an open transaction is not a promise that the transaction will
commit.

Inside an explicit transaction, each write reports how long executing and staging
that write took. The final `COMMIT` reports how long applying the commit took,
including waits required to complete it durably. Neither interval includes the
client's pauses between statements. Do not measure from `BEGIN` to `COMMIT`, or
from an individual write until the eventual commit: the client controls how long
the transaction stays open. Commit cost belongs to `COMMIT`, not retroactively to
the writes it commits.

For a bare write in a single-statement implicit transaction, execution through
successful implicit commit is a useful total: it covers completing the write,
not just staging it. Keep commit-inclusive timing for implicit transactions,
subject to the batch attribution and protocol-boundary constraints below. A
timing that includes an implicit commit must not claim success before that
commit succeeds.

A batch with several statements and one implicit commit has no natural owner for
the commit cost. The original proposal charges it to the last write. That is an
accounting convention, not the duration of that statement: measuring from that
write's start until commit also includes any following statements. A late notice
can arrive after those statements' completion messages.

Batch attribution must therefore be settled before implementing the public
contract. The choices are to expose shared transaction completion separately, or
choose and clearly label an attribution rule. Neither may silently assign a late
notice to whichever statement the client most recently displayed. The same issue
applies to extended-protocol executions whose implicit commit waits for `Sync`,
potentially including a client-controlled delay. This is part of the SQL-client
scope, not something to assume away because the Console uses WebSocket.

### Delivery and compatibility

The adapter owns execution and transaction semantics. Protocol layers own result
delivery. The timing contract spans both, so neither an adapter response nor a
protocol completion message alone is a universal end point.

Notices are an existing cross-client mechanism and avoid a Console-only API.
Their cost is additional response data and special handling by interested clients.
The contract must establish unambiguous association and at most one value per
supported execution, including when delivery follows statement completion.
Respect normal notice filtering, so opting in does not override
`client_min_messages`.

When timing is unavailable, unsupported, or filtered, the Console displays only
its existing elapsed time. Old-server capability handling must work with the
responses those servers already produce. Giving a startup warning a new code in
new servers cannot make old servers return that code. Do not suppress unrelated
warnings while negotiating support.

The Console's existing slow-query/Query Insights trigger remains based on the
user's wait, not on the new duration. These are different questions.

## Scope

Cover ordinary successful reads, writes, DDL, session commands, and transaction
control. Include a small, documented timing response, not a new persistent log.

Defer phase breakdowns, failure/cancellation timings, empty requests, `COPY`,
`SUBSCRIBE`, and MCP integration. Long-lived streams need a different latency
question. Finite `COPY` could have a useful duration, but is a separate extension,
not conceptually unmeasurable. MCP is deferred without assuming agents cannot
benefit from timing.

For suspended or incrementally fetched portals, withhold the new duration until
there is a contract that distinguishes server execution from pauses between
fetches. This deliberately differs from emitting a cursor-lifetime value with a
caveat: an arbitrarily long client pause should not appear as server execution.

## Prior designs and current constraints

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
  Current [pgwire](../../../src/pgwire/src/protocol.rs) and
  [HTTP/WebSocket](../../../src/environmentd/src/http/sql.rs) handling also place
  implicit commit after statement results. Those boundaries explain why timing
  cannot simply stop at every completion message.
- [Plan Insights notices](../../../src/adapter/src/notice.rs) provide a dedicated
  SQLSTATE and structured-content precedent. Their pre-execution delivery does
  not solve post-commit association. The
  [Console notice handler](../../../console/src/platform/shell/machines/webSocketFsm.ts)
  currently attaches ordinary notices to the latest result.
- The [Console timing helper](../../../console/src/platform/shell/timings.ts)
  measures the first result from command submission, and later results from the
  previous result's completion. In a batch these are client-observed intervals,
  not independent request round trips. Do not assume they have the same bounds as
  a commit-inclusive server interval.

## Alternatives and tradeoffs

**Time to first row.** A useful, cheaper diagnostic of when results become
available, but not a substitute for completion time on large results. Retain it
as a possible secondary diagnostic. The original proposal includes it in the
payload, but that extra public field need not be required for the initial UX.

**Exclude socket waits.** This would better separate server work from client
backpressure, but needs more measurement and a precise definition of which waits
to remove. Subtracting socket waits still does not measure CPU time. Prefer the
simpler elapsed duration only if validation shows it answers the user question.

**Measure from request arrival or parse.** Includes more server work for simple
queries, but has no single per-execution meaning for reused prepared statements
or separate protocol messages. The original proposal reports low average parse
and bind times from production. Those figures have not been independently
verified here, and averages cannot establish that preparation is negligible for
the slow statements users investigate. Choose and document the boundary on
semantic grounds, not on an assertion that excluded work is always cheap.

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
capability. Prefer notices if they can satisfy the association contract.

**Use `EXPLAIN` timing or display every phase.** Useful for deeper diagnosis,
but not the elapsed time of the ordinary statement the user just ran. Keep this
feature to two numbers rather than building a profiler into the result footer.

## Validation and decisions before implementation

No prototype accompanies this document. First validate the meaning and usefulness
of the two numbers over realistic browser connections, with small, empty, and
large results, slow readers, and writes with appreciable commit latency. Use
per-execution observations. Aggregate histograms cannot establish which costs
belonged to the same query. Compare with client timers such as `psql`'s `\timing`
as measures of user wait, not as ground truth for server execution.

Exercise explicit and multi-statement implicit transactions, extended-protocol
commit delays, errors at commit, and suspended portals. In an explicit
transaction, inserting a client pause between a write and `COMMIT` must not
inflate either duration. Delaying commit processing must affect the `COMMIT`
duration, not the earlier write's staging duration. For a bare implicit write,
that commit processing must be included in its total. Check that timing never
lands on another statement or includes an unexplained client pause. Check both
supported and older servers and notice filtering.

Before proceeding, decide:

1. Does the streaming duration remain useful under realistic backpressure, and
   does “server elapsed” communicate its limits? If not, revise the measurement
   rather than relying on a tooltip to rescue a misleading headline.
2. How will shared commit cost and delayed notices be represented and associated
   across all three transports?
3. How will the Console detect support without errors or hidden unrelated notices
   on older servers?

Once these are resolved, choose the smallest implementation that meets this
contract. Exact field names, storage of timestamps, emission sites, and PR
sequencing are implementation choices, not requirements of this design.
