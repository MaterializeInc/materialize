# Report server-side time to first row to SQL clients

- Associated: [CS-218](https://linear.app/materializeinc/issue/CS-218)
- Based on: [database-time proposal, #38443](https://github.com/MaterializeInc/materialize/pull/38443)
- Status: draft. Measurement and delivery direction proposed, batch attribution
  and validation still open. This document does not specify an implementation.

## The problem

The Console SQL Shell shows `Returned in 148ms`. This measures elapsed time at the
client, including work in Materialize, result transfer, and client-side delays.
It describes the user's wait, but cannot say how much of that wait Materialize
observed. The same distinction matters to other SQL clients.

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
  are unaffected, and the Console remains usable against older servers.

## Proposal

Expose opt-in **server-side time to first row (TTFR)** for reads through pgwire
and the HTTP/WebSocket SQL APIs. Use the existing notice facility, with a stable
machine-readable discriminator and structured timing data. The Console displays
it beside its existing client-observed completion time:

```text
Returned in 148ms · 12ms server TTFR
```

This answers when Materialize began making rows available, not when the browser
received its first row or when all server work finished. The tooltip states that
preparation and binding are excluded and result retrieval and processing can
continue afterward. The difference from client completion time is not “network
time” or “client time”.

Writes and other commands report completion durations instead, with labels that
distinguish staging and commit from read TTFR. Returning rows from a write does
not replace its staging or commit measurement with a first-row measurement.

### Why TTFR fits Materialize reads

An ordinary compute peek does not stream partial query answers from individual
workers to the SQL client. The replica response waits for all worker responses.
For a successful offloaded peek, the scan and any stash upload finish before the
worker responds. Thus, by the first adapter result, the chosen replica's work to
produce the peek result has completed. This does not mean that ongoing dataflow
maintenance has stopped.

For inline results, the adapter receives the result collections and performs
merging and finishing before exposing the row iterator. Much of the subsequent
work is iterating/projecting rows, protocol encoding, and delivery. Waiting until
the last row is handed to the transport mixes initial query latency with those
costs and slow-client backpressure. TTFR is a useful diagnostic alongside the
client timer precisely because it stops earlier.

Large stashed results qualify this argument: the response can contain references
to completed persist batches, not all the rows in `environmentd` memory. The
adapter fetches, consolidates, re-encodes, and incrementally finishes these results.
Some of that work can happen after the first row, so TTFR is not total database
work even though the replica has produced its result. Constant queries can also
be answered within `environmentd`, without a replica round trip. The common
contract is the adapter's first-row observation, not a particular compute path.

These boundaries follow from the code. Whether client consumption dominates the
remaining elapsed time is a workload-dependent question to measure, not an
assumption needed to justify TTFR.

### What the duration means

Start when Materialize begins executing a bound statement. Preparation and binding
are outside this interval. A prepared statement can be executed repeatedly, so
charging its preparation to an arbitrary execution is not a useful default.
This boundary also excludes real server work, not just client/network time.

| Operation | End of the measured interval |
| --- | --- |
| Read returning rows | The server-side result consumer first observes an actual result row, before protocol encoding |
| Read with an empty result | Successful empty-result completion is established. Display this as an empty-result duration, not a literal first row |
| Single-statement implicit write transaction | The implicit commit has completed successfully |
| Write inside an explicit transaction | The statement has finished executing and staging its changes, without waiting for a later commit |
| Explicit `COMMIT` | The commit has completed successfully, measured from the start of executing `COMMIT` |
| DDL, session commands, other transaction control | The command completes according to its SQL semantics |

An empty intermediate batch is not a first row and does not establish that the
result is empty. This distinction matters for stashed results and must hold across
transports. Observing a first row is also not proof of successful completion:
under this proposal's success-only scope, a later failure must not be presented
as a successfully completed read with a timing.

TTFR includes execution work and waits up to the observation, but excludes
subsequent result processing and delivery. It is less exposed to result-streaming
backpressure than last-row timing, not independent of transport: scheduling and
protocol messages sent before the result is polled can delay the observation.
For DDL, completion does not mean that all ongoing work initiated by the command,
such as maintaining a materialized view, has finished.

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
delivery. Read TTFR stops at result observation before protocol encoding. Write
completion still depends on the transaction boundary, so there is no universal
end point for all statement types. The time a value is delivered to the client
does not redefine the interval it measures.

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

- The [compute response merger](../../../src/compute-client/src/service.rs) waits
  for every worker shard, and the
  [offloaded peek](../../../src/compute/src/compute_state/peek_offload.rs) finishes
  scanning and stash upload before responding. The
  [adapter result stream](../../../src/adapter/src/coord/peek.rs) distinguishes
  fully received inline results from incrementally retrieved stashed results.
- The [existing TTFR histogram](../../../src/adapter/src/client.rs) records the
  first `Rows` response, even if that batch is empty. It establishes a useful
  measurement boundary but does not exactly implement the proposed actual-row
  contract. Do not publish its current value as TTFR without resolving this.
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

**Report last-row/total server elapsed time (the original proposal).** This
captures result processing that TTFR omits, including retrieval of stashed rows,
and is useful when the consumer needs the entire result. It can expose a slow
tail hidden by an early first row. But pgwire and WebSocket also wait for slow
clients while streaming, whereas buffered HTTP builds its response before
transfer. This makes the number both client-sensitive and transport-dependent.
Prefer TTFR as the added read diagnostic while retaining client completion time.
A first row in 2ms followed by a long transfer is not a misleading TTFR, provided
we do not call it completion time. Last-row timing remains a useful possible
secondary diagnostic, not the proposed headline metric.

**Exclude socket waits from total time.** This would better separate result
processing from client backpressure, but needs more measurement and a precise
definition of which waits to remove. It still does not measure CPU time. It is a
different diagnostic rather than a prerequisite for reporting first-row latency.

**Time to the first page or batch.** One row followed by a long stall is not a
responsive result browser. Time to a useful page can better describe that
experience, but requires choosing a page size. First-batch timing matches an
existing observation, but batch size is an implementation detail and a batch can
be empty. Prefer an actual first-row contract for this API.

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
of TTFR beside client completion over realistic browser connections. Cover inline
and stashed results, constant queries, truly empty results, empty intermediate
batches, and slow readers. Distinguish delays before the first-row observation
from delays retrieving and delivering the rest. Check that a later failure is
not presented as success. Do not assume the tail is client-dominated. Use
per-execution observations, since aggregate histograms cannot attribute costs to
the same query. Compare with client timers such as `psql`'s `\timing` as measures
of user wait, not as ground truth for server execution.

Exercise explicit and multi-statement implicit transactions, extended-protocol
commit delays, errors at commit, and suspended portals. In an explicit
transaction, inserting a client pause between a write and `COMMIT` must not
inflate either duration. Delaying commit processing must affect the `COMMIT`
duration, not the earlier write's staging duration. For a bare implicit write,
that commit processing must be included in its total. Check that timing never
lands on another statement or includes an unexplained client pause. Check both
supported and older servers and notice filtering.

Before proceeding, decide:

1. Does server TTFR provide a useful distinction from client completion across
   inline and stashed results? Are the labels clear about read readiness versus
   write/commit completion, and do they describe empty results accurately?
2. How will shared commit cost and delayed notices be represented and associated
   across all three transports?
3. How will the Console detect support without errors or hidden unrelated notices
   on older servers?

Once these are resolved, choose the smallest implementation that meets this
contract. Exact field names, storage of timestamps, emission sites, and PR
sequencing are implementation choices, not requirements of this design.
