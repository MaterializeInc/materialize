# Report database time to SQL clients

- Associated: [CS-218](https://linear.app/materializeinc/issue/CS-218/console-response-time-metric-should-show-actual-database-time)

## The Problem

The Console SQL Shell reports one number, `Returned in 148ms`. It is a client-side
round trip: `calculateCommandDuration`
(`console/src/platform/shell/timings.ts:28-50`) subtracts two `performance.now()`
stamps. For the first statement of a command the start is `commandSentTimeMs`
(`console/src/platform/shell/machines/webSocketFsm.ts:224`); for later statements
it is the *previous* statement's `endTimeMs` (`timings.ts:41-43`). The tooltip says
as much.

The number is therefore correct and also unable to answer the question users
actually ask, which is how much of that time was Materialize. That gap is not
Console-specific: a `psql` user and a dbt run are equally unable to separate their
latency from ours.

## Success Criteria

- A client can obtain Materialize's own measure of how long a statement took.
- The number covers the whole of Materialize's work on that statement, not a
  prefix of it. It must not read as fast because a result was slow to stream, and
  it must not read as fast because a write had not yet been made durable.
- Delivery is deterministic: the value is unambiguously attributable to the
  statement it describes, and is never emitted twice for one execution.
- Available on pgwire and the HTTP/WebSocket API, so this is a platform capability
  rather than a Console feature.
- No behaviour change for clients that do not opt in.
- The Console works, and looks deliberate, against environments predating this.

## Out of Scope

- **`SUBSCRIBE`, and `COPY` in both directions.** A bare `SUBSCRIBE` and
  `COPY (SUBSCRIBE ...) TO STDOUT` do not terminate on their own, so "how long did
  this take" has no answer while they run. `COPY (SELECT ...) TO STDOUT` does
  terminate, so the exclusion there is not about termination: it takes `copy_rows`
  (`src/pgwire/src/protocol.rs:2750`, reached from `:2290`), and `COPY ... FROM
  STDIN` takes `copy_from` (`:2403-2414`, completing at `:3241`). Neither passes an
  emission point, and adding two more for a statement class that is rare in a SQL
  shell is not worth it in this change.
- **MCP.** An agent has no use for a latency figure, and it would be added tokens
  on every tool call. MCP also composes its own SQL (`mcp.rs:1254-1272`), so a
  client could not opt in even if it wanted to.
- **A phase breakdown.** `mz_statement_lifecycle_history` already models phases
  (`src/adapter/src/statement_logging.rs:40-58`).

## Solution Proposal

Report **database time**: how long Materialize worked on the statement. Clients opt
in; the Console renders it beside the round trip it already shows.

```
Returned in 148ms · 12ms database time
```

Two numbers, one of which users already understand.

### Why not time to first row

The adapter already records `time_to_first_row_seconds`
(`src/adapter/src/client.rs:2222-2229`) and discards the per-request value, which
makes it the tempting choice. It is the wrong quantity to display. A query
returning 100,000 rows can reach its first row in 2ms and then occupy the server
for hundreds of milliseconds streaming the rest. Reporting 2ms would tell the user
Materialize was instant when it was not, and it would be wrong in precisely the
common case: a SQL shell mostly runs statements that return real result sets.

Time to first row remains useful for diagnosis, so it travels in the payload. It is
not the headline number.

### The measurement

`execute_started` is stamped at entry to `SessionClient::execute`
(`client.rs:809-830`) and is a public field on `RecordFirstRowStream`
(`client.rs:2146`), reachable through the public `InProgressRows.remaining`
(`src/adapter/src/session.rs:1049-1054`). It is also a parameter of
`send_execute_response` (`protocol.rs:2100`) and is destructured alongside the
response on the HTTP/WebSocket path (`sql.rs:1721`). Database time is therefore
one clock read at the point the statement finishes.

Time to first row is likewise already computed in `recv` (`client.rs:2222-2229`);
the design stores the observed `Duration` on the struct so the value shipped is
identical to the one the histogram records.

### Where a statement actually finishes

This is the substantive part of the design, and it is not the same moment for
every statement.

| Statement | Finishes when |
|---|---|
| Row-returning | the last row has been accepted by the transport |
| Write in an implicit transaction | the implicit commit has completed |
| Write in an explicit transaction | its rows are staged; the `COMMIT` reports the commit |
| DDL, `SET`, transaction control | the completion message is about to be sent |

**Writes stage, they do not commit.** `send_diffs` calls
`add_transaction_ops(TransactionOps::Writes(...))`
(`src/adapter/src/coord/sequencer.rs:948`) and returns `ExecuteResponse::Inserted`
immediately (`:974`); the non-constant path is the same
(`src/adapter/src/frontend_read_then_write.rs:988`). The durable commit runs later:
on pgwire at the end of the `Query` message (`protocol.rs:1355-1359`), or via
`txn_needs_commit` (`:1184-1186`, `:2076-2077`) on the extended path; on
HTTP/WebSocket at `sql.rs:1596-1603`.

Anchoring a write to its completion message would therefore report the staging cost
only. An `INSERT` that spent most of its time in group commit would render
`Returned in 350ms · 2ms database time`, which is exactly the failure mode this
design rejects time-to-first-row for. So a write in an implicit transaction emits
after the commit instead.

In an explicit transaction the commit belongs to `COMMIT`, not to any one write, so
each write reports its staging cost and `COMMIT` reports the commit. That is
complete and unambiguous. The one ragged case is a multi-statement implicit
transaction, where a single commit covers every statement in the message
(`protocol.rs:1355-1359` runs after the whole loop): the commit is attributed to
the last write in the message, and the docs say so.

**What the interval includes and excludes.** Parse and bind are excluded, since it
begins at entry to `execute` for an already-bound portal. Serialization is
included.

**Network transfer is not excluded, and this is a real limitation.** Both
transports flush inside the row loop and await the socket, so a slow client applies
backpressure that lands inside the measured interval: `self.conn.flush().await?`
after every pgwire batch (`protocol.rs:2655`), and one awaited frame per row on
WebSocket (`sql.rs:1289-1302`, `:493-499`), whose own doc comment describes the
intent as "flushing between batches so a slow client applies real backpressure"
(`sql.rs:1218-1219`).

The consequence is that database time degrades toward round-trip time exactly when
the network is slow, which is when a user most wants the two numbers separated.
Excluding socket waits would mean instrumenting around every flush and subtracting,
which is a materially larger change than one clock read. The proposal is therefore
to anchor at the last row and state the limitation in the tooltip and the docs
rather than imply a precision the number does not have. If the MVP shows the two
converging on real Console traffic, that is the trigger to reconsider.

**Resumed portals measure elapsed time, not server time.** `execute_started` is
stamped once per `execute` and resumption does not re-stamp: `PortalState::InProgress`
calls `send_rows` directly without a new `Instant` (`protocol.rs:1768-1779`). So for
`DECLARE ... FETCH ... FETCH`, the value emitted on the exhausting `FETCH` spans
every intervening client round trip. Intermediate `FETCH`es emit nothing. Cursors
are therefore explicitly outside the "Materialize's own time" guarantee, and the
docs must say so. The Console is unaffected, since `Fetch` is rejected on HTTP and
WebSocket (`sql.rs:1516-1520`), but `psql` and dbt do use cursors, so this cannot be
left implicit.

**One dependency worth pinning with a test**: an empty result still flips
`saw_rows`, because a zero-row result arrives as a `PeekResponse` with no rows. The
comment asserting this hedges with "currently" (`protocol.rs:2727-2729`), so a
`SELECT` returning no rows should be covered explicitly. If
`recorded_first_row_instant` is ever `None`, `time_to_first_row_us` is omitted from
the payload rather than reported as zero.

### Delivery

Every site queues through `Session::add_notice` and then flushes, rather than
sending a `BackendMessage` directly. That choice matters: `Session::notice_filter`
(`session.rs:558`) is private to `mz_adapter` and only runs on the queue path, so a
direct send would bypass `client_min_messages` and could not fix it from
`mz_pgwire`. Queueing gets the filter for free on every transport.

**pgwire reads: `add_notice` then `send_pending_notices()` (`protocol.rs:3247-3256`)
inside the existing `no_more_rows && !metric_recorded` block (`:2708-2739`).** The
existing statement-scoped flushes cannot carry the value, because all of them run
before it exists: `:1152` and `:1166` on the simple path, `:1748` and `:1762` on the
extended path, each immediately preceding a `send_execute_response`; and `:884` runs
at the top of the next command loop, after `ready()` has sent `ReadyForQuery`.
Flushing explicitly at the emission site sidesteps that entirely, and the queue is
quiescent by then because the loop's live notice consumer (`:2561`, `:2657-2660`)
has exited.

This block is reached by `ExecuteResponse::Subscribing` as well (`:2221-2257`,
calling `send_rows` at `:2243`), so a terminating `SUBSCRIBE ... UP TO` would emit
unless guarded. Emission is therefore conditioned on statement type, not just on
`no_more_rows`.

**pgwire writes and other non-row statements: the `command_complete!` macro
(`:2104-2114`), which resolves `execute_started` from the enclosing function
parameter exactly as it already resolves `tag`.** Five call sites (`:2119`, `:2123`,
`:2219`, `:2418`, `:2462`). Writes in an implicit transaction defer instead to
after `commit_transaction()` (`:1444-1465`), reached at `:1355-1359`.

Three non-row completions bypass the macro and so emit nothing:
`ExecuteResponse::EmptyQuery` (`:2125-2128`, and the zero-statement case at
`:1362-1364`), `COPY ... FROM STDIN` (`:3241`), and a replayed
`PortalState::Completed` (`:1832-1845`).

**HTTP and WebSocket: `add_notice` before the response is built.** Reads queue in
`SqlResult::rows` (`sql.rs:644-735`) before `notices: make_notices(client)` at
`:733`, and in the `None` arm of `stream_ws_peek_rows` (`:1331-1344`), which hands
off to `ws_peek_result` (`:1356-1370`). Non-row statements queue in `SqlResult::ok`
(`:744-750`) before `make_notices` at `:748`, covering both transports at once.
Writes in an implicit transaction defer to after the commit at `:1596-1603`.

Position is deterministic on both. The WebSocket streaming loop's live
`recv_notice` select (`:1208-1209`) has exited by the `None` arm, and the
`emit_streaming_notices` pre-drain (`:1715-1719`) runs before `SqlResult::ok` is
built, so `make_notices` sees only what the emission queues.

**Position relative to `CommandComplete`.** Every site emits *after* it. On pgwire
reads the block at `:2708` runs after the send at `:2706`; the write sites flush
after the macro's own send; on WebSocket the notice is appended after
`CommandComplete` by construction. PR 2 and PR 3 pin this in tests.

### Re-entrancy

Row-returning statements reuse the existing `no_more_rows && !metric_recorded`
guard, whose comment names the exact hazard: "Only record once per stream to avoid
polluting the histogram when an exhausted cursor is FETCHed again"
(`protocol.rs:2708-2709`). Re-fetching an exhausted cursor is already suppressed,
so emission needs **no new flag**. Under `ExecuteCount::Count` (`:988-990`,
bounding the send loop at `:2547-2550`) intermediate batches complete with
`PortalSuspended` and emit nothing.

Non-row statements produce exactly one completion message and cannot be resumed, so
the write and DDL sites need no guard either.

### Wire shape

- Session variable `emit_timing_notice`, `bool`, default `false`, following the
  existing `emit_*_notice` naming (`definitions.rs:1276-1295`, `:1329-1334`).
- A dedicated SQLSTATE alongside `MZ001` (`src/adapter/src/notice.rs:352`) so
  clients dispatch on the code rather than parsing text. No user-facing registry of
  `MZ0xx` codes exists today; PR 5 creates one.
- Payload JSON in the notice's `detail` field:

  ```json
  {"database_time_us": 12000, "time_to_first_row_us": 1200}
  ```

  `database_time_us` is the displayed number. `time_to_first_row_us` is carried for
  tooling, omitted for statements that returned no rows. A large gap between the two
  identifies a fast query with a big result or a slow client, which is a different
  problem with a different fix. Keeping it on the wire means surfacing it later is a
  Console-only change.
- Note that `psql` at default verbosity prints `DETAIL:`, so an opted-in `psql` user
  sees the JSON. That is acceptable for an explicitly requested diagnostic.
- The stale doc comment at `protocol.rs:2034-2037`, claiming `send` filters by
  severity, should be corrected while in the area; the body does not.

### Display

```
Returned in 148ms · 12ms database time      ⓘ
```

The tooltip states what the number covers and, explicitly, what it does not: it
runs from the start of execution to the last row being accepted by the transport,
or for a write to the commit; it excludes parse and bind; and **a slow network
counts against it**, because Materialize waits for the client to accept each batch.

Where no value arrives, the Console renders today's line unchanged:

```
Returned in 148ms                           ⓘ
```

Absence is not a single condition. It means an environment predating the feature,
or one of the named exclusions: `SUBSCRIBE`, `COPY`, an empty query on pgwire, or
an intermediate `FETCH`. All of those are visibly distinct statements, so the
ambiguity a user can actually hit is narrow. It is not the per-statement randomness
a reads-only version would have produced, since an `INSERT` already displays a
duration today (`CommandResult.tsx:157` renders it for every statement type, not
just row-returning ones).

**Query Insights stays pinned to round-trip time.** `PlanInsightsNotice.tsx:128`
calls the same `calculateCommandDuration` helper to decide when a statement has run
long enough to surface insights, on a threshold timer (`:186-209`). Switching it to
database time would quietly raise that threshold by however much of the wait is
network, and a user who waited three seconds wants the insight regardless of where
the three seconds went. PR 4 must not let the change reach it as an inherited side
effect.

## Minimal Viable Prototype

**Run this before PR 1, and gate PR 1 on the result.** The premise is that database
time is materially smaller than round trip, often enough for the distinction to be
worth two numbers. That is currently an inference from a demo report.

On a real environment, compare `mz_time_to_first_row_seconds`,
`mz_result_rows_first_to_last_byte_seconds` (`src/adapter/src/metrics.rs:227-231`),
and what the Shell displays, for a query returning a realistic number of rows over
a browser WebSocket rather than a loopback connection. Two outcomes would change
the plan: if database time dominates the round trip, the feature tells users little
they did not already know; and given that backpressure is inside the interval, if
the two numbers converge for large results then the last-row anchor needs revisiting
before anything ships. Also run an `INSERT`, to confirm the post-commit anchor
produces a number that reflects the write.

## Implementation Plan

Split by semantic change rather than by ownership: PRs 1 to 3 all fall under
`@MaterializeInc/adapter` (`.github/CODEOWNERS:39, 70, 98, 113`), PR 4 under
`@MaterializeInc/console` (`:18`), PR 5 under `@MaterializeInc/docs` (`:19`).

**PR 1 — measurement and session variable.** Store the observed time-to-first-row
`Duration` on `RecordFirstRowStream` (`client.rs`). Add `emit_timing_notice`
(`definitions.rs`) and the `AdapterNotice` variant with its SQLSTATE (`notice.rs`).
Give `BadStartupSetting` a distinct SQLSTATE while here, since it currently carries
`SUCCESSFUL_COMPLETION` (`notice.rs:327`) and PR 4 otherwise has to match on message
prefix; that file is adapter-owned, so it belongs in this PR rather than PR 4. No
emission yet.

**PR 2 — pgwire emission.** Reads: snapshot `execute_started` at `protocol.rs:2689`
alongside the existing snapshot, emit in the `no_more_rows && !metric_recorded`
block, guarded on statement type so a terminating `SUBSCRIBE` does not emit. Non-row
statements: emit in `command_complete!`. Writes in implicit transactions: defer to
after `commit_transaction()`, which requires carrying the statement's
`execute_started` out of `one_query`. Tests: `testdrive` asserting the notice for a
`SELECT` and for an `INSERT` when opted in, that the `INSERT` value exceeds its
staging time, absence when not opted in, a single emission across a second `FETCH`
of an exhausted cursor, no emission on `PortalSuspended`, no emission for
`SUBSCRIBE`, and a zero-row `SELECT`.

**PR 3 — HTTP and WebSocket emission.** Queue in the `None` arm of
`stream_ws_peek_rows`, before `make_notices` in `SqlResult::rows`, and in
`SqlResult::ok`. `SqlResult::ok` currently takes `(client, tag, params)`
(`sql.rs:744`) and needs `execute_started` threaded through its three call sites
(`:1771`, `:1793`, `:1813`), which is the one API change in this design. Writes in
implicit transactions defer to after the commit at `:1596-1603`. Tests alongside the
existing wire-shape tests (`src/environmentd/tests/server.rs:1301-1346`), pinning
position after `CommandComplete`.

**PR 4 — Console.** Opt in at handshake, dispatch on the SQLSTATE, carry the value
through the FSM, render beside the existing number with fallback, rewrite the
tooltip to cover the exclusions and the backpressure caveat, leave the Query
Insights caller on round-trip time, add `timings.test.ts` (none exists). Filter the
`BadStartupSetting` notice an older server returns for an unknown option
(`src/environmentd/src/http.rs:892-903`, `notice.rs:453-455`), using the SQLSTATE
added in PR 1.

**PR 5 — Docs.** `doc/user/content/console/sql-shell.md` (27 lines, silent on this
metric), the notice and session variable in the SQL reference, an `MZ0xx` registry,
and a forward-compatibility sentence in
`doc/user/content/integrations/websocket-api.md`. Must state the three caveats:
network backpressure counts against database time, cursors report elapsed rather
than server time, and in a multi-statement implicit transaction the commit is
attributed to the last write.

### Observability and cost

Both intervals are already recorded as histograms,
`mz_time_to_first_row_seconds` (`metrics.rs:157-162`) and
`mz_result_rows_first_to_last_byte_seconds` (`:227-231`), so no new server metric is
needed and existing dashboards are the check on whether client-reported values are
plausible.

Cost is one extra message per opted-in statement. On WebSocket each message is one
frame (`sql.rs:493-499`).

### Rollout

The Console deploys independently of `environmentd`, so both states are live
simultaneously and indefinitely on self-managed. The fallback is permanent, not
transitional.

## Alternatives

**Report time to first row.** Rejected above: understates Materialize's work
whenever a result set is large, which is the common case in a SQL shell.

**Report both intervals separately in the Shell.** Rejected on display grounds: three
numbers on one line is worse than two, and the boundary between them is not
something a user has a decision to make about. The split ships in the payload, where
it remains useful for diagnosis.

**Start the interval at parse rather than execute.** Rejected on measurement. Across
production us-east-1 over six hours on 2026-08-25, using

```promql
sum by (message_type) (rate(mz_pgwire_message_processing_seconds_sum[6h]))
  / sum by (message_type) (rate(mz_pgwire_message_processing_seconds_count[6h]))
```

`parse` averages 0.050ms and `bind` 0.032ms, and the share of each exceeding the 1ms
first bucket is 0.134% and 0.114%. Against the 12ms worked example above, 82
microseconds is roughly 0.7%, which is at the edge of the Shell's 0.1ms display
quantum (`timings.ts:58-66` uses `toFixed(1)` below one second) and invisible above
one second. It would also cost real complexity: in the extended protocol `Parse`,
`Bind` and `Execute` arrive as separate client messages (`protocol.rs:954-1000`)
with client-controlled gaps, and a prepared statement parsed once and executed many
times has no clean answer for which execution pays the parse cost.

The same telemetry is consistent with optimization happening inside `execute` rather
than at parse time, and therefore already inside the reported number. A mean alone
does not establish that, so it is a supporting observation rather than a finding.

**Replace the total with the server number.** Best demo optic. Rejected: round trip
is a real part of the user's experience, and hiding it trades one inaccuracy for
another.

**A new `WebSocketResponse` message type.** Rejected: it can only reach WebSocket
clients, so it would serve the Console and nothing else, permanently, while adding
to a public API's observable output.

**Deliver through the pre-existing notice flushes.** Rejected: every statement-scoped
flush runs before the value exists, as traced in Delivery. `emit_plan_insights_notice`
escapes this only because it fires *before* execution, added during sequencing
(`src/adapter/src/frontend_peek.rs:1239`, and also
`src/adapter/src/coord/sequencer/inner/peek.rs:792`); its description says "before
executing a SELECT statement" (`definitions.rs:1279`). A post-execution measurement
cannot reuse that shape.

**Tighten the client-side measurement only.** Cheap and confined to the Console.
Rejected: it still measures a round trip, so it does not answer the question, and it
does nothing for other clients.

**Query `mz_recent_activity_log` afterwards.** Rejected: sampled and throttled
(`statement_logging_sample_rate` default 0.1, `definitions.rs:1370-1376`, plus a
token bucket at `src/adapter/src/statement_logging.rs:418-448`), requires
`mz_monitor`, and adds a round trip to display a latency number.

**`EXPLAIN ... WITH(TIMING)`.** Prior art (`src/repr/src/explain.rs:199-200`), scoped
to optimization and requiring the query be rewritten, so it does not close CS-218.
