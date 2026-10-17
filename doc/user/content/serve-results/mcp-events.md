---
title: "Subscribe to query results via MCP"
description: "Stream changes to SELECT results through the agent MCP endpoint."
menu:
  main:
    parent: "serve-results"
    weight: 56
    name: "MCP events"
---

{{< private-preview enabled-by-default="false" />}}

The agent MCP endpoint provides a `subscribe` event that streams changes to a
`SELECT` result using server-sent events (SSE). Each HTTP connection owns one
subscription. Subscriptions are ephemeral and end when the connection closes.

The wire format follows the experimental [MCP Events proposal at revision
28ec35e](https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/28ec35e905daa241f019981e2836b4a02f1c0368/docs/design-sketch-proposal.md).

The feature requires `enable_mcp_agent_events`, which enables the MCP Events
methods, and `enable_mcp_agent_subscribe`, which offers the `subscribe` event.
Both are disabled by default in production. Events are available on `/api/mcp/agent` and use that endpoint's
existing authentication and Origin policy. The developer MCP endpoint does not
provide subscriptions.

## Discover the event

Send a JSON-RPC request to `/api/mcp/agent`:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "events/list"
}
```

When enabled, the response advertises `subscribe` and its input schema. When
disabled, the server does not advertise or accept the event.

## Open a subscription

Send an HTTP POST to `/api/mcp/agent` with `Content-Type: application/json` and
`Accept: application/json, text/event-stream`:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "events/stream",
  "params": {
    "name": "subscribe",
    "arguments": {
      "query": "SELECT id, status FROM public.orders WHERE id = $1::bigint",
      "parameters": [42],
      "cluster": "quickstart",
      "snapshot": false,
      "ttlMs": 3600000
    },
    "cursor": null
  }
}
```

| Argument | Meaning |
| --- | --- |
| `query` | Required. One `SELECT` statement. Writes, multiple statements, and explicit `SUBSCRIBE` statements are rejected. |
| `parameters` | Optional array of JSON scalars bound to `$1`, `$2`, and so on. Objects and arrays are rejected. Use SQL casts when a parameter's type is ambiguous. |
| `cluster` | Required cluster in which to run the query. The caller must have `USAGE` on the selected cluster. |
| `snapshot` | Optional boolean, default `false`. Set to `true` to receive the initial result followed by changes. |
| `ttlMs` | Optional positive lifetime in milliseconds, default `3600000` (one hour). Cannot exceed the configured maximum lifetime. |
| `cursor` | Optional. `null` starts a new subscription. A cursor from an earlier notification resumes from that point. See [Resume from a cursor](#resume-from-a-cursor). Cannot be combined with `snapshot: true`. |

The caller needs the privileges required for normal SQL reads of the query.
Privileges are checked when the subscription starts, as for a `SUBSCRIBE`
statement. Revoking a privilege does not end an active subscription. Dropping
or replacing a query dependency ends it. Open a new subscription to use a
replacement view definition.

The successful response has `Content-Type: text/event-stream`. Each SSE `data`
field contains a JSON-RPC message. Startup validation errors are returned as
ordinary responses before the subscription becomes active.

## Read the stream

The first notification is `notifications/events/active`. Every notification
contains the opening request ID in
`params._meta["io.modelcontextprotocol/subscriptionId"]` and a `params.cursor`.
Use this request ID to correlate notifications on this connection. The `active`
notification includes `truncated: false`.

`params.cursor` is the subscription's progress, a logical timestamp encoded as a
decimal string. A cursor of `T` means every change at a logical timestamp less
than `T` has been delivered. The cursor is `null` until the subscription first
makes progress, and never decreases.

The largest cursor is `18446744073709551616`, one past the maximum SQL logical
timestamp. A finite query can deliver its last changes at that maximum timestamp.
Resuming from this cursor delivers no further changes.

All changes at one logical timestamp arrive together as a
`notifications/events/event`:

```text
data: {"jsonrpc":"2.0","method":"notifications/events/event","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":2},"cursor":"1720000000001","name":"subscribe","timestamp":"2026-10-05T12:00:00Z","eventId":"event-id","data":{"logicalTimestamp":"1720000000000","final":true,"columns":[{"name":"id","type":"int8"},{"name":"status","type":"text"}],"changes":[{"operation":"delete","count":"1","row":["42","pending"]},{"operation":"insert","count":"1","row":["42","ready"]}]}}}

```

`logicalTimestamp` is the SQL subscription's logical time, encoded as a decimal
string. It is not the time the HTTP client received the message. Each change's
`operation` is `insert` or `delete`, and `count` is a positive decimal string
that represents the row's multiplicity. An update appears as a deletion of the
old row and an insertion of the new row in the same event. Results have multiset
semantics. Apply `count` copies of the corresponding insertion or deletion.

`columns` is ordered metadata, and each `row` has a value at the corresponding
position. Duplicate column names retain their separate positions. Each `row`
cell is a JSON string containing the value's PostgreSQL text representation, or
JSON `null` for SQL NULL. This preserves numeric precision and array dimension
bounds. Boolean values are `"t"` or `"f"`. A JSONB null value is the string
`"null"`, which is distinct from SQL NULL. Use the corresponding column type to
decode each cell.

When a timestamp's changes would exceed `mcp_max_response_size`, they are split
across several events with the same `logicalTimestamp`. Every part except the
last has `final: false` and leaves the cursor unchanged. The last part has
`final: true`, and its cursor is past `logicalTimestamp`. Apply a timestamp's
changes once its final part arrives.

`name` identifies the event as `subscribe`. `timestamp` is the RFC 3339 time
when the notification was created. `eventId` identifies a notification within
the stream. Resume with `cursor`, not `eventId`.

The server sends `notifications/events/heartbeat` on a timer, including when no
rows change or progress is stalled. A heartbeat is evidence that the connection
is alive. An unchanged cursor does not mean the data is fresh.

When the server ends the stream, it sends `notifications/events/terminated` with
`params.error` containing a JSON-RPC error `code`, a `message`, and
`data.reason`. It then sends the final JSON-RPC response for the opening request:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": {
    "_meta": {}
  }
}
```

Transport failures and client disconnects can prevent delivery of the terminal
notification and final response. Treat an unexpected connection closure as a
lost subscription.

## Baselines and reconnects

With `snapshot: false`, existing rows are suppressed. Only changes beyond the
subscription's initial logical baseline are delivered. The `active` notification
confirms activation, but does not prove that the initial progress frontier has
arrived.

Running a separate query and then opening a changes-only subscription leaves a
race between the query and the subscription baseline. Changes in that interval
can be missed. To maintain a complete result, open with `snapshot: true` and
build the result from that stream's initial insertions and subsequent changes.

## Resume from a cursor

After a disconnect or a terminated stream, send `events/stream` again with the
same arguments, `snapshot` absent or `false`, and the last cursor you received.
The new stream delivers every change at a logical timestamp at or after the
cursor. Because a final event's cursor is past its timestamp, you miss no
changes and receive none twice. Discard the parts of a timestamp whose final
part had not arrived. The new stream resends that timestamp in full.

Resuming requires the subscription's inputs to still retain history back to the
cursor. By default, Materialize retains about one second of history, so resume
from a materialized view created with [`RETAIN HISTORY`](/serve-results/durable-subscriptions/)
when reconnects must not resend the full result. If the history is gone, the
server rejects the request:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "error": {
    "code": -32011,
    "message": "history unavailable for cursor",
    "data": { "reason": "history_unavailable" }
  }
}
```

Then open a new subscription with `cursor: null` and `snapshot: true`, and
rebuild the result.

There is no durable state, acknowledgment, or webhook delivery. The server
keeps no state for a closed stream. A cursor only identifies a logical time.

## Resource limits

| System parameter | Default | Limit |
| --- | --- | --- |
| `mcp_events_max_per_role` | `16` | Active event streams for one Materialize role, shared across listeners. |
| `mcp_events_max_concurrent` | `128` | Active event streams on this server process, shared across listeners. |
| `mcp_events_max_lifetime` | `24h` | Maximum requested stream lifetime. |
| `mcp_events_heartbeat_interval` | `30s` | Interval between timer heartbeats. |
| `mcp_max_response_size` | `1000000` bytes | Maximum serialized event notification size, including its envelope. |
| `mcp_request_timeout` | `60s` | Subscription startup timeout. |

The subscription lifetime is also bounded by authentication expiry. Disconnecting
releases the subscription's resources and admission slot. Slow consumers, an
oversized event, execution errors, dependency loss, or expiration
can terminate a stream. Read and apply notifications promptly, and narrow the
query if its rows exceed the event payload limit.
