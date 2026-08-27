---
source: src/adapter/src/active_compute_sink.rs
revision: a1bcaebfe6
---

# adapter::active_compute_sink

Defines the coordinator's bookkeeping for running compute sinks: `ActiveComputeSink` (an enum over `ActiveSubscribe` and `ActiveCopyTo`), `ActiveCopyFrom`, and the `ActiveComputeSinkRetireReason` enum.
`ActiveSubscribe` processes incoming `SubscribeBatch` responses from the controller, sorts rows according to the requested output envelope, and forwards them to the client channel.
`ActiveSubscribeOwner` is an enum distinguishing who owns a subscribe: `Session { conn_id, session_uuid }` for subscribes belonging to a SQL session, and `Background` for subscribes owned by a coordinator background task. Background subscribes are always `internal` (no `mz_subscriptions` row). `ActiveSubscribe` holds an `owner: ActiveSubscribeOwner` field replacing the former flat `conn_id`/`session_uuid` fields. `introspection_session_uuid()` returns `Some(uuid)` only for non-internal session subscribes; `connection_id()` returns `Some` only for session subscribes. `ActiveComputeSink::connection_id()` returns `Option<&ConnectionId>`, returning `None` for background subscribes and `Some` for session subscribes and copy-to sinks.
`ActiveSubscribe` carries an `internal: bool` field; when `true`, the subscribe is not advertised via `mz_subscriptions` (builtin table updates are skipped in both `add_active_compute_sink` and `remove_active_compute_sink`).
In the upsert envelope path, the number of value columns is computed as `self.arity.saturating_sub(order_by_keys.len())` (stored in a local `value_columns` variable) and guarded by a `soft_assert_or_log!` that the KEY column count does not exceed the relation arity, preventing a potential coordinator OOM from integer underflow if the planner were to produce an invalid plan.
`ActiveCopyTo` holds the oneshot channel used to return the final row count once the COPY TO operation completes.
All active sinks must be retired via `retire` before being dropped, which notifies the client of the outcome (success, cancellation, dependency drop, or buffer exceeded).
`SubscribeBacklogAccounting` tracks the per-message memory footprint of subscribe messages queued in the channel but not yet drained by the client writer. The producer records each message's footprint via `push`; the receiver side calls `pop` as it drains. `backlog_size` returns the bytes queued behind the message currently being drained (the front message is always tolerated, however large), and the coordinator checks this against `max_buffered_bytes` after each `process_response` to decide whether to retire the subscribe with `ActiveComputeSinkRetireReason::BufferExceeded`. A fixed per-message overhead (`SUBSCRIBE_MESSAGE_OVERHEAD_BYTES` = 1024 bytes) is charged on top of every message's payload so that frontier-only progress messages, which carry no rows, still count against the budget.
