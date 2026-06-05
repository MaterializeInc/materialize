---
source: src/adapter/src/active_compute_sink.rs
revision: 8c16a83847
---

# adapter::active_compute_sink

Defines the coordinator's bookkeeping for running compute sinks: `ActiveComputeSink` (an enum over `ActiveSubscribe` and `ActiveCopyTo`), `ActiveCopyFrom`, and the `ActiveComputeSinkRetireReason` enum.
`ActiveSubscribe` processes incoming `SubscribeBatch` responses from the controller, sorts rows according to the requested output envelope, and forwards them to the client channel.
`ActiveSubscribe` carries an `internal: bool` field; when `true`, the subscribe is not advertised via `mz_subscriptions` (builtin table updates are skipped in both `add_active_compute_sink` and `remove_active_compute_sink`).
In the upsert envelope path, the number of value columns is computed as `self.arity.saturating_sub(order_by_keys.len())` (stored in a local `value_columns` variable) and guarded by a `soft_assert_or_log!` that the KEY column count does not exceed the relation arity, preventing a potential coordinator OOM from integer underflow if the planner were to produce an invalid plan.
`ActiveCopyTo` holds the oneshot channel used to return the final row count once the COPY TO operation completes.
All active sinks must be retired via `retire` before being dropped, which notifies the client of the outcome (success, cancellation, or dependency drop).
