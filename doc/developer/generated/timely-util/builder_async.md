---
source: src/timely-util/src/builder_async.rs
revision: 5427dc5764
---

# timely-util::builder_async

Provides `OperatorBuilder`, which wraps timely's synchronous operator builder to let operator logic be written as an async `Future`.
Key types include `AsyncInputHandle` (a `Stream` of `Event<Data|Progress>` events with synchronous `next_sync` and async `ready`), `AsyncOutputHandle` (with optional back-pressure via `give_fueled`, which accepts an explicit `size_bytes` parameter and works with any `Clone + 'static` element type), `InputConnection` (controlling capability routing via `Disconnected`, `ConnectedToOne`, `ConnectedToMany`), `Button`/`ButtonHandle` for coordinated cross-worker shutdown, and `PressOnDropButton` as a deadman's-switch wrapper.
`OperatorBuilder` exposes `new_input_for`, `new_input_for_many`, `new_disconnected_input`, and `new_input_connection` for flexible input wiring.
`build_fallible` wraps the async future to emit errors on a dedicated stream while safely managing capability lifetimes via `CapabilitySet` references.
