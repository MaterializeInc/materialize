---
source: src/timely-util/src/builder_async.rs
revision: e79a6d96d9
---

# timely-util::builder_async

Provides `OperatorBuilder`, which wraps timely's synchronous operator builder to let operator logic be written as an async `Future`.
Key types include `AsyncInputHandle` (a `Stream` of `Event<Data|Progress>` events), `AsyncOutputHandle` (with optional back-pressure via `give_fueled`), `InputConnection` (controlling capability routing via `Disconnected`, `ConnectedToOne`, `ConnectedToMany`), and `Button`/`ButtonHandle` for coordinated cross-worker shutdown.
`build_fallible` further wraps the async future to emit errors on a dedicated stream while safely managing capability lifetimes.
