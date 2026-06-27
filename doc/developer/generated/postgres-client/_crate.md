---
source: src/postgres-client/src/lib.rs
revision: d6dff42fd6
---

# mz-postgres-client

Provides `PostgresClient`, a pooled Postgres/CockroachDB client built on `deadpool-postgres`, with configurable pool size, connection TTL, keepalive settings, and TLS.

The crate exposes:
* `PostgresClientKnobs` — a trait for supplying runtime-configurable pool parameters.
* `PostgresClientConfig` — bundles a connection URL, a `PostgresClientKnobs` impl, and `PostgresClientMetrics`. Defaults to `SERIALIZABLE` isolation; callers can supply an `IsolationLevelFn` closure via `PostgresClientConfig::with_isolation` to select the isolation level per connection dynamically (the resolver runs once per new connection, so a dyncfg-backed closure lets a flag change take effect as the pool cycles connections).
* `IsolationLevel` — an enum with `Serializable` and `ReadCommitted` variants used by the isolation resolver.
* `PostgresClient` — opened from a config; provides `get_connection()` which acquires a pooled connection and updates Prometheus metrics on each call.
* `error` — `PostgresError` with `Determinate`/`Indeterminate` classification.
* `metrics` — `PostgresClientMetrics` for observing pool behavior.

The pool defaults to serializable transaction isolation on every new connection and staggered TTL-based reconnection to avoid stampeding the backing store.

Key dependencies: `deadpool-postgres`, `mz-ore`, `mz-tls-util`.
Downstream consumers include Materialize components that require a managed Postgres/CRDB connection pool (e.g., consensus storage, catalog).
