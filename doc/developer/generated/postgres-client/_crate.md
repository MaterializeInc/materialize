---
source: src/postgres-client/src/lib.rs
revision: 5d439bc71e
---

# mz-postgres-client

Provides `PostgresClient`, a pooled Postgres/CockroachDB client built on `deadpool-postgres`, with configurable pool size, connection TTL, keepalive settings, and TLS.

The crate exposes:
* `PostgresClientKnobs` — a trait for supplying runtime-configurable pool parameters.
* `PostgresClientConfig` — bundles a connection URL, a `PostgresClientKnobs` impl, and `PostgresClientMetrics`.
* `PostgresClient` — opened from a config; provides `get_connection()` which acquires a pooled connection and updates Prometheus metrics on each call.
* `error` — `PostgresError` with `Determinate`/`Indeterminate` classification.
* `metrics` — `PostgresClientMetrics` for observing pool behavior.

The pool enforces serializable transaction isolation on every new connection and staggered TTL-based reconnection to avoid stampeding the backing store.

Key dependencies: `deadpool-postgres`, `mz-ore`, `mz-tls-util`.
Downstream consumers include Materialize components that require a managed Postgres/CRDB connection pool (e.g., consensus storage, catalog).
