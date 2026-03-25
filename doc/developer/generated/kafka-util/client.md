---
source: src/kafka-util/src/client.rs
revision: e757b4d11b
---

# mz-kafka-util::client

Defines `MzClientContext`, a `ClientContext` implementation that routes librdkafka log/error callbacks through `tracing` and exposes a channel of structured `MzKafkaError` values for error monitoring.
`TunnelingClientContext<C>` wraps any `ClientContext` and overrides `resolve_broker_addr` to rewrite broker host/port through SSH tunnels (managed by `SshTunnelManager`), static PrivateLink host substitution, or per-broker rewrites; it also handles AWS IAM OAuth token generation via `generate_oauth_token`.
`TimeoutConfig` centralises all configurable rdkafka timeout parameters (socket, transaction, connection-setup, metadata-fetch, progress-record-fetch) with validation and clamping in `TimeoutConfig::build`.
`create_new_client_config` constructs a `ClientConfig` from a tracing level and `TimeoutConfig`, bridging Rust tracing levels to rdkafka log levels.
