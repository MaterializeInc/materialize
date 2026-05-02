---
source: src/kafka-util/src/client.rs
revision: 44d6b9ac6a
---

# mz-kafka-util::client

Defines `MzClientContext`, a `ClientContext` implementation that routes librdkafka log/error callbacks through `tracing` and exposes a channel of structured `MzKafkaError` values for error monitoring.
`TunnelingClientContext<C>` wraps any `ClientContext` and overrides `resolve_broker_addr` to rewrite broker host/port through SSH tunnels (managed by `SshTunnelManager`), static PrivateLink host substitution, rule-based PrivateLink routing, or per-broker rewrites; it also handles AWS IAM OAuth token generation via `generate_oauth_token`.
`TimeoutConfig` centralises all configurable rdkafka timeout parameters (socket, transaction, connection-setup, metadata-fetch, progress-record-fetch) with validation and clamping in `TimeoutConfig::build`.
`create_new_client_config` constructs a `ClientConfig` from a tracing level and `TimeoutConfig`, bridging Rust tracing levels to rdkafka log levels.

`BrokerAddr` holds a broker's `host: String` and `port: u16`. Its `to_socket_addrs()` method resolves the address to a `Vec<SocketAddr>`.

`BrokerRewrite` holds a rewritten `host: String` and optional `port: Option<u16>`. Its `rewrite(address: &BrokerAddr) -> BrokerAddr` method applies the rewrite, using the rewrite's host and, if set, the rewrite's port, otherwise falling back to the original broker's port.

`ConnectionRulePattern` represents a broker-matching pattern with fields `prefix_wildcard: bool`, `literal_match: String`, and `suffix_wildcard: bool`. Its `Display` impl prints the pattern with `*` characters for wildcards. Its `matches(address: &str) -> bool` method tests whether a `"{host}:{port}"` string fits the pattern.

`HostMappingRules` holds `rules: Vec<(ConnectionRulePattern, BrokerRewrite)>`. Its `rewrite(src: &BrokerAddr) -> Option<BrokerAddr>` method returns the rewrite from the first matching rule, or `None` if no rule matches.

`TunnelConfig` is an enum with variants: `Ssh(SshTunnelConfig)` for SSH tunnels, `StaticHost(String)` for static PrivateLink host substitution, `Rules(HostMappingRules)` for rule-based PrivateLink routing that rewrites broker addresses according to an ordered list of pattern/rewrite pairs, and `None` for no rewriting.

In `TunnelingClientContext::resolve_broker_addr`, the `TunnelConfig::Rules` branch resolves the broker address via `HostMappingRules::rewrite`, falling back to the original address if no rule matches.
