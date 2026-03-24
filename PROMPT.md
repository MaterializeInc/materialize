# AWS PRIVATELINKS syntax changes

## Problem

When using the top-level `AWS PRIVATELINK` or `AWS PRIVATELINKS` syntax (without `BROKERS`), `bootstrap.servers` is set to the VPC endpoint hostname (e.g. `vpce-xxx...amazonaws.com:9092`). librdkafka uses this hostname for TLS SNI. Confluent Cloud rejects TLS handshakes with an unrecognized SNI, so connections fail with "SSL handshake failed: Disconnected".

The `BROKERS` syntax works because `bootstrap.servers` is set to the original Kafka hostname (e.g. `lkc-xxx...confluent.cloud:9092`), and `resolve_broker_addr` routes the TCP connection through the VPC endpoint without changing the hostname librdkafka uses for TLS.

## Current code paths

Both paths are in `KafkaConnection::create_with_context` in `src/storage-types/src/connections.rs`.

**Top-level path** (lines 944-962): Sets `bootstrap.servers` to `vpc_endpoint_host(connection_id, None)`. Sets `default_tunnel` to `TunnelConfig::StaticHost` (singular `AWS PRIVATELINK`) or `TunnelConfig::Rules` (plural `AWS PRIVATELINKS`). The `HostMappingRules` struct in `src/kafka-util/src/client.rs` always has a `default: BrokerRewrite` field — this is the default rule that catches any broker address not matched by a pattern.

**BROKERS path** (line 964): Sets `bootstrap.servers` to the user-provided broker addresses. Per-broker `AWS PRIVATELINK` tunnels are registered as individual `BrokerRewrite` entries via `add_broker_rewrite`.

## Changes

Two changes to the `AWS PRIVATELINKS` syntax:

### 1. Remove the default (pattern-less) rule

The `AwsPrivatelinks` struct currently requires a `default: AwsPrivatelink` field — the catch-all rule for brokers that don't match any pattern. Remove this. `AWS PRIVATELINKS` should only contain pattern rules. Every rule must have a pattern.

This affects:
- `AwsPrivatelinks` struct in `src/storage-types/src/connections.rs` — remove `default` field
- `HostMappingRules` struct in `src/kafka-util/src/client.rs` — remove `default` field; `rewrite()` should return `None` when no pattern matches (instead of applying a default)
- `from_aws_privatelinks` in `src/storage-types/src/connections.rs` — stop building a default
- SQL parser in `src/sql-parser/src/parser.rs` — reject pattern-less entries in `AWS PRIVATELINKS (...)`
- AST types in `src/sql-parser/src/ast/defs/statement.rs` — `ConnectionAwsPrivatelinkRule` no longer needs a default variant
- Planning in `src/sql/src/plan/statement/ddl/connection.rs` — update accordingly

### 2. Exact-match patterns become bootstrap brokers

Host patterns without wildcards (no leading or trailing `*`) are exact matches — they match a single known broker address. These should be treated like `BROKERS` entries: their addresses should be included in `bootstrap.servers`, and the VPC endpoint routing should happen via `resolve_broker_addr` (same as per-broker `USING AWS PRIVATELINK` in the `BROKERS` syntax).

This means in `create_with_context`:
- Collect exact-match patterns from `AWS PRIVATELINKS` rules
- Include their addresses in `bootstrap.servers` (comma-separated, like the `BROKERS` path)
- Register their VPC endpoint routing via `add_broker_rewrite` (like the `BROKERS` path does at lines 1137-1141)
- Wildcard patterns still go into `TunnelConfig::Rules` for dynamic matching in `resolve_broker_addr`

The `assert!(self.brokers.is_empty())` on line 947 stays — users still don't specify `BROKERS` alongside `AWS PRIVATELINKS`. The bootstrap broker addresses come from the exact-match patterns instead.

## Expected SQL after changes

```sql
-- Exact-match patterns: addresses go into bootstrap.servers,
-- routed through their respective PrivateLink endpoints.
-- Wildcard patterns: applied dynamically to discovered brokers.
CREATE CONNECTION kafka_conn TO KAFKA (
  SECURITY PROTOCOL = SASL_SSL,
  SASL MECHANISMS = 'PLAIN',
  SASL USERNAME = '...',
  SASL PASSWORD = SECRET pw,
  AWS PRIVATELINKS (
    'lkc-xxx.domyyy.us-east-1.aws.confluent.cloud:9092' TO privatelink_svc (PORT 9092),
    '*.use1-az1.*' TO privatelink_svc (AVAILABILITY ZONE 'use1-az1'),
    '*.use1-az4.*' TO privatelink_svc (AVAILABILITY ZONE 'use1-az4')
  )
);
```

With the old syntax, you'd also need a pattern-less default at the end:
```sql
    privatelink_svc  -- default, catch-all: REMOVED
```

## Documentation

Update both documentation files to reflect the syntax changes.

### `doc/user/content/sql/create-connection.md`

**"Dynamic broker discovery" section** (lines 326-347, anchor `#kafka-privatelinks`):
- Update the description: exact-match (no-wildcard) patterns are used as bootstrap brokers. Wildcard patterns route dynamically discovered brokers.
- Update the example SQL: remove the bare `privatelink_svc` default line, add an exact-match pattern for the bootstrap broker address.

**"Default connections" section** (lines 404-429, anchor `#kafka-privatelink-default`):
- This section documents the singular `AWS PRIVATELINK` (default connection) syntax for Redpanda Cloud. It is unchanged by this work, but verify the description still makes sense in context after the `AWS PRIVATELINKS` changes above.

### `doc/user/data/examples/create_connection.yml`

**`syntax-kafka-aws-privatelinks` example** (lines 255-293):
- Remove `<default_privatelink_connection> (PORT <default_port>)` from the syntax template.
- Add an exact-match pattern line to the template (no wildcards).
- Update the `syntax_elements` descriptions:
  - Remove the "`<default_connection_name>`" element.
  - Remove the sentence "If no rule matches, Materialize will attempt to connect through the default PrivateLink connection listed at the end."
  - Add a note that patterns without wildcards are used as bootstrap broker addresses.

## What NOT to change

- The singular `AWS PRIVATELINK` syntax (top-level, non-Kafka connections like Postgres) is unchanged.
- The `BROKERS (...) USING AWS PRIVATELINK` syntax is unchanged.
- The `ConnectionRulePattern` struct and its `matches()` method are unchanged.
