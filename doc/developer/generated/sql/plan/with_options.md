---
source: src/sql/src/plan/with_options.rs
revision: f40272e177
---

# mz-sql::plan::with_options

Defines the `TryFromValue` and `ImpliedValue` traits used by `generate_extracted_config!`-generated types, plus `TryFromValue` implementations for a large set of Rust types (`String`, `bool`, numerics, `Duration`, `Interval`, `ByteSize`, connection references, etc.).
This is the type conversion layer that bridges raw AST `WithOptionValue` tokens to typed Rust values consumed by planner and purification logic.
`BrokersList` is a struct with fields `static_entries: Vec<KafkaBroker<Aug>>` and `matching_rules: Vec<KafkaMatchingBrokerRule<Aug>>`.
It implements `TryFromValue<WithOptionValue<Aug>>` by parsing a `Sequence` of `WithOptionValue` entries, routing `ConnectionKafkaBroker` variants into `static_entries` and `KafkaMatchingBrokerRule` variants into `matching_rules`; single non-sequence values of either kind are also accepted.
`KafkaMatchingBrokerRule<Aug>` implements `TryFromValue<WithOptionValue<Aug>>` (extracts from `WithOptionValue::KafkaMatchingBrokerRule`) and `ImpliedValue` (bails with an error).
In the generic `TryFromValue<WithOptionValue<Aug>>` fallback, `WithOptionValue::KafkaMatchingBrokerRule(_)` is excluded from generic scalar extraction alongside `ConnectionAwsPrivatelink`; the error label for `ConnectionAwsPrivatelink` is `"connection privatelink"`. `WithOptionValue::ClusterAutoScalingStrategyOptionValue(_)` is likewise excluded, with the error label `"cluster auto scaling strategy"`.
`ClusterAutoScalingStrategyOptionValue` implements `TryFromValue<WithOptionValue<Aug>>` (extracts from `WithOptionValue::ClusterAutoScalingStrategyOptionValue`) and `ImpliedValue` (bails with an error requiring an explicit value).
`IcebergAccessDelegation` implements `TryFromValue<WithOptionValue<Aug>>`: accepts the case-insensitive string `"vended-credentials"`, mapping it to `IcebergAccessDelegation::VendedCredentials`; any other string is rejected. `try_into_value` serializes via `as_header_value()`. `ImpliedValue` bails with an error requiring an explicit value.
