---
source: src/sql-parser/src/ast/defs/statement.rs
revision: fca741734d
---

# mz-sql-parser::ast::defs::statement

Defines `Statement<T>`, the top-level enum of all SQL statement types supported by Materialize, including DML (SELECT, INSERT, UPDATE, DELETE, COPY), DDL (CREATE/ALTER/DROP for connections, databases, schemas, sources, sinks, tables, views, materialized views, indexes, secrets, clusters, roles, types, functions), and control statements (SUBSCRIBE, EXPLAIN, SHOW, SET, RESET, BEGIN, COMMIT, ROLLBACK, PREPARE, EXECUTE, EXECUTE UNIT TEST, DECLARE, FETCH, CLOSE, INSPECT, RAISE).
Each statement variant has a corresponding struct with its specific fields.

`ExecuteUnitTestStatement<T>` represents an `EXECUTE UNIT TEST <name> FOR <target> [AT TIME <expr>] [MOCK <view_def>, ...] EXPECTED <result_def>` statement. Fields: `name: Ident`, `target: T::ItemName`, `at_time: Option<Expr<T>>`, `mocks: Vec<MockViewDef<T>>`, `expected: ExpectedResultDef<T>`.

`MockViewDef<T>` represents a single `MOCK <name>(<cols>) AS (<query>)` clause within an `EXECUTE UNIT TEST` statement. Fields: `name: T::ItemName`, `columns: Vec<ColumnDef<T>>`, `query: Query<T>`.

`ExpectedResultDef<T>` represents the `EXPECTED (<cols>) AS (<query>)` clause of an `EXECUTE UNIT TEST` statement. Fields: `columns: Vec<ColumnDef<T>>`, `query: Query<T>`.

`ClusterOptionName` includes an `AutoScalingStrategy` variant for the `AUTO SCALING STRATEGY [[=] (...)]` cluster option. Its `AstDisplay` impl prints `AUTO SCALING STRATEGY`. `redact_value()` returns `false` for this variant (along with most other cluster option names).

`ClusterAutoScalingStrategyOptionValue` represents the value of the `AUTO SCALING STRATEGY` option: a parenthesized autoscaling policy block with an optional `on_hydration: Option<OnHydrationOptionValue>` field. An empty block (all sub-policies absent) disables autoscaling, equivalent to `RESET (AUTO SCALING STRATEGY)`. The struct derives `Serialize`/`Deserialize` and implements `AstDisplay`, which prints `(` followed by the optional sub-policy and `)`.

`OnHydrationOptionValue` represents the `ON HYDRATION (HYDRATION SIZE = '...' [, LINGER DURATION = '...'])` autoscaling sub-policy. Fields: `hydration_size: Value` (required) and `linger_duration: Option<Value>` (optional). While un-hydrated objects exist, an extra replica at `hydration_size` is run to accelerate hydration, lingering for `linger_duration` after the steady-state replicas hydrate. Its `AstDisplay` impl prints the full `ON HYDRATION (...)` clause.

`WithOptionValue` includes a `ClusterAutoScalingStrategyOptionValue(ClusterAutoScalingStrategyOptionValue)` variant. This variant is not redacted in `AstDisplay` output.

`ConnectionRulePattern` represents a parsed broker-matching pattern with optional leading and trailing `*` wildcards. Fields: `prefix_wildcard: bool`, `literal_match: String`, `suffix_wildcard: bool`. Its `AstDisplay` impl prints the pattern as a single-quoted string with `*` where wildcards are enabled. Derives `Serialize`/`Deserialize`.

`KafkaMatchingBrokerRule<T: AstInfo>` represents a `MATCHING` rule inside a `BROKERS (...)` clause that routes brokers matching a pattern through an AWS PrivateLink tunnel. Fields: `pattern: ConnectionRulePattern` and `tunnel: KafkaBrokerAwsPrivatelink<T>`. Its `AstDisplay` impl prints `MATCHING {pattern} {tunnel}`.

`WithOptionValue<T>` includes a `KafkaMatchingBrokerRule(KafkaMatchingBrokerRule<T>)` variant. Its `AstDisplay` impl delegates to the inner rule's display. This variant is not redacted.
In redacted `AstDisplay` output, `WithOptionValue::Secret` is not redacted: a secret reference is a catalog item name, not the secret value, so it is safe to show. An option that accepts an inline credential is parsed as `WithOptionValue::Value`, which is redacted by the `Value` arm together with the option's `redact_value()`. `WithOptionValue::ConnectionKafkaBroker` is still unconditionally redacted.

`CreateTableFromSourceStatement<T>`'s `AstDisplay` impl opens a parenthesized column/constraint list only when `columns` is not `NotSpecified` or the `constraints` list is non-empty. When `columns` is `NotSpecified` but constraints exist, the constraints are written directly without a preceding `, ` separator.

`SelectStatement<T>`'s `AstDisplay` impl wraps the query in parentheses when `query.body.starts_with_show()` is true (i.e. the leftmost operand of the query body is a `Show` node), because a top-level leading `SHOW` is dispatched as `Statement::Show` and would terminate the statement before any following set operator or ORDER BY/LIMIT/OFFSET. The parser unwraps redundant outer parens during `parse_query_tail`, so wrapping round-trips.

`CreateIndexStatement<T>`'s `AstDisplay` impl force-quotes an index name whose text is `in` (case-insensitive) to avoid ambiguity with the optional `IN CLUSTER` clause that follows the name position.

`StatementKind` exposes two classification methods. `is_secret() -> bool` returns `true` for `CreateSecret` and `AlterSecret`, indicating the statement carries secret values that must never be persisted verbatim (e.g. in `mz_statement_execution_history`), even in error messages. `is_sensitive() -> bool` is a superset of `is_secret()` and additionally returns `true` for `Insert`, `Update`, and `Execute`.

`CommentStatement<T>`'s `AstDisplay` impl redacts the comment body when `f.redacted()` is true, writing `'<REDACTED>'` in place of the escaped literal. `NULL` stays verbatim in all modes.

`TableOptionName::PartitionBy` and `TableFromSourceOptionName::PartitionBy` return `true` from `redact_value()`, so scalar literals in `PARTITION BY` option values are redacted. Column-list identifiers (e.g. `PARTITION BY (a, b)`) remain verbatim because `WithOptionValue`'s per-type logic redacts only scalar literals.

`RoleAttribute::Password`'s `AstDisplay` impl prints `PASSWORD NULL` for `Password(None)` and `PASSWORD '<REDACTED>'` for `Password(Some(_))`. The redacted form is a parseable placeholder string rather than a bare `PASSWORD` keyword (which would fail to reparse without a following `NULL` or string literal).

`SubscribeStatement<T>`'s `AstDisplay` impl emits the optional `TO` keyword before the relation name when `SubscribeRelation::needs_explicit_to` returns true — that is, when in simple (non-stable) mode and the relation name starts with the bare token `to`. Without the keyword, `SUBSCRIBE to` would consume `to` as the optional keyword and drop the relation name.

`SubscribeRelation<T>` exposes `needs_explicit_to(bare_identifiers: bool) -> bool`, which returns true for `SubscribeRelation::Name` variants whose printed form starts with the bare identifier `to` (or `to.` for qualified names) in simple mode.

`FetchStatement<T>`'s `AstDisplay` impl force-quotes a cursor name equal to `forward` (case-insensitive) when no count is present, preventing the optional leading `FORWARD` keyword from consuming the cursor name on reparse.

`GrantTargetSpecification<T>`'s `AstDisplay` impl uses the private `write_grant_object_type_plural` helper to pluralize the object type keyword. `ObjectType::NetworkPolicy` pluralizes to `POLICIES`; all other types append `S` to their keyword. The preceding space before `IN DATABASE` and `IN SCHEMA` is emitted by the helper as part of the separator string.
