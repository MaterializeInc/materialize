---
source: src/sql-parser/src/ast/defs/statement.rs
revision: 72277f8ac9
---

# mz-sql-parser::ast::defs::statement

Defines `Statement<T>`, the top-level enum of all SQL statement types supported by Materialize, including DML (SELECT, INSERT, UPDATE, DELETE, COPY), DDL (CREATE/ALTER/DROP for connections, databases, schemas, sources, sinks, tables, views, materialized views, indexes, secrets, clusters, roles, types, functions), and control statements (SUBSCRIBE, EXPLAIN, SHOW, SET, RESET, BEGIN, COMMIT, ROLLBACK, PREPARE, EXECUTE, EXECUTE UNIT TEST, DECLARE, FETCH, CLOSE, INSPECT, RAISE).
Each statement variant has a corresponding struct with its specific fields.

`ExecuteUnitTestStatement<T>` represents an `EXECUTE UNIT TEST <name> FOR <target> [AT TIME <expr>] [MOCK <view_def>, ...] EXPECTED <result_def>` statement. Fields: `name: Ident`, `target: T::ItemName`, `at_time: Option<Expr<T>>`, `mocks: Vec<MockViewDef<T>>`, `expected: ExpectedResultDef<T>`.

`MockViewDef<T>` represents a single `MOCK <name>(<cols>) AS (<query>)` clause within an `EXECUTE UNIT TEST` statement. Fields: `name: T::ItemName`, `columns: Vec<ColumnDef<T>>`, `query: Query<T>`.

`ExpectedResultDef<T>` represents the `EXPECTED (<cols>) AS (<query>)` clause of an `EXECUTE UNIT TEST` statement. Fields: `columns: Vec<ColumnDef<T>>`, `query: Query<T>`.

`ConnectionRulePattern` represents a parsed broker-matching pattern with optional leading and trailing `*` wildcards. Fields: `prefix_wildcard: bool`, `literal_match: String`, `suffix_wildcard: bool`. Its `AstDisplay` impl prints the pattern as a single-quoted string with `*` where wildcards are enabled. Derives `Serialize`/`Deserialize`.

`KafkaMatchingBrokerRule<T: AstInfo>` represents a `MATCHING` rule inside a `BROKERS (...)` clause that routes brokers matching a pattern through an AWS PrivateLink tunnel. Fields: `pattern: ConnectionRulePattern` and `tunnel: KafkaBrokerAwsPrivatelink<T>`. Its `AstDisplay` impl prints `MATCHING {pattern} {tunnel}`.

`WithOptionValue<T>` includes a `KafkaMatchingBrokerRule(KafkaMatchingBrokerRule<T>)` variant. Its `AstDisplay` impl delegates to the inner rule's display. This variant is not redacted.

`CreateTableFromSourceStatement<T>`'s `AstDisplay` impl opens a parenthesized column/constraint list only when `columns` is not `NotSpecified` or the `constraints` list is non-empty. When `columns` is `NotSpecified` but constraints exist, the constraints are written directly without a preceding `, ` separator.

`SelectStatement<T>`'s `AstDisplay` impl wraps the query in parentheses when `query.body.starts_with_show()` is true (i.e. the leftmost operand of the query body is a `Show` node), because a top-level leading `SHOW` is dispatched as `Statement::Show` and would terminate the statement before any following set operator or ORDER BY/LIMIT/OFFSET. The parser unwraps redundant outer parens during `parse_query_tail`, so wrapping round-trips.

`CreateIndexStatement<T>`'s `AstDisplay` impl force-quotes an index name whose text is `in` (case-insensitive) to avoid ambiguity with the optional `IN CLUSTER` clause that follows the name position.

`RoleAttribute::Password`'s `AstDisplay` impl prints `PASSWORD NULL` for `Password(None)` and `PASSWORD '<REDACTED>'` for `Password(Some(_))`. The redacted form is a parseable placeholder string rather than a bare `PASSWORD` keyword (which would fail to reparse without a following `NULL` or string literal).

`SubscribeStatement<T>`'s `AstDisplay` impl emits the optional `TO` keyword before the relation name when `SubscribeRelation::needs_explicit_to` returns true — that is, when in simple (non-stable) mode and the relation name starts with the bare token `to`. Without the keyword, `SUBSCRIBE to` would consume `to` as the optional keyword and drop the relation name.

`SubscribeRelation<T>` exposes `needs_explicit_to(bare_identifiers: bool) -> bool`, which returns true for `SubscribeRelation::Name` variants whose printed form starts with the bare identifier `to` (or `to.` for qualified names) in simple mode.

`FetchStatement<T>`'s `AstDisplay` impl force-quotes a cursor name equal to `forward` (case-insensitive) when no count is present, preventing the optional leading `FORWARD` keyword from consuming the cursor name on reparse.

`GrantTargetSpecification<T>`'s `AstDisplay` impl uses the private `write_grant_object_type_plural` helper to pluralize the object type keyword. `ObjectType::NetworkPolicy` pluralizes to `POLICIES`; all other types append `S` to their keyword. The preceding space before `IN DATABASE` and `IN SCHEMA` is emitted by the helper as part of the separator string.
