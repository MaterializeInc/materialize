---
source: src/sql-parser/src/ast/defs/statement.rs
revision: 9f1a1d48fa
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
