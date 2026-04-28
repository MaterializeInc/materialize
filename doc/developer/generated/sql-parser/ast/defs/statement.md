---
source: src/sql-parser/src/ast/defs/statement.rs
revision: 44d6b9ac6a
---

# mz-sql-parser::ast::defs::statement

Defines `Statement<T>`, the top-level enum of all SQL statement types supported by Materialize, including DML (SELECT, INSERT, UPDATE, DELETE, COPY), DDL (CREATE/ALTER/DROP for connections, databases, schemas, sources, sinks, tables, views, materialized views, indexes, secrets, clusters, roles, types, functions, continual tasks), and control statements (SUBSCRIBE, EXPLAIN, SHOW, SET, RESET, BEGIN, COMMIT, ROLLBACK, PREPARE, EXECUTE, DECLARE, FETCH, CLOSE, INSPECT, RAISE).
Each statement variant has a corresponding struct with its specific fields.

`ConnectionRulePattern` represents a parsed broker-matching pattern with optional leading and trailing `*` wildcards. Fields: `prefix_wildcard: bool`, `literal_match: String`, `suffix_wildcard: bool`. Its `AstDisplay` impl prints the pattern as a single-quoted string with `*` where wildcards are enabled. Derives `Serialize`/`Deserialize`.

`KafkaMatchingBrokerRule<T: AstInfo>` represents a `MATCHING` rule inside a `BROKERS (...)` clause that routes brokers matching a pattern through an AWS PrivateLink tunnel. Fields: `pattern: ConnectionRulePattern` and `tunnel: KafkaBrokerAwsPrivatelink<T>`. Its `AstDisplay` impl prints `MATCHING {pattern} {tunnel}`.

`WithOptionValue<T>` includes a `KafkaMatchingBrokerRule(KafkaMatchingBrokerRule<T>)` variant. Its `AstDisplay` impl delegates to the inner rule's display. This variant is not redacted.
