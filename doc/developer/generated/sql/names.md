---
source: src/sql/src/names.rs
revision: 73b1bd8e63
---

# mz-sql::names

Defines all structured name types used throughout the SQL layer: `FullItemName`, `PartialItemName`, `QualifiedItemName`, `FullSchemaName`, `QualifiedSchemaName`, `DatabaseId`, `SchemaId`, `ObjectId`, `SystemObjectId`, `CommentObjectId`, and the `Aug` AST info type that replaces raw string identifiers with resolved catalog IDs.
The `Aug` type parameterizes the SQL AST after name resolution and is the key distinction between pre- and post-resolution plans.
Also contains `NameResolver` (the AST fold that performs name resolution against a `SessionCatalog`) and `NameSimplifier` (which collapses fully-qualified names back to the shortest unambiguous form for display).
`NameResolver` allocates CTE `LocalId`s from a monotonically increasing `next_cte_id` counter via `allocate_cte_id()`. This guarantees every CTE in a statement receives a unique id, even when CTE names shadow each other across nested scopes. Later phases (HIR lowering's `CteMap`, the planner's `qcx.ctes`) key CTEs by `LocalId` and rely on this uniqueness.
`ResolvedItemName` has three variants: `Item` (a fully-resolved catalog item), `Cte` (a CTE local ID), and `Error` (a sentinel for name-resolution failures).
`CommentObjectId` identifies the subject of a `COMMENT ON` statement; variants cover tables, sources, views, materialized views, sinks, indexes, connections, types, secrets, roles, databases, schemas, clusters, cluster replicas, and network policies. New variants require corresponding additions to `mz_catalog_protos::objects::CommentObject` and to both CASE expressions in the `mz_internal.mz_comments` materialized view, which reads `CommentObject` out of `mz_catalog_raw` as serde JSON.
The `NameResolver`'s `Fold<Raw, Aug>` impl handles all `WithOptionValue` variants; `KafkaMatchingBrokerRule(x)` is resolved by calling `self.fold_kafka_matching_broker_rule(x)`.
