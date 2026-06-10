---
source: src/sql/src/plan/scope.rs
revision: db271c31b1
---

# mz-sql::plan::scope

Implements SQL scoping: a `Scope` is a list of `ScopeItem`s (each carrying an optional table name, column name, and source expression) that represents the columns visible at one level of a `Query`.
Name resolution (`resolve_column`, `resolve_table_column`) searches inward to outer scopes and returns an error on ambiguity.
Scopes are consumed by `plan::query` and threaded through join, subquery, and CTE planning.
