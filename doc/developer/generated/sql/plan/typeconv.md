---
source: src/sql/src/plan/typeconv.rs
revision: 95b5b8287f
---

# mz-sql::plan::typeconv

Maintains the catalog of valid implicit, assignment, and explicit casts between `SqlScalarType`s and implements type coercion for the planner.
Key exports: `plan_cast` (inserts a cast expression given a `CastContext`), `guess_best_common_type` (finds the best common type for a set of types), and `plan_coerce` / `plan_hypothetical_cast` for coercion paths.
Used extensively by `plan::query` and `func` for argument type checking and implicit conversion.
The string-to-reg* cast template (`STRING_REG_CAST_TEMPLATE`) handles the absent reference: the input string `'-'` casts to OID 0 for any reg* type, matching PostgreSQL's `parseDashOrOid` behavior. This applies in all cases including explicit `text::regclass` casts.
The reg*-to-string cast template (`REG_STRING_CAST_TEMPLATE`) renders OID 0 as `'-'`, matching PostgreSQL's `regprocout`, `regclassout`, and `regtypeout` functions. A nonzero OID that names nothing still renders as its decimal digits.
`plan_cast` gives `"char"` (`PgLegacyChar`) special treatment when casting from a string-like type: if a registered cast from `PgLegacyChar` to the target type exists it is used directly (e.g. to `int4` by byte value); otherwise the value is rendered to text first and the text-source cast path proceeds from there.
