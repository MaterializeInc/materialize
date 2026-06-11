# Architecture review — `mz-repr`

Scope: `src/repr/src/` and `src/repr/src/adt/` (≈ 35,001 LOC of Rust).

## 1. Dual-type system: justified complexity, minor friction at the seam

**Files**
- `src/repr/src/scalar.rs` — `SqlScalarType` (36 variants) and `ReprScalarType`
  (29 variants) both defined here.
- `src/repr/src/relation.rs` — parallel `SqlColumnType`/`SqlRelationType` vs
  `ReprColumnType`/`ReprRelationType`.

**What it is**
The SQL layer must preserve type modifiers (`VarChar(n)`, `Char(n)`, Oid
subtypes) for type-checking and pg-compat. The compute/storage layer doesn't
need them and benefits from a collapsed enum. The dual-type system is the
deliberate interface *seam* between planning and execution.

**Friction**
`From<SqlColumnType> for ReprColumnType` is a one-way lossy conversion.
`backport_nullability` is the reverse adapter: it reconciles nullability from
the repr level back into a SQL type. This reverse path is a signal that the
boundary is not perfectly clean — callers in `mz-storage` or `mz-catalog` must
reconstruct SQL-level context from repr-level data. The conversion is narrow
today, but adding new modifiers (e.g., a future `Numeric(p,s)` precision) would
require updating both enums and the backport logic.

**Verdict**
The split is correct architecture. The backport path is a known seam cost, not
a deepening opportunity. Monitor if new type modifiers proliferate — at that
point a single richer enum with a `to_repr()` method may outperform two enums.

## 2. `OptimizerFeatures` in the data-representation crate

**File:** `src/repr/src/optimize.rs`

**Observation**
`OptimizerFeatures` and `OverrideFrom` live in `mz-repr` — a foundational
data-representation crate — rather than in `mz-compute` or `mz-adapter`.
The rationale is sharing: `mz-adapter` (catalog persistence), `mz-compute`
(cluster features), and EXPLAIN output all need these types without creating
upward deps.

**Problem**
`mz-repr` now carries optimizer semantics. If optimizer flag count grows (it
is macro-generated today), this module is the wrong home — it sits below all
the layers that govern optimizer policy.

**Solution sketch**
Introduce a thin `mz-repr-optimizer` or extend `mz-compute-types` (which
already bridges compute ↔ repr) to host `OptimizerFeatures`. Keep `OverrideFrom`
in `mz-ore` as a generic layered-config trait. This would remove the optimizer
concept from the core data layer.

**Risk**
Many crates import `mz_repr::optimize::OptimizerFeatures`. The migration is
mechanical but touches ~10 crates. Defer until the flag count grows or until
a `mz-compute-types` restructure is already in flight.

## 3. `mz-sql-parser` dependency pulled in for one type

**File:** `src/repr/src/explain/tracing.rs:15`

`use mz_sql_parser::ast::NamedPlan;`

`mz-sql-parser` is a build-heavy dependency. `NamedPlan` is a 6-variant enum
with `of_path` / `paths` helpers — no parser machinery is needed.

**Solution sketch**
Move `NamedPlan` to `mz-repr::explain` or to a new `mz-repr-explain` crate.
The 6 variants and their string paths have no intrinsic connection to SQL
parsing; they describe optimizer plan stages. The `tracing` feature in repr
would then need no parser dep.

**Leverage**
Removes a heavy transitive dep from `mz-repr`'s non-tracing build path.
`NamedPlan` also has better Locality in `repr::explain` where it is used.

## What this review did *not* reach

- `strconv.rs` internal format conformance vs PostgreSQL — a behavioral audit,
  not structural.
- `adt/datetime.rs` (3,923 LOC) — timezone/DST edge cases; would require a
  dedicated behavioral review.
- `row/encode.rs` — Arrow columnar codec alignment with persist's codec layer.
