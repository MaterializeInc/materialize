# Formal Verification of Materialize's Type System

## Motivation

Materialize's SQL type system has two layers: `SqlScalarType` (~30 base variants including recursive types) and `ReprScalarType` (runtime representation). The cast system (`VALID_CASTS` in `src/sql/src/plan/typeconv.rs`) defines ~600 casts with context-sensitive validity (Implicit/Assignment/Explicit/Coerced). Function resolution (`find_match` in `src/sql/src/func.rs`) implements PostgreSQL's overload resolution algorithm with Materialize extensions.

This complexity produces real bugs. A recent sqlsmith nightly run panicked in `src/transform/src/typecheck.rs:1672` with a type error in a transient query — the optimizer's type checker caught an inconsistency that the planner allowed through.

Goals:
1. **Find bugs** in the cast graph, coercion logic, and function resolution
2. **Build confidence** via machine-checked proofs of key invariants
3. **Evaluate tooling** — determine whether Coq, Lean 4, or Rust property tests (or a combination) is the right long-term investment for formal verification at Materialize

## Scope

### What we model

- `ScalarBaseType` — the ~30 base type tags (Bool, Int16, Int32, Int64, UInt16, ..., Jsonb, Uuid)
- The cast graph — `VALID_CASTS` as a labeled directed graph (nodes = base types, edges = cast context)
- Cast context ordering — Implicit < Assignment < Explicit < Coerced

### What we defer

- Parameterized types (Numeric with scale, Char with length, List/Map/Record element types)
- Function resolution (`find_match`) — milestone 2
- Type inference soundness — milestone 3
- Runtime cast implementations (we verify structural properties, not that `CastInt32ToFloat64` produces the right IEEE754 value)

### Properties to verify

- **P1: Implicit cast DAG** — the implicit cast subgraph is acyclic
- **P2: Cast context monotonicity** — no `(from, to)` pair has contradictory context entries
- **P3: Coercion coherence** — multiple implicit cast paths between the same pair produce the same result type
- **P4: Function resolution determinism** — `find_match` returns at most one candidate (deferred)

## Design: Three Parallel Approaches

All three approaches share a common cast corpus: a JSON file extracted from the Rust `VALID_CASTS` table listing all `(from, to, context)` triples. A Rust build script generates this file; CI regenerates it and checks for regressions.

### Approach A: Coq Spec

**Location:** `formal/coq/`

**Files:**
- `Types.v` — inductive `ScalarBaseType` with one constructor per Rust variant
- `CastGraph.v` — cast relation as `cast : ScalarBaseType -> ScalarBaseType -> CastContext -> Prop`, populated from the shared corpus via a computed lookup over a list of triples
- `Properties.v` — theorems for P1-P3

**Proof strategy:** P1-P3 are decidable properties over a finite graph. Use `vm_compute` or `decide` to evaluate them over the concrete ~600-entry graph.

**Conformance:** Python script generates `CastGraph.v` from `cast_corpus.json`. CI regenerates and re-checks proofs. If a new Rust cast breaks a property, `coqc` fails.

**Evaluation criteria:** Proof engineering effort, `vm_compute` performance at this scale, sync burden for the type enum.

### Approach B: Lean 4 Spec

**Location:** `formal/lean/`

**Files:**
- `Types.lean` — inductive `ScalarBaseType` with `deriving DecidableEq, Repr`
- `CastGraph.lean` — cast relation as a decidable function over an `Array` of triples
- `Properties.lean` — theorems for P1-P3 using `native_decide`

**Conformance:** Two options to evaluate:
1. Lean compiles to C shared library, Rust links via FFI and checks agreement on all cast entries
2. Lean binary reads `cast_corpus.json`, verifies properties, exits 0/1 (simpler)

**Evaluation criteria:** Tactic ergonomics, `native_decide` performance, IDE experience, C FFI practicality.

### Approach C: Rust Property-Based Tests

**Location:** Tests within `src/repr/` or a dedicated test crate

**Tests:**
- **T1: Implicit cast DAG** — build implicit subgraph from `VALID_CASTS`, DFS cycle detection
- **T2: Cast context monotonicity** — assert no contradictory context entries for same `(from, to)`
- **T3: Coercion coherence** — for pairs with multiple implicit paths, verify same output type
- **T4: No implicit cast panics** — for every implicit cast, construct a canonical `Datum` of the source type (0 for integers, false for bool, empty string for text, epoch for timestamps, etc.), execute the cast function, assert no panic
- **T5: SqlScalarType/ReprScalarType consistency** — verify the `From` impl preserves cast graph structure

**Corpus generation:** Build script or standalone binary dumps `VALID_CASTS` to `formal/shared/cast_corpus.json`.

**Evaluation criteria:** Bugs found by T4, coverage of parameterized types, ease of extending to function resolution.

## Phases

### Phase 1: Cast graph basics
- Generate shared `cast_corpus.json` from Rust
- All three approaches model `ScalarBaseType` + cast graph
- Verify P1 (implicit DAG) and P2 (context monotonicity) in all three
- Rust T4 (no implicit cast panics) runs and reports findings

### Phase 2: Coherence + evaluation
- All three tackle P3 (coercion coherence)
- Coq and Lean set up CI conformance checks
- Rust adds T3 (multi-path coherence) and T5 (repr consistency)
- Compare approaches on evaluation rubric

### Evaluation rubric

| Criterion | Weight | What we measure |
|-----------|--------|-----------------|
| Bugs found | High | Real issues surfaced |
| Proof/test confidence | High | How much of P1-P3 is verified vs. spot-checked |
| Maintenance burden | High | Lines of spec per cast entry; effort to add a new type |
| CI integration | Medium | Seamlessness of automated conformance |
| Ergonomics | Medium | Pain level of proof engineering / test writing |
| Path to P4 | Low | How naturally does the model extend to `find_match` |

### Decision point

After phase 2, pick one formal approach (Coq or Lean) for long-term investment alongside Rust property tests. The Rust tests stay regardless.

### Future phases
- Phase 3: Extend formal model to parameterized types (Numeric, List, Record)
- Phase 4: Formalize `find_match`, prove P4 (function resolution determinism)
- Phase 5: Type inference soundness

## Key Source Files

- `src/repr/src/scalar.rs` — `SqlScalarType`, `ReprScalarType` definitions
- `src/sql/src/plan/typeconv.rs` — `VALID_CASTS`, `CastContext`, `CastImpl`, `plan_cast()`
- `src/sql/src/func.rs` — `find_match()`, `PG_CATALOG_BUILTINS`, `Candidate` scoring
- `src/transform/src/typecheck.rs` — optimizer type checker (where the sqlsmith panic occurred)
