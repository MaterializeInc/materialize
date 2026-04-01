# Formal Type System Verification — Evaluation

## Findings

The Rust property tests surfaced real structural findings on the first run:

### 1. Implicit cast cycles (NOT a DAG)

The implicit cast subgraph contains cycles in two families:

**Oid family:** Oid ↔ RegProc, Oid ↔ RegType, Oid ↔ RegClass (mutual implicit casts)

**String family:** String ↔ Char, String ↔ VarChar, Char ↔ VarChar, String/Char/VarChar → PgLegacyName, PgLegacyName → String

These mirror PostgreSQL's behavior, but they mean the implicit cast graph is not a DAG. This has implications:
- Coercion path search must be bounded or cycle-aware
- Function resolution's "preferred type" logic is the only thing preventing ambiguity
- Whether these should actually be implicit (vs. assignment) casts is worth revisiting

### 2. Implicit self-casts

Four types have implicit A → A casts: Char, VarChar, List, Record. These exist for parameterized coercion (e.g., `Char(5)` → `Char(10)`, `List(Int32)` → `List(Int64)`). The base type is the same but the parameters differ. This is a modeling artifact of `SqlScalarBaseType` erasing parameters.

### 3. 58 divergent implicit cast path pairs

Types like `Int32` have implicit casts to both `Float64` and `MzTimestamp`, where neither target can reach the other. This means function resolution must pick a preferred type in these cases. The `UInt16` → `{Int32, UInt32}` divergence is notable — unsigned and signed integer hierarchies don't converge.

### 4. Numeric subgraph IS a DAG

Restricting to numeric types (Int16, Int32, Int64, UInt16, ..., Float32, Float64, Numeric), the implicit cast graph is acyclic. Widening among numbers does not loop.

### 5. All implicit sources are printable

Every type that participates in implicit casts also has a cast to String (at any context level).

## Approach Comparison

| Criterion | Rust Tests | Coq | Lean 4 |
|-----------|-----------|-----|--------|
| **Bugs found** | 3 structural findings on first run | N/A (unverified) | N/A (unverified) |
| **Build time** | ~3 min (first compile), <1s (incremental) | N/A (not installed) | N/A (not installed) |
| **Spec LOC** (hand-written) | 250 | 55 (Types.v) + 90 (Properties.v) | 50 (Types.lean) + 65 (Properties.lean) |
| **Generated LOC** | 0 (reads VALID_CASTS directly) | ~1100 (CastGraph.v) | ~1100 (CastGraph.lean) |
| **Proof confidence** | Exhaustive test (all pairs) | Machine-checked (if verified) | Machine-checked (if verified) |
| **CI integration** | Zero effort (`cargo test`) | Moderate (install coqc) | Moderate (install lean/lake) |
| **Conformance** | Direct (reads live Rust data) | Indirect (JSON corpus, regenerate) | Indirect (JSON corpus, regenerate) |
| **Maintenance** | Low (tests auto-update) | Medium (regenerate on cast changes) | Medium (regenerate on cast changes) |

## Key Observations

### Rust tests gave immediate value
- Found findings on the first run with zero setup beyond writing the tests
- No conformance gap — tests read the live `VALID_CASTS` directly
- Easy to extend (just add more `#[mz_ore::test]` functions)
- Already integrated into CI (`cargo test -p mz-sql`)

### Coq/Lean provide stronger guarantees — when verified
- Machine-checked proofs are more rigorous than tests (proofs cover ALL cases by construction)
- But we couldn't verify them without installing the toolchains
- The JSON corpus conformance bridge introduces drift risk
- Properties needed to be rewritten after Rust tests revealed the graph isn't a DAG

### The conformance gap is real
- The subagents initially wrote proofs claiming "the graph is acyclic" — which was wrong
- The JSON corpus (initially Python-extracted) missed 3 entries vs the Rust-generated one
- Any formal model is only as good as its conformance to the real implementation

## Recommendations

### Short term: Invest in Rust property tests
- Extend T4 to actually execute cast functions with representative `Datum` values
- Add property tests for `find_match` (function resolution determinism)
- Add tests for the type inference pipeline
- These run in CI today with zero additional infrastructure

### Medium term: Evaluate Lean 4 as the formal tool
- Install Lean 4 and verify the proofs actually go through
- If `native_decide` handles the 214-entry graph, Lean is viable
- The C FFI bridge makes a tighter conformance loop possible (vs Coq's OCaml extraction)
- Start with the numeric DAG property and extend

### Long term: Investigate the findings
- Are the Oid/Reg* implicit cast cycles causing real bugs?
- Should the String/Char/VarChar mutual implicit casts be Assignment instead?
- Do the 58 divergent path pairs cause function resolution ambiguity in practice?
- The sqlsmith panic in `typecheck.rs:1672` may be related to these structural properties

### Defer: Coq
- Unless the team has existing Coq expertise, Lean 4 is the better investment
- More modern tooling, better IDE, C FFI, growing community
- The proof patterns are identical (decidable properties over finite graphs)
