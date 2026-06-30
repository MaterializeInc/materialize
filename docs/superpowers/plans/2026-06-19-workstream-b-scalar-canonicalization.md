# Workstream B: scalar canonicalization implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store `MirScalarExpr::reduce`-canonicalized scalar payloads at lower time, so the e-graph carries `ReduceScalars`-equivalent forms and raise emits folded scalars.

**Architecture:** The eqsat lowering pass (`src/transform/src/eqsat/lower.rs`) already calls `MirScalarExpr::reduce` on Filter and Map scalars but discards the result, keeping only a boolean `lit` flag and storing the original unfolded expression.
This work changes the stored payload to the reduced expression, and extends reduction to the scalar sites that currently store unfolded expressions verbatim (`Join` equivalences, `Reduce` group keys and aggregates, `TopK` limit).
Raise already returns `EScalar::expr` verbatim, so storing the reduced form canonicalizes the output with no raise-side change.

**Tech Stack:** Rust, `mz_expr::MirScalarExpr`, `mz_transform::eqsat`.

## Global Constraints

* No `as`/as_conversions; use `mz_ore::cast::{CastFrom, CastLossy}`.
* No `unsafe` without a `SAFETY` comment.
* No em-dashes or structuring semicolons in comments; doc comment states the contract, reasoning lives inline.
* No vendor names in user-facing surfaces.
* Never drop existing comments when editing.
* Format with `cargo fmt` after editing Rust; run `bin/lint` and `cargo clippy` before any commit.
* The `enable_eqsat_optimizer` flag is currently defaulted ON, so lowering changes alter plans across the SLT corpus. Do **not** mass-rewrite SLT expected output in these tasks; verify correctness on targeted files and let flag-on CI surface the full corpus churn. Full-corpus rewrite is gated behind the roadmap deletion phases, not this workstream.
* `doc/developer/generated/` is read-only.
* Commit message footer lines:
  ```
  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_016CwwG1wCgqf3v8g6uhf1Nx
  ```

## Soundness invariant (applies to every task)

`MirScalarExpr::reduce` may use column **nullability** to simplify (for example, dropping a null check on a non-nullable column).
The reduced form is therefore valid only in a context whose referenced columns carry the same nullability as the `col_types` passed to `reduce`.
The active relational rewrite rules only permute a scalar's columns or move it into a context that preserves each referenced column's type: filter pushdown past a `Project` or into a `Join` input keeps the referenced columns' types, and column permutation renames indices without changing types.
So a payload reduced once at lower time stays valid through saturation.
A future rule that relaxes nullability (for example, pushing a predicate below an outer join) would break this and must re-reduce or carry the assumption explicitly.
This invariant is the Lean obligation noted in Task 4.

## File structure

* `src/transform/src/eqsat/lower.rs` — all behavioral changes live here. Introduce one private helper `reduced_escalar` and route every scalar lower site through it.
* `src/transform/src/eqsat/ir.rs` — docstring contract update only (Task 4): `EScalar::expr` is now the reduced payload.
* `src/transform/src/eqsat/raise.rs` — docstring contract update only (Task 4): round-trip is semantics-preserving and canonicalizing, not byte-identical.
* `src/transform/tests/test_transforms/eqsat.spec` — one integration case (Task 1).
* `docs/superpowers/specs/2026-06-19-mir-egraph-status.md` — coverage-matrix update (Task 4).

---

### Task 1: Reduce Filter and Map scalar payloads

**Files:**
- Modify: `src/transform/src/eqsat/lower.rs` (the `escalars_in_context` and `escalars_in_map_context` helpers, lines ~151-195)
- Test: `src/transform/src/eqsat/lower.rs` (the `#[cfg(test)] mod tests` block at the bottom)
- Test: `src/transform/tests/test_transforms/eqsat.spec`

**Interfaces:**
- Produces: `fn reduced_escalar(expr: &MirScalarExpr, col_types: &[ReprColumnType]) -> EScalar` — reduces `expr` against `col_types`, stores the reduced form as `EScalar::expr`, and records the `lit` fact from the reduced form. Tasks 2 and 3 reuse this exact helper.

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module at the bottom of `src/transform/src/eqsat/lower.rs`. Extend the existing imports in that module so the following compile: `use mz_expr::BinaryFunc;` and `use mz_repr::Datum;` (the module already imports `MirRelationExpr`, `MirScalarExpr`, `ReprRelationType`, `ReprScalarType`).

```rust
    #[mz_ore::test]
    fn filter_predicate_payload_is_reduced() {
        // `#0 AND true` over a boolean column reduces to `#0`. The stored
        // payload must be the reduced form, not the original conjunction.
        let typ = ReprRelationType::new(vec![ReprScalarType::Bool.nullable(false)]);
        let r = MirRelationExpr::constant(vec![], typ)
            .filter(vec![MirScalarExpr::column(0).and(MirScalarExpr::literal_true())]);
        let rel = lower(&r);
        match rel {
            Rel::Filter { predicates, .. } => {
                assert_eq!(predicates.len(), 1);
                assert_eq!(
                    predicates[0].expr,
                    MirScalarExpr::column(0),
                    "filter predicate payload was not reduced"
                );
            }
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn map_scalar_payload_is_reduced() {
        // `1 + 1` is constant-folded to `2`. The stored Map payload must be the
        // folded literal.
        let one = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
        let sum = one.clone().call_binary(one, BinaryFunc::AddInt64);
        let r = base(1).map(vec![sum]);
        let rel = lower(&r);
        match rel {
            Rel::Map { scalars, .. } => {
                assert_eq!(scalars.len(), 1);
                assert_eq!(
                    scalars[0].expr,
                    MirScalarExpr::literal_ok(Datum::Int64(2), ReprScalarType::Int64),
                    "map scalar payload was not constant-folded"
                );
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p mz-transform --lib eqsat::lower::tests::filter_predicate_payload_is_reduced eqsat::lower::tests::map_scalar_payload_is_reduced`
Expected: FAIL. The stored payload is `e.clone()` (original), so `filter_predicate_payload_is_reduced` sees `#0 AND true` not `#0`, and `map_scalar_payload_is_reduced` sees `1 + 1` not `2`.

- [ ] **Step 3: Introduce the `reduced_escalar` helper**

In `src/transform/src/eqsat/lower.rs`, add this private helper immediately above `escalars_in_context` (after the `lower` function, around line 148):

```rust
/// Reduce `expr` against `col_types` (its evaluation context) and wrap the
/// reduced form in an [`EScalar`], recording the `lit` fact off the reduced
/// expression. This is `ReduceScalars` performed once at lower time, so every
/// scalar canonicalization MIR's simplifier provides (constant folding, CASE
/// coalescing, literal CASE rewriting) lands in the interned payload and rides
/// through saturation unchanged.
///
/// `reduce` may use column nullability to simplify, so the reduced form is only
/// valid in a context whose referenced columns share the nullability of
/// `col_types`. The active rules preserve those types (see the soundness
/// invariant in the workstream plan), so reducing once here is sound.
fn reduced_escalar(expr: &MirScalarExpr, col_types: &[ReprColumnType]) -> EScalar {
    let mut folded = expr.clone();
    folded.reduce(col_types);
    let lit = if folded.is_literal_true() {
        Some(true)
    } else if folded.is_literal_false() {
        Some(false)
    } else {
        None
    };
    EScalar::new(folded, lit)
}
```

- [ ] **Step 4: Route Filter and Map through the helper**

Replace the body of `escalars_in_context` (lines ~151-167) with:

```rust
/// Reduce each scalar against `col_types` and wrap it in an [`EScalar`].
fn escalars_in_context(exprs: &[MirScalarExpr], col_types: &[ReprColumnType]) -> Vec<EScalar> {
    exprs.iter().map(|e| reduced_escalar(e, col_types)).collect()
}
```

Replace the body of `escalars_in_map_context` (lines ~173-195) with:

```rust
/// Like [`escalars_in_context`], but for `Map` scalars: each scalar may
/// reference the columns produced by preceding scalars in the same `Map`, so
/// the fold context grows by each scalar's type as we go. Folding against a
/// context missing those columns would index out of bounds in `typ`/`reduce`.
fn escalars_in_map_context(
    exprs: &[MirScalarExpr],
    mut col_types: Vec<ReprColumnType>,
) -> Vec<EScalar> {
    let mut out = Vec::with_capacity(exprs.len());
    for e in exprs {
        let escalar = reduced_escalar(e, &col_types);
        // Extend the context with this scalar's type so the next scalar can
        // reference it. `reduce` preserves the type, so the reduced payload's
        // type matches the original's.
        let typ = escalar.expr.typ(&col_types);
        out.push(escalar);
        col_types.push(typ);
    }
    out
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p mz-transform --lib eqsat::lower`
Expected: PASS, including the two new tests and the existing lowering tests (the existing tests use bare column references, which reduce to themselves).

- [ ] **Step 6: Confirm the raise round-trip tests still pass**

Run: `cargo test -p mz-transform --lib eqsat::raise`
Expected: PASS. The round-trip tests use bare columns (`column(0)`, `column(1)`), which reduce to themselves, so `raise(lower(x)) == x` still holds for them.

- [ ] **Step 7: Add an end-to-end datadriven case**

Append to `src/transform/tests/test_transforms/eqsat.spec`. First confirm the test-DSL scalar syntax for a constant by grepping an existing spec that writes arithmetic literals: `grep -n "1 + 1\|true AND\|AND true" src/transform/tests/test_transforms/*.spec`. Use the literal-false case already in the file (case (d)) as the syntax reference. Add a Map-fold case:

```
# Case (e): a constant Map scalar is folded at lower time.
#
# `reduced_escalar` runs `MirScalarExpr::reduce` on the payload, so `1 + 1`
# is stored (and raised) as the literal `2`.
apply pipeline=eqsat
Map (1 + 1)
  Get t0
----
Map (2)
  Get t0
```

If the test-DSL parser does not accept `1 + 1` in that position, drop this datadriven case (the two unit tests in Step 1 are the binding coverage) and note in the commit message that the integration case was omitted for DSL-syntax reasons.

- [ ] **Step 8: Generate expected output and verify the spec passes**

Run: `REWRITE=1 cargo test -p mz-transform --test test_transforms` then `cargo test -p mz-transform --test test_transforms`
Expected: the `eqsat.spec` block matches; review the diff to confirm the new case raises `Map (2)` and no unrelated case regressed.

- [ ] **Step 9: Spot-check a scalar-heavy SLT file for correctness**

Run: `bin/sqllogictest --optimized -- test/sqllogictest/arithmetic.slt`
Expected: all queries pass (the STATUS doc records 206/206 here). This confirms folded payloads produce correct results. Do not rewrite the file. If any query returns a wrong **result** (not merely a different plan), stop and report it as a regression.

- [ ] **Step 10: Commit**

```bash
cargo fmt
bin/lint
cargo clippy -p mz-transform
git add src/transform/src/eqsat/lower.rs src/transform/tests/test_transforms/eqsat.spec
git commit -m "eqsat: reduce Filter and Map scalar payloads at lower time

Store the MirScalarExpr::reduce result instead of discarding it, so the
e-graph carries ReduceScalars-equivalent forms and raise emits folded
scalars. This subsumes FoldConstants, CoalesceCase, and CaseLiteral on
Filter and Map payloads."
```

---

### Task 2: Reduce Reduce group keys, aggregates, and TopK limit

**Files:**
- Modify: `src/transform/src/eqsat/lower.rs` (the `Reduce` and `TopK` match arms in `lower`, lines ~97-131)
- Test: `src/transform/src/eqsat/lower.rs` (the `tests` module)

**Interfaces:**
- Consumes: `reduced_escalar(&MirScalarExpr, &[ReprColumnType]) -> EScalar` from Task 1.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/transform/src/eqsat/lower.rs`:

```rust
    #[mz_ore::test]
    fn reduce_group_key_payload_is_reduced() {
        // A group key `#0 AND true` over a boolean column reduces to `#0`.
        let typ = ReprRelationType::new(vec![ReprScalarType::Bool.nullable(false)]);
        let input = MirRelationExpr::constant(vec![], typ);
        let r = MirRelationExpr::Reduce {
            input: Box::new(input),
            group_key: vec![MirScalarExpr::column(0).and(MirScalarExpr::literal_true())],
            aggregates: vec![],
            monotonic: false,
            expected_group_size: None,
        };
        let rel = lower(&r);
        match rel {
            Rel::Reduce { group_key, .. } => {
                assert_eq!(group_key.len(), 1);
                assert_eq!(
                    group_key[0].expr,
                    MirScalarExpr::column(0),
                    "reduce group key payload was not reduced"
                );
            }
            other => panic!("expected Reduce, got {other:?}"),
        }
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p mz-transform --lib eqsat::lower::tests::reduce_group_key_payload_is_reduced`
Expected: FAIL. `group_key` is built with `EScalar::plain(e.clone())`, so the stored payload is the unreduced `#0 AND true`.

- [ ] **Step 3: Reduce the Reduce arm payloads**

In the `Reduce { .. }` arm of `lower` (lines ~97-112), replace the arm body with the following. The arm currently has `input`, `group_key`, `aggregates`, `monotonic`, `expected_group_size` bound.

```rust
        Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        } => {
            // Group keys and aggregate input expressions reference the input
            // columns; fold them against the input type.
            let col_types = input.typ().column_types;
            let aggregates = aggregates
                .iter()
                .map(|agg| {
                    let mut agg = agg.clone();
                    agg.expr.reduce(&col_types);
                    agg
                })
                .collect();
            Rel::Reduce {
                input: Box::new(lower(input)),
                group_key: group_key
                    .iter()
                    .map(|e| reduced_escalar(e, &col_types))
                    .collect(),
                aggregates,
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            }
        }
```

- [ ] **Step 4: Reduce the TopK limit**

In the `TopK { .. }` arm of `lower` (lines ~113-131), the `shape.limit` is an `Option<MirScalarExpr>`. Replace the arm body so the limit is reduced against the input type:

```rust
        TopK {
            input,
            group_key,
            order_key,
            limit,
            offset,
            monotonic,
            expected_group_size,
        } => {
            // The limit is a scalar evaluated over the input; fold it.
            let col_types = input.typ().column_types;
            let limit = limit.as_ref().map(|l| {
                let mut l = l.clone();
                l.reduce(&col_types);
                l
            });
            Rel::TopK {
                input: Box::new(lower(input)),
                shape: TopKShape {
                    group_key: group_key.clone(),
                    order_key: order_key.clone(),
                    limit,
                    offset: *offset,
                    monotonic: *monotonic,
                    expected_group_size: *expected_group_size,
                },
            }
        }
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p mz-transform --lib eqsat::lower`
Expected: PASS, including the new test and all existing lowering and raise tests.

- [ ] **Step 6: Commit**

```bash
cargo fmt
bin/lint
cargo clippy -p mz-transform
git add src/transform/src/eqsat/lower.rs
git commit -m "eqsat: reduce Reduce group keys, aggregates, and TopK limit

Extend lower-time scalar reduction to the Reduce group keys, aggregate
input expressions, and TopK limit, which previously stored unfolded
payloads verbatim."
```

---

### Task 3: Reduce Join equivalence scalars

**Files:**
- Modify: `src/transform/src/eqsat/lower.rs` (the `Join` match arm in `lower`, lines ~71-81)
- Test: `src/transform/src/eqsat/lower.rs` (the `tests` module)

**Interfaces:**
- Consumes: `reduced_escalar(&MirScalarExpr, &[ReprColumnType]) -> EScalar` from Task 1.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/transform/src/eqsat/lower.rs`:

```rust
    #[mz_ore::test]
    fn join_equivalence_payload_is_reduced() {
        // A two-input join over boolean columns with an equivalence containing
        // `#0 AND true`. Join equivalences reference the concatenated input
        // column space, so column 0 of the first input is `#0`. The payload
        // must reduce to `#0`.
        let typ = ReprRelationType::new(vec![ReprScalarType::Bool.nullable(false)]);
        let a = MirRelationExpr::constant(vec![], typ.clone());
        let b = MirRelationExpr::constant(vec![], typ);
        let r = MirRelationExpr::join_scalars(
            vec![a, b],
            vec![vec![
                MirScalarExpr::column(0).and(MirScalarExpr::literal_true()),
                MirScalarExpr::column(1),
            ]],
        );
        let rel = lower(&r);
        match rel {
            Rel::Join { equivalences, .. } => {
                assert_eq!(equivalences.len(), 1);
                assert_eq!(
                    equivalences[0][0].expr,
                    MirScalarExpr::column(0),
                    "join equivalence payload was not reduced"
                );
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p mz-transform --lib eqsat::lower::tests::join_equivalence_payload_is_reduced`
Expected: FAIL. Equivalences are built with `EScalar::plain(e.clone())`, so the stored payload is the unreduced `#0 AND true`.

- [ ] **Step 3: Reduce the Join equivalences**

In the `Join { .. }` arm of `lower` (lines ~71-81), replace the arm body. Join equivalence scalars reference the concatenated input column space, so the fold context is the concatenation of each input's column types in input order:

```rust
        Join {
            inputs,
            equivalences,
            ..
        } => {
            // Equivalence scalars reference the concatenated input column space
            // (input i's columns offset by the sum of prior input arities), so
            // fold them against the concatenation of input column types.
            let mut col_types = Vec::new();
            for inp in inputs {
                col_types.extend(inp.typ().column_types);
            }
            Rel::Join {
                inputs: inputs.iter().map(lower).collect(),
                equivalences: equivalences
                    .iter()
                    .map(|class| class.iter().map(|e| reduced_escalar(e, &col_types)).collect())
                    .collect(),
            }
        }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p mz-transform --lib eqsat::lower`
Expected: PASS, including the new test and all existing tests.

- [ ] **Step 5: Run the WCOJ and live-contract tests**

Run: `cargo test -p mz-transform --test wcoj_decision`
Expected: PASS. Join lowering feeds the WCOJ machinery, so confirm reduced equivalences did not disturb delta-query synthesis or the logical-phase Unimplemented contract.

- [ ] **Step 6: Commit**

```bash
cargo fmt
bin/lint
cargo clippy -p mz-transform
git add src/transform/src/eqsat/lower.rs
git commit -m "eqsat: reduce Join equivalence scalars at lower time

Fold equivalence-class scalars against the concatenated input column
space, completing lower-time scalar reduction across all scalar sites."
```

---

### Task 4: Update contracts, coverage matrix, and measure

**Files:**
- Modify: `src/transform/src/eqsat/ir.rs` (the `EScalar` docstring, lines ~45-60)
- Modify: `src/transform/src/eqsat/raise.rs` (the module and `raise` docstrings, lines ~10-36)
- Modify: `docs/superpowers/specs/2026-06-19-mir-egraph-status.md` (the coverage matrix and the "What is left" Lean note)

**Interfaces:**
- Consumes: the behavioral changes from Tasks 1-3. No code changes here; documentation and measurement only.

- [ ] **Step 1: Update the `EScalar` contract in `ir.rs`**

The `EScalar` docstring (lines ~45-60) currently says "`expr` is the authoritative value (raise returns it verbatim)". Add that `expr` is the reduced payload. Replace the first paragraph of the docstring with:

```rust
/// A scalar payload: the reduced [`MirScalarExpr`] plus one precomputed fact.
///
/// `expr` is the authoritative value (raise returns it verbatim). It is the
/// `MirScalarExpr::reduce` canonical form computed once at lower time against
/// the column types of the relation the scalar is evaluated over, so the
/// e-graph carries `ReduceScalars`-equivalent payloads. The column support and
/// bare-column-reference facts the rewrite rules read are computed live from
/// `expr` (see [`EScalar::cols`], [`EScalar::is_col`]).
```

Leave the `lit` paragraph unchanged.

- [ ] **Step 2: Update the round-trip contract in `raise.rs`**

The module docstring (lines ~16-17) claims `raise(lower(x)) == x` for structurally-lowered plans. That is no longer byte-identical once scalars are reduced. Replace that sentence:

```rust
//! This is the structural inverse of [`crate::eqsat::lower::lower`]. The
//! round-trip is semantics-preserving and scalar-canonicalizing rather than
//! byte-identical: lower reduces every scalar payload, so `raise(lower(x))`
//! returns `x` with its scalars in `MirScalarExpr::reduce` canonical form.
```

- [ ] **Step 3: Update the coverage matrix in the STATUS doc**

In `docs/superpowers/specs/2026-06-19-mir-egraph-status.md`:

In the **Covered** table, change the `FoldConstants` row to cover general folding:

```
| `FoldConstants` | empty-propagation rules, the nullability lit-flag, and lower-time `MirScalarExpr::reduce` on every scalar payload |
```

Add these rows to the **Covered** table:

```
| `ReduceScalars` | lower-time `MirScalarExpr::reduce` on Filter, Map, Join-equivalence, Reduce-key/aggregate, and TopK-limit payloads |
| `CoalesceCase` | subsumed by lower-time `reduce` (CASE coalescing) |
| `CaseLiteralTransform` | subsumed by lower-time `reduce` (literal CASE rewriting) |
```

In the **Missing** "Scalar layer" bullet, remove `ReduceScalars`, general `FoldConstants`, `CoalesceCase`, and `CaseLiteralTransform` from the list (they are now Covered). Keep `LiteralLifting`, `LiteralConstraints`, and `CanonicalizeMfp` (no MFP node) in Missing. The bullet should read approximately:

```
* **Scalar layer**: `LiteralLifting`, `LiteralConstraints`, and `CanonicalizeMfp` (there is no MFP node).
```

In workstream **B** under the Roadmap, update the status to note it is landed: prefix the bullet with "**(done)**" and adjust the tense from "De-opaque" to "De-opaqued".

- [ ] **Step 4: Add the Lean obligation note**

In the STATUS doc "What is left" section, add a bullet:

```
* Add a Lean obligation for the lower-time reduction soundness invariant: reduced scalar payloads are valid only because no active rule moves a scalar into a context with relaxed nullability for its referenced columns. Encode the invariant so a future nullability-relaxing rule is forced to discharge it.
```

- [ ] **Step 5: Measure harness loss reduction**

Run: `cargo test -p mz-transform --test compare_real -- --nocapture`
Expected: the differential harness summary line. Record the new win/loss/tie counts. The STATUS doc currently records 3 wins, 4 losses, 13 ties; the losses traced to missing scalar folding, so they should drop. Update the "Status at a glance" and "Key findings" bullets in the STATUS doc with the measured counts.

> **Premise disproven (post-implementation):** the harness counts were UNCHANGED (3 wins, 4 losses, 13 ties) after the workstream. The 4 losses are structural empty-propagation gaps (eqsat keeps a residual `Filter`; the real optimizer collapses to an empty/constant leaf), not scalar-folding gaps. The workstream's value (subsuming `ReduceScalars`/`CoalesceCase`/`CaseLiteralTransform`, finer hash-consing, and the index-selection prerequisite) is independent of the harness counts. See the STATUS doc "Key findings" for the recorded result.

- [ ] **Step 6: Commit**

```bash
cargo fmt
bin/lint
git add src/transform/src/eqsat/ir.rs src/transform/src/eqsat/raise.rs docs/superpowers/specs/2026-06-19-mir-egraph-status.md
git commit -m "eqsat: document scalar-canonicalization contract and update coverage

Record that EScalar payloads are now reduced canonical forms, the
round-trip is canonicalizing not byte-identical, and move ReduceScalars,
CoalesceCase, CaseLiteral, and general FoldConstants to Covered. Note the
lower-time reduction soundness invariant as a Lean obligation."
```
