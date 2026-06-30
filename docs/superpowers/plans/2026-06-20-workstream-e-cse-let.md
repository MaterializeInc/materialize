# Workstream E: CSE via extraction (Let for shared e-classes) implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Wire the existing extraction-time common-subexpression elimination into the live eqsat path so the raised plan binds shared subterms with `Let`, subsuming the production `RelationCSE` (and reducing `NormalizeLets` to a no-op).

**Architecture:** Extraction (`EGraph::extract_with`) produces a `Rel` TREE that duplicates any e-class used by more than one parent. `cse::eliminate_common_subexpressions` already rewrites that tree into a DAG with `Rel::Let` bindings and `Rel::LocalGet { get: None }` references, but it is dead code: never called from `optimize_inner`, and `raise` panics on a `LocalGet { get: None }`. This workstream calls CSE between extraction and raise, and teaches `raise` to emit a real `MirRelationExpr::Get { Id::Local, typ }` for a CSE-bound local by threading a scope of bound types.

**Tech Stack:** Rust, `mz_transform::eqsat::{cse, raise}`.

## Global Constraints

* No `as`/as_conversions; use `mz_ore::cast`. No `unsafe` without `SAFETY`.
* No em-dashes or structuring semicolons in comments. No vendor names in user-facing surfaces. Never drop comments.
* `cargo fmt`; `bin/lint`; `cargo clippy -p mz-transform` before commit (ignore buf/trufflehog env failures).
* `doc/developer/generated/` read-only.
* `enable_eqsat_optimizer` is default ON, so CSE changes the raised plan across the SLT corpus. A wrong `Get` type would crash Typecheck at startup (the same failure class as a prior canonicalization bug), so the arithmetic.slt gate is BINDING for E-T1.
* Process hygiene for environmentd/SLT: ONE run at a time; `killall clusterd sqllogictest` (exact names, NEVER `pkill -f`, which kills the shell; you are alone on the machine).
* Commit footer:
  ```
  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_016CwwG1wCgqf3v8g6uhf1Nx
  ```

## Reference (from exploration)

* `cse::eliminate_common_subexpressions(rel: &Rel) -> Rel` (`src/transform/src/eqsat/cse.rs` ~28): counts subtrees, binds compound ones with count >= 2 as `Rel::Let` (smallest innermost) + `Rel::LocalGet { id, arity, get: None }`. Excludes `Get`/`Constant`/`Opaque`/`LocalGet`. Correct, unit-tested, `pub`, but never called live.
* Live path (`src/transform/src/eqsat.rs` `optimize_inner` ~89): `let best = optimizer.optimize(rel).plan;` (line ~101, a tree) then `raise::raise(&best, commit_wcoj)` (~105) then `coalesce_mfp`. The CSE call hook is between these.
* `raise.rs` `Rel::Let` arm (~87) already emits `MirRelationExpr::Let { id, value, body }` correctly. The `Rel::LocalGet { get, id, .. }` arm (~47) panics when `get` is `None`. That is the only blocker.
* `Rel` carries arity but NOT column types; the local `Get` needs a `RelationType`. Obtain it by raising the `Let` value and calling `.typ()` on the resulting `MirRelationExpr`.
* CSE ids start at 0 and can clash with lower'd `Let`/`LocalGet` ids (real `LocalId`s). Use fresh ids above the max id already present.
* Production `RelationCSE` (`src/transform/src/cse/relation_cse.rs`) + `NormalizeLets` (`src/transform/src/cse/normalize_lets.rs`) are what this subsumes.

## File structure

* `src/transform/src/eqsat/cse.rs` — make CSE ids fresh relative to ids already in the tree (E-T1).
* `src/transform/src/eqsat/raise.rs` — thread a `BTreeMap<usize, ReprRelationType>` scope so `LocalGet { get: None }` raises to a local `Get` with the bound value's type (E-T1).
* `src/transform/src/eqsat.rs` — call `cse::eliminate_common_subexpressions` in `optimize_inner` (E-T1).
* `docs/superpowers/specs/2026-06-19-mir-egraph-status.md` — coverage (E-T2).

---

### Task 1: Wire CSE into the live path with a correct raise round-trip

**Files:**
- Modify: `src/transform/src/eqsat.rs` (`optimize_inner`: call CSE after extraction, before raise)
- Modify: `src/transform/src/eqsat/raise.rs` (scope-threaded raise for `LocalGet { get: None }`)
- Modify: `src/transform/src/eqsat/cse.rs` (fresh ids)
- Test: `src/transform/src/eqsat/raise.rs` and/or `cse.rs` tests; `eqsat.spec` datadriven

**Interfaces:**
- Consumes: `cse::eliminate_common_subexpressions(&Rel) -> Rel`.

- [ ] **Step 1: Write the failing test**

In `raise.rs` tests, build a `Rel` containing a `Rel::Let { id, value, body }` where `body` references the binding via `Rel::LocalGet { id, arity, get: None }` (the shape CSE produces), and assert `raise` returns a `MirRelationExpr::Let` whose body contains a `Get { Id::Local(id) }` with the correct arity/type, WITHOUT panicking. (Currently raise panics on `get: None`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p mz-transform --lib eqsat::raise`
Expected: FAIL (panic "raise of a placeholder LocalGet ... without an original node").

- [ ] **Step 3: Thread a binding-type scope through raise**

Change `raise` so it carries a `BTreeMap<usize, mz_repr::ReprRelationType>` of in-scope CSE-bound local types (or refactor into an inner `raise_inner(rel, commit_wcoj, scope: &mut BTreeMap<usize, ReprRelationType>)`). In the `Rel::Let { id, value, body }` arm: raise `value` to `mir_value`, compute `let typ = mir_value.typ();`, insert `(id, typ)` into the scope, raise `body`, then emit `MirRelationExpr::Let { id, value: mir_value, body }` and remove `id` from scope on the way out. In the `Rel::LocalGet { get, id, arity }` arm: if `get` is `Some(g)`, return it verbatim (unchanged behavior for lower'd locals); if `None`, look up `scope[id]` and emit `MirRelationExpr::Get { id: Id::Local(LocalId::new(u64::cast_from(*id))), typ: scope[id].clone(), access_strategy: AccessStrategy::UnknownOrLocal }`. If the id is not in scope, keep the existing panic (a genuine bug). Preserve the public `raise(rel, commit_wcoj)` signature (it initializes an empty scope and calls the inner).

- [ ] **Step 4: Make CSE ids fresh**

In `cse.rs`, before assigning CSE binding ids, compute the maximum `id` already present in the tree (`Rel::Let { id }` and `Rel::LocalGet { id }` across the tree) and start CSE ids at `max + 1`, so CSE-introduced ids never clash with lower'd `Let`/`LocalGet` ids. Add a helper that walks the `Rel` collecting ids. Keep the existing CSE logic otherwise.

- [ ] **Step 5: Call CSE in the live path**

In `eqsat.rs` `optimize_inner`, after `let best = optimizer.optimize(rel).plan;` and before `raise::raise(&best, ...)`, insert `let best = crate::eqsat::cse::eliminate_common_subexpressions(&best);`. Add a comment: this hoists e-classes referenced more than once into `Let` bindings, subsuming `RelationCSE`.

- [ ] **Step 6: Run unit tests**

Run: `cargo test -p mz-transform --lib eqsat`
Expected: PASS, including the new raise test and the existing cse.rs tests. Existing round-trip tests that have no shared subterms are unaffected (CSE is a no-op on them).

- [ ] **Step 7: Add a datadriven witness**

Add an `eqsat.spec` case whose extracted plan has a shared subterm (e.g. a self-union or self-join of an identical filtered Get) and assert the output contains a `Let` binding with the shared subterm bound once. Generate with `REWRITE=1 cargo test -p mz-transform --test test_transforms`, verify the `Let` appears.

- [ ] **Step 8: BINDING GATE - arithmetic.slt with the flag on**

CSE changes the raised plan, and a wrong local `Get` type crashes Typecheck at catalog init. Run: `killall clusterd sqllogictest`, then `bin/sqllogictest --optimized -- test/sqllogictest/arithmetic.slt`. It MUST complete (exit 0), report a pass count (expect 206/206), have ZERO "Non-positive multiplicity", and NO Typecheck panic. killall after. If it panics at startup (a type mismatch on a CSE-bound Get), the type threading is wrong: fix it (the bound value's `.typ()` must match what the body expects) before declaring done. Report the wall time and pass count.

- [ ] **Step 9: wcoj + roundtrip + compare_real**

Run: `cargo test -p mz-transform --test wcoj_decision --test roundtrip`; `cargo test -p mz-transform --test compare_real -- --nocapture` (still terminates; record counts).

- [ ] **Step 10: Commit**

```bash
cargo fmt
bin/lint
cargo clippy -p mz-transform
git add src/transform/src/eqsat.rs src/transform/src/eqsat/raise.rs src/transform/src/eqsat/cse.rs src/transform/tests/test_transforms/eqsat.spec <test files>
git commit -m "eqsat: emit Let for shared e-classes at extraction (CSE)

Wire cse::eliminate_common_subexpressions into the live path between
extraction and raise, and teach raise to emit a local Get for a
CSE-bound LocalGet by threading the bound value's type. CSE ids are
made fresh to avoid clashing with lowered Let ids. Subsumes RelationCSE."
```

---

### Task 2: Coverage, measure, and deferral notes

**Files:**
- Modify: `docs/superpowers/specs/2026-06-19-mir-egraph-status.md`

- [ ] **Step 1: Update coverage**

Move `RelationCSE` to **Covered** (mechanism: extraction-time CSE hoists shared e-classes into `Let`, wired into the live path). Note `NormalizeLets` is **Partial** (eqsat CSE produces well-formed Let/Get with fresh ids; a following `NormalizeLets` is reduced to renumbering/inlining). Update the Missing list: remove `RelationCSE`. Keep `FlatMap`/`ArrangeBy`/`LetRec`/non-empty `Constant` in Missing with a note that structural de-opaquing is DEFERRED (low value without rules that see through them; the engine already peels recursive scopes; non-empty Constant rows are not tracked).

- [ ] **Step 2: Deletion-phase note**

Note that workstream E supplies the `RelationCSE`/`NormalizeLets` capability for deletion phase 2 (with C), gated on flag-on SLT parity.

- [ ] **Step 3: Measure**

Run `cargo test -p mz-transform --test compare_real -- --nocapture`; record counts.

- [ ] **Step 4: Commit**

```bash
cargo fmt
bin/lint
git add docs/superpowers/specs/2026-06-19-mir-egraph-status.md
git commit -m "eqsat: document CSE coverage (RelationCSE), defer de-opaque variants"
```
