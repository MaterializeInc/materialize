# Eqsat WMR version-lift experiment implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the equality-saturation optimizer share and hoist subexpressions across `WITH MUTUALLY RECURSIVE` bindings soundly, by making each recursive reference's iteration version (previous / current / final) explicit in the e-node so congruence stops conflating different versions.

**Architecture:** A recursive `Get` inside a `LetRec` denotes a different collection depending on the binding's position in the sequence (Materialize WMR is sequential: a binding sees the *current* value of earlier siblings and the *previous* value of itself and later siblings, confirmed in `src/compute/src/render.rs:907-923`). Today the e-graph treats every `LocalGet{id}` as one e-class and the CSE pass refuses to hoist anything containing a recursive reference (`is_closed` in `src/transform/src/eqsat/cse.rs:136`), so cross-binding sharing is impossible. This experiment tags each in-`LetRec` reference with its version at lower time, so `Prev(bar)` and `Cur(bar)` become distinct e-nodes that congruence keeps apart. Cross-binding common subexpressions over the same versioned leaves then share soundly, recursion-free subterms hoist out of the loop entirely, and raise reconstructs a binding order that realizes every reference's version. The whole thing is gated behind a default-off feature flag, and with the flag off behavior is byte-identical to today.

**Tech Stack:** Rust, `mz-transform` crate (`src/transform/src/eqsat/`), `mz-repr` (`OptimizerFeatures`), `mz-sql` (system var), `cargo nextest` / `bin/cargo-test`, `bin/sqllogictest`.

## Global Constraints

* Soundness boundary: enable ONLY within-iteration sharing (CSE across bindings) and loop-invariant code motion (hoisting recursion-free subterms out of the `LetRec`). NEVER rewrite across the loop-back edge: no pushing filters/projections into what feeds a `Prev` leaf, no reassociation through the feedback. Those can change the fixpoint trajectory and are unsound with non-monotonic operators (aggregation, negation, retraction) inside the loop.
* Preserve `LetRecLimit`: per-binding limits are carried verbatim today (`Rel::LetRec.limits`); any new binding introduced by hoisting inherits a limit consistent with its consumers, and hoisting must not change the iteration count of existing bindings.
* Feature-flag gated: new flag `enable_eqsat_wmr_lift`, default `false`. With the flag `false`, lowering must set version `None` everywhere so congruence, CSE, raise, and all goldens are unchanged from today.
* No `as` conversions: use `mz_ore::cast::CastFrom` / `CastLossy` (the crate already does, e.g. `usize::cast_from(u64::from(id))` in `lower.rs:39`).
* Comments: no em-dashes or structuring semicolons; doc comment states the contract, reasoning goes inline at the decision point.
* Run `cargo fmt` after editing Rust; run `bin/lint` and `cargo clippy` before committing.

---

## File structure

* `src/transform/src/eqsat/ir.rs` — add `RecVersion` enum and a `version: Option<RecVersion>` field to `Rel::LocalGet`. Owner of the e-node algebra.
* `src/transform/src/eqsat/lower.rs` — classify recursive references as `Prev`/`Cur`/`Final` when the flag is on; thread per-`LetRec` binding context. Owner of MIR to e-graph translation.
* `src/transform/src/eqsat/raise.rs` — version-aware reconstruction: erase versions back to plain `Get(Local)`, reconstruct a binding order that realizes every version, place hoisted bindings. Owner of e-graph to MIR translation.
* `src/transform/src/eqsat/cse.rs` — relax the closed test so subterms closed over versioned leaves can hoist into a shared binding; add loop-invariant hoist. Owner of extraction-time sharing.
* `src/transform/src/eqsat/analysis.rs` — recognize versioned gets as recursive references in the fixpoint analysis.
* `src/transform/src/eqsat/cost.rs` — cost versioned gets as zero (same as `LocalGet` today).
* `src/repr/src/optimize.rs` — add `enable_eqsat_wmr_lift: bool` to `OptimizerFeatures`.
* `src/sql/src/session/vars/definitions.rs` — add the `enable_eqsat_wmr_lift` system var and wire it into the `OptimizerFeatures` mapping.
* `src/transform/tests/eqsat_wmr_lift.rs` — new integration test: round-trip, cross-binding sharing, LICM, and flag-off identity.

---

## Task 1: Add the version tag and the feature flag (flag-off identity)

**Files:**
- Modify: `src/transform/src/eqsat/ir.rs:263-267` (the `Rel::LocalGet` variant)
- Modify: `src/repr/src/optimize.rs` (the `OptimizerFeatures` struct and its `Default`)
- Modify: `src/sql/src/session/vars/definitions.rs` (system var + `OptimizerFeatures` mapping, following `enable_eqsat_ilp_extraction`)
- Test: `src/transform/src/eqsat/ir.rs` (inline `#[mz_ore::test]`)

**Interfaces:**
- Produces: `enum RecVersion { Prev, Cur, Final }` (derives `Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash`); `Rel::LocalGet { id, arity, get, version: Option<RecVersion> }`; `OptimizerFeatures.enable_eqsat_wmr_lift: bool`.

- [ ] **Step 1: Write the failing test** in the `tests` module of `src/transform/src/eqsat/ir.rs`:

```rust
#[mz_ore::test]
fn rec_version_distinguishes_localgets() {
    let prev = Rel::LocalGet { id: 0, arity: 1, get: None, version: Some(RecVersion::Prev) };
    let cur = Rel::LocalGet { id: 0, arity: 1, get: None, version: Some(RecVersion::Cur) };
    // Same id, different version: not equal, so congruence keeps them apart.
    assert_ne!(prev, cur);
    // Version None is the default, flag-off form.
    let plain = Rel::LocalGet { id: 0, arity: 1, get: None, version: None };
    assert_ne!(plain, prev);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform -- eqsat::ir::tests::rec_version`
Expected: compile error, `RecVersion` and field `version` do not exist.

- [ ] **Step 3: Add the enum and field** in `src/transform/src/eqsat/ir.rs`. Above the `Rel` enum:

```rust
/// Which iteration of a `LetRec` binding a recursive reference reads.
///
/// Materialize WMR is sequential: within an iteration a binding sees the
/// current value of bindings ordered before it and the previous value of
/// itself and bindings ordered after it. The body reads the converged value.
/// Tagging the reference makes that distinction explicit in the e-node, so
/// congruence does not merge reads of different versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecVersion {
    /// Previous iteration (the differential feedback variable).
    Prev,
    /// Current iteration (an earlier binding, already bound this iteration).
    Cur,
    /// Converged fixpoint value, read from the `LetRec` body.
    Final,
}
```

Then change the `LocalGet` variant (`ir.rs:263-267`) to:

```rust
    LocalGet {
        id: usize,
        arity: usize,
        get: Option<Box<MirRelationExpr>>,
        /// `None` for ordinary `Let` locals and CSE placeholders. `Some(_)` for a
        /// reference bound by an enclosing `LetRec`, classified by iteration
        /// version. Only set when `enable_eqsat_wmr_lift` is on.
        version: Option<RecVersion>,
    },
```

- [ ] **Step 4: Fix all construction sites to compile.** Every `Rel::LocalGet { .. }` constructor must add `version: None`. Known sites: `lower.rs:42`, `cse.rs` (the placeholder builder), `cost.rs`, `analysis.rs`, `raise.rs` tests, and any `src/transform/tests/` constructors. Pattern matches using `Rel::LocalGet { id, arity, get }` must add `version` or `..`. Use the compiler error list to find them all.

- [ ] **Step 5: Add the feature flag.** In `src/repr/src/optimize.rs`, add `pub enable_eqsat_wmr_lift: bool,` to `OptimizerFeatures` next to `enable_eqsat_ilp_extraction`, and `enable_eqsat_wmr_lift: false,` to its `Default` impl. In `src/sql/src/session/vars/definitions.rs`, copy the `enable_eqsat_ilp_extraction` system-var definition and its line in the `OptimizerFeatures` `From<&SystemVars>` mapping, renaming to `enable_eqsat_wmr_lift` with default `false`.

- [ ] **Step 6: Run tests and verify they pass**

Run: `bin/cargo-test -p mz-transform -- eqsat::ir::tests::rec_version` then `cargo check -p mz-transform -p mz-repr -p mz-sql`
Expected: PASS, workspace compiles.

- [ ] **Step 7: Commit**

```bash
git add src/transform/src/eqsat/ir.rs src/repr/src/optimize.rs src/sql/src/session/vars/definitions.rs
git commit -m "eqsat: add RecVersion tag on LocalGet and enable_eqsat_wmr_lift flag"
```

---

## Task 2: Version-explicit lowering

**Files:**
- Modify: `src/transform/src/eqsat/lower.rs:29-132` (the `lower` entry and `LetRec`/`Get(Local)` arms)
- Test: `src/transform/src/eqsat/lower.rs` (inline `#[mz_ore::test]`)

**Interfaces:**
- Consumes: `RecVersion`, `Rel::LocalGet.version` from Task 1.
- Produces: `pub fn lower_with(expr: &MirRelationExpr, enable_wmr_lift: bool) -> Rel`; `pub fn lower(expr) = lower_with(expr, false)` preserved for existing callers and tests.

Lowering classifies a reference to `LetRec`-bound id `j` from inside binding `i` as `Cur` if `j` is ordered before `i`, else `Prev`; references from the body are `Final`. Classification needs the enclosing `LetRec`'s id-to-position map, threaded through a context.

- [ ] **Step 1: Write the failing test** in `lower.rs` tests:

```rust
#[mz_ore::test]
fn lower_classifies_letrec_versions() {
    // LetRec with two bindings; binding 1 references binding 0 (earlier => Cur),
    // binding 0 references binding 1 (later => Prev). Body reads binding 0 (Final).
    let typ = ReprRelationType::new(vec![ScalarType::Int64.nullable(false)]);
    let g = |id: u64| MirRelationExpr::Get {
        id: Id::Local(LocalId::new(id)),
        typ: typ.clone(),
        access_strategy: AccessStrategy::UnknownOrLocal,
    };
    let r = MirRelationExpr::LetRec {
        ids: vec![LocalId::new(0), LocalId::new(1)],
        values: vec![g(1), g(0)],
        limits: vec![None, None],
        body: Box::new(g(0)),
    };
    let lowered = lower_with(&r, true);
    let versions = collect_localget_versions(&lowered); // test helper, see Step 3
    // (id, version) pairs, sorted.
    assert_eq!(
        versions,
        vec![
            (0, Some(RecVersion::Final)), // body read of 0
            (0, Some(RecVersion::Prev)),  // binding 1 -> 0? no: see below
            (1, Some(RecVersion::Cur)),
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
    );
}
```

Correct the expected set to match the semantics: binding 0 is `g(1)` so it reads id 1, and 1 is ordered after 0, so `Prev`; binding 1 is `g(0)` reading id 0 ordered before 1, so `Cur`; body reads id 0 as `Final`. Final assertion:

```rust
    let expected: std::collections::BTreeSet<(usize, Option<RecVersion>)> = [
        (1, Some(RecVersion::Prev)),  // binding 0 reads later id 1
        (0, Some(RecVersion::Cur)),   // binding 1 reads earlier id 0
        (0, Some(RecVersion::Final)), // body reads id 0
    ]
    .into_iter()
    .collect();
    assert_eq!(versions, expected);
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform -- eqsat::lower::tests::lower_classifies`
Expected: compile error, `lower_with` and `collect_localget_versions` do not exist.

- [ ] **Step 3: Implement.** Add a lowering context and the classification. In `lower.rs`:

```rust
/// Per-`LetRec` lowering context: maps an in-scope recursive id to the version
/// a reference to it should carry from the current position. Absent ids are
/// not bound by an enclosing `LetRec` (ordinary `Let` or outer scope) and stay
/// `version: None`.
#[derive(Clone, Default)]
struct LowerCtx {
    enable_wmr_lift: bool,
    rec_version: std::collections::BTreeMap<usize, RecVersion>,
}

pub fn lower(expr: &MirRelationExpr) -> Rel {
    lower_with(expr, false)
}

pub fn lower_with(expr: &MirRelationExpr, enable_wmr_lift: bool) -> Rel {
    lower_ctx(expr, &LowerCtx { enable_wmr_lift, ..Default::default() })
}
```

Rename the existing `lower` body to `fn lower_ctx(expr: &MirRelationExpr, ctx: &LowerCtx) -> Rel`, replace its internal recursive `lower(x)` calls with `lower_ctx(x, ctx)`, and change the two arms:

`Get(Local)` arm (was `lower.rs:32-47`):

```rust
        Get { id: Id::Local(local), typ, .. } => {
            let id_usize = usize::cast_from(u64::from(local));
            let version = if ctx.enable_wmr_lift {
                ctx.rec_version.get(&id_usize).copied()
            } else {
                None
            };
            Rel::LocalGet {
                id: id_usize,
                arity: typ.arity(),
                get: Some(Box::new(expr.clone())),
                version,
            }
        }
```

`LetRec` arm (was `lower.rs:112-132`):

```rust
        LetRec { ids, values, limits, body } => {
            let id_usizes: Vec<usize> =
                ids.iter().map(|id| usize::cast_from(u64::from(id))).collect();
            let bindings = id_usizes
                .iter()
                .enumerate()
                .zip(values.iter())
                .map(|((i, &id), v)| {
                    // From binding i, an id ordered before i is Cur (already
                    // bound this iteration), id i or later is Prev (the feedback
                    // variable). Outer-scope ids inherit ctx.rec_version.
                    let mut rec_version = ctx.rec_version.clone();
                    for (k, &kid) in id_usizes.iter().enumerate() {
                        rec_version.insert(
                            kid,
                            if k < i { RecVersion::Cur } else { RecVersion::Prev },
                        );
                    }
                    let inner = LowerCtx { enable_wmr_lift: ctx.enable_wmr_lift, rec_version };
                    (id, lower_ctx(v, &inner))
                })
                .collect();
            // Body reads converged values.
            let mut body_version = ctx.rec_version.clone();
            for &id in &id_usizes {
                body_version.insert(id, RecVersion::Final);
            }
            let body_ctx = LowerCtx { enable_wmr_lift: ctx.enable_wmr_lift, rec_version: body_version };
            Rel::LetRec {
                bindings,
                limits: limits.clone(),
                body: Box::new(lower_ctx(body, &body_ctx)),
            }
        }
```

Add the test helper in the tests module:

```rust
fn collect_localget_versions(r: &Rel) -> std::collections::BTreeSet<(usize, Option<RecVersion>)> {
    let mut out = std::collections::BTreeSet::new();
    fn walk(r: &Rel, out: &mut std::collections::BTreeSet<(usize, Option<RecVersion>)>) {
        if let Rel::LocalGet { id, version, .. } = r {
            out.insert((*id, *version));
        }
        for c in r.children() {
            walk(c, out);
        }
    }
    walk(r, &mut out);
    out
}
```

`Rel::children()` (`ir.rs:301`) returns binding values and bodies, so the walk reaches recursive references. Confirm `children()` includes `LetRec` bindings; if it returns only the body, extend the walk to iterate `bindings` explicitly.

- [ ] **Step 4: Run tests and verify they pass**

Run: `bin/cargo-test -p mz-transform -- eqsat::lower::tests`
Expected: PASS.

- [ ] **Step 5: Verify flag-off identity.** Add and run:

```rust
#[mz_ore::test]
fn lower_flag_off_sets_no_versions() {
    // Same LetRec as above, flag off: every LocalGet has version None.
    // ... build r as in lower_classifies_letrec_versions ...
    let lowered = lower_with(&r, false);
    assert!(collect_localget_versions(&lowered).iter().all(|(_, v)| v.is_none()));
}
```

Run: `bin/cargo-test -p mz-transform -- eqsat::lower::tests::lower_flag_off`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/transform/src/eqsat/lower.rs
git commit -m "eqsat: classify LetRec references by iteration version at lower time"
```

---

## Task 3: Version-aware raise round-trip (identity order)

**Files:**
- Modify: `src/transform/src/eqsat/raise.rs:96-114` (the `LocalGet` arm) and the `LetRec` arm (around `raise.rs:278`)
- Test: `src/transform/src/eqsat/raise.rs` tests module (next to `roundtrip_letrec`, `raise.rs:958`)

**Interfaces:**
- Consumes: `Rel::LocalGet.version`, `RecVersion`.
- Produces: raise erases `version` (every versioned `LocalGet` becomes a plain `Get(Local id)`), and for an unmodified lifted `LetRec` keeps the original binding order, so the round trip is semantics-preserving.

For an unmodified plan, version is purely informational on the way out: a `Prev(j)`, `Cur(j)`, or `Final(j)` all raise to `Get(Local j)`, and because the binding order is unchanged each one resolves to exactly the version it was tagged with. Order reconstruction for *hoisted* bindings is deferred to Task 5.

- [ ] **Step 1: Write the failing test** in `raise.rs` tests:

```rust
#[mz_ore::test]
fn roundtrip_letrec_versioned() {
    // The same LetRec as roundtrip_letrec, but lowered with the lift on.
    // Lowering tags versions; raise must erase them and reproduce identical MIR.
    let r = /* build the two-binding LetRec from Task 2's test */;
    let lowered = crate::eqsat::lower::lower_with(&r, true);
    let raised = raise(&lowered, false, &BTreeMap::new());
    assert_eq!(raised, r, "versioned round trip must be identity");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform -- eqsat::raise::tests::roundtrip_letrec_versioned`
Expected: PASS already if `version` is ignored by the `LocalGet` arm via `..`, OR FAIL if a match is non-exhaustive. If it already passes, this task is a no-op for identity order; still add the test as a regression guard and proceed to Step 3 to make the `version` handling explicit.

- [ ] **Step 3: Make version erasure explicit** in the `LocalGet` arm (`raise.rs:96`). The arm already destructures `{ get, id, .. }`, so `version` is ignored by `..`. Add a one-line comment documenting that erasure is intentional and add a debug assertion that catches an unraisable combination:

```rust
        Rel::LocalGet { get, id, version, .. } => {
            // Version is informational only on the way out: binding order (set by
            // the LetRec arm) realizes the version, so every versioned get raises
            // to a plain Get(Local id). A None get with a Final/outer version
            // would be a placeholder we cannot type; that must not occur.
            debug_assert!(
                get.is_some() || *version != Some(RecVersion::Final),
                "Final-version placeholder without an original node (id {id})"
            );
            match get {
                Some(g) => (**g).clone(),
                None => { /* unchanged scope lookup */ }
            }
        }
```

- [ ] **Step 4: Run tests and verify they pass**

Run: `bin/cargo-test -p mz-transform -- eqsat::raise::tests`
Expected: PASS (both `roundtrip_letrec` and `roundtrip_letrec_versioned`).

- [ ] **Step 5: SLT correctness with the flag on.** The on-path is exercised by Task 7's harness; here just guard flag-off. Run the WMR corpus unchanged to confirm no golden drift from Tasks 1-3 (flag defaults off):

Run: `bin/sqllogictest --optimized -- test/sqllogictest/with_mutually_recursive.slt`
Expected: PASS, no diffs.

- [ ] **Step 6: Commit**

```bash
git add src/transform/src/eqsat/raise.rs
git commit -m "eqsat: erase RecVersion on raise; versioned LetRec round-trips to identity"
```

---

## Task 4: Recursion-aware analysis and cost for versioned gets

**Files:**
- Modify: `src/transform/src/eqsat/analysis.rs` (every site that special-cases a recursive `LocalGet`, e.g. `analysis.rs:81, 128, 207, 296, 579`)
- Modify: `src/transform/src/eqsat/cost.rs:400` (the `LocalGet` cost arm)
- Test: `src/transform/src/eqsat/analysis.rs` tests module

**Interfaces:**
- Consumes: `Rel::LocalGet.version`.
- Produces: a `Prev(j)`/`Cur(j)`/`Final(j)` get is treated as a recursive reference to id `j`, identical to today's `LocalGet { get: Some }` recursive back-edge, so the fixpoint analysis and cost are unchanged in value.

The analysis already iterates a recursive reference as the fixpoint variable. Versioned gets must be recognized as recursive references; otherwise saturation under the flag would lose proven facts and regress.

- [ ] **Step 1: Write the failing test** in `analysis.rs` tests:

```rust
#[mz_ore::test]
fn versioned_get_is_recursive_reference() {
    // A keys/recursion analysis over a LetRec lowered with the lift must produce
    // the same facts as without the lift, because Prev/Cur/Final are still
    // references to the same bindings.
    let r = /* a LetRec whose binding has a provable key, as in existing analysis tests */;
    let off = analyze(&crate::eqsat::lower::lower_with(&r, false));
    let on = analyze(&crate::eqsat::lower::lower_with(&r, true));
    assert_eq!(off, on, "versioning must not change analysis facts");
}
```

Use the analysis entry point the existing analysis tests call (locate it in the `analysis.rs` tests module and reuse it as `analyze`).

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform -- eqsat::analysis::tests::versioned_get`
Expected: FAIL, facts differ (versioned gets not recognized as recursive references).

- [ ] **Step 3: Implement.** At each analysis site that matches `Rel::LocalGet { get: Some(..), .. }` or otherwise detects a recursive reference, also treat `Rel::LocalGet { version: Some(_), .. }` as the same recursive reference to `id`. Concretely, where the code currently checks the recursive-reference condition, add the version arm. Example shape (apply at every cited site):

```rust
    Rel::LocalGet { id, version, get, .. } => {
        let is_recursive = version.is_some() || /* existing recursive condition */;
        if is_recursive {
            // use the Rel-level recursion fact for `id`, as today
        } else {
            // non-recursive local, as today
        }
    }
```

In `cost.rs:400`, the arm `Rel::Let { .. } | Rel::LetRec { .. } | Rel::LocalGet { .. } => {}` already costs `LocalGet` as zero regardless of fields, so no cost change is needed. Add a comment noting versioned gets are also zero-cost leaves.

- [ ] **Step 4: Run tests and verify they pass**

Run: `bin/cargo-test -p mz-transform -- eqsat::analysis::tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/transform/src/eqsat/analysis.rs src/transform/src/eqsat/cost.rs
git commit -m "eqsat: treat versioned LocalGets as recursive references in analysis and cost"
```

---

## Task 5: Carry version through ENode, then relax cross-binding CSE

**Files:**
- Modify: `src/transform/src/eqsat/egraph.rs` (the `ENode::LocalGet` variant at `egraph.rs:193-197`, the `Rel`→`ENode` boundary at `egraph.rs:515`, the `ENode`→`Rel` boundary at `egraph.rs:1687`)
- Modify: `src/transform/src/eqsat/cse.rs:91-139` (`worth_binding`, `is_closed`) and the binding-emission logic (`cse.rs:54-78`)
- Modify: `src/transform/src/eqsat/raise.rs` (the `LetRec` arm) to place a hoisted binding at an order slot that realizes its versions
- Test: `src/transform/tests/eqsat_wmr_lift.rs` (new)

**Interfaces:**
- Consumes: `RecVersion`, lowering and analysis from Tasks 2 and 4.
- Produces (Part A): `ENode::LocalGet { id, arity, get, version: Option<RecVersion> }` — the e-graph node carries the version, so two references that differ only in version intern to DISTINCT e-classes. This is the mechanism the whole experiment rests on: without it, congruence (which operates on `ENode`, not `Rel`) re-merges `Prev(j)` and `Cur(j)`.
- Produces (Part B): a subterm whose only outer references are versioned recursive leaves (`Prev`/`Cur`, no ordinary outer-`Let` `get:Some` with `version: None`, no nested `Let`) is eligible to hoist into a new `LetRec` binding; raise places that binding so every `Cur(j)` it reads has `j` ordered before it, every consumer is ordered after it, and every `Prev(m)` it reads has `m` ordered at or after it. Such a slot always exists because the original order satisfies all constraints.

### Part A: carry the version tag through ENode (the congruence-separation mechanism)

> CONTEXT (discovered during implementation): the e-graph interns a SEPARATE `ENode` type, not `Rel`. `ENode::LocalGet` (`egraph.rs:193-197`) had only `{id, arity, get}`, and the `Rel`→`ENode` conversion at `egraph.rs:515` dropped `version` via `..`. `ENode` derives `PartialEq, Eq, PartialOrd, Ord, Hash` over all fields (`egraph.rs:97`), so adding `version` makes congruence distinguish versions. With the flag off, `version` is always `None`, so hashing and congruence are byte-identical to today (no golden drift).

- [ ] **Step A1: Write the failing test** in the new file `src/transform/tests/eqsat_wmr_lift.rs`. It interns two `Rel::LocalGet` differing only in `version` into a fresh `EGraph` and asserts they receive DIFFERENT e-class ids, while two with the SAME version share one id, and (flag-off analog) two `version: None` gets share one id:

```rust
#[mz_ore::test]
fn versioned_localgets_get_distinct_eclasses() {
    use mz_transform::eqsat::egraph::EGraph; // adjust to the real path/visibility
    use mz_transform::eqsat::ir::{Rel, RecVersion};
    let mk = |version| Rel::LocalGet { id: 0, arity: 1, get: None, version };
    let mut g = EGraph::default(); // use the crate's real constructor
    let prev = g.add_rel(&mk(Some(RecVersion::Prev))); // use the crate's real add/intern fn
    let cur = g.add_rel(&mk(Some(RecVersion::Cur)));
    let prev2 = g.add_rel(&mk(Some(RecVersion::Prev)));
    let none1 = g.add_rel(&mk(None));
    let none2 = g.add_rel(&mk(None));
    assert_ne!(prev, cur, "different versions must be different e-classes");
    assert_eq!(prev, prev2, "same version must hash-cons to one e-class");
    assert_eq!(none1, none2, "flag-off (None) gets still share one e-class");
}
```

The exact `EGraph` constructor and intern-method names are in `egraph.rs` (the `Rel`→`ENode` interning path that contains the `egraph.rs:515` arm). Use whatever that path is actually called; do not invent names. If interning is not reachable through a public API, add the test inside `egraph.rs`'s own `tests` module where the internal API is visible.

- [ ] **Step A2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform -- versioned_localgets_get_distinct_eclasses`
Expected: FAIL, `prev == cur` because `ENode::LocalGet` drops the version.

- [ ] **Step A3: Add the field and carry it.** In `egraph.rs`, add `version: Option<RecVersion>` to `ENode::LocalGet` (`egraph.rs:193`). At the `Rel`→`ENode` arm (`egraph.rs:515`) change `Rel::LocalGet { id, arity, get, .. }` to bind `version` and set `version: *version` on the `ENode`. At the `ENode`→`Rel` arm (`egraph.rs:1687`) bind `version` and set `version: *version` on the `Rel` (replacing the hard-coded `version: None`). Import `RecVersion` in `egraph.rs` if needed. Confirm `Sym::LocalGet` and the analysis/`cost` arms that match `ENode::LocalGet { .. }` still use `..` so they are unaffected.

- [ ] **Step A4: Run the test and the full suite**

Run: `bin/cargo-test -p mz-transform -- versioned_localgets_get_distinct_eclasses` then `bin/cargo-test -p mz-transform -- eqsat`
Expected: PASS, no regressions (flag-off plans unchanged because `version` is `None`).

- [ ] **Step A5: Commit**

```bash
git add src/transform/src/eqsat/egraph.rs src/transform/tests/eqsat_wmr_lift.rs
git commit -m "eqsat: carry RecVersion through ENode so congruence separates iteration versions"
```

### Part B: relaxed cross-binding CSE with order-aware placement

- [ ] **Step 1: Write the failing test** in the file `src/transform/tests/eqsat_wmr_lift.rs`:

```rust
// Two bindings sharing an identical subexpression over the SAME versioned leaves
// must, with the lift on, share that subexpression in one hoisted binding, and
// the optimized plan must compute the same result as the input.
#[mz_ore::test]
fn cross_binding_subexpression_is_shared() {
    let input = /* MIR LetRec: bindings b1, b2 each = Filter(p, Map(f, Cur(b0)))
                   over a common b0; the Filter(Map(..)) subterm is identical */;
    let shared_count_before = count_distinct_subterm(&input, /* the Filter(Map) shape */);
    let optimized = run_eqsat_with_wmr_lift(&input); // helper: lower_with(true) -> saturate -> cse -> raise
    // The shared subterm now appears once, behind a new local binding.
    assert!(letrec_binding_count(&optimized) > letrec_binding_count(&input),
        "a shared binding should have been introduced");
    // Differential correctness: same rows at the fixpoint.
    assert_eq!(eval_to_fixpoint(&input), eval_to_fixpoint(&optimized));
}
```

Implement `run_eqsat_with_wmr_lift`, `letrec_binding_count`, and `eval_to_fixpoint` as test helpers. For `eval_to_fixpoint`, reuse the differential evaluation harness in `src/transform/src/eqsat/validation.rs` if it exposes one; otherwise compare via the existing optimizer-equivalence test utility used by `wcoj_decision.rs`.

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform --test eqsat_wmr_lift -- cross_binding`
Expected: FAIL, no shared binding introduced (current `is_closed` rejects versioned subterms).

- [ ] **Step 3: Relax the closed test.** In `cse.rs`, change `is_closed` (`cse.rs:136`) so a recursive *versioned* leaf does not block hoisting, while an ordinary outer-`Let` reference still does:

```rust
/// Returns true iff `rel` can be hoisted into a binding at this scope: it
/// contains no nested `Rel::Let`, and no `LocalGet` that refers to an enclosing
/// ordinary `Let` (`get: Some` with `version: None`). Versioned recursive leaves
/// (`Prev`/`Cur`/`Final`) are allowed: the hoisted binding is placed by raise at
/// an order slot that realizes their versions.
fn is_closed(rel: &Rel) -> bool {
    match rel {
        Rel::Let { .. } => false,
        Rel::LocalGet { version: Some(_), .. } => true,
        Rel::LocalGet { get: Some(_), version: None, .. } => false,
        Rel::LocalGet { .. } => true,
        _ => rel.children().iter().all(|c| is_closed(c)),
    }
}
```

NOTE: hoisting a versioned subterm is only allowed when the lift flag is on. Thread the flag into `cse` (the CSE entry already receives the e-graph/context; pass `enable_wmr_lift` and gate the `version: Some(_) => true` arm on it, falling back to `false` when off so flag-off behavior is unchanged).

- [ ] **Step 4: Implement order-aware placement in raise.** When the `LetRec` arm encounters a hoisted binding (a new id introduced by CSE whose value contains versioned leaves), compute its slot:

```rust
// For a hoisted binding `s` with value V:
//   lower bound  = max position among Cur(j) reads in V (s must come after j)
//   upper bound  = min position among consumers of s (s must come before them)
//   Prev(m) reads impose m at-or-after s, satisfied because consumers (>= s)
//   are the bindings that read s and they sit at/after every Prev target.
// Insert s at lower_bound + 1. The original order proves the interval is
// non-empty.
```

Implement this as a topological placement over the binding list: build the precedence constraints (`Cur(j) -> i` and `i -> Prev` are leaves), insert the new binding, and stable-sort to a valid order. Keep all original bindings in their original relative order.

- [ ] **Step 5: Run tests and verify they pass**

Run: `bin/cargo-test -p mz-transform --test eqsat_wmr_lift -- cross_binding`
Expected: PASS.

- [ ] **Step 6: Verify flag-off unchanged.** Run the full eqsat suite and the WMR SLT with the flag off:

Run: `bin/cargo-test -p mz-transform -- eqsat` then `bin/sqllogictest --optimized -- test/sqllogictest/with_mutually_recursive.slt`
Expected: PASS, no golden drift.

- [ ] **Step 7: Commit**

```bash
git add src/transform/src/eqsat/cse.rs src/transform/src/eqsat/raise.rs src/transform/tests/eqsat_wmr_lift.rs
git commit -m "eqsat: hoist shared cross-LetRec-binding subterms with order-aware placement"
```

---

## Task 6: Loop-invariant code motion out of the LetRec

**Files:**
- Modify: `src/transform/src/eqsat/cse.rs` (recognize recursion-free closed subterms inside a `LetRec` and hoist them above it)
- Modify: `src/transform/src/eqsat/raise.rs` (emit a hoisted invariant as an ordinary `Let` enclosing the `LetRec`)
- Test: `src/transform/tests/eqsat_wmr_lift.rs`

**Interfaces:**
- Consumes: Task 5 hoisting plus the version tags.
- Produces: a subterm inside a `LetRec` binding that contains NO versioned recursive leaf (no `Prev`/`Cur`/`Final`) and no in-`LetRec` reference is loop-invariant and is bound once outside the `LetRec`, computed a single time rather than every iteration.

- [ ] **Step 1: Write the failing test** in `src/transform/tests/eqsat_wmr_lift.rs`:

```rust
#[mz_ore::test]
fn loop_invariant_subterm_hoisted_out() {
    // A binding body contains Filter(p, Get(global g)) with no recursive reference.
    // With the lift on, that subterm must bind outside the LetRec.
    let input = /* MIR LetRec whose binding = Union(Cur(b0), Filter(p, Get(g))) */;
    let optimized = run_eqsat_with_wmr_lift(&input);
    assert!(invariant_bound_outside_letrec(&optimized, /* the Filter(Get g) shape */));
    assert_eq!(eval_to_fixpoint(&input), eval_to_fixpoint(&optimized));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform --test eqsat_wmr_lift -- loop_invariant`
Expected: FAIL, invariant subterm still inside the loop.

- [ ] **Step 3: Implement.** Add a recursion-free predicate and use it to classify a hoisted binding as loop-invariant:

```rust
/// True iff `rel` contains no versioned recursive leaf, so its value does not
/// change across iterations and may be computed once outside the LetRec.
fn is_loop_invariant(rel: &Rel) -> bool {
    match rel {
        Rel::LocalGet { version: Some(_), .. } => false,
        _ => rel.children().iter().all(|c| is_loop_invariant(c)),
    }
}
```

When CSE hoists a subterm that `is_loop_invariant` and `is_closed`, mark its new binding as invariant. In raise, emit invariant bindings as ordinary `MirRelationExpr::Let` wrapping the whole `LetRec` (so they evaluate once), and non-invariant hoists as `LetRec` bindings (Task 5). Gate on the flag.

- [ ] **Step 4: Run tests and verify they pass**

Run: `bin/cargo-test -p mz-transform --test eqsat_wmr_lift`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/transform/src/eqsat/cse.rs src/transform/src/eqsat/raise.rs src/transform/tests/eqsat_wmr_lift.rs
git commit -m "eqsat: hoist loop-invariant subterms out of LetRec"
```

---

## Task 7: SQL-level correctness differential and arrangement measurement

> REFRAMED during execution: there is no differential-dataflow runtime in `mz-transform` unit tests, so `eval_to_fixpoint` does not exist and must NOT be built. The real, load-bearing correctness gate is at the SQL level: run actual `WITH MUTUALLY RECURSIVE` queries with the flag OFF and ON and assert IDENTICAL results. The eqsat flags are settable in sqllogictest via `ALTER SYSTEM SET` (precedent: `test/sqllogictest/transform/eqsat_wins.slt`, `test/sqllogictest/ldbc_bi_physical.slt`).

**Files:**
- Create: `test/sqllogictest/transform/eqsat_wmr_lift.slt` (the correctness differential)
- Create/extend: `src/transform/tests/eqsat_wmr_lift.rs` (the arrangement-count measurement)

**Interfaces:**
- Consumes: everything above; the `enable_eqsat_wmr_lift` system var; the `ArrangementCount` objective in `src/transform/src/eqsat/objective.rs`.
- Produces: a SQL differential proving flag-ON results equal flag-OFF results on a recursive query where the hoisting actually fires and where `Prev`/`Cur` distinctions affect the result, plus a Rust measurement showing the arrangement-count reduction.

### Part 1: SQL-level correctness differential (load-bearing)

This is the gate the Task 5 review flagged as required: it must exercise a MULTI-binding recursive query where the hoisting fires AND where a wrong `Prev`/`Cur` resolution would change the answer. Use a non-monotonic body (aggregation / `EXCEPT` / negation) so the iteration trajectory, not just the fixpoint, is observable.

- [ ] **Step 1: Write `test/sqllogictest/transform/eqsat_wmr_lift.slt`.** Model the header on `test/sqllogictest/transform/eqsat_wins.slt` (`mode cockroach`, the same `ALTER SYSTEM SET` preamble enabling the eqsat optimizer). Create a base table, then a `WITH MUTUALLY RECURSIVE` query with at least two bindings that share an identical subexpression built over a common recursive binding (so cross-binding hoisting fires), with a non-monotonic operator in the loop. Run the query with `enable_eqsat_wmr_lift` OFF and assert its result rows. Then `ALTER SYSTEM SET enable_eqsat_wmr_lift = true` and run the SAME query, asserting the SAME result rows. Identical expected output under both settings is the correctness proof.

Concrete skeleton (fill the exact shapes so hoisting fires; verify with `EXPLAIN` that the two bindings share a subterm):

```
mode cockroach

statement ok
ALTER SYSTEM SET enable_eqsat_optimizer = true

statement ok
CREATE TABLE edges (src int, dst int)

statement ok
INSERT INTO edges VALUES (1,2),(2,3),(3,1),(3,4)

# Flag OFF baseline.
statement ok
ALTER SYSTEM SET enable_eqsat_wmr_lift = false

query II rowsort
WITH MUTUALLY RECURSIVE
  reach(a int, b int) AS (
    SELECT src, dst FROM edges
    UNION SELECT r.a, e.dst FROM reach r JOIN edges e ON r.b = e.src
  ),
  -- two bindings sharing a subterm over `reach`, with a non-monotonic EXCEPT/aggregate
  ...
SELECT ... ORDER BY 1,2
----
<expected rows>

# Flag ON: identical result required.
statement ok
ALTER SYSTEM SET enable_eqsat_wmr_lift = true

query II rowsort
<the EXACT same query>
----
<the same expected rows>
```

- [ ] **Step 2: Generate the expected output and confirm hoisting fires.** Run with `--rewrite-results` to fill the expected rows from the flag-OFF run, then confirm the flag-ON block has identical expected rows (do not hand-edit them apart). Separately run `EXPLAIN OPTIMIZED PLAN FOR <the WMR query>` with the flag on and confirm in the plan text that a shared binding is introduced (the hoist actually fires); if it does not fire, reshape the query until it does, otherwise the test proves nothing.

Run: `bin/sqllogictest --optimized -- test/sqllogictest/transform/eqsat_wmr_lift.slt`
Expected: PASS, both blocks identical.

- [ ] **Step 3: Commit Part 1.**

```bash
git add test/sqllogictest/transform/eqsat_wmr_lift.slt
git commit -m "eqsat: WMR lift SQL correctness differential (flag on == off)"
```

### Part 2: arrangement-count measurement (the sharing win)

- [ ] **Step 4: Add a Rust measurement test** in `src/transform/tests/eqsat_wmr_lift.rs`. Reuse the fixtures already built in Tasks 5 and 6. For each, lower with the lift on, saturate, extract, and compute the arrangement count via the `ArrangementCount` objective (`src/transform/src/eqsat/objective.rs` — use the same path the existing eqsat tests use to score a plan). Assert `count_with_lift <= count_without_lift`, and `eprintln!` both counts so the win is visible with `--nocapture`. Drive at least one fixture to a STRICT reduction.

```rust
#[mz_ore::test]
fn wmr_lift_reduces_arrangement_count() {
    for fixture in wmr_fixtures() {
        let with_lift = arrangement_count_with(&fixture.mir, true);
        let without = arrangement_count_with(&fixture.mir, false);
        eprintln!("{}: arrangements with-lift={} without={}", fixture.name, with_lift, without);
        assert!(with_lift <= without, "{}: lift must not increase arrangements", fixture.name);
    }
}
```

Implement `arrangement_count_with(mir, enable_wmr_lift)` using the crate's existing lower/saturate/extract path and the `ArrangementCount` objective; do NOT invent a new scoring path. If the existing API does not expose a count cheaply, count distinct `(e-class, key)` arrangements on the extracted `Rel` the same way `ArrangementCount` does.

- [ ] **Step 5: Run, lint, format.**

Run: `bin/cargo-test -p mz-transform --test eqsat_wmr_lift -- --nocapture` then `cargo fmt`, `bin/lint`, `cargo clippy -p mz-transform`
Expected: PASS with a printed arrangement-count reduction on at least one fixture; no warnings.

- [ ] **Step 6: Commit Part 2.**

```bash
git add src/transform/tests/eqsat_wmr_lift.rs
git commit -m "eqsat: WMR lift arrangement-count measurement"
```

---

## Self-review notes

* Soundness: Tasks 5 and 6 only share within-iteration subterms and hoist loop-invariant ones. Neither crosses the loop-back edge, so the non-monotonic hazard in the Global Constraints does not arise. The differential gate in Task 7 is the backstop.
* Flag-off identity: Tasks 1-3 set `version: None` when the flag is off and erase versions on raise, and Tasks 5-6 gate hoisting on the flag, so default behavior and all goldens are unchanged. Task 3 Step 5 and Task 5 Step 6 verify this.
* Type consistency: `RecVersion` and `Rel::LocalGet.version: Option<RecVersion>` are introduced in Task 1 and used unchanged in Tasks 2-6. `lower_with(expr, bool)` (Task 2) is the single on-path entry the harness in Tasks 5-7 calls.
* Open risk to watch: order-aware placement (Task 5 Step 4) is the subtle part. If a fixture ever fails to find a valid slot, that signals a real conflict (a binding both `Cur` and `Prev` of the same id at incompatible positions), which should not occur for inputs derived from a well-formed `LetRec`. Surface it as a panic with the constraints, do not silently fall back.
* `eval_to_fixpoint` depends on a differential evaluation helper. If `validation.rs` does not expose one, Task 5 Step 1 must first add a minimal fixpoint evaluator over the MIR `LetRec`, or reuse the optimizer-equivalence harness from `src/transform/tests/wcoj_decision.rs`. Confirm before starting Task 5.
