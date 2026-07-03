# SP2b Slice 6d: `flatten_assoc` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Port `flatten_assoc` (`scalar/rules.rs:334`) into the CombinedLang DSL for all five associative variadics (And, Or, Coalesce, Greatest, Least), behavior-neutral against the standalone `ScalarEGraph` oracle.

**Architecture:** A new `RestFilter::FlattenSameFunc { func }` on 6c's `TElem::FilterSplice` scaffold, backed by a `rest_filters::rest_flatten` helper that splices non-circular same-func operands, guarded by a `Cond::FlattenApplies` fire guard. Five per-func rules. Lean: And/Or proved outright, Coalesce/Greatest/Least opaque permanent-sorry (count 7 -> 10). This mirrors 6e's `absorb` machinery (carry-func-kw filter + shared guard core) at every seam.

**Tech Stack:** Rust (`mz-transform`), the eqsat DSL, Lean 4 (`lake`).

## Global Constraints

- Behavior-neutral. Never run sqllogictest with `--rewrite`. Production untouched. No production-default flag flips.
- All five rules are `colored: false`. No analysis-lattice additions (slice-3 freeze). Keep the build-enforced FilterSplice/color boundary 6c established.
- Port BOTH termination guards exactly: the circular-ref skip (`inner_canons.contains(&canon)`, rules.rs:359-370) and the `FLATTEN_MAX_OPERANDS = 4096` cap (rules.rs:395). These are termination guards, not semantic guards; the rule is otherwise UNCONDITIONAL (no could_error/nullability gate).
- Adding a matcher/cond/codegen surface -> sweep ALL `MatchGraph` impls incl `ColoredView` (slice-3 E0046). Run `cargo check --tests`, not bare `cargo check` (slice-3 E0502).
- Error operands in tests: Bool-typed `(1/0)=(1/0)` under And/Or (the 6c ill-typed-collapse trap; flatten-then-collapse trips it), well-typed operands for Coalesce/Greatest/Least.
- Lean image does not build in CI (owner-confirmed). Permanent-sorry trip-wire bump (7 -> 10) and aggregate `lake build` are hand-run MANDATORY gates. Do not propose CI wiring.
- `doc/developer/generated/` is read-only. Format with `bin/fmt`, `cargo check` before reporting. Stage only your own files; never broad-clean unstaged scratch.

---

### Task 1: DSL surface + grammar

**Files:**
- Modify: `src/transform/src/eqsat/dsl.rs` (add `RestFilter::FlattenSameFunc`, `Cond::FlattenApplies`)
- Modify: `src/transform/build/grammar.rs` (parse `flatten(xs, func)` and `flatten_applies(xs, func)`; extend `variadic_func_kw` to 5 funcs)

**Interfaces:**
- Produces: `RestFilter::FlattenSameFunc { func: String }`, `Cond::FlattenApplies { list: String, func: String }`, grammar productions `flatten(list, funckw)` -> `TElem::FilterSplice { list, filter: FlattenSameFunc { func } }` and `flatten_applies(list, funckw)` -> `Cond::FlattenApplies { list, func }`.

- [ ] **Step 1: Add the DSL variants.** In `dsl.rs`, add to `RestFilter` (after `AbsorbSubsumed`, dsl.rs:293) a variant mirroring `AbsorbSubsumed`'s shape:
```rust
    /// Splice each operand that is a non-circular same-`func` variadic node
    /// (replacing it with that node's inner operands), leaving other operands.
    /// `func` is the parent variadic keyword. Backs `flatten_assoc`.
    FlattenSameFunc { func: String },
```
Add to `Cond` (near `AbsorbApplies`) a variant:
```rust
    /// `flatten_applies(list, func)`: some operand of `list` is a non-circular
    /// same-`func` variadic node, so `flatten` would change the list. The fire
    /// guard for `flatten_assoc`, sharing `rest_filters`' flatten core.
    FlattenApplies { list: String, func: String },
```

- [ ] **Step 2: Grammar.** In `grammar.rs`, extend `variadic_func_kw` (grammar.rs:157) to accept `coalesce`, `greatest`, `least` in addition to `and`/`or`. Add a `flatten` `TElem` parser mirroring `absorb` (grammar.rs:499): `flatten(ident, variadic_func_kw)` -> `TElem::FilterSplice { list, filter: RestFilter::FlattenSameFunc { func } }`, wired into the listtmpl element `choice` alongside `absorb`. Add a `flatten_applies` cond parser mirroring `absorb_applies`, wired into the scalar-cond sub-`choice`. (Watch the chumsky 26-tuple limit; put the new cond in the existing nested sub-choice, a sibling group if full, never a 3rd nesting level.)

- [ ] **Step 3: Parser unit tests.** Add tests mirroring the `absorb`/`absorb_applies` parser tests: `flatten(xs, and)` and `flatten(xs, coalesce)` parse to the right `TElem`; `flatten_applies(xs, or)` to the right `Cond`; element ordering (flatten before a following item) holds.

- [ ] **Step 4: `cargo check -p mz-transform` + run parser tests.**
Run: `bin/cargo-test -p mz-transform eqsat` (parser tests live under the build/grammar module; if they run via a dedicated bin, run that). Expected: parser tests pass; note the 5 expected `cargo check` errors from the not-yet-added codegen arms (Task 2 handles them).

- [ ] **Step 5: `bin/fmt`, commit** (stage only `dsl.rs`, `grammar.rs`).

---

### Task 2: `rest_filters` core + cond sweep + codegen

**Files:**
- Modify: `src/transform/src/eqsat/rest_filters.rs` (add `flatten_applies`, `rest_flatten`, `FLATTEN_MAX_OPERANDS`)
- Modify: `src/transform/src/eqsat/egraph/view.rs` and the ColoredView impl (`colored/view.rs`) (`cond_flatten_applies`)
- Modify: `src/transform/build/codegen.rs` (FilterSplice/Cond emit + `is_color_exact` + serialization + `variadic_func_value` 3 keywords)

**Interfaces:**
- Consumes: `CNode`, `EGraph`, `Id`, `SNode::CallVariadic`, `mz_expr::VariadicFunc`, `g.nodes`, `g.find` (same imports `rest_filters.rs` already uses for `inner_set`/`absorb_drop_index`).
- Produces: `pub(crate) fn flatten_applies(g: &EGraph, ids: &[Id], func: &VariadicFunc) -> bool`, `pub(crate) fn rest_flatten(g: &EGraph, ids: &[Id], func: &VariadicFunc) -> Vec<Id>`; `MatchGraph::cond_flatten_applies(&self, ids: &[Id], func: &VariadicFunc) -> bool`.

- [ ] **Step 1: Port the flatten core into `rest_filters.rs`.** Transcribe `rules.rs:334-402` into the `rest_filters` idiom (helpers take `&EGraph`, use `g.find`/`g.nodes`, same as `inner_set`/`absorb_drop_index`). Add:
```rust
/// Hard cap on the operand vector `rest_flatten` produces, defense-in-depth
/// against transitive same-func cycles the one-hop circular-ref skip misses.
/// Above any realistic predicate width and above `MAX_ENODES`, so it never
/// triggers on legitimate input. Declining only means less flattening.
const FLATTEN_MAX_OPERANDS: usize = 4096;

/// The canonical, non-circular inner ids of `operand` under `func`, if its class
/// holds a same-`func` variadic node whose canonicalized children do not contain
/// `find(operand)` (the circular-ref skip: after `and_or_single` collapses
/// `f(x)` into x's class, x's class holds `f`-nodes pointing back, and splicing
/// them would replace one operand with N copies, exponential across iterations).
fn flatten_inner(g: &EGraph, operand: Id, func: &VariadicFunc) -> Option<Vec<Id>> {
    let canon = g.find(operand);
    for node in g.nodes(canon) {
        if let CNode::Scalar(SNode::CallVariadic { func: inner_func, exprs }) = node {
            if &inner_func != func {
                continue;
            }
            let inner_canons: Vec<Id> = exprs.iter().map(|&e| g.find(e)).collect();
            if inner_canons.contains(&canon) {
                continue;
            }
            return Some(inner_canons);
        }
    }
    None
}

/// Whether `flatten` would change `ids`: some operand is a non-circular same-func
/// node. The fire guard's core, shared with `rest_flatten` so they never disagree.
pub(crate) fn flatten_applies(g: &EGraph, ids: &[Id], func: &VariadicFunc) -> bool {
    ids.iter().any(|&id| flatten_inner(g, id, func).is_some())
}

/// Splice every non-circular same-`func` operand's inner ids in place, keeping
/// other operands. Caps the result at `FLATTEN_MAX_OPERANDS` (declining returns
/// the input unchanged). Ports `scalar::rules::flatten_assoc`.
pub(crate) fn rest_flatten(g: &EGraph, ids: &[Id], func: &VariadicFunc) -> Vec<Id> {
    let mut out: Vec<Id> = Vec::with_capacity(ids.len());
    let mut spliced = false;
    for &id in ids {
        if let Some(inner) = flatten_inner(g, id, func) {
            out.extend(inner);
            spliced = true;
        } else {
            out.push(id);
        }
    }
    if !spliced || out.len() > FLATTEN_MAX_OPERANDS {
        return ids.to_vec();
    }
    out
}
```
(NOTE: match the actual `SNode::CallVariadic` field/enum access already used by `inner_set` at rest_filters.rs:60-73; the `CNode::Scalar(SNode::CallVariadic { .. })` destructure and `&inner_func != func` comparison must compile against the real types.)

- [ ] **Step 2: `cond_flatten_applies` sweep.** Add to the `MatchGraph` trait and BOTH impls, mirroring `cond_absorb_applies`: BaseView delegates to `flatten_applies(self.eg, ids, func)`; ColoredView returns inert `false`. Add a doc line matching the absorb precedent.

- [ ] **Step 3: Codegen.** In `codegen.rs`:
  - `listtmpl_stmts` (codegen.rs:1134): add a `RestFilter::FlattenSameFunc { func }` arm mirroring `AbsorbSubsumed` (codegen.rs:1142), calling `rest_flatten(g, &<ids>, &{variadic_func_value(func)})`.
  - Cond emit (codegen.rs:564): add a `Cond::FlattenApplies { list, func }` arm mirroring `AbsorbApplies`, calling `cond_flatten_applies`.
  - `cond_is_color_exact` (codegen.rs:67): `Cond::FlattenApplies { .. } => false` (structural read, mirror `HasDuplicateId`/`AbsorbApplies`).
  - Serialization arms (codegen.rs:1516 for the `RestFilter`, codegen.rs:1699 for the `Cond`) mirroring `AbsorbSubsumed`/`AbsorbApplies`.
  - `variadic_func_value` (codegen.rs:188): add `"coalesce"`, `"greatest"`, `"least"` arms mirroring `and`/`or`: `mz_expr::VariadicFunc::Coalesce(mz_expr::func::variadic::Coalesce)`, `...::Greatest(...::Greatest)`, `...::Least(...::Least)`. `cargo check` validates the exact module paths.

- [ ] **Step 4: `cargo check --tests -p mz-transform`.** Expected: clean (the DSL from Task 1 now has all codegen arms). Run `bin/cargo-test -p mz-transform eqsat::egraph` (view tests) + `eqsat::rest_filters` if present. All pass.

- [ ] **Step 5: `bin/fmt`, commit** (stage only `rest_filters.rs`, `egraph/view.rs`, `colored/view.rs`, `build/codegen.rs`).

---

### Task 3: The five rules

**Files:**
- Modify: `src/transform/src/eqsat/rules/scalar.rewrite`

**Interfaces:**
- Consumes: Task 1-2 grammar + helpers. Produces: 5 new `SCALAR_COMPILED_RULES` (26 -> 31); `COMPILED_RULES` unchanged (37).

- [ ] **Step 1: Add the five rules** (after the 6e absorb rules), mirroring the absorb rule form:
```
rule flatten_and { doc "And(.., And(inner..), ..) = And(.., inner.., ..) (associativity)"
    Variadic[and](xs...) => Variadic[and](flatten(xs, and)) where flatten_applies(xs, and) }
rule flatten_or  { doc "Or(.., Or(inner..), ..) = Or(.., inner.., ..) (associativity)"
    Variadic[or](xs...)  => Variadic[or](flatten(xs, or))   where flatten_applies(xs, or) }
rule flatten_coalesce { doc "Coalesce(.., Coalesce(inner..), ..) = Coalesce(.., inner.., ..) (associativity)"
    Variadic[coalesce](xs...) => Variadic[coalesce](flatten(xs, coalesce)) where flatten_applies(xs, coalesce) }
rule flatten_greatest { doc "Greatest(.., Greatest(inner..), ..) = Greatest(.., inner.., ..) (associativity)"
    Variadic[greatest](xs...) => Variadic[greatest](flatten(xs, greatest)) where flatten_applies(xs, greatest) }
rule flatten_least { doc "Least(.., Least(inner..), ..) = Least(.., inner.., ..) (associativity)"
    Variadic[least](xs...) => Variadic[least](flatten(xs, least)) where flatten_applies(xs, least) }
```

- [ ] **Step 2: `cargo check -p mz-transform`** (build.rs regenerates). Expected: clean; a missing keyword or helper name fails here.

- [ ] **Step 3: Confirm counts.** `SCALAR_COMPILED_RULES` 26 -> 31, `COMPILED_RULES` unchanged (37). Run `bin/cargo-test -p mz-transform eqsat 2>&1 | tail -20`, no failures introduced (the known corpus `test_runner` line-4 failure predates the branch, ignore).

- [ ] **Step 4: `bin/fmt`, commit** (stage only `scalar.rewrite`).

---

### Task 4: Lean — And/Or proved outright, Coalesce/Greatest/Least opaque

**Files:**
- Modify: `src/transform/lean/MirRewrite/Semantics.lean` (Lean flatten helper + proofs for and/or; opaque variadic constructor + denotation for coalesce/greatest/least)
- Modify: `src/transform/src/eqsat/lean.rs` (`scalar_variadic_ctor` 3 arms; `FlattenSameFunc` render; `choose_proof` arms)
- Regenerate: `src/transform/lean/MirRewrite/Generated.lean` (via `cargo run -p mz-transform --example gen-lean`)
- Modify: `ci/test/lean-mir-rewrite.sh` (`expected_permanent` 7 -> 10, comment)

**Interfaces:**
- Consumes: the five rules (Task 3). The Lean side is NOT generic on the func name (`translate_tmpl`/`scalar_variadic_ctor` panic on unmapped funcs, and the named Lean function must exist), the same landmine as slice 6b.

- [ ] **Step 1: Lean model additions (`Semantics.lean`).**
  - Add a Lean list-flatten helper `flattenSameFuncAnd` / `flattenSameFuncOr` (or one parameterized helper) that, given a `List ScalarExpr`, replaces each `andE ys` (resp `orE ys`) element with `ys`, keeping others. Model on the existing `dedupById` / `absorbInnerOr` structural helpers.
  - Prove `denoteSFold_and_flatten` / `_or_flatten`: `denoteSFold env (flattenSameFuncAnd xs) true (&&) = denoteSFold env xs true (&&)` via `denoteSFold_and_eq_all` + `List.all` over the flattened list (a nested `andE ys` denotes to `all ys`, and `all` distributes over the splice). Mirror the `denoteSFold_and_dedup` proof shape (Semantics.lean:469).
  - For the non-Bool funcs: add a generic opaque variadic representation mirroring `binaryE`/`denoteBin`. A small `VFunc` tag (`coalesce`/`greatest`/`least`), a `ScalarExpr.variadicOpaqueE : VFunc -> List ScalarExpr -> ScalarExpr` constructor, and `denoteS (variadicOpaqueE f es) => denoteVariadicOpaque f (es.map (denoteS env))` with `opaque denoteVariadicOpaque : VFunc -> List Bool -> Bool`. (Keep `denoteS` total; the mutual/`denoteSFold` structure must still terminate.)

- [ ] **Step 2: `lean.rs` render + proofs.**
  - `scalar_variadic_ctor` (lean.rs:550): add `"coalesce"`/`"greatest"`/`"least"` arms mapping to `ScalarExpr.variadicOpaqueE VFunc.coalesce` etc.
  - `FlattenSameFunc` render in the `TElem`/list translation (mirror `AbsorbSubsumed`'s render at lean.rs:598): for `and`/`or` render `(flattenSameFuncAnd {list})` / `(flattenSameFuncOr {list})`; for the opaque funcs render a type-correct flatten (an opaque or structural list fn is fine, since the proof is a sorry).
  - `choose_proof`: the `and`/`or` flatten obligations prove via the Step-1 lemmas (keyed on `flattenSameFunc` + `andE`/`orE`, mirror the dedup arm at lean.rs:887); the `variadicOpaqueE` obligations return the permanent sorry string (mirror the builtin arm at lean.rs:795), each `-- PERMANENT SORRY: RHS is a Rust builtin`-style marker (use a marker describing the opaque non-Bool variadic).

- [ ] **Step 3: Regenerate + inspect.** `cargo run -p mz-transform --example gen-lean`. Confirm: `flatten_and`/`flatten_or` obligations have real proofs (no sorry); `flatten_coalesce`/`greatest`/`least` each carry a `-- PERMANENT SORRY` marker. Exactly THREE new `PERMANENT SORRY` markers, total 10.

- [ ] **Step 4: Bump trip-wire.** `ci/test/lean-mir-rewrite.sh`: `expected_permanent=7` -> `10`; extend the comment to list `flatten_coalesce, flatten_greatest, flatten_least` (opaque non-Bool variadic RHS).

- [ ] **Step 5: Aggregate `lake build` clean rebuild + count.** Run `bash ci/test/lean-mir-rewrite.sh`. Confirm: build GREEN, `grep -rho "PERMANENT SORRY" src/transform/lean/MirRewrite | wc -l` == 10, script exit 0. `#print axioms` on `rule_flatten_and`/`rule_flatten_or` shows NO `sorryAx` (they are proved); the three opaque ones carry it. If the Lean toolchain is unavailable on the host, run the Docker path the script uses and report exactly what ran.

- [ ] **Step 6: Commit** (stage `Semantics.lean`, `lean.rs`, `Generated.lean`, `lean-mir-rewrite.sh`).

---

### Task 5: Differential corpus + termination + slice gate

**Files:**
- Modify: `src/transform/src/eqsat/scalar_saturate.rs` (`scalar_parity_slice6d`, `flatten_terminates`)
- Modify (if used): `src/transform/tests/testdata/eqsat_scalar_corpus` (`corpus_covers_slice6d`)

**Interfaces:**
- Consumes: `canonicalize_combined` + the standalone `scalar::canonicalize` oracle (see `scalar_parity_slice6e`).

- [ ] **Step 1: `scalar_parity_slice6d`.** Mirror `scalar_parity_slice6e`: build each case, run both engines, `assert_eq!` per case. Cases (types set correctly):
  - Nested same-op leading/middle/trailing for And AND Or: `And([a, And([b,c]), d]) -> And([a,b,c,d])`; assert flat (no nested same-op operand) AND `combined == oracle`.
  - At least one Coalesce/Greatest/Least nested case (the new-domain proof): `Coalesce([a, Coalesce([b,c])]) -> Coalesce([a,b,c])`, `combined == oracle`, flat.
  - Deep nesting: `And([c0, And([c1, And([c2,c3])])])` -> 4 flat operands, `combined == oracle`.
  - Flatten -> cascade: nested with a `false`/`true` (short-circuit), an identity literal (drop_unit), cross-boundary duplicates (dedup), collapsing to single/empty. Each `combined == oracle` through the cascade.
  - Mixed-op negative control: `And([Or([a,b]), c])` -> `combined == oracle`, unchanged (no flatten).
  - Error operands Bool-typed `(1/0)=(1/0)` inside And/Or nesting (the collapse trap); well-typed operands for the Coalesce/Greatest/Least cases.

- [ ] **Step 2: `flatten_terminates`.** Assert saturation converges (iters <= a small bound, mirror `absorb_terminates`) on the deep-nesting + cascade corpus, including a case exercising the `and_or_single` self-loop shape the circular-ref skip guards (`Or([c0])`-collapsed feeding a flatten). Confirm no exponential operand growth.

- [ ] **Step 3: Corpus coverage.** If prior slices added `corpus_covers_slice6X` to `tests/testdata/eqsat_scalar_corpus`, add `corpus_covers_slice6d` in the same pattern. (Known pre-existing `test_runner` line-4 parse failure predates the branch; ignore.)

- [ ] **Step 4: Full gate.** Record every command + output in the report:
  - `bin/cargo-test -p mz-transform eqsat` — all pass (except the known corpus `test_runner` line-4).
  - Relational goldens, NO `--rewrite`: run the eqsat slt set prior slices used (grep the ledger for the command; prior runs reported 1187/1187 across 37 files). Confirm success == total.
  - `bash ci/test/lean-mir-rewrite.sh` exit 0, permanent count == 10 (re-confirm Task 4).

- [ ] **Step 5: `bin/fmt`, commit** (stage only `scalar_saturate.rs` and, if touched, the corpus fixture).

---

## Self-review notes

- `flatten_applies` and `rest_flatten` share `flatten_inner`, so the guard and the filter cannot disagree (a fire always changes the list).
- Both termination guards (circular-ref skip, 4096 cap) are ported; `flatten_terminates` is their coverage.
- The Lean count moves 7 -> 10, isolated to Task 4 and re-confirmed in Task 5. The and/or obligations stay proved (no sorry); only the three non-Bool funcs are opaque.
- No variadic-func metavar axis: five explicit per-func rules keep the machinery a clean extension of 6e's absorb, not a new pattern-binding store.
