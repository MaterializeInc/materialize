# SDD ledger — SP4b: colored contextual equalities

Plan: `docs/superpowers/plans/2026-06-28-eqsat-colored-contextual-sp4b.md`
Spec: `doc/developer/design/20260628_eqsat_colored_contextual_sp4b.md`
Branch: `claude/mir-equality-optimizer-sodbej`; worktree `.claude/worktrees/mir-equality-optimizer`
BASE (before Task 1): b7d92e584e
Memory: [[eqsat-shared-core-extraction]] (SP1/2a/3a/3b/4a ✅ → SP4b now; SP4c/SP2b pending)

SP4b = replace eqsat Phase-2a payload rewriting + re-home the eqsat Equivalences
analysis onto the SP3b colored mechanism. Refit (post viability review): pre-freeze
intern of `reduce_escalar` spellings + per-scope colored EQUALITY; application
restricted to Filter predicates + Map scalars; colors confine only the equality
(terms are sound base terms). Removes need for color-aware raise / add_colored /
close() from SP4b (SP4c). Behavior-neutral through Task 6 (EquivMode::Phase2a
default); behavior flip isolated to Task 7; deletions in Task 8.

## Tasks
- Task 1: egraph.rs split (pure movement) — COMPLETE (commit 271b0ca3c2). Review (sonnet): Spec ✅ Quality ✅. Full 4-way split (node/combined/build/saturate + root façade); surface preserved; blast radius 5 files; 3 visibility widenings all pub(super)-bounded (no external widening). 330/330 pass. 1 Minor (M1, below).
- Task 2: colored_class_members accessor — COMPLETE (commit e807a86cc8). Review (sonnet): Spec ✅ Quality ✅. 1 test pass, clippy clean. 1 Minor (M2, below).
- Task 3: colored_derive::derive_facts (merged per-class equivalences) — COMPLETE (commit 0ee1addb16). Review (opus): Spec ✅ Quality ✅. Faithful reuse verified byte-identical to saturate's analysis construction (Equivalences{locals}, RelCtx, run_analysis_bounded, MAX_EQUIVALENCES_ANALYSIS_ITERS=4); run_analysis_bounded already returns the per-class map. fact_is_empty exact. Additive (const pub(self)→pub(crate)). 3 meaningful tests. Full crate 334 pass. 3 Minors (M3-M5).
- Task 4: colored_derive::derive (reduced-spelling intern, scopes, unsat) — COMPLETE (commit d6107a6bc0). Review (opus): Spec ✅ Quality ✅. PARITY VERIFIED byte-for-byte vs Phase-2a: Filter→input-class reducer+max_col=arity(input); Map→own reducer+max_col=arity(input)+pos (forward-ref guard preserved); only Filter/Map; reduce_escalar reused unchanged (pub(crate)). Interning sound, empty_classes captures lattice-propagated None, scope dedup OK. 7/7 colored_derive, 338 full. 4 Minors (M6-M9). NOTE: implementer committed its report into d6107a6bc0 (harmless scratch).
- Task 5: build_colored_layer + thread EquivMode/ColoredLayer (dormant) — COMPLETE (commit a749104172). Review (opus): Spec ✅ Quality ✅. Threading complete (Extractor trait + Greedy/Ilp + extract_with + all engine arms incl. TimeFirst + scope helpers), None everywhere, Phase2a default → byte-identical. build_colored_layer correct (re-canon through frozen base). 339 pass. 2 Minors (M10-M11) to address in T6: prefer pub(crate) on Extractor/extract_with/extract over #[allow(private_interfaces)]; rework the `colored_layer()` placeholder &mut-from-&self signature.
- Task 6: color-aware Filter/Map resolution + empty-fold (EquivMode::Colored) — COMPLETE (commits 04a730cef4 + fix 762c177c45). Review (opus): Spec PASS (in substance) Quality PASS. 344 pass, clippy clean.
  * DEVIATION (CORRECT, plan text was imprecise): resolution reads `color_of[class]` (node's OWN class), NOT `color_of[input]` as spec §5/plan said. Reviewer traced derive: it FILES each node's equality under the node's own class (class_scope keyed by class); the reducer CHOICE (Filter→input's reducer, Map→own) is orthogonal and correct (Task 4 parity). The plan's own test used own-class, contradicting its prose. color_of[input] would silently no-op. Sound: equality applied only to that class's own Filter/Map payloads, no leak. → SPEC/PLAN doc-debt: "color(input)" phrasing should be corrected to "color(class)" (note for final review / spec amend).
  * I1/I2 fixed (762c177c45): added colored_extraction_substitutes_redundant_map_scalar (asserts colored→#0, baseline→#1+1) and colored_extraction_empty_folds_contradiction (asserts colored→Constant{card:0,arity:1}, baseline→Filter); both verified fail-on-no-op (branch disabled → fail) and run on UNSATURATED graph so Phase-2a can't mask the colored path.
  * Minors M12 (within-class merge theoretical: exotic congruence of two Filters w/ shared pred id but different inputs could mis-scope — Task-4 recording property, awareness only), M13 (stale EquivMode comments saying "Task 6 will…").
- Task 7: differential + flip default to Colored + regenerate/review goldens — COMPLETE (commits 88ce6b0e91 guard, 231e9efa86 tie-break, 835606014d differential, a7fab14b54 flip+goldens). Review (opus): Spec ✅ Quality ✅.
  * GATE CAUGHT 2 REAL BUGS (fixed): (1) coalesce_mfp panic (out-of-range column emission) → column-range guard in resolve_scalar_colored (Filter max_col=arity(input), Map=arity(input)+pos, EXACT match to reduce_escalar; reviewer verified completeness); (2) non-canonical tie spelling (#1+1 vs reducer #0+1) → lower-column-support tie-break (reducer parity). Both tested + reviewed correct.
  * DIFFERENTIAL: cargo 346/346 UNCHANGED (full reducer parity); slt 192/195 unchanged; only 3 EXPLAIN files (physical_plan_as_{json,text,text_redacted} — one query, 3 renderings) changed, ALL name-annotation-only (#2{d}→#2{a}, indices identical), reviewer-verified strictly cosmetic (SP4a TreatAsEqual class). The ~7 earlier "changed" files were panic artifacts, now pass unchanged. No structural/arity/result regressions. Differential report: .superpowers/sdd/sp4b-differential.md.
  * 2 Minors: M14 tie-break is empirically-validated heuristic proxy for reducer canonicalization (not proven identical in all multi-col cases; corpus confirms parity); M15 per-element Vec build in min_by_key (negligible).
  * NOTE for T8: colored_derive::derive_facts REUSES the eqsat Equivalences analysis (run_analysis_bounded) — T8 must KEEP the Equivalences analysis, deleting only its Phase-2a/cond_unsatisfiable consumers.
- Task 8: delete Phase-2a + unsat machinery + EquivMode — COMPLETE (commits d035f0cc1a fixpoint, 66249427ab deletions+goldens, 2e2d78b8c1 review-fixes). Review (opus): Spec ✅ Quality ✅. KEEP-list intact (Equivalences analysis, reduce_escalar, empty_false_filter, EquivalenceClasses/ExpressionReducer); deletions complete (Phase-2a loop/rewrite_escalars/reduce_escalar_list/filter_input_reducer/Analyses.eq + orphaned tests/helpers; cond_unsatisfiable; collapse_unsatisfiable; Cond::Unsatisfiable dsl/grammar/codegen; EquivMode/with_equiv_mode; rel_class_ids). cargo 339 pass, clippy clean.
  * PROCESS: agent hit session-limit mid-T8 (left non-compiling: deleted rewrite_escalars method but not its 6 tests). Controller took over: finished test/helper deletion + comment cleanup. The agent's task-8-report.md is STALE (pre-existing, about WMR-lift; references non-existent commit 72e9e0a746) — ignore it.
  * KEY FINDING (user chose to investigate): T7's differential was INVALID — Phase-2a ran in BOTH arms (never gated by EquivMode), so T7 measured hybrid output. T8 is the REAL behavior change. Re-ran true colored-only differential.
  * FIXPOINT FIX (d035f0cc1a): derive applied reduce_escalar ONCE; Phase-2a iterated every saturation round, reaching deeper fixpoint on nested equivalences. Added reduce_escalar_fixpoint (bounded 16, max_col-guarded each step). Resolved the catalog_server_explain de-opt → net-zero. Reviewer verified terminating + range-safe + both sites + depth-faithful.
  * FINAL DIFFERENTIAL (colored-only vs SP4b base): 3 physical_plan files NET-ZERO (revert T7 hybrid artifact #2{a}→#2{d}); catalog NET-ZERO (fixpoint fix); eqsat.spec = ONE accepted sound delta (tautology Filter(#0=#0) sits above join vs pushed onto t0 — extraction-time-vs-saturation-loop tradeoff, spec §2/§9 deferred colored saturation to SP4c; join already enforces #0 non-null so redundant either way; full pipeline pushdown normalizes). cargo datadriven all unchanged.
- Final: whole-branch review — COMPLETE. Review (opus): **Ready-to-merge YES**. No Critical/Important. Verified: (1) colored substitution soundness (only proven-equal in-range spellings; max_col guard + reduce_escalar_fixpoint range-safe+terminating); (2) empty-fold soundness (None/unsatisfiable only, propagates to parents, arity rel-only); (3) Equivalences re-homing faithful; (4) deletions clean (no dangling refs, clippy clean, rel_class_ids restored as codegen ABI); (5) behavior gate sound (only eqsat.spec filter-placement delta, identical results; physical_plan net-zero; cargo datadriven unchanged); (6) borrow/threading coherent. Ran 267 eqsat unit + roundtrip + test_transforms + test_runner, all pass. All logged Minors → defer to SP4c.

## SP4b COMPLETE (2026-06-29)
All 8 tasks + final review done. Colored contextual equalities is the SOLE equivalence mechanism (Phase-2a fully removed). SP4b range b7d92e584e..HEAD. Final review YES. Behavior: near-perfect parity with Phase-2a — only delta is one sound eqsat.spec filter-placement (extraction-time tradeoff). Notable: the gate caught + fixed 2 real bugs (coalesce_mfp out-of-range panic → column-range guard; optimization-completeness gap → reduce_escalar_fixpoint) and 1 codegen-ABI overshoot (rel_class_ids). Deferred to SP4c: colored saturation (would recover the filter-placement), M2/M6/M7/M8 perf/cosmetic minors, SP4a M1.1/M1.2.

## Minor findings (for final-review triage)
- M1 (task 1): a non-movement doc-comment word change ("placing"→"causing") inside the #[cfg(test)] mod tests in egraph.rs root. Cosmetic, more accurate; no logic impact.
- M2 (task 2): colored_class_members has a redundant `base.find(y)==y` filter — class_ids() already returns only canonical keys, so it's a no-op (defensive). Harmless; could drop if touched.
- M3 (task 3): derive_facts hard-codes locals: empty (Let-free scope by design); a future LetRec consumer must supply locals. Caveat at call site when consumed.
- M4 (task 3): `assert!(ec.reducer().values().any(|_| true))` style nit (from plan skeleton) — prefer `!is_empty()`. Cosmetic.
- M5 (task 3): test (c) uses full EquivalenceClasses eq across merge orders (relies on minimize confluence); column-canon sanity asserts mitigate. Not fragile in practice.
- M6 (task 4): fresh_reducer clone+unbounded-minimize fallback can theoretically over-reduce vs Phase-2a's bounded minimize in a non-convergent corner (sound, never fires in practice). Consider dropping/documenting.
- M7 (task 4): empty-but-unsatisfiable Filter routed to empty_classes & skipped (Phase-2a would still rewrite preds) — sound, arguably better; intentional divergence.
- M8 (task 4): derive iterates scalar classes too (wasted empty-minimize work); a CNode::Rel pre-filter would be cleaner. Negligible cost.
- M9 (task 4): redundant `pub(crate) use self::saturate::reduce_escalar;` — already covered by `pub use self::saturate::*;`. Harmless.
- M10 (task 5): `#[allow(private_interfaces)]` on pub Extractor::extract/extract_with carrying pub(crate) ColoredLayer — nothing external uses Extractor; cleaner to narrow Extractor/extract_with/extract to pub(crate). Address in T6.
- M11 (task 5): `colored_layer(&self) -> Option<&mut ColoredLayer>` placeholder signature only compiles because it returns None; rework in T6 when the Colored branch builds a real layer.
- Process note: Task 2 implementer edited the ledger (overstep); Task 4 implementer committed its report file (Task 5 implementer correctly did neither). Remind implementers: do NOT touch .superpowers/sdd/ except writing your own report, and do NOT git-add it.

## Log
- (start) ledger created at BASE b7d92e584e.
- Task 1: complete (commits 4688c0337b..271b0ca3c2, review clean). Next BASE 271b0ca3c2.
- Task 2: complete (commit e807a86cc8). colored_class_members accessor + test. 1/1 pass, clippy clean.

PUSHED: origin == local c2a6dcfb6a (range b7d92e584e..c2a6dcfb6a). SP4b CLOSED.
