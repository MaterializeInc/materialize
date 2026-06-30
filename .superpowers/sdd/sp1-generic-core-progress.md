# SDD ledger — SP1: generic language-parameterized e-graph core

Plan: `docs/superpowers/plans/2026-06-27-eqsat-generic-core-sp1.md`
Spec: `doc/developer/design/20260627_eqsat_generic_core.md`
Branch: `claude/mir-equality-optimizer-sodbej`; worktree `.claude/worktrees/mir-equality-optimizer`
BASE (before Task 1): 6537758de7
Memory: [[eqsat-shared-core-extraction]] (full effort: SP1-SP4, colored-e-graph end state)

This is the FIRST sub-project (SP1) of the relational+scalar eqsat unification.
SP1 = behavior-neutral refactor extracting `EGraph<L: Language>`, congruence, and
the `Analysis<L>` framework into a new `eqsat/core` module, relational sole instance.

## Tasks
- Task 1: generic core module (`Id`, `Language`, `EGraph<L>`, congruence, `index`) + toy-language tests. STATUS: COMPLETE
- Task 2: instantiate relational on the core (`EGraph<RelLang>`, `available`→`RelGraphData`, type-alias). STATUS: COMPLETE
- Task 3: move `Analysis` framework into core (`Analysis<L>` + driver + migrate 5 analyses). STATUS: COMPLETE
- Final: whole-branch review (opus) + broad slt behavior-neutral check. STATUS: COMPLETE — Ready to merge: YES.

## SP1 COMPLETE (2026-06-27)
All 3 tasks implemented + reviewed clean; final whole-branch review (opus) = "Ready to merge: Yes"; final-review polish folded in; behavior-neutral verified (eqsat 226/226, datadriven goldens unchanged, transform slt 1174/1175 — the one failure is the known case_literal list-mode artifact, passes 15/15 standalone). Generic `eqsat/core` (Language, EGraph<L>, Analysis<L>+driver) now the relational engine's substrate.
HEAD after SP1: 7f28fb118e. NEXT: SP2 (scalar as a second instance on the core) — own spec+plan cycle.

## Minor findings (for final review triage)
- [T1, self-resolves T3] core.rs:47 module doc `[Analysis]` intra-doc link dangles until the Analysis trait is added to core.rs in Task 3. VERIFY resolved after T3.
- [T1, RESOLVED T2] dead_code warnings on core's pub items — gone now that RelLang consumes them (T2 build warning-clean).
- [T1 #1, RESOLVED T3] core.rs:47 module-doc `[Analysis]` link — Analysis trait now lives in core.rs (T3), link resolves. VERIFY in final.
- [T1 #3, open] core.rs index() doc: add precondition note "call rebuild() first for canonical node shapes".
- [T2, open] core.rs index() is `pub fn` vs the old relational `pub(crate)` — harmless visibility widening; tighten to `pub(crate)` if desired.
- [T3, open] egraph.rs MAX_EQUIVALENCES_ANALYSIS_ITERS doc: `[MAX_ANALYSIS_ITERS]` intra-doc link now dangles (constant moved to core.rs). Fix to plain backtick or full path.

## Consolidated Minor-fix list (one fix wave after final review)
1. egraph.rs: `[MAX_ANALYSIS_ITERS]` doc link → plain backtick or `[crate::eqsat::core::MAX_ANALYSIS_ITERS]`.
2. core.rs: verify `[Analysis]` module-doc link resolves now; fix if not.
3. core.rs: add index() doc precondition note (rebuild first).
4. core.rs: optionally tighten index() to `pub(crate)`.
5. (+ anything the final whole-branch review surfaces)

## Log
- Task 1: complete (commits 6537758de7..6ccfd4272a, review clean — Approved; 3 Minor recorded above)
- Task 2: complete (commits 6ccfd4272a..5dd23fd362, review clean — Approved opus, faithful lift verified; 1 Minor; case_literal.slt confirmed pre-existing list-mode artifact, passes 15/15 standalone)
