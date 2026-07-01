# SDD ledger — SP2a: scalar as a second instance of the generic core

Plan: `docs/superpowers/plans/2026-06-27-eqsat-scalar-instance-sp2a.md`
Spec: `doc/developer/design/20260627_eqsat_scalar_instance.md`
Branch: `claude/mir-equality-optimizer-sodbej`; worktree `.claude/worktrees/mir-equality-optimizer`
BASE (before Task 1): 7dfa1bb556
Memory: [[eqsat-shared-core-extraction]] (full effort SP1-SP4; SP1 done, this is SP2a)

SP2a = behavior-neutral migration of the scalar engine onto SP1's core::EGraph<L>.
Delete the duplicated ScalarEGraph substrate; ScalarEGraph becomes a type alias.
Analysis maintained incrementally via on_add/on_union hooks + a recompute_analysis
fixpoint called from the free saturate driver. Gate: 88 scalar tests + full eqsat
suite, behavior-neutral, no golden rewrites.

## Tasks
- Task 1: core on_add/on_union hooks + pub(crate) accessors (nodes/class_ids/node_count/data/data_mut) + toy-lang tests. STATUS: pending
- Task 2: scalar/lang.rs (ScalarLang/ScalarSym/ScalarGraphData + Language impl w/ hooks). STATUS: pending
- Task 3: migrate egraph.rs to alias + free saturate/recompute_analysis + inherent analysis/col_types; rewire scalar.rs + 2 rules tests. STATUS: pending
- Final: whole-branch review (opus) + behavior-neutral slt sweep. STATUS: pending

## Log
- Task 1: complete (commits 7dfa1bb556..493bdd8957, review clean — Approved sonnet). 6/6 core + 229/229 eqsat green. Minor (no action): non-compressing find_in (intended for disjoint borrow); dead_code warns on class_ids/node_count/data_mut — consumed by Task 3 saturate/canonicalize (SP1-style transient).
- Task 2: complete (commits 493bdd8957..8ecdd50302, review clean — Approved sonnet). 2/2 lang + 231/231 eqsat. Named-risk verified: on_union is winner-first (merge(wi,lo)), expect-not-default, matching ScalarEGraph::union. Minor (no action): on_union test doesn't adversarially distinguish merge arg order.
- Task 3: complete (commits 8ecdd50302..e24e5a914f, review clean — Approved opus). 90/90 scalar + 231/231 eqsat + 296/296 mz-transform. All 5 named risks confirmed (faithful saturate lift; recompute_analysis matches old recompute incl. seed-before-fixpoint; no rebuild-without-recompute read path; no test assertion changes; complete struct deletion — remaining "hashcons" hits are doc comments). Minor (cosmetic, deferred to final): analysis() panic msg says "call saturate after union" (was "rebuild").

## ALL 3 TASKS COMPLETE — pending final whole-branch review + behavior-neutral slt sweep
HEAD after Task 3: e24e5a914f. SP2a code range: 7dfa1bb556..e24e5a914f.

## FINAL REVIEW (opus) — Ready to merge: With fixes (doc-only)
Behavior-neutral verified: 90/90 scalar + 231/231 eqsat + 296/296 mz-transform; transform slt 1174/1175 — the sole failure is the known case_literal.slt:246 list-mode artifact (passes 15/15 standalone), identical to SP1. enable_eqsat_scalar_canonicalize defaults to true, so slt genuinely exercises the engine.
Final-review Important finding (doc footgun: recompute moved out of rebuild into saturate) addressed inline at commit 9758fbb63d — doc caveats on analysis()/recompute_analysis + corrected panic message. No logic change; 90/90 scalar re-confirmed. Minors (ScalarSym unused-by-design; cosmetic) accepted.

## SP2a COMPLETE (2026-06-27)
HEAD: 9758fbb63d. SP2a code range: 7dfa1bb556..9758fbb63d. NEXT: SP3 (colored e-graph mechanism) — own spec+plan+SDD cycle. (SP2b — declarative DSL port — also remains, can precede or follow SP3.)
