# SDD ledger — SP3b: full colored e-graph mechanism

Plan: `docs/superpowers/plans/2026-06-27-eqsat-colored-full-sp3b.md`
Spec: `doc/developer/design/20260627_eqsat_colored_full_sp3b.md`
Branch: `claude/mir-equality-optimizer-sodbej`; worktree `.claude/worktrees/mir-equality-optimizer`
BASE (before Task 1): 64f4fc2a95
Memory: [[eqsat-shared-core-extraction]] (SP1✅ SP2a✅ SP3a✅ → SP3b now; SP2b pending)

SP3b = production colored e-graph on core::EGraph<L>: sparse delta union-find,
color hierarchy + layered find, batch colored congruence (kernel reuse), per-color
colored-conclusion hash-cons (≤2× via canonical recanonicalize), color-aware
extraction (pluggable CostModel + toy cost). Generic over Language, ToyLang-only.
Anchored by an independent all-pairs clones oracle. Incremental congruence, real
cost, rule-driven conclusions, color derivation, and UF-edge `remove`-in-close all
deferred to SP4.

Pre-flight conflict RESOLVED with human (2026-06-27): UF-edge minimization
(remove-in-close) deferred to SP4; close holds ≤2× via recanonicalize only.
remove is built + unit-tested but uncalled in close. Spec §6 step 5 + §12 + plan
self-review updated to match — that is the contract reviewers check against.

## Tasks
- Task 1: complete (commit 64f4fc2a95..de94cd55bc, review clean; ✅ spec, Approved)
- Task 2: complete (commits de94cd55bc..92d57ecf7d, review clean; ✅ spec, Approved). 14/14 tests. Debug derive correctly omitted from ColoredEGraph.
- Task 3: complete (commits 92d57ecf7d..cee90cbb0b, review clean [opus, algorithm traced]; ✅ spec, Approved). 20/20 tests incl. random + hierarchy oracle anchors. Added pub(crate) canon() helper (FnMut/Fn fix). remove correctly uncalled in close.
- Task 4: complete (commits cee90cbb0b..3619b9ab73, review clean; ✅ spec, Approved). 32/32 tests. core::lookup letter-perfect; all_base_children guard correct; add_colored uses canon().
- Task 5: complete (commits 3619b9ab73..19e47ad3b9, review clean [opus, fixpoint traced]; ✅ spec, Approved). 27/27 tests; both extraction tests green. visible_nodes promoted to pub(crate) (anticipated).
- Task 6: complete (commits 19e47ad3b9..a86f1df0f5, review clean; ✅ spec, Approved). 27/27 tests; sweep GATE HOLDS (all 4 SP3a §8 properties reproduce; numbers near-identical, 5/6 base_size rows exact).
- Final: whole-branch review (opus). STATUS: COMPLETE — Ready to merge: YES (34/34 tests, no Critical/Important, behavior-neutral confirmed, M6.1 convergence drift verdict BENIGN).
- Post-review CI fix: colored/mod.rs → colored.rs (mod_module_files lint) + visible_nodes &self (needless_pass_by_ref_mut). commit f1ad0fb261. clippy -p mz-transform --all-targets clean except pre-existing SP2a warning (see below).

## SP3b COMPLETE (2026-06-28)
All 6 tasks implemented + reviewed clean (no fix waves needed during tasks; one
post-final-review CI fix for clippy mod_module_files/needless_pass_by_ref_mut).
Final whole-branch review (opus): Ready to merge YES, behavior-neutral,
convergence proven against independent oracle (unique fixpoint). 34/34 tests pass.
GATE re-validation HOLDS (all 4 SP3a §8 properties reproduce).

SP3b range: 64f4fc2a95..f1ad0fb261 (code) on top of spec/plan commits
c78f66ddab..64f4fc2a95. Local ahead of origin — push when user confirms.

PRE-EXISTING CI NOTE (NOT SP3b): clippy flags `get(&winner).is_none()` in
core.rs SumLang test, introduced by SP2a commit 493bdd8957. Latent on the branch
since SP2a; would fail `clippy -D warnings` in CI. Out of SP3b scope (plan
restricts core.rs to lookup/uf_len). Flagged to user for a separate fix.

NEXT: SP2b (declarative DSL port) OR SP4 (multi-sort runtime integration, which
consumes this colored mechanism). Each its own spec+plan+SDD cycle.

## Minor findings (for final-review triage)
- M1.1 (task 1): remove() doesn't decrement size on non-root removal (union-by-size drift). Unused in close (deferred to SP4); no exercised path affected.
- M1.2 (task 1): ToyLang lacks #[derive(Debug)] (verbatim from SP3a; missing_debug_implementations allowed in eqsat module).
- M2.1 (task 2): colored/mod.rs re-exports Id via `pub(crate) use core::Id` for test ergonomics; a test-local private `use` would be cleaner. Low ambiguous-import risk.
- M3.1 (task 3): same_partition does full i×j incl. i==j/both orders (test-only, cosmetic).
- M3.2 (task 3): visible_nodes ancestor-delta branch + recanonicalize_delta_nodes body unexercised until Task 4 (Task 4's close_sees_delta_nodes covers it). RESOLVED by Task 4.
- M4.1 (task 4): add_colored returns a raw ancestor delta_nodes id without find(c,id) — can be stale after merges. Spec-compliant ("return that id"); callers find() if needed. Consider documenting/wrapping in SP4.
- M5.1 (task 5): oracle_extract uses unbounded loop vs mechanism's bounded MAX_ANALYSIS_ITERS (test-only; terminates by monotone decrease).
- M5.2 (task 5): extract_matches_oracle asserts got→oracle only (one-directional); same node set makes missing class unlikely.
- M5.3 (task 5): extract.rs cmap.get(&ch).unwrap_or(ch) fallback is dead (cmap always contains ch) but harmless/defensive.
- M6.1 (task 6): SP3b sweep delta_nodes drifts ≤2.3% from SP3a §8 at edge configs only (base_size=100: 602→616; fan_out=2: 342→338; fan_out=1 max_iters 8→9). base_size 250–5000 EXACT. Cascade matches exactly; oracle test proves true-fixpoint convergence. Assessed benign (SP3a-era table artifact, not a convergence bug). Final review re-checked.

## Model plan
- Task 1: sonnet (mechanical move + a small data structure with full code).
- Task 2: sonnet (full code, single file).
- Task 3: opus (the congruence kernel + oracle — the correctness crux).
- Task 4: sonnet (full code; touches core).
- Task 5: opus (extraction fixpoint + borrow structure — subtle).
- Task 6: sonnet (mechanical port) + opus sanity-check on the gate verdict.
- Reviews: sonnet for mechanical tasks (1,2,4,6), opus for 3 and 5.
- Final whole-branch review: opus.

## Log
Task 1: complete (commits 64f4fc2a95..de94cd55bc, review clean). 10/10 tests pass.
  Minor (non-blocking, reviewer-noted): ToyLang missing #[derive(Debug)] (verbatim from SP3a, won't fail clippy).
  Minor (non-blocking): remove() doesn't decrement size for non-root removal (union-by-size optimization drift, correct behavior).
