# SDD ledger — SP4a: multi-sort e-graph fusion (context-free)

Plan: `docs/superpowers/plans/2026-06-28-eqsat-multisort-fusion-sp4a.md`
Spec: `doc/developer/design/20260628_eqsat_multisort_fusion_sp4a.md`
Branch: `claude/mir-equality-optimizer-sodbej`; worktree `.claude/worktrees/mir-equality-optimizer`
BASE (before Task 1): 43facc98f9
Memory: [[eqsat-shared-core-extraction]] (SP1/SP2a/SP3a/SP3b ✅ → SP4a now; SP4b/SP4c/SP2b pending)

SP4a = make scalars first-class e-classes inside the relational e-graph: one
`core::EGraph<CombinedLang>` (`CNode = Rel(ENode) | Scalar(SNode)`), ENode's
EScalar payloads → scalar Id children, relational code reads scalar facts via a
cached EScalar map. Behavior-neutral (scalars inert: no scalar rules/unions/colors);
gate = byte-identical goldens/slt. EScalar SURVIVES (Rel payload + cache value);
only ENode flips. Critical invariant: extraction/cost/CSE keying resolves
Id→EScalar so tie-breaks stay byte-identical.

Structural note: Task 1 (the flip) is compilation-coupled / atomic — the ENode
field change breaks every consumer at once, no independently-compiling smaller
cut. User accepted this at plan review ("start"). If Task 1 BLOCKED on size,
re-dispatch on a stronger model, do NOT artificially split.

## Tasks
- Task 1: COMPLETE (commits 675e3e4d92 flip + 46b149f59f lit-clobber fix; Option A). Review (opus): Needs fixes → fix → re-review APPROVED. Keying invariant / on_union guard / Option A / scope / lit-determinism all VERIFIED. 262 eqsat tests, goldens unchanged, clippy clean. 2 Minor findings deferred (see below).
- Task 2: COMPLETE (commit 43c910877d). Review (sonnet): Approved — faithful movement, public surface preserved (no consumer files changed), no mod.rs, 20 tests relocated, 262/262 eqsat green. 2 trivial Minors (see below).
- Task 4 COMPLETE: determinism fix d5ebe6bd46 + goldens 0f2de9c7a2 (55) + straggler 8ec1dcbf28 (distinct_arrangements.slt, Debug-format name). COMPLETENESS sweep (bg) over all 196 EXPLAIN-bearing slt files: 195 PASS / 1 FAIL → the 1 (distinct_arrangements) regenerated+committed → now all 196 pass standalone. 56 goldens total, every diff controller-verified name-only. Regeneration COMPLETE. Next: final whole-branch review.
- Task 3: tests COMPLETE (commit bdfb45193e: round_trip_corpus, keying-safeguard [discriminating, PASS], scalar_sharing measurement; crate suite 329 pass byte-identical, clippy clean). GATE found a REAL Task1/2 regression: EXPLAIN key-column NAME annotations ({name}) drift post-fusion (19 files fail standalone, name-only; plans semantically identical, strip {…}→equal). Root cause: MirScalarExpr::Column carries TreatAsEqual name; fusion hash-conses column refs globally → shared class caches one arbitrary name (text.rs:1288-1294). HUMAN CHOSE Option A (keep byte-identical). NB list-mode cross-file sweep variance is a separate pre-existing slt artifact (case_literal/uniqueness_propagation_filter pass standalone) — gate must be evaluated PER-FILE STANDALONE.
- Task 4: column-name handling — NEARLY DONE. (1) Determinism fix COMMITTED d5ebe6bd46 (EScalar::name_key prefer-Some `(name.is_none(),name)` + set_escalar keeps order-independent min representative; comprehensive determinism test both-orders incl. present-vs-None-not-stripped; controller-reviewed clean — I caught+corrected an initial None-preferred ordering bug that would have stripped names). (2) Goldens COMMITTED 0f2de9c7a2: 55 slt files regenerated, ALL verified name-only by controller (50 via strip-{…} heuristic + 5 JSON/physical-LIR forms inspected manually). (3) COMPLETENESS sweep running (bg baxfritcu) over 196 EXPLAIN-bearing slt files to catch any drifters beyond the 55 the agent's narrower scan found — regenerate stragglers (name-only verified) before closing. NB sp4a-regen produced the fix+regen but stalled repeatedly; controller did the name-only verification + golden commit + completeness sweep.
  (history) Task 4: column-name handling — IN PROGRESS (opus). Strip-at-lower EMPIRICALLY REFUTED (77→170 name diffs; carried per-use names are authoritative — join-impl/physical-LIR/CTE keys the column_names analysis can't reproduce). Per-occurrence preservation would achieve byte-identical but re-touches the approved flip. HUMAN RE-DECIDED: **Accept + regenerate goldens** (the name drift is provably semantically-null). Revised Task 4 = enumerate name-only-drifting EXPLAIN goldens across test/sqllogictest, VERIFY each diff is name-only (strip {…}→equal; STOP+report any non-name diff = real regression), regenerate per-file standalone, commit under test/**. BASE: c0139ee043. Model: opus.
  GATE REDEFINED: SP4a is behavior-neutral EXCEPT cosmetic EXPLAIN column-name annotations on fused (hash-consed) scalar classes, which are regenerated. Spec §1 claim "scalar CSE cannot alter output plans" was INACCURATE (true for plan structure/arity/exprs; false for TreatAsEqual column-name display).
  UPDATE (2026-06-28): the cosmetic name drift is NONDETERMINISTIC — `escalar: HashMap<Id,EScalar>` last-write-wins, the column name is TreatAsEqual (overwrite guard can't normalize it), surviving name depends on HashMap per-process random seed → same query flips `#0{x}`/`#0{a}` run-to-run. Strip-at-lower refuted (77→170; carried names authoritative — join-impl/physical-LIR/CTE keys analysis can't reproduce). So "accept+regenerate" needs a SOURCE determinism fix first (else goldens flake). HUMAN CHOSE: deterministic representative name (name-inclusive lexicographic-min at set_escalar, order-independent) + regenerate. Agent sp4a-regen doing it: Step1 src fix (src/transform) + determinism unit test; Step2 confirm flaky file byte-identical 3×; Step3 regenerate name-only goldens standalone (re-verify name-only + flake-recheck), commit test/**. Two commits. (sp4a-task1 persistently stalled on this; replaced by fresh sp4a-regen.)
- Task 2: analysis.rs split (pure movement) — PENDING. Model: sonnet.
- Task 3: keying safeguard + round-trip corpus + measurement + full behavior-neutral gate — PENDING. Model: opus (gate judgment).
- Final: whole-branch review — PENDING. Model: opus.

## Model plan
- Task 1: opus implement (large multi-file refactor, subtle behavior-neutrality), opus review (algorithm + behavior-neutrality crux).
- Task 2: sonnet implement (code movement), sonnet review.
- Task 3: opus implement (gate judgment + keying test), opus review.
- Final whole-branch review: opus.

## SP4a COMPLETE (2026-06-28)
Final whole-branch review (opus): **Ready to merge YES**. Spec ✅. All 3
behavior-neutrality invariants CONFIRMED: (1) keying resolves Id→EScalar
(extract.rs:417-470; tiebreak test proves content-based selection), (2) lit_of
+ name_key order-independent deterministic (both directions tested), (3) scalars
inert / raise reproduces input (on_union sort discriminator sound — each Id in
exactly one sort, sorts never congruent). Accepted cosmetic deviation (column
NAME annotations) provably bounded to name-level (never structural). No
Critical/Important. 330 crate tests pass, clippy clean, all 196 EXPLAIN slt
files pass standalone.
Tasks: T1 flip (675e3e4d92 + lit fix 46b149f59f, reviewed) · T2 analysis split
(43c910877d, reviewed) · T3 tests+gate (bdfb45193e) · T4 determinism fix
(d5ebe6bd46) + 56 goldens (0f2de9c7a2, 8ec1dcbf28). All controller-verified.
Non-blocking observations (final review): O1 reduce_escalar lit re-derivation
(sound, required by shared-class design); O2 map_payload_cols FlatMap arm
unreachable today (more correct for SP4b); optional one-line comment at
egraph.rs:1116 (Id-keyed BFS/ILP var sort vs content tie-break).
PUSHED: origin == local d220008e89 (incl. final-review polish). SP4a CLOSED.

## Minor findings (for final-review triage) — all triaged ACCEPTABLE (defer to SP4c/cosmetic)
- M1.1 (task 1): `arity()` panics on a scalar class (arity_guarded reads rel_class_nodes → scalar id None → panic). Only called on relational inputs today; harden with a guarded fallback/debug_assert. egraph.rs ~diff 2360.
- M1.2 (task 1): extraction cost fixpoint loops `0..(classes.len()+1)` and run_analysis computes bottom() for every (inert) scalar class — wasted work growing with scalar count. Add a comment / rel-only count if profiling flags it later.
- M2.1 (task 2): `rec_solve` (analysis/recursion.rs:85) and `combine_join_keys` (analysis/keys.rs:110) promoted to pub(crate) unnecessarily (only called within their own module). Behavior-neutral; tighten to private if touched.

## Log
- Task 1 COMPLETE: fix commit 46b149f59f (EScalar::lit_of canonical derivation;
  intern_scalar re-derives lit; reduced_escalar uses lit_of; set_escalar
  debug_assert; doc fix; TDD clobber test RED None→GREEN Some(true)). Re-review
  (opus) APPROVED: clobber closed at root, lit_of byte-identical to pre-SP4a
  reduced_escalar derivation (behavior-neutral verified), TDD reproduces bug,
  no new issues. 262 eqsat tests pass, goldens unchanged, clippy clean.
  → Task 2 next (analysis.rs split), BASE 46b149f59f.
- Task 1 review (opus): Needs fixes. Important #1 = escalar cache lit-clobber:
  intern_scalar is NOT write-once (hash-cons shares classes); reduce_escalar
  interns EScalar::plain(expr) lit:None, so a rewrite folding a predicate to a
  bare literal true/false hash-conses to the shared literal class and overwrites
  its cached lit → cond_all_true/any_false/no_false misfire → non-byte-identical
  plan (latent: in-crate goldens miss the coincidence; Task 3 sweep would hit it).
  Fix dispatched to sp4a-task1: make cached EScalar.lit a deterministic function
  of expr (recompute in reduce_escalar to match reduced_escalar), debug_assert
  no conflicting overwrite in set_escalar, doc fix, TDD clobber test. Re-review
  pending. Review verified keying/on_union/Option-A/scope all HOLD.
- Task 1 in progress (opus implementer `sp4a-task1`, background). Mid-task fork
  raised + resolved: matcher port. Implementer found spec §5.1 (Binding=Id +
  cache-resolving helpers) hits a real borrow wall — codegen emits nested
  `&mut eg` payload calls that don't borrow-check; needs codegen let-hoisting
  the spec omitted. Spec was also internally inconsistent (§3.2 "matcher
  unaffected" vs §5.1 "Binding=Id"). HUMAN CHOSE **Option A** (matcher in Id
  domain now, incl. codegen let-hoisting + cond_* re-typing) over Option B
  (keep Binding=EScalar, change only the 2 codegen boundaries, defer matcher
  Id-port to SP4b). Relayed to implementer; spec §3.2/§5.1 + plan C1 (added
  C1a/C1b) + this ledger updated to match. Reviewer must check C1 against
  Option A.
