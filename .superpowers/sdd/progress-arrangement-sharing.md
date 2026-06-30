# Progress: reuse-aware arrangement-sharing cost model

Plan: docs/superpowers/plans/2026-06-22-eqsat-arrangement-sharing-cost-model.md
Branch: claude/mir-equality-optimizer-sodbej
Base: f7ddcc887e

## Tasks
- Task 1: complete (commits f7ddcc8..e6193ea, review clean)
- Task 2: complete (commits e6193ea..e68955c, review clean)
- Task 3: complete (commits e68955c..e17717d, review clean; no issues)
- Task 4: complete (commits e17717d..ed8722e, review clean)
- Task 5: complete (commits ed8722e..8e8393e, review clean; default objective now ArrangementCount; 0 golden diffs)
- Task 6: complete (commits 8e8393e..f5ab97f, review Approved; Important dead_code fixed via #[allow], clippy clean; re-review skipped for mechanical attr add)
- Task 7: complete (commit f5ab97f..5cf94bb, binarize_join_first only; review Approved; commutativity removed as unsound, see Task 7b). Important: positive-utility test missing (folded into 7b).
- Task 7b: complete (commit 73b539e..10a2786, review Approved by opus; commutativity sound via Project wrapper, panic gone, 82 goldens = arrangement-neutral tie-break reorderings under stats-off, +drop_identity_project rule)
- Task 8: DEFERRED (follow-up). eqsat join order currently survives only incidentally (JoinImplementation tie-break, stats off). Follow-up: make differential::plan/delta_queries::plan accept a fixed order so eqsat order is authoritative (cement it). Touches prod join_implementation.rs = high risk; revisit post-validation/when stats-on matters.
- Task 9: complete (commits 10a2786..642c738 + fix af8dee1; re-review by opus = Approved). ILP extractor default ON; polarity gate COMPLETE+verified (sound fallback), catch_unwind panic fallback, var-backed kill-switch (default true).
- Task 10: complete (review Approved) (commit af8dee1..b1f953a, DONE_WITH_CONCERNS = correct: MFP crutch NOT removable, 2 gaps documented, transform.rs audit note added; goldens at baseline). Also fixed Task 7b stale default.slt goldens (separate commit, arrangement-neutral reorderings, 139/139).
- Task 11: complete (commits 5a9d211..d9d46b2 + fix 21a3aa1; honesty-corrected). Proxy gate documented honestly: Proxy A catches spurious-arrangement regressions, Proxy B non-regression guard (equality on corpus). No invariant violations.

## Minor findings (for final review triage)
- Task 10 Minor (comment polish for final review): eqsat.rs ~180 "null-check filters strengthened" should say "removed/made vacuous"; "composing Map output cols into Project above" inverts substitution direction. Cosmetic, no correctness impact.
- Task 7b Important (PR-desc, non-blocking): the 82 golden diffs are tie-break join reorderings under disabled cardinality stats (arrangement-neutral, no index/arrangement change). PR description must frame them as such, not as cost-improving. Value = commutativity capability for future stats-on/reuse wins.
- Task 6 Minor: lean.rs EquivsInner/Outer/SwapEquivs arms drop numeric args (like Iota); fine until Lean obligations un-sorry-ed. Also: #[allow(dead_code)] on the 3 helpers MUST be removed in Task 7 when the rule wires them in.
- Task 5 Minor: time-alternate extraction (engine.rs ~217) bypasses self.extractor, always greedy+TimeFirst. Intentional, but undocumented; add comment, and at Task 9 confirm IlpExtractor-on-primary/greedy-on-time-alt asymmetry is fine.
- Task 4 Minor: [IlpExtractor] intra-doc link in extract.rs module doc is unresolved until Task 9 adds the type; self-heals at Task 9. If Task 9 slips, change to plain `IlpExtractor` code span to avoid cargo doc broken-link warning.
- Task 2 Minor: arrange_by_oracle_covered uses is_col() for want but as_column() for have (harmless, both delegate to MirScalarExpr::as_column). And test name says "credits_oracle" but empty available map never exercises oracle credit; rename or extend.
- Task 1 Minor: no TimeFirst unit test (symmetric to PeakDegree); observation, not required.

## Notes
- USER DIRECTIVE (2026-06-22): enable_eqsat_ilp_extraction must default ON (true), not off. PR will not land soon, so exercise ILP by default. Apply as first step of Task 9 acceptance; Task 11 validation runs with ILP on. Greedy fallback-on-solver-error must be robust since microlp is early-stage.
- (prior M1 plan ledger overwritten; that plan is complete in git history)

## FINAL-REVIEW must harden (Task 9 test-quality Minors, code verified correct):
- Polarity reproducer test (extract.rs ~453) does NOT exercise the gate: ILP tie-break picks the safe form before validate_polarity fires; final assert is circular. HARDEN: force the Negate form as the ONLY form in its class so the gate must fire and extract returns None (reviewer's probe confirmed this path works).
- Finding-3 oracle test (extract.rs ~389) uses CostModel::new() (empty available) so the oracle-suppression fix is never exercised. HARDEN: seed CostModel::with_available(index on t covering col0).
- arr_from_arrange_by restricts oracle suppression to ArrangeBy, but cost model also suppresses join-input arrangements (cost.rs:424,452); ILP now over-counts those (conservative, not unsound). Add a comment or extend the flag to join inputs.

## FOLLOW-UP (Task 10 partial subsume): coalesce_mfp NOT fully subsumable yet
The FIRST coalesce_mfp does real work the saturation rules do not cover, so it must stay (CanonicalizeMfp only partially subsumed):
1. Predicate simplification (null-check removal) — needs a saturation rule that drops provably-true/redundant IS NOT NULL predicates.
2. Scalar inlining through Map nodes (composition of map scalars through a project) — needs a rule fusing/inlining map scalars across project.
Once both shapes have saturation rules, the first coalesce_mfp can be removed. Until then, audit status = INCLUDE (not subsume) for these shapes.

## FOLLOW-UP / FINDING (Task 11 review, metric design):
Cost.arrangements (the optimizer's primary objective, Task 2) counts per-input join arrangements + explicit ArrangeBy/Reduce/TopK nodes, but NOT binary-join INTERMEDIATE arrangements (those are priced only on the memory-degree vec). Consequence: ArrangementCount alone cannot distinguish an n-ary binary join (N inputs + N-2 intermediates) from a WCOJ (N inputs) - both count N. The right join form is still chosen via the time/AGM tiebreak (which agrees), so plans are fine, but the count objective's DISTINCT value over peak-degree is not exercised by simple join corpora. DECISION NEEDED (research): either (A) extend the metric to count join intermediates as distinct arrangements (faithful to "minimize maintained arrangements"; cascades to re-validation), or (B) accept the metric as persistent/shared-arrangement count with join-form memory governed by the degree/time axis, and document it. The arrangement-sharing value shows in ArrangeBy-dedup unit tests + SLT goldens, not the join corpus.

## FINAL whole-branch review (opus, f7ddcc8..21a3aa1): READY-TO-MERGE as research prototype, no Critical defects.
- Soundness composes (commute schema-safe, ILP->greedy polarity fallback sound, drop_identity_project guarded, termination MAX_ENODES-bounded). Golden churn provably arrangement-neutral. No new as/unsafe. Trait/type consistency good.
- I-1 metric: omitting binary-join intermediates is CORRECT (matches differential execution), accept+document. Decision resolved.
- Fixed post-review (commit 56000dd): 3 new em-dashes, MFP-gap wording, time-first-alt comment.
- Deferred (prototype-acceptable): Task 8 realizer; strengthen ILP tests (I-2/M-2); Proxy-B diverging corpus entry; 2 MFP subsume rules.

================================================================================
## SESSION STATE @ HEAD=origin=f47f62c1b2 (pre-compaction snapshot, 2026-06-23)
================================================================================

### Where we are
Branch claude/mir-equality-optimizer-sodbej, PR #37160 (MaterializeInc/materialize, draft). Tree CLEAN, all pushed. HEAD=f47f62c1b2.
Plan tasks 1-7b, 9, 10, 11 DONE+reviewed; Task 8 DEFERRED. CI is RED (build #126357 failed).

### Commits this session (f7ddcc887e..f47f62c1b2, ~19)
Cost model/objective/extractor (Tasks 1-5), arrangement count, join binarization+commutativity (7/7b), ILP extractor default-ON (9), MFP audit (10), validation (11), comment fixes; then CI-driven fixes: bb2aeaa4ed (43 SLT goldens regen), f47f62c1b2 (binarization guard `has_inner_equiv` to stop clique-join CrossJoins).

### CI status
- #126340: caught corpus-wide golden churn + the clique-join CrossJoin regression. Both fixed+pushed.
- #126357 (HEAD): FAILED. SLT 1/2/3/4 fail = (a) stale goldens (binarize-guard f47f62c1b2 changed plans AFTER the regen, which only re-ran 4 files) + (b) the index-lookup->full-scan regression. Testdrive 7 + Console SQL = mzbuild-image-not-ready timing FLAKES (not plan failures). 13 "broken" = canceled-downstream (no-LTO builds/deploy/nightly/coverage), fail-fast.

### THE OPEN BUG: index point-lookup -> full scan (default-on regression)
Symptom: relation_cse.slt (561/782/820) etc. `i1=[lookup value=(1)]` becomes `Filter(#0=1) ReadIndex i1=[*** full scan ***]`; at 820 two lookups merged into one shared full scan.
ROOT CAUSE (corrected via instrumentation, supersedes an earlier wrong "RelationCSE hoists into Let" theory):
- eqsat input is FLAT, no Let. The cte l0 structure in the OUTPUT is eqsat's OWN post-extraction CSE (cse.rs), not upstream.
- `collect_indexed_filter_seeds` (transform.rs:225-284) only matches a BARE `Filter(Get global)`. The plan is `Filter(Project(Get global))` (a Project between). seeds=0 -> IndexedFilter lookup never enters the e-graph -> full scan wins.
- Cost model ALREADY prefers the lookup (time=0) when it is a candidate. Problem is ATTACHMENT, not cost.
- Production LiteralConstraints sees through Map/Project (extract_non_errors_from_expr) and runs after eqsat (lib.rs:899) but cannot rescue eqsat's output because eqsat already emitted `Filter(Get l0)` (LOCAL get) where MFP extraction halts (Id::Global check fails).
- Seeding is one-shot union vs initial structure; it cannot see forms later rules expose. THIS is the fragility.

### DECISIONS MADE (this session)
- Commutativity KEPT (user: "always a problem, can't avoid it"); churns goldens corpus-wide; opus-verified arrangement-neutral.
- Cross joins BAD: single-worker, not data-parallel (user). Binarization guard `has_inner_equiv` prevents creating them on clique joins.
- Cost.arrangements omitting binary-join INTERMEDIATES is CORRECT (matches differential execution: inputs arranged once, output streamed; nested Join charges inner output as outer input). Accept+document. (opus final review)
- Scalar e-class support: NOT needed. Slice equality safe by same-subtree identity; once seeding peels MFP, add CANONICAL PREDICATE-LIST ORDER at lower time (sort+dedup Vec<EScalar> in lower.rs, mirrors production canonicalize_predicates step 6). Scalar congruence only if a rewrite rule must discover scalar equalities (none do).
- Cross-binding (Let) optimization: NOT needed (input is flat). Sound reason eqsat does not cross LetRec (matching through recursion back-edge unsound); non-recursive lets get fact-propagation via union_let_defs.
- ILP extractor default ON (user). Polarity gate verified complete; catch_unwind fallback; var kill-switch.

### THE TWO REWRITE ALTERNATIVES (discussed, to choose post-compaction) for the lookup fix
Distinction: arrangement-reuse (`Get == ArrangeBy[k](Get)`, joins) vs literal point-lookup (`Filter[col=lit](Get) == IndexedFilter`, parameterized by predicate, what relation_cse needs).
- ALT 1 (eager arranged Get variants): insert ArrangeBy[k](Get) into each Get e-class for available index keys. Sound (ArrangeBy=identity), covers arrangement-reuse only, NOT lookups, eager=blowup. Keep only as on-demand `Get => ArrangeBy[k](Get)` RULE, not eager.
- ALT 2 (seeding as a REWRITE rule) -- RECOMMENDED: physical-phase rules that union RHS into the matched REACHABLE class (no orphans), re-applied every iteration so they compose with `push_filter_past_project` (which exposes the inner `Filter(Get)` from `Filter(Project(Get))`, then the lookup rule fires). Rules: `Filter[lit](Get) => IndexedFilter(...)` (lookup) and optionally `Get => ArrangeBy[k](Get)` (arrangement reuse).
  Obstacles: (a) thread the index oracle (`available`) into the matcher CONDITION-eval context (today it lives in cost model + raise, not saturation); conditions already can read egraph/analyses so this is plumbing. (b) RHS is detector-backed, not pure-syntactic: a template helper runs the production LiteralConstraints detector on matched (Get, predicates) to build the committed IndexedFilter MIR. (c) MUST be physical-phase (committed IndexedFilter is physical). (d) needs canonical predicate order for guard robustness.
  WHY better than MFP-aware seeding: subsumes it (no hand-peeling Project; saturation composition does it) and fixes the one-shot/structural-mismatch fragility at the root.

### WHAT'S LEFT (to green CI + finish)
1. Implement the lookup fix: choose ALT 2 rewrite (recommended) OR tactical MFP-aware seeding; PLUS canonical predicate-list order in lower.rs. Verify on repro `Filter[#0=5](Project[#0](Get global))` -> must extract IndexedFilter, and relation_cse 561/782/820 -> lookups restored.
2. Regenerate goldens corpus-wide with a HARDENED neutrality check that explicitly flags lookup->scan, lost-index, cross-join-added, arrangement-added per file (the FIRST regen's check MISSED the lookup->scan; do not trust net-line-count alone).
3. Push, re-monitor a fresh CI build (monitor on the umbrella buildkite/test context; previous monitor's completion logic was flaky -- verify build state directly via mcp buildkite get_build).
4. Confirm Testdrive 7 / Console SQL were flakes (re-run) vs real.
5. Deferred follow-ups: Task 8 join realizer (cement eqsat order over JoinImplementation); strengthen ILP polarity reproducer (currently circular) + oracle test (empty available); Proxy-B diverging corpus entry; 2 MFP subsume rules (null-check removal, scalar inlining); cost penalty for cross-joins/single-worker ops (defense-in-depth, complements the guard); comment-polish minors.

### Scratch / housekeeping
- scratchpad has lookup_repro.slt (harmless). Reports in .superpowers/sdd/: ci-fix-*-report.md, task-*-report.md (gitignored).

---

## SESSION STATE @ post-compaction 2026-06-23 (lookup fix IMPLEMENTED, chose tactical MFP-aware seeding over ALT 2)

### Decision: tactical MFP-aware seeding (NOT ALT 2 rewrite), because the e-graph already does the hard part
Repro test (`eqsat::transform::tests::seed_attaches_through_project`) PROVED that saturation's `push_filter_past_project` exposes the bare `Filter(Get)` node post-saturation, even when the input is `Filter(Project(Get))`. The OLD bug was purely that `seed_indexed_filters` ran PRE-saturation, so the exposed node did not exist yet. So ALT 2's full detector-backed rewrite (oracle-in-conditions plumbing) is unnecessary; two small changes suffice:
  1. ENGINE (engine.rs, both extract sites): moved `eg.seed_indexed_filters(&self.seeds)` from BEFORE `saturate` to AFTER. Now the push-exposed `Filter(Get)` class gets the IndexedFilter union. (egraph.rs doc comment updated to match.)
  2. COLLECTOR (transform.rs): new `bare_filter_over_global_get(expr)` helper peels a chain of `Project`s off `Filter(...(Get global))`, permuting predicates down via `MirScalarExpr::permute(outputs)` (same op as PredicatePushdown / push_filter_past_project). Detector runs on the BARE filter (arity = Get arity), lower(bare) gives seed.predicates. So committed has Get arity and attaches to the exposed `Filter(Get)` class; the surrounding Project (from push) restores arity.
Map-wrapped filters still left to downstream LiteralConstraints (scoped out; comment says so).

### Why this is sound + non-brittle
Predicate match between `lower(bare_filter)` and the e-graph's pushed `Filter[remap](Get)` holds even for NON-identity projections -- verified by `seed_attaches_through_reordering_project` (Project[2,0], filter on #1 -> bare #0). remap in the rule and permute+lower agree (both reduce to same EScalar). So the slice-equality fragility the user worried about does NOT bite here; canonical-predicate-order in lower.rs is NOT needed for this fix (still a nice-to-have, deferred).

### Tests added (all PASS; src/transform/src/eqsat/transform.rs `mod tests`)
- seed_attaches_to_bare_filter_over_get (baseline, layer 2)
- seed_attaches_through_project (the repro; FAILED before engine fix)
- seed_attaches_through_reordering_project (non-identity projection robustness)
- bare_filter_reduces_bare_filter_over_get / bare_filter_peels_reordering_project / bare_filter_rejects_non_get (layer 1, pure helper, no oracle needed)
Full eqsat suite: 107 passed. Hand-built seeds + a MARKER global Get stand in for the committed IndexedFilter (detect via visit_pre for Transient(999)), so layer-2 tests need no TransformCtx/oracle.

### Files touched (uncommitted)
- src/transform/src/eqsat/engine.rs (seed post-saturation, both sites)
- src/transform/src/eqsat/egraph.rs (seed_indexed_filters doc comment)
- src/transform/src/eqsat/transform.rs (collector generalized + bare_filter_over_global_get helper + test module)

### NEXT
1. IN PROGRESS: running relation_cse.slt (--optimized) to confirm production regression fixed (bg task bbym9qgbd). If green, the SLT 1-4 stale files likely also recover.
2. Run remaining stale SLT files; corpus golden regen with HARDENED check (lookup->scan flagger).
3. bin/lint + cargo clippy, commit, push, re-monitor CI.

---

## SESSION STATE @ post-compaction 2026-06-23 (CONT) -- lookup fix needed THREE layers, not two

Instrumented relation_cse.slt query at line 824 (`s1.f1=1 AND s2.f1=2`, two DIFFERENT literals over self-left-joins). EQSAT_DBG showed `has_let=true seeds=0`: eqsat input IS Let-bound (logical RelationCSE shares the common `Project(Get)` as `l0`; the two distinct-literal filters sit over local `Get l0` in the body). 781 (`=1 AND =1`, SAME literal) passes because its shared binding is the WHOLE `Filter(Project(Get))`, which downstream LiteralConstraints recovers; 820's binding is just `Project(Get)` -> filters over local get -> unrecoverable downstream. So eqsat MUST handle it.

THREE independent gaps, all fixed + each pinned by a Rust test in eqsat::transform::tests (fast, no SLT):
1. ENGINE Let-free path (engine.rs ~209/263): seed AFTER saturate (was before) so push_filter_past_project-exposed Filter(Get) is matchable. Tests: seed_attaches_through_project / _reordering_project.
2. ENGINE Let path (engine.rs optimize_body_with_let_union ~401): that path unions the binding def into the local-get class (so push can expose the global Filter(Get)) and saturates, but NEVER called seed_indexed_filters. Added the call. Test: seed_attaches_through_let_shared_project.
3. COST (cost.rs size_degree): `Rel::IndexedFilter` returned size_degree(input)=1.0 (full-relation output degree). A literal point lookup output is BOUNDED -> degree 0. Was the real reason extraction picked Filter(LocalGet) over Project(IndexedFilter): both tied at time=[1.0] (the Project above the lookup was charged a full scan), then the lookup lost the node-count tiebreak. Now Project(IndexedFilter) time=[] beats Filter(LocalGet) time=[1.0]. (collect_work already returned 0.0 for IndexedFilter; size_degree was the missed half.)
4. COLLECTOR (transform.rs): bare_filter_over_global_get now also follows non-recursive Let bindings (let_defs map; LetRec excluded as unsound) AND peels Project chains, permuting predicates via MirScalarExpr::permute. Without this, production seeds=0 for the Let case. Tests: bare_filter_peels_let_bound_project + _reordering_project + _reduces_bare + _rejects_non_get.

Diagnostic method that nailed it: temporary eprintln in optimize_body_with_let_union dumping indexed_filter_enodes count (=1, so it ATTACHED) + extracted plan (=Filter(LocalGet), so it was a COST/extraction loss, not an attachment failure). Removed after.

All 109 eqsat unit tests pass. IN PROGRESS: relation_cse.slt full run (bg bbrkq9yy8) to confirm 49/49.
NEXT after green: cargo clippy + bin/lint; run remaining stale SLT files; corpus golden regen w/ hardened check; commit; push; re-monitor CI.

---

## SESSION STATE @ post-compaction 2026-06-23 (CONT2) -- the REAL root cause was the ILP tie-break

CORRECTED the earlier Let theory. Dumped the actual eqsat physical input for relation_cse line-824 (`s1.f1=1 AND s2.f1=2`): it is FLAT (has_let=false), and the collector DID produce seeds=2. So my Let-resolution + Let-union-seeding (CONT layers 2 + collector Let-following) are correct and useful, but were NOT what this query needed. The input is a CrossJoin of 4 `Filter[#0=v](Project[#0](Get u1))` branches over two literals (1 and 2), sharing `Project[#0](Get u1)`.

THE bug: GREEDY extractor picks the lookup (passes); the ILP extractor (production default, enable_eqsat_ilp_extraction=true) picks the FULL SCAN. Localized with a fast unit test that runs BOTH extractors: `flat_shared_project_two_values_both_lookup` (failed only on use_ilp=true).
WHY: lookup and full-scan tie at 0 arrangements (arrangements_of(IndexedFilter)=[], collect_memory charges nothing, committed is MIR not a Rel child so its inner ArrangeBy is uncounted). The ILP's tie-break was PURE NODE-COUNT (epsilon per selected node). The full-scan form SHARES `Project[#0](Get u1)` across both branches (fewer used classes) while the two distinct lookups don't share -> node-count rewarded the full scan. The tie-break was blind to the time axis where greedy correctly prefers the lookup (IndexedFilter work=0 vs Filter work=1).

FIX (extract.rs IlpExtractor::solve): replaced the pure node-count tie-break with THREE strict tiers: (1) arrangements (primary, integer), (2) TIME = sum of per-node work-term degree, (3) node-count. New helpers `best_output_degrees` (per-class min output degree, bottom-up fixpoint; IndexedFilter=0) and `node_work_degree` (mirrors CostModel::collect_work, reads child sizes from best_output_degrees so an op above a lookup is charged the bounded output). Tier weights scaled to the subgraph: w_time=0.5/(sum_work+1), w_nodes=0.5*w_time/(n+1) -> one arrangement > all time terms > all node terms. Join degree uses a sum-of-inputs proxy (cancels across alternatives). Updated stale comments in extract.rs.

All 110 eqsat unit tests pass (greedy + ILP). IN PROGRESS: relation_cse.slt full run (bg b2ajwm2jo) expecting 49/49.

### Full change set (uncommitted) for the lookup fix
- engine.rs: seed AFTER saturate on BOTH Let-free extract sites + in optimize_body_with_let_union.
- egraph.rs: seed_indexed_filters doc comment (now post-saturation).
- cost.rs: size_degree(IndexedFilter)=0 (bounded lookup output, was size_degree(input)).
- transform.rs: collector generalized -- bare_filter_over_global_get peels Project chains (permute) AND follows non-recursive Let bindings (let_defs map); + test module (10 tests).
- extract.rs: ILP tie-break now arrangements>time>nodes (best_output_degrees + node_work_degree helpers).

### NEXT after relation_cse green
1. Run the other SLT 1-4 stale files (the regression touched several); corpus golden regen w/ hardened lookup->scan check.
2. cargo clippy + bin/lint.
3. Remove any leftover scratch; commit; push; re-monitor CI build.

---

## SESSION STATE @ post-compaction 2026-06-23 (CONT3) -- determinism fix (ENode: Ord) + corpus regen

After the ILP fix, relation_cse=49/49 and join_index gained a lookup (improvement). Corpus audit of 11 failing files: NO regressions (no lookup->scan, no new cross-join, no arrangement increase); 10 are arrangement-neutral join reorderings from the ILP time-tier, 1 (join_index) is a scan->lookup win.

BUT catalog_server_explain.slt oscillated (1 vs 2 failures across identical re-runs) = NON-DETERMINISM. Root cause: EGraph::reachable collected each class's nodes via HashSet::iter() (egraph.rs ~830), whose order is randomized per process. That feeds the ILP variable order, and microlp returns different tied-optimal solutions across runs -> flaky goldens (scalar-CSE map diff: actual reuses precomputed col #3 vs recompute full chain). Pre-existing (ILP default-on), exposed by the new tie-break.
FIX (user-directed: "This is why we use BTreeMaps elsewhere. Can ENode impl Ord?"): derived PartialOrd, Ord on ENode (all field types already derive Ord: MirScalarExpr, MirRelationExpr, EScalar, TopKShape, AggregateExpr, ReprColumnType). reachable now `nodes.sort()` (stable Ord) instead of a Debug-string hack. Deterministic node order -> deterministic ILP -> stable goldens.

Because the determinism change shifts tie-broken node order EVERYWHERE, reset ALL test/sqllogictest changes and regen fresh under the deterministic binary. IN PROGRESS: full corpus read-only run (bg bra9gd0wd) to find the failing set under determinism; then audit (no lookup->scan / cross-join-added / arrangement-up) + regen + verify each file stable across repeated runs.

Code changes now also include: egraph.rs ENode derive (+PartialOrd,Ord) + reachable sort.
NEXT: regen failing files, verify determinism (run 2-3x), cargo clippy + bin/lint, commit, push, re-monitor CI.

---

## SESSION STATE @ 29eb605aef (lookup fix COMMITTED + PUSHED) 2026-06-23

Committed+pushed 29eb605aef on claude/mir-equality-optimizer-sodbej (origin=antiguru fork). Lookup regression FIXED end-to-end. relation_cse 49/49, all 11 regenerated goldens pass per-file deterministically, 110 eqsat unit tests pass, clippy+fmt+cargo-check clean. bin/lint: only `buf` fails (tool not installed; no .proto touched).

Final fix = 4 layers (root causes, in order of discovery):
1. SEEDING ATTACHMENT (engine.rs + transform.rs): seed post-saturation (Let-free + let-union paths); collector bare_filter_over_global_get peels Project chains (permute) + follows non-recursive Let bindings.
2. COST (cost.rs): size_degree(IndexedFilter)=0 (bounded output, not input scan).
3. ILP TIE-BREAK (extract.rs): was pure node-count (rewarded sharing the full scan over 2 distinct lookups); now arrangements>time>nodes via best_output_degrees + node_work_degree. THIS was what the actual relation_cse query needed (input was FLAT, not Let; greedy already worked, ILP didn't).
4. DETERMINISM (egraph.rs): ENode derives Ord; reachable sorts nodes (was HashSet order -> non-deterministic ILP -> flaky goldens). User-directed ("we use BTreeMaps elsewhere").

Goldens regenerated per-file with CI args (--replica-size=scale=1,workers=2 --replicas=1); CI runs each .slt in its own invocation (test/sqllogictest/mzcompose.py ~line 401). Full-corpus single-invocation mode shows catalog_server_explain failing = STATE BLEED, not a real failure (passes 289/289 isolated 3x). Diff audit: lookups +1/-0, fullscan 2/2, CrossJoin 1/1, ArrangeBy 26/26 -> net +lookups, zero regressions.

Tests added (eqsat::transform::tests, fast, no SLT): seed_attaches_{to_bare_filter_over_get,through_project,through_reordering_project,through_let_shared_project}, flat_shared_project_two_values_both_lookup (runs BOTH extractors -- this is the relation_cse repro), bare_filter_{reduces_bare,peels_reordering_project,peels_let_bound_project,rejects_non_get}.

NEXT: monitor CI on 29eb605aef. Deferred follow-ups unchanged (Task 8 realizer; ILP test hardening; MFP subsume rules; cross-join cost penalty). The ILP node_work_degree join proxy (sum of input degrees) is approximate but cancels across alternatives -- revisit if a join-heavy plan mis-tie-breaks.

---

## ARRANGEMENT-SHARING BENCHMARK (new plan, SDD) 2026-06-23

Plan: docs/superpowers/plans/2026-06-23-eqsat-arrangement-sharing-benchmark.md. Goal: decide if eqsat (esp. ILP) earns its keep by running the REAL production optimizer (full pipeline, eqsat off/greedy/ilp via OptimizerFeatures toggles) on sharing-hard queries and counting arrangements. Harness reachable in src/transform/tests/ (TransformCtx::global + IndexOracle; full pipeline via Optimizer::{logical_optimizer,logical_cleanup_pass,physical_optimizer}; templates: tests/eqsat_indexed_filter.rs + tests/test_runner.rs:51-76). OptimizerFeatures fields are pub.
* Task 1 (harness: MultiIndex oracle, optimize_full 3-way runner, count_arrangements, smoke test): DISPATCHED to subagent (sonnet).
* Task 2 (fixtures f1 shared-key-fanout, f2 diamond-shared-filter, f3 four-way-chain + report): pending Task 1.
Decisive output: per-fixture prod|greedy|ilp arrangement counts; whether ILP<greedy ever (the unanswered question), and whether eqsat<=production holds. count_arrangements is a structural proxy (ArrangeBy/Reduce/TopK deduped) -- fair across modes.
Task 1: complete (commit 93bd569, review clean)

Task 2: complete (commit b7e1e33, review clean). + eqsat-active indicator (28c6ec4e2a, orchestrator).
BENCHMARK RESULT (local commits 93bd569/b7e1e33/28c6ec4e2a, UNPUSHED):
  smoke/filter-over-index   prod=2 greedy=2 ilp=2  tie  [converges to production]
  f1/shared-key-fanout      prod=3 greedy=3 ilp=3  tie  [converges to production]
  f2/diamond-shared-filter  prod=1 greedy=1 ilp=1  tie  [eqsat ACTIVE, diff plan, equal count]
  f3/four-way-chain         prod=4 greedy=4 ilp=4  tie  [converges to production]
VERDICT: on arrangement COUNT (the metric the cost model + ILP were built for), eqsat never beats production; ILP never differs from greedy; 3/4 eqsat produces a byte-identical plan to production. f2 active proves the toggle works (not bailing). Arrangement-sharing thesis UNSUPPORTED by this evidence; ILP unjustified over greedy. Real eqsat wins (ldbc_bi narrowing, demand fold, record CSE) are congruence/CSE/fold wins on DIFFERENT metrics (plan width/fold), not arrangement count. Recommend: shelve ILP; re-measure value as CSE/fold/narrowing; decide if congruence needs an e-graph vs cheaper passes.

Task 3: complete (commit 3ded5ee, WMR fixtures, review clean). Full benchmark (6 fixtures, local commits 93bd569/b7e1e33/28c6ec4e2a/3ded5ee, UNPUSHED):
  smoke=2/2/2 tie active=no | f1-fanout=3/3/3 tie active=no | f2-diamond=1/1/1 tie active=YES | f3-chain=4/4/4 tie active=no | w1-reach=3/3/3 tie active=no | w2-reach-env=3/3/3 tie active=no
FINAL: 6/6 ties on arrangement count; ILP==greedy everywhere; only f2 eqsat even changes the plan (equal cost). WMR opaque (lower.rs:198 LetRec->Opaque), so eqsat does nothing for recursion (active=no). Bail set (lower.rs:186-198) = {Constant, Global Get(benign leaves), FlatMap, multi-key ArrangeBy, LetRec}; consequential black boxes = FlatMap (table fns), multi-key ArrangeBy (undercuts arrangement-sharing thesis on composite keys!), LetRec (recursion). Real eqsat wins live elsewhere (congruence CSE/fold/narrowing: demand.slt, ldbc_bi, record) on different metrics. RECOMMEND: shelve ILP (zero benefit, real cost); re-measure value as CSE/fold/narrowing not arrangement count; WMR/FlatMap need lowering-into-egraph to help (substantial; LetRec has back-edge soundness).

---

## NEXT PLAN (write done, EXECUTE post-compaction): model FlatMap + multi-key ArrangeBy in e-graph
Plan: docs/superpowers/plans/2026-06-23-eqsat-model-flatmap-and-multikey-arrangeby.md (SDD). User directive: lower FlatMap and multi-key ArrangeBy into the e-graph (de-opacify); LetRec DEFERRED (needs viable-plan extraction guard, the extractor assumes a DAG).
Design: additive variants. Phase 1 FlatMap = new Rel::FlatMap/ENode::FlatMap {input,func:TableFunc,exprs:Vec<EScalar>} (Tasks 1-3: IR+ENode, lower+raise roundtrip, cost). Phase 2 multi-key = NEW Rel::ArrangeByMany/ENode::ArrangeByMany {input,keys:Vec<Vec<EScalar>>} (Tasks 4-5) -- keep single-key Rel::ArrangeBy untouched (12-file ripple incl arrange_idempotent rule + DSL + lean.rs); lower emits ArrangeBy for keys.len()==1, ArrangeByMany for >=2. Phase 3 Task 6 = benchmark de-opacify check (success = eqsat-active flips true on a FlatMap plan, NOT necessarily a win). Deferred: push-rules (push_filter/project_past_flatmap), LetRec.
Key facts: lower.rs:186-198 bail arm = {Constant, Global Get, FlatMap, ArrangeBy(multi-key), LetRec}; single-key ArrangeBy supported at lower.rs:176. TableFunc at src/expr/src/relation/func.rs:3387 (must satisfy Rel derives Ord/Hash/Eq -- if not, escalate). raise roundtrip test model: roundtrip_single_key_arrange_by (~608). cost touchpoints: size_degree ~256, collect_work ~314, collect_memory_into ArrangeBy arm ~404.

## UNPUSHED benchmark commits on claude/mir-equality-optimizer-sodbej (origin at 29eb605aef = lookup fix, pushed):
93bd569 harness, b7e1e33 fixtures, 28c6ec4e2a eqsat-active flag, 3ded5ee WMR fixtures. Decide whether to push these (documented negative result) -- user hasn't said.

---
## FlatMap/ArrangeByMany plan EXECUTION (SDD) 2026-06-23, base 3ded5ecea2
Task 1: complete (commit 3ded5ec..7a48f99, review clean). Rel/ENode::FlatMap + Sym::FlatMap + all non-exhaustive arms. cargo check/clippy clean, 100/100 eqsat tests. lower still bails FlatMap->Opaque (correct, Task 2 changes it).
  MINOR (defer to final review): (1) rewrite_escalars skips FlatMap exprs with weak comment rationale -- exprs eval only over input cols (0..input_arity, like Filter), func output arity irrelevant; arm is correct/conservative but comment misleads. (2) analysis.rs Equivalences::make comment names production's Equivalences::derive (comment-rot risk, vendor-ish).
Task 2: complete (commit 7a48f99..b7d2781, review clean). lower emits Rel::FlatMap (exprs reduced vs input col_types, FlatMap dropped from bail arm); raise reconstructs MIR FlatMap via s.expr.clone() (Map/Filter path); roundtrip_flatmap test asserts matches! lower==FlatMap then roundtrip. 101 tests, check/clippy clean.
Task 3: complete (commit b7d2781..c8d056e, review clean). Test flatmap_costs_no_arrangement_and_input_scaled_work pins arrangements==0 + time==Map-over-same-input. Cost arms already correct from Task 1 (passed first run). 102 tests, check/clippy clean. PHASE 1 (FlatMap) DONE.
  MINOR (note): GenerateSeriesInt64 test uses exprs:vec![] (cost layer ignores expr values, AST-shape only) -- benign.
Task 4: complete (commit c8d056e..b5db774, review clean). Rel/ENode::ArrangeByMany {input, keys: Vec<Vec<EScalar>>} + Sym + arrangements_of (one entry per key list) + all non-exhaustive arms (analysis identity w/ ArrangeBy; cost.rs collect_memory_into deferred to Task 5 w/ comment; raise arm already production-ready). Single-key ArrangeBy untouched, lower still bails multi-key->Opaque. 102 tests, check/clippy clean.
Task 5: complete (commits b5db774..22876f14 impl + d398d30 comment fix, review clean after fix). lower splits ArrangeBy: keys==1 -> Rel::ArrangeBy (unchanged), keys>=2 -> ArrangeByMany, keys==0 -> bare input; ArrangeBy dropped from bail. cost collect_memory_into charges 1 term/key deduped via ArrId::Node(ArrangeBy{input,key}). 3 tests (roundtrip_multi_key_arrange_by + 2 cost: 2 distinct=2 arrs, dup=1 arr). 111 tests. Review Important: 2 stale lower.rs comments + 1 raise.rs narration -> FIXED inline in d398d30 (self-verified vs code). PHASE 2 (ArrangeByMany) DONE.
Task 6: complete (commits d398d30..953a6f24 impl + e<fix> assertion fix, review clean). flatmap_shared fixture: Union(FlatMap(R,gen_series),FlatMap) -> compare reports eqsat-active greedy=false ilp=false. Reviewer: TRUSTWORTHY (FlatMap survives to eqsat; non-literal args so FlatMapElimination can't fold; false = no push-rules, NOT degenerate). Multi-key ArrangeBy covered by raise/cost unit tests (comment). Both sampled SLT files (list.slt, transform/relax_must_consolidate.slt) PASS unchanged; full corpus regen deferred to CI. Minor vacuous assertion fixed to p==g==i (de-opacification preserves arrangement count). PHASE 3 DONE. ALL 6 TASKS COMPLETE.

## FINAL WHOLE-BRANCH REVIEW (opus) 2026-06-23: READY TO MERGE (prototype)
No Critical/Important. Congruence (children/map_children/sym), lower/raise roundtrip (keys==0->input SOUND, reduced_escalar canonicalization consistent w/ Map), cost dedup (ArrangeByMany==2 single-key ArrangeBys), analysis parity (FlatMap correctly ABSENT from Keys=empty per prod, ArrangeByMany added everywhere) all confirmed correct. 105 eqsat + 7 benchmark tests pass, clippy clean.
3 Minor: M1 stale arrangements_of doc -> FIXED. M2 leftover "Task 5 wires" comments (ir.rs:196, egraph.rs:168) -> FIXED (CLAUDE.md no-future-narration rule). M3 cost-vs-ILP non-col-key edge (pre-existing single-key ArrangeBy too, exotic) -> NO FIX.
CI WATCH (full SLT corpus regen deferred): table-fn (generate_series/unnest/jsonb_each/regexp/*explode) near self-join or Union-of-identical-arms; composite-key joins producing multi-key ArrangeBy; subtrees that bailed to Opaque because a FlatMap sat near top.
Fix commit: see git log (after fc669015a7).

## NEXT PLAN (written, not executed): model LetRec (connect lowering bridge)
Plan: docs/superpowers/plans/2026-06-23-eqsat-model-letrec.md (SDD, 4 tasks / 2 phases).
VERIFICATION FINDING: recursion infra is built + unit-tested but DISCONNECTED at lowering. lower bails LetRec->Opaque (lower.rs:227), raise unreachable! (raise.rs:272). Everything else handles Rel::LetRec: variant exists (ir.rs:250, MISSING limits field), LocalGet sealed leaf, add_rel PANICS on scopes (egraph.rs:512) so each fragment optimized in own EGraph = back-edge-safety by disjoint e-graphs (user's "name only in scope" = existing architecture), optimize_scope recursive arm (engine.rs:286), optimize_around_scopes option A (422), letrec_local_facts nonneg/monotonic/keys fixpoints (analysis.rs:850, equiv+const-cols conservatively inherited), contains_scope/hoist_scopes/normalize_push_into_scopes all match LetRec, cost+extract arms exist.
GAP = 3 mechanical: (1) Rel::LetRec needs limits: Vec<Option<LetRecLimit>>; (2) lower MIR LetRec->Rel::LetRec; (3) raise reconstruct. PLUS untested end-to-end optimize_scope recursive path + LetRec hoist path (Task 3 budgets small engine fix; escalate if not small). MIR LetRec{ids,values,limits,body} relation.rs:158; LetRecLimit=mz_expr::LetRecLimit; LocalId<->usize via usize::cast_from(u64::from(id)). Tasks: T1 limits field, T2 lower+raise+roundtrip, T3 e2e+nested-scope+viability, T4 benchmark(w1/w2 re-measure)+WMR SLT drift sample (full corpus->CI).

## LetRec plan EXECUTION (SDD) 2026-06-23, base c0795a2f46
Task 1 (limits field): implementer commit 5a52d66 FALSELY reported clean cargo check -- it did NOT compile (missed analysis.rs:782 rec_eval Rel::LetRec{bindings,body} match, a non-test arm not enumerated in brief). Controller caught via authoritative cargo check, fixed (added `..`), AMENDED to d98e4483dd. Now compiles, clippy clean, 105 eqsat tests pass. LESSON: always run authoritative cargo check on Task 1-style ripple tasks regardless of reported status.
Task 1: complete (commit c0795a2..d98e4483 amended, review clean). limits: Vec<Option<LetRecLimit>> added, threaded through all sites aligned 1:1 w/ bindings. 105 eqsat tests, clippy clean.
Task 2: complete (commit d98e4483..87d6f63d, review clean). lower builds Rel::LetRec (ids/values zipped, limits cloned aligned, LetRec dropped from bail); raise reconstructs MIR LetRec direct (no scope dance, back-edges verbatim via LocalGet{get:Some}). roundtrip_letrec passes, 106 tests. RECURSION DE-OPACIFIED END TO END.
Task 3: complete (commit 87d6f63..f5b924c impl + <strengthen> review fix, review clean after fix). 3 e2e tests in validation.rs: body rewrite (negate_negate fires inside LetRec body), unary push (normalize_push_into_scopes), union hoist (optimize_around_scopes). ALL PASSED FIRST RUN -- recursive optimize_scope + hoist/splice work end to end, NO engine fix needed (validates disjoint-per-fragment-egraph back-edge-safety). dangling_locals scope-aware walk confirms no leaks. Review Important+2Minor (test strength): strengthened Test3 explicit back-edge assertion, Test2 pin Negate at body root, Test1 comment. 109 eqsat tests. PHASE 1 (lower bridge) DONE.
Task 4: complete (commit 5ebe454..5fba0ae, review clean). w1/w2 WMR fixtures FLIPPED eqsat-active false->TRUE (recursion de-opacified, eqsat now optimizes recursive plans); counts tie prod (no arr-reducing rules yet, expected for transitive closure). New w3_letrec_body_shared fixture (identity Project on LetRec body) active=true. Both WMR SLT files pass NO drift (with_mutually_recursive 60/60, union_cancel 33/33). MINOR (note for final review): redundant [[test]] entry added to src/transform/Cargo.toml (autotests on, so benchmark already auto-discovered) -- harmless, matches sibling convention, documented in commit. PHASE 2 DONE. ALL 4 LETREC TASKS COMPLETE.

## FINAL WHOLE-BRANCH REVIEW (opus) + follow-ups: LetRec MERGE-READY (prototype)
Verdict: merge-ready research prototype, full-corpus WMR SLT regen deferred to CI. LOAD-BEARING ANSWER: de-opacification CANNOT alter least-fixpoint semantics -- mechanical evidence: (1) no rewrite rule matches the sealed LocalGet back-edge (grep: 0 hits in rules), (2) add_rel panics on scopes so x=body(x) is never a syntactic identity to any rule, (3) each binding value + body optimized in its own fresh EGraph (disjoint), (4) limits captured once + reattached at reassembly, never reordered/resized. Roundtrip fidelity + limits alignment verified for n>1/nested/distinct-limits.
4 Minor follow-ups ALL FIXED in commit a39b9b64f5: (1) cse::max_local_id missing Rel::LetRec arm = latent unreferenced-binding id-collision/shadowing-panic, now reachable -> added arm + regression test (confirmed fails without fix); (2) 2 structuring semicolons in comments removed (ir.rs:253, lower.rs:114); (3) redundant [[test]] Cargo entry dropped; (4) added n>1 mutually-recursive e2e test (ids[0,1] mutual refs, distinct limits [Some,None], pins reassembly+alignment+disjointness). 111 eqsat tests, clippy clean.
NOTE: extract.rs LetRec arms (is_nonneg_safe/validate_polarity) are DEAD (scopes never enter e-graph) -- plan's claim they needed limits was wrong, correctly no extract.rs change made.
LETREC FEATURE COMPLETE. Commits c0795a2..a39b9b64 (6): d98e448 T1 limits, 87d6f63 T2 lower+raise, f5b924c T3 e2e, 5ebe454 T3 strengthen, 5fba0ae T4 benchmark, a39b9b64 review-fixes. UNPUSHED (origin at c0795a2f46 = FlatMap feature tip).

## NEXT PLAN EXECUTING: FlatMap push rules (SDD) 2026-06-23, base a39b9b64
Plan: docs/superpowers/plans/2026-06-23-eqsat-flatmap-push-rules.md. User picked track "FlatMap push rules" (over recursion-equiv-fixpoint / ArrangeByMany rules). FINDING: threshold_elision over nonneg recursion ALREADY fires (rule exists relational.rewrite:220 + fact injected by optimize_around_scopes), so that recursion lever is pulled. FlatMap/ArrangeByMany have ZERO rules (modeling-only). The rule push_filter_past_flatmap is a near-clone of push_filter_through_map (relational.rewrite:25): Filter[p](Map[s] r)=>Map[s](Filter[p] r) where uses_only_input(p,r) where no_error(p). FlatMap appends func cols like Map appends scalars -> identical soundness. Conditions UsesOnlyInput(dsl.rs:194)+NoError(dsl.rs:248) ALREADY EXIST, no change.
BLOCKER the plan solves: DSL doesn't know FlatMap operator. Rule DSL = relational.rewrite parsed at BUILD time by build/grammar.rs (chumsky) -> build/codegen.rs -> $OUT_DIR/eqsat_rules.rs. Pat/Tmpl enums in dsl.rs (shared via include). FlatMap modeled on Reduce (two bracket payloads [group_key,aggregates] = FlatMap's [func,exprs] + input child). Plan: T1 add FlatMap to dsl.rs Pat/Tmpl + grammar.rs flatmap parser + codegen.rs all arms (inert until a rule uses it). T2 add push_filter_past_flatmap rule + firing test + NEGATIVE test (func-col predicate must NOT push, pins uses_only_input guard). T3 benchmark flatmap/filtered flips eqsat-active + SLT drift sample. Deferred: push_project_past_flatmap (unsound in general), ArrangeByMany rules.
Task 1 (DSL FlatMap operator): complete (commit a39b9b64..01c7f94, review clean). Pat/Tmpl::FlatMap{func,exprs,input} in dsl.rs; flatmap parser in BOTH pattern+template choice lists (grammar.rs); codegen arms (sym_name/node-bind/tmpl-reconstruct/pat()/tmpl()); matcher.rs Payload::FlatMapFunc(TableFunc)/FlatMapExprs(Vec<EScalar>) + into_* + all exhaustive matches; lean.rs binder+translate arms. Reviewer trace: func/exprs/input land in correct ENode fields, no swap, both choice lists wired. 111 tests, clippy clean. DSL extension INERT until Task 2 rule. (Note: subagent hallucinated a "cargo clean" user msg -- ignored, commit correct. E0004 diagnostics were stale RA lag, authoritative cargo check clean.)
Task 2 (push_filter_past_flatmap rule): complete (commit 01c7f94..9bf2c8ce, review clean). Rule = clone of push_filter_through_map: Filter[p](FlatMap[f,es] r)=>FlatMap[f,es](Filter[p] r) where uses_only_input(p,r) where no_error(p). 2 e2e tests: fires (col0 input-only -> pushed below, confirmed-FAIL before rule), blocked (col2 func-output -> stays above, pins guard). Reviewer verified GENERATED code: cond_uses_only_input uses FlatMap input-child arity (correct boundary), payloads threaded verbatim no swap. 113 tests. MINOR (not acted on, defensible): guard comment verbatim-copied from Map rule (self-contained, accurate).
Task 3 SUPERSEDED by user: full-corpus EXPLAIN SLT regen (185 files, ONE invocation, catalog explains first) running bg bq07tlsfj -- user clarified run-order divergence = buggy slt files that don't self-cleanup, single-invocation order is the correct reference. flatmap_filtered benchmark fixture (Task 3 Step1) still TODO after regen (hold to avoid build-lock contention).
Task 3 (benchmark fixture only; SLT drift superseded by full regen): flatmap_filtered fixture committed (9bf2c8ce..<this>). Result: flatmap/filtered prod=0 greedy=0 ilp=0 tie eqsat-active=FALSE -- as predicted, production predicate_pushdown also pushes filter past FlatMap so full-pipeline plans converge; rule firing proven by eqsat-only unit test push_filter_past_flatmap_fires. FLATMAP PUSH RULES FEATURE COMPLETE (Tasks 1-3). Full EXPLAIN SLT regen (185 files, bg bq07tlsfj) running -- audit+commit goldens when done.

## FULL EXPLAIN SLT REGEN (user-requested) 2026-06-23: committed 154c623bfc
One invocation, catalog explains first, 185 files run, 24 changed (862 lines). Opus audit verdict: 22 benign (join commutativity arrangement-neutral, CSE, FlatMap/CTE column remaps, delta-join relation-index reorders, LetRec de-opacification recursion/limits/arity intact; 0 net cross joins, 0 arity, 0 altered recursion). 1 REAL regression class (2 queries): projection pushed below index Get defeats maintained-arrangement reuse -> lookup->fullscan +1 arrangement, at catalog_server_explain.slt:1094 (mz_frontiers_ind) + singlereplica_attribution_sources.slt:1929 (u13). NOT from this session's filter-push rule (projection push on non-FlatMap); from prior FlatMap/ArrangeByMany/LetRec de-opacification already on origin. freshmart idx_sales benign (was already fullscan, -1 arr via CSE). USER CHOICE: commit goldens now (accurate -> CI passes), track fix separately -> memory project_egraph_projection_index_reuse_regression. UNPUSHED on branch: 01c7f94(DSL) 9bf2c8ce(rule) a1aa6a33(fixture) 154c623b(goldens); origin at a39b9b64.
