# Progress: SP-B1 eqsat cost-model-native join commit + emitter

Plan: docs/superpowers/plans/2026-06-30-eqsat-native-join-commit.md
Spec: docs/superpowers/specs/2026-06-30-eqsat-native-join-commit-design.md
Branch: claude/mir-equality-optimizer-sodbej
Base (this plan's review root): fcf93816d0

Goal: commit acyclic Rel::Join to a cost-model-chosen Differential at eqsat raise
time so fixpoint_join_impl no-ops on eqsat joins, the spelling selector survives,
and the VOJ cross disappears. Flag enable_eqsat_native_join_commit (default ON
in-branch for A/B); flag-off byte-identical.

PRIOR CYCLE (delta-join-cost): COMPLETE + landed inert, range de2d2f0e34..78609cf04b
(+ JI-internals doc 72927398d9). Do NOT re-review it. This plan builds on top.
Pre-existing branch drift: ldbc_bi.slt + tpch_select.slt LOGICAL goldens fail at
HEAD, unrelated (do not attribute to this plan).

## Tasks
- Task 1: complete (fcf93816d0..315dee298f, review clean; ✅ spec, Approved). Minor: unreachable `if rest != 0` guard (mirrors DpSub; harmless, left as-is). 31/31 cost tests + 2/2 new.
- Task 2: complete (315dee298f..d8e3965359, review clean; ✅ spec, Approved, 0 issues). 2 necessary deviations confirmed: ReprRelationType/ReprScalarType (brief had nonexistent type names); added `use mz_expr::Columns` for permute. 1/1 new + 2/2 JI tests.
- Task 3: COMPLETE (60d38150f6..e295f758ff; review clean — Spec PASS, Quality Approved, only Minors). flag+threading+routing+keyed-ness+determinism(canonicalize)+15 goldens. Details below.
  - 60d38150f6: flag+threading+routing landed; frontier_key_cols directional bug fixed (LIR panic). VOJ NOT yet fixed (order chooser picked non-keyed order); flag defaulted false (impl deviation).
  - c1fc9f353b (fix): keyed-ness-primary best_left_deep_sequence -> VOJ cross GONE flag-on (type=differential), present flag-off; default restored to TRUE; bootstrap-safe.
  - OWNER DECISION: default TRUE + rewrite the 71 moved transform goldens (moves verified benign: 0 new crosses, no result changes; spot-checked join_index/case_literal/union/literal_constraints).
  - BLOCKER found by golden-rewrite gate: committed plan is build-PROFILE-dependent (dev `else #2`/probe %2»%0 vs --optimized `else #0`/probe %0»%2). dev stable 3x -> not HashMap-random, not FP. Determinism fix LANDED (see below).
  - DETERMINISM FIX (75fae4d3de, controller-authored): root cause = the native-commit path skipped equivalence canonicalization (to preserve the selector's #2 spelling), so the extracted #2-vs-#0 spelling was build-profile-dependent -> flipped the chosen order. Fix: call production canonicalize_equivalences in the raise commit path BEFORE computing the differential order (the same mechanism that makes every other optimizer path profile-stable). Committed differential is keyed even with canonical #0 (no cross). NOTE: this makes the spelling SELECTOR moot for SP-B1's differential commit (canonicalize overrides #2->#0); the selector remains for the future delta path. Flag-off + all fallback paths keep the original spelling. GATE PASSED: eqsat_delta_join_cost.slt dev x2 + --optimized x2 against one golden; flag-on type=differential no cross, flag-off type=delta cross.
  - PROCESS NOTE: two subagents on the long --optimized build appeared to zombie (output-file mtime froze at 191 bytes) and were killed; their 191-byte output file is NOT a reliable liveness signal (they were actually working). Determinism fix + verification done in the MAIN session via background bash instead. Lesson: don't judge agent liveness by output-file mtime.
  - NEXT: golden rewrite of ~71 transform slt (in progress, bg bt6skks01; ldbc_bi/tpch_select excluded as pre-existing drift), then Task 3 review, Task 4, final review.
- Task 4: DROPPED (owner decision). eager_delta defaults OFF; JI already skips committed differentials when eager ON (line 162); the VOJ differential survives JI regardless (its delta upgrade needs new arrangements); the coarse guard would also suppress JI's own non-eqsat second-pass upgrades corpus-wide. SP-B1 goal achieved by Tasks 1-3. Precise "JI skips eqsat plans" deferred to the skip-JI project (needs a marker).

## Task 3 golden-rewrite notes (controller)
- GOTCHA: `bin/sqllogictest -- transform/*.slt` shares one catalog across the batch, so GlobalId::User(N) values in EXPLAIN AS JSON drift by run context. Batch rewrite produced false-positive "moves" (e.g. case_literal t: User(1)->User(13)) that vanish when the file is rewritten in ISOLATION. => goldens MUST be rewritten per-file (fresh catalog), not in a batch. Per-file isolation run in progress (bg bjy2pp7tt) to find TRUE movers + rewrite each alone.
- Pre-existing/unrelated (do NOT rewrite, NOT SP-B1): demand.slt panics in scalar/analysis.rs:114 (scalar eqsat canonicalizer, enable_eqsat_scalar_canonicalize, prior cycle) on a TimestampOutOfRange fuzzer query that folds to a Constant (no join). ldbc_bi.slt + tpch_select.slt = pre-existing LOGICAL-goldens drift. All three excluded from rewrite; flag for final review / user.

## FINAL WHOLE-BRANCH REVIEW (opus): "Not ready" — found CRITICAL C1
- C1 (wrong results, caught by final review, missed by per-task reviews + my EXPLAIN-only golden gate): binary_join_order gave the Differential START an over-wide arrangement key (join_key_cols_for_input = all cols equated to ANY input). The start is the stream side of the FIRST binary join; its key must line up component-for-component with the first lookup key (equal length). When the start is a join-graph hub the key is too wide -> first join stage produces ZERO rows -> silently empty results. Live regression: join_index.slt `select foo.a,baz.b ...` went 1|0 -> empty; my golden rewrite baked the empty result in (my non-plan scan only checked ADDED lines, missed REMOVED result rows).
- FIX (controller-authored, in commit_differential): derive the start key exactly as JI does (join_implementation.rs:1305-1318) via map_expr_to_global -> find_bound_expr(&[start], equivalences) -> map_expr_to_local, aligned 1:1 with the first lookup; fall back to Unimplemented if lengths can't match. Handles EXPRESSION start keys (e.g. bar[abs(#0)]). New test commit_differential_aligns_hub_start_key_to_first_lookup. Canary join_index.slt: results restored to 1|0; start key %1:bar[#0] (arity 1, was [#0,#1]).
- PROCESS GAP CLOSED: golden gate now diffs vs BASE for RESULT-ROW changes (both +/-), not just EXPLAIN structure. Regen-with-fix + C1 gate running (bg b6fmftxpp). After it's clean: commit fix+regenerated goldens, then re-review C1.

## C1 FIX LANDED (a0dac0d72a) + verification
- commit_differential now derives the start key via JI's find_bound_expr alignment; new hub test passes; expression start keys handled (e.g. %1:bar[abs(#0)]).
- Result-row gate vs BASE: clean (only flag-toggle setup lines + a rowsort reorder with identical multiset). C1 empty-results regression GONE.
- Final corpus verification: 36/36 transform files PASS (excl demand.slt panic [pre-existing scalar-eqsat], ldbc_bi/tpch_select [pre-existing drift]); working tree clean.
- Flagship four-run gate re-passed (dev x2 + opt x2); start key now %1[#0] arity 1.
- PLAN-CHANGE CHARACTER (for the record): dominant effect is delta-query -> single differential chain on ~13 indexed multi-way joins. This trades JI's free delta plans (better incremental maintenance) for differential (fewer arrangements) — a real perf tradeoff, AGM-only cost model driving it. Correct but possibly sub-JI-optimal until SP-B2/B3 (cardinality/selectivity) or an acyclic-delta path. The flag (default-on in-branch) is for A/B measuring exactly this.
- C1 re-review dispatched (opus, agent a4a1c25a24e7ee7bf). On clean verdict -> finish branch.

## SDD COMPLETE — SP-B1 (range fcf93816d0..a0dac0d72a)
Tasks 1-3 done; Task 4 dropped (owner). Final review: C1 (critical wrong-results) found, fixed, re-reviewed "C1 resolved", no new issues. I1 process gap addressed (result-row gate + executed verification). M1 sound (frontier-restricted non-start keys). M2 non-issue (Columns used).
Verification: 307/307 eqsat unit tests; 36/36 transform slt (excl demand.slt scalar-eqsat panic [pre-existing], ldbc_bi/tpch_select [pre-existing drift]); determinism dev==optimized (four-run gate); result rows == base.
Commits: 315dee298f (cost binary_join_order) · d8e3965359 (join_commit emitter) · 60d38150f6 (flag+routing) · c1fc9f353b (keyed-ness) · 75fae4d3de (determinism/canonicalize) · e295f758ff (goldens v1) · a0dac0d72a (C1 start-key fix + goldens v2).
OUTCOME: LIVE (flag default on in-branch). VOJ cross gone; acyclic joins commit as cost-model differential. Headline effect: delta->differential on ~13 indexed multi-way joins (trades JI's free delta for differential; AGM-only cost; evaluate before prod). Spelling selector now moot for the differential commit (canonicalize overrides); remains for future delta path. NOT pushed yet.

# SDD — Fix A: index-aware differential join orderer (closes E4 C_some)
Plan: docs/superpowers/plans/2026-07-01-eqsat-fix-a-index-aware-orderer.md
BASE (pre-Task-1): f9ed2f01c3
From E4-RC: differential orderer (best_left_deep_sequence) is index-blind (AGM+equivalence-keyed only, no available). cost() IS available-aware but unwired from the orderer. delta_join_order equally blind (deferred, out of scope). Both E4 regressions are B/additive.
Constraints: ONE notion of arranged (reuse input_already_arranged's core, no 2nd seam); greedy not exhaustive; start-key C1 correctness by EXECUTION; differential only.
Acceptance: C_some 3->2, E4 5 ties unchanged, C_skew stays worse, rows identical across arms, goldens re-spelling-only, compile-time OK.
Model: implementer opus; reviewer opus (watch the one-notion seam + start-key correctness).
- [x] Task 1: COMPLETE (f9ed2f01c3..39fb12dae7; review Spec PASS, Quality Approved, 0 Crit/Imp, 2 trivial Minors left as-is). Shared key_cols_arranged (ONE notion; cost() layers conservative bare-Get gate before it; strip_arrangement_wrappers Filter|Map == per_input_available Filter|Map). DP tuple +unarranged dim, rank (crosses,unarranged,terms). C_some 3->2 parity; 5 ties preserved; C_skew unchanged (Fix B); exec rows identical (cskew/csome 5 each); goldens demand.slt (neutral start-swap)+relation_cse.slt (i1 full-scan->differential-join reuse) pass under wrapper; full transform suite green (no missed movers); data_safety 0; MAX_EXACT_JOIN_INPUTS=8 unchanged. delta_join_order confirmed same-class blind (deferred to B1 delta-commit). NOT pushed.

# SDD — Fix B: reuse index under a liftable projection (closes E4 C_skew; SUBSUMPTION gate)
Plan: docs/superpowers/plans/2026-07-01-eqsat-fix-b-projection-reuse.md
BASE (pre-Task-1): 39fb12dae7 (Fix A HEAD)
Root: per_input_available (raise.rs) stops at Project -> commit doesn't credit wa_x under Project(wa) -> fresh build. Fix (a): credit base index mapped UP to projected space (needed/available are input-space in implement_arrangements; lift iff needed subset available, line 985 permutes down to base). implement_arrangements/install_lifted_mfp UNCHANGED (already lift+renumber). Fix-A lockstep: extend cost.rs key_cols_arranged with same Project see-through (keep input_already_arranged bare-Get gate conservative).
Guard: pure key-preserving Project only (index key cols all survive outputs; never through Map-computed col).
Acceptance: C_skew 1->0, C_some STAYS 2 (lockstep), 5 ties, 0-WORSE full E4' (subsumption gate), join_index 20->17, rows identical (cskew+csome), goldens reuse-only, compile-time OK. Deliver WHOLE Pareto table.
Model: implementer opus; reviewer opus (watch two-IR seam parity + renumbering + lockstep).
- [ ] Task 1: projection see-through in per_input_available + cost.rs key_cols_arranged

# SDD — Fix C: expression-level arranged check (closes #2449 residual; STRICT E4' gate)
Plan: docs/superpowers/plans/2026-07-01-eqsat-fix-c-expr-key-arranged.md
BASE (pre-Task-1): d5c8723cdf (Fix B HEAD)
Root: orderer is_arranged/key_cols_arranged is COLUMN-SET based; commit reuses at EXPRESSION granularity (implement_arrangements available.contains on MirScalarExpr; differential START key find_bound_expr-derived). #2449 (foo,bar,baz; foo.a=bar.a; foo.a+4=baz.a): eqsat orders foo>baz>bar (start key expr a+4, foo_idx on a can't cover -> full scan) vs JI foo>bar>baz (start key a -> reuse). Fix = match at expression level, REUSE commit's predicate (no third notion), start key = commit's find_bound_expr-aligned expr. Tightening-only (removes false-positive arranged). Lockstep: only ORDERER moves to expr granularity; per_input_available/commit already expr-level; keep input_already_arranged bare-Get gate.
Acceptance: #2449 foo>bar>baz reuse foo_idx; STRICT 0-worse full E4' (report whole Pareto); C_some/C_skew/join_fusion preserved; rows identical; corpus-wide golden regen (all sqllogictest, drift=order/reuse only, 0 rows); delta-path soundness. THEN STOP for stock-take (overseer: do not roll into E2/join_fusion-win without go-ahead).
Model: implementer opus; reviewer opus.
- [x] Task 1: COMPLETE (review Spec PASS, Quality Approved). gate MET (d5c8723cdf..a2043e356f, cost.rs only +245/-8), pending review. STRICT 0-worse E4' CONFIRMED independently: 6 hand-authored TIE (C_some+C_skew lockstep held), join_index PARITY (full-scan 13=13, reuse 22=22), join_fusion BETTER (12<14). #2449 closed (foo_idx reused; eqsat lands bar»foo»baz = equal-cost tie w/ JI foo»bar»baz). Execution identical (cskew/csome/#2449). Reuses commit predicate (expr_key_arranged=available.contains MirScalarExpr; start_key_exprs=find_bound_expr replica); only ORDERER start check moved to expr granularity; key_cols_arranged/input_already_arranged untouched. 203 EXPLAIN files = Task 2 driftable set.
Task 1 Minors (non-blocking, from review): M1 start_key_exprs skips pure-literal equiv members (find_bound_expr accepts them) -> exotic residual mis-rank (commit authoritative, no wrong rows); M2 find_map over classes vs single-class (harmless, classes disjoint). Add NOTE comments or align; deferred to final review.
- [ ] Task 2: corpus-wide golden regen + delta-path soundness (owed, non-optional for merge)

# SDD — SP2b Slice 1: the scalar-DSL seam (go/no-go)
Plan: docs/superpowers/plans/2026-07-01-eqsat-sp2b-slice1-seam.md
Spec: docs/superpowers/specs/2026-07-01-eqsat-sp2b-scalar-dsl-port-design.md
BASE (pre-Task-1): a68c28d57e
Goal: port not_not declaratively; prove one-grammar-over-CNode seam end-to-end
(scalar match view -> Pat::SUnary+grammar -> codegen scalar arms + SCALAR_COMPILED_RULES
split -> lower_scalar -> determinism-parity extractor -> scalar saturate + canonicalize_combined
-> not_not rule -> differential parity harness -> Lean -> go/no-go gate). Production stays
on old engine (delete-last slice 7). Behavior-neutral, strict gate (no --rewrite).
PRE-EXISTING DRIFT (do NOT attribute to this slice, Task 10): demand.slt panics
(scalar-eqsat canonicalizer, enable_eqsat_scalar_canonicalize); ldbc_bi/tpch_select
LOGICAL goldens fail. Exclude from the relational-golden regression.
Model: implementer sonnet; reviewer sonnet (escalate on subtle codegen/e-graph).
## Tasks
- [x] Task 1: complete (a68c28d57e..4d643bedc7, review clean; Spec ✅, Quality Approved, 0 Crit/Imp). 320/320 eqsat tests. Necessary deviations (all reviewer-confirmed): ColoredView MatchGraph stubs (E0046-forced, empty-vec sound), #[allow(dead_code)] on scalar_index() (core.rs precedent), trimmed 2 dead test imports.
  MINOR (forward-looking, for final review): ColoredView scalar stubs have no build-time guard (unlike cond_* stubs). A future colored-scalar wire-up must add real logic or a build-time assertion so a missed wire-up fails loudly.
- [~] Task 2: LANDED (7ea9cedd84, dsl.rs+grammar.rs additive). Build intentionally RED (new Pat::SUnary makes codegen+lean matches non-exhaustive). Grammar test inert (build-script module, no test cfg) — behavioral parse check deferred to Task 7. Reviewed JOINTLY with Task 3 (they are coupled: variant + its match arms must land together for green). Discovery: non-exhaustive Pat matches at codegen.rs sym_name/Matcher::node/pat() AND lean.rs collect_binders/translate_pat.
- [x] Task 2+3: complete (7ea9cedd84 dsl+grammar; 535167523c codegen split; cb3074f604 review-fix). Joint review: Spec ✅, Quality Approved after fix. Relational COMPILED_RULES byte-identical (37 rules), empty SCALAR_COMPILED_RULES. GREEN build.
  CRITICAL (fixed cb3074f604): scalar Matcher::node arm passed `e{c}` not `*e{c}` -> match-ergonomics `&Id` mismatch -> emitted scalar matcher would not compile at Task 7. Fixed to `*e{c}` (matches relational `*in{c}`) + dropped redundant fresh id.
  WATCH @ Task 7: full compile-verification of the emitted scalar find/apply is gated on Task 7 (first scalar rule). The deref fix is unverified-by-build until then.
  NOTE (lean sequencing): translate_pat has a `todo!("Task 9")` stub reachable once Task 7 adds not_not. Do NOT run `gen-lean` between Task 7 and Task 9 (Task 9 fills the real translation).
  MINOR (forward): lean collect_binders recurse-input correct/permanent.
- [x] Task 4: complete (0b2640521d impl; c24c68b904 review-fix). Spec ✅, Quality Approved after fix. IMPORTANT (fixed): lower_scalar duplicated existing scalar::lower::lower_into verbatim -> DROPPED lower_scalar, downstream uses lower_into (already pub, used by intern_scalar). Test retargeted -> lower_into_builds_scalar_nodes (PASS; first direct combined-lowering unit test). Plan doc updated (Tasks 5/6 now reference lower_into). No new EGraph method added.
- [x] Task 5: complete (fd515bd8f5 impl; 5820ab3bc5 comment-polish). Spec ✅, Quality Approved. PARITY VERIFIED line-by-line vs scalar/raise.rs (tie-break, And/Or sort, node_cost, compute_costs, build all identical; only diff = scalar_nodes() filters CNode::Scalar). Deviations sound: import via public re-export crate::eqsat::egraph::{CNode,EGraph} (matches lower.rs), module #![allow(dead_code)]. Round-trip test PASS. Minors fixed: restored parity-rationale comments + semicolon.
- [x] Task 6: complete (83d3c450c1 impl; be7f0a9e3e parity-polish). Spec ✅, Quality Approved. recompute_analysis + saturate loop PARITY IDENTICAL to scalar/egraph.rs. No visibility widening (egraph-root re-exports). Identity test PASS (empty rule set). Polish: dropped trailing rebuild + used node_count() to match standalone exactly.
- [x] Task 7: complete (293fc3a891 codegen SNode-qualify; d3f2af3063 not_not rule + wire + binding_arities->try_arity + compiled_and_ast_agree fix). Spec ✅, Quality Approved. SEAM WORKS: not_not_rewrites_via_combined PASS, eqsat 331/331. binding_arities VERDICT: relational apply UNCHANGED (try_arity returns Some(same value) where arity did; only newly tolerates scalar bindings no relational rule creates). Emitted scalar matcher compiles (Task-3 deref + SNode-qualify both needed).
  MINOR (final-review): (1) codegen.rs:1002 rules_ast doc says "relational.rewrite" but now also scalar.rewrite (cosmetic). (2) colored/view.rs arity_of/binding_arities same panic-on-scalar latent pattern, unexercised (not_not is colored:false); needs try_arity-style fix if a future slice marks a scalar rule colored. [same family as Task-1 colored minor]
- [x] Task 8: complete (03b855406a). Spec ✅, Quality Approved. GO/NO-GO PARITY GATE GREEN: scalar_parity_not_not new==old on all 3 not_not cases (not(not(#0)), not(not(not(#0))), #0); real differential (canonicalize_combined vs separate old scalar::canonicalize). In-crate #[cfg(test)] in scalar_saturate.rs, no visibility widened. corpus_covers_slice1 via include_str! real. LOW note: report imprecisely said both symbols pub(crate) (scalar::canonicalize is actually pub; the pub(crate) wall is scalar_saturate module) - no code impact, placement still correct.
- [x] Task 9: complete (b1bf76c76a). Spec ✅, Quality Approved. Bounded ScalarExpr(var,notE)+denoteS; todo! replaced; rule_not_not emitted with real simp[denoteS] proof (not sorry); PERMANENT SORRY==0. DRIFT verdict: 6-added/1-dropped relational theorems = PURE stale-catch-up (empirically proven: reproduces at pre-scalar commit; relational.rewrite never changed; lean.rs relational paths untouched). No Lean toolchain -> proof inspection-only (acceptable, not the gate). gen-lean reproducible zero-diff; eqsat 326 pass.
- [x] Task 10: complete (gate marker 06a521b1aa). GATE PASSES: eqsat 333 pass; scalar_parity_not_not PASS (go/no-go GREEN); transform slt 1346/1346 across 55 files, 0 failures, 0 golden diffs (no --rewrite), demand.slt passed 6/6 (pre-existing panic didn't manifest); no NEW regressions. Production path unchanged (canonicalize_combined test-only).

ALL 10 SLICE-1 TASKS COMPLETE. Next: final whole-branch review, then finishing-a-development-branch.

## QUEUED (post-slice-1, after final review): delta-EXPLAIN mislabel fix
See memory queued-delta-explain-mislabel-fix.md. Fixes delta-join EXPLAIN impl-block names (humanizer context?) + markers (commit_delta_query JoinInputCharacteristics?). INTENTIONALLY --rewrites goldens (opposite of SP2b no-rewrite gate). Confirm locus per sub-bug first. Also: regen all slts + review vs base. Runs ONLY after SP2b slice-1 lands.

## SLICE-1 COMPLETE (range a68c28d57e..3ecf2d42dd)
All 10 tasks done. Final whole-branch review (opus): READY TO MERGE, 0 Critical/Important. Behavior-neutral + relational-byte-identical confirmed branch-wide (codegen split byte-identical; binding_arities->try_arity relational-unchanged). Both ports (scalar_extract vs scalar/raise.rs; scalar_saturate vs scalar/egraph.rs) parity-verified. Gate: eqsat 333 pass; scalar_parity_not_not GREEN; transform slt 1346/1346 no --rewrite. Minor cleanup 3ecf2d42dd (stale docs + colored forward-guard). NOT branch-finished (branch has other in-flight work; user queued delta-EXPLAIN fix next). DEFERRED for later slices: saturate parity above copied bounds (MATCH_LIMIT/MAX_ENODES); colored-scalar wiring (stubs+try_arity, forward-guard NOTE added); Lean toolchain verification.
Key commits: 4d643bedc7 view · 7ea9cedd84 dsl/grammar · 535167523c+cb3074f604 codegen split+deref · c24c68b904 lower_into · fd515bd8f5+5820ab3bc5 extractor · 83d3c450c1+be7f0a9e3e saturate driver · 293fc3a891+d3f2af3063 not_not rule · 03b855406a parity harness · b1bf76c76a lean · 06a521b1aa gate marker · 3ecf2d42dd cleanup.

# SDD — delta-EXPLAIN mislabel fix (post-slice-1)
BASE (pre-fix): 3ecf2d42dd
Spec+loci: memory queued-delta-explain-mislabel-fix.md. INTENTIONALLY --rewrites goldens (opposite of SP2b gate).
LOCI CONFIRMED: NAMES=renderer text.rs:648-654 (thread per-input column_names into join_key_to_string; both delta+differential; names-only corpus-wide). MARKERS=join_commit.rs step_characteristics+commit_delta_query (real FilterCharacteristics from predicates + cumulative OR; DELTA-ONLY, differential keeps none()).
Fixes independent (different files: src/expr/src/explain/text.rs vs src/transform/src/eqsat/join_commit.rs).
## Tasks
- [x] Task N1: NAMES fix done (b4c70d9973). text.rs renderer threads inputs[pos].column_names(ctx) into join_key_to_string, both delta+differential. mz-expr+mz-transform green. Name change observed (Q05). 20 tpch_select failures = PRE-EXISTING (identical fix-stashed), regen in N3. Review pending (batch w/ N2).
- [x] Task N2: MARKERS fix done (6f4e7052). join_commit.rs: step_characteristics +filters param; commit_delta_query derives per-input FilterCharacteristics from raised_inputs (raise.rs:272) mirroring JI 233-296 + IndexedFilter add_literal_equality + cumulative OR (JI 848-854); commit_differential passes none() (UNCHANGED). 7 join_commit tests pass (+2 new). CAVEAT: mfp_above unreachable at bottom-up commit -> markers slightly leaner than JI-native where unpushed filters above join. Review pending (batch w/ N1).
- [ ] Task N3: regenerate affected goldens (--rewrite) + per-hunk review (impl-block-only, names match per-input keys, markers correct, no plan/arity/row change) + count mislabeled delta goldens.
- [ ] Task N4: regenerate ALL slts + review diff vs base commit; verify rows-identical + Used-Indexes-unchanged.

## DELTA-EXPLAIN FIX COMPLETE (range 3ecf2d42dd..cb86d0e70a)
- N1 NAMES (b4c70d9973): text.rs renderer resolves lookup-key #N from per-input schema (both delta+differential). Review: Spec ✅ Quality Approved.
- N2 MARKERS (6f4e7052 + d42139a027 correction): join_commit.rs delta lookups get real FilterCharacteristics (per-input-own, NOT cumulative; cumulative was differential-only — review-caught HIGH, fixed). differential passes none() (unchanged). Review: Approved after correction. mfp_above omitted (LOW documented gap).
- N3/N4 GOLDEN REGEN (cb86d0e70a): whole-drifted-corpus regen per user decision. 48 files, per-file... actually BATCH run (list + --reset, per user guidance not per-file). Full-diff review (opus) CERTIFIED: zero genuine data-result changes; all 6640 lines plan/EXPLAIN/introspection-descriptor. Categories: NAMES 38 files (~1100 annots, no wrong names), MARKERS 6 files (delta-marker BLAST RADIUS=6), pre-existing plan drift (ldbc/tpch/introspection: join flips, delta<->differential, KAiif->KA diff-marker loss), physical/introspection name-drops ~1350 (branch drift, NOT fixed by N1 which only touched EXPLAIN join-impl renderer), uN id-drift=0.
- with_mutually_recursive.slt: bail=1, EXCLUDED from regen (pre-existing real error, unrelated). Still failing. Flag for separate investigation.
- MISLABELED-DELTA-GOLDEN COUNT = 6 (input to the wider golden audit follow-up).
- KNOWN RESIDUAL (surfaced by regen, NOT in this fix's scope): (1) physical/introspection renderer STILL drops names (escalar collapse in LIR path, different from EXPLAIN join-impl renderer N1 fixed); (2) eqsat differential commit impoverishes differential markers (KAiif->KA) — N2 was delta-only by scope; (3) with_mutually_recursive bail=1.

## WMR BAIL TRIAGE (2026-07-02): CLASSIFIED — not eqsat, not WMR
- WMR passes 60/60 clean (alone + corpus). The "bail=1/total=25559" was a PARSING ARTIFACT: awk attached the corpus GRAND-TOTAL summary line to WMR (last file marker before summary). eqsat is default-on; WMR/LetRec fine through it.
- Real corpus bail = postgres/timestamp.slt:26: `statement ok; INSERT ... 'Mon Feb 10 17:32:01 1997 PST'` -> PlanFailure "invalid input syntax for type timestamp: unknown units Feb". A timestamp INPUT-PARSING compat gap (PG v6.0 verbose format MZ doesn't accept), thrown at parse/plan time BEFORE any optimizer. Upstream-PG-derived test, pre-existing MZ-vs-PG limitation. UNRELATED to eqsat/LetRec. No fix warranted.
- Residual list updated: only the two cosmetic residuals remain (physical/introspection name-drops; differential-marker impoverishment). Both display-only.

## RESIDUAL (a) PHYSICAL NAME-DROP TRIAGE (2026-07-02): renderer fix INFEASIBLE, no fix.
Task-1 gate: (1) eqsat-specific=YES (toggle proof: EXPLAIN PHYSICAL PLAN, enable_eqsat_physical_optimizer on drops lookup/source key names, off keeps them, identical structure). (2) render-site-has-schema=NO: DeltaPathPlan/DeltaStagePlan/LinearJoinPlan fmt sites (compute-types/src/explain/text.rs ~1244/1310/1448/1504) are DisplayText impls with only ctx+self; inputs only at parent Join node (not threaded); ctx.annotations=MIR analyses (column_names analysis is MIR-only, not on LIR); only reachable fallback humanizer.column_names_for_id=base catalog schema, wrong for CTE/LocalId + projected inputs. Clean b4c70d9973 analog unavailable. Root: eqsat commits MIR join keys as Column(i,None) (escalar TreatAsEqual), lowering clones raw (delta_join.rs:220); stream_key keeps names only via find_bound_expr. Alternatives (deferred, need user decision): (B) LIR lowering attach names; (C) eqsat commit/raise preserve baked name. Blast radius ~13 slt files. Full verdict in memory queued-delta-explain-mislabel-fix.md. USER: accepted physical name-drop, move to SP2b slice 2.

## SDD — SP2b SLICE 2 (variadic scalar rules)
Plan: docs/superpowers/plans/2026-07-02-eqsat-sp2b-slice2-variadic.md. BASE (pre-slice-2): cb86d0e70a.
Rule set VERIFIED vs rules.rs 20-list: and_single, or_single, not_demorgan_and, not_demorgan_or (fixed-func splits of and_or_single + not_demorgan). flatten_assoc DEFERRED (needs middle-splice matching ListPat can't express + imperative cycle-guard/cap; USER decision). Behavior-neutral, no --rewrite gate. Old EGraph<ScalarLang> engine = differential oracle (delete = slice 7).
## Tasks
- [ ] Task 1: scalar variadic/unary AST variants + grammar (dsl.rs + grammar.rs)
- [ ] Task 2: codegen scalar-variadic match arms + child scalar-detection widen
- [x] Task 3: codegen scalar variadic/unary template build arms
- [x] Task 4: port 4 rules to scalar.rewrite
- [ ] Task 5: grow corpus + differential parity (slice-2 + slice-1)
- [ ] Task 6: Lean And/Or list denotation + 4 theorems (no permanent-sorry regression)
- [ ] Task 7: slice-2 gate + regression sweep

Task 1: complete (commits 055277c0..5e414ae8, review clean — Spec ✅ Quality Approved, 0 findings; build intentionally RED until codegen catch-up Tasks 2-3)

Task 2: complete (commit fa3fd5acb0, review clean — Spec ✅ Quality Approved, 0 findings; only 2 Tmpl-side E0004 remain, cleared in Task 3)

Task 3: complete (commit 27d2cf2e02, review clean — Spec ✅ Quality Approved). Codegen fully done. DISCOVERY: lean.rs is NOT catch-all — 3 real E0004 (Pat::SVariadic, Tmpl::SUnary, Tmpl::SVariadic at lean.rs:184/255/314), previously masked by build-script short-circuit. Crate won't compile until Task 6 (lean).
REORDER: Task 4 (rules) -> Task 6 (lean: arms green crate + emit 4 theorems, needs rules present) -> Task 5 (differential + deferred Task-4 smoke) -> Task 7 (gate). Task 4 build stays RED on lean-only (accepted, same intentional-red pattern as Tasks 1-2); Task-4 smoke test deferred to after Task 6.

Task 4: complete (commits b95a1d0cf0 + comment-style fix — Spec ✅; Quality fix applied inline: clause-joining semicolon->period in de Morgan comment, comment-only no test impact). SCALAR_COMPILED_RULES=5, all 4 find/apply generated. Build red on lean-only (3 E0004, Task 6).

Task 6: complete (commits 086e760253 + fix aa4c8db208 — Spec ✅; Quality fix applied). Reviewer installed Lean 4.12.0, found 2 real defects (denoteS termination fail; first|simp|sorry never falls back). Both FIXED + toolchain-verified: denoteS -> mutual + denoteSFold walker; de Morgan proof -> `first | (simp [...]; done) | sorry`. Build green, 42 theorems (2 proved: and/or_single; 2 graceful-sorry: demorgan), PERMANENT SORRY=0. emit_rule retype fix (List Bag->List ScalarExpr, scalar-only) side-effect-free (relational theorems byte-identical).
DEFERRED DEBT (future task, NOT slice-2 scope): full `lake build` never green on this branch — missing root MirRewrite.lean aggregator for lean_lib target; pre-existing Inhabited-synthesis failures on opaque JoinSpec/catSpec; 2 unsolved-goal/omega gaps in filter_unionAll/negate_unionAll. Plan defers Lean toolchain verification. Schedule a dedicated "make lake build green" gate task before Lean debt compounds across slices.

Task 5: complete (commit 26d9d9320b + strengthening — Spec ✅ Quality Approved, GATE GREEN 4/4 differential). Reviewer-verified strengthening added: not(and(not#0,not#1)) -> Or(#0,#1) in BOTH engines (pushed form wins extraction, cost 3<6), first case where multi-operand de Morgan OBSERVABLY changes output — closes overseer trivial-pass concern. NOTE: de Morgan does not change extracted output for bare-column n>=2 (tree-size keeps Not(And), cost n+2 < 2n+1) in both engines; Task-4 smoke test correctly NOT resurrected (its shape assumption was wrong).

Task 7: complete (gate-record commit 61b49c14f0 — GATE GREEN). eqsat unit 337/337; differential parity 4/4 no divergence; relational goldens NO --rewrite 1346/1346 zero diffs (55 files, additive codegen did not perturb relational engine); termination converges.
SLICE 2 ALL 7 TASKS DONE. Range cb86d0e70a..61b49c14f0. Final whole-slice review next.

FINAL WHOLE-SLICE REVIEW (opus): READY TO MERGE. All 6 cross-cutting properties confirmed (relational non-perturbation, match/build symmetry, Lean retype scalar-gated, de Morgan complete+sound, extractor untouched, no PERMANENT SORRY). 3 minor nits fixed inline: Semantics.lean semicolon, lean.rs:429 stale List Bag doc, corpus_covers_slice2 added or(#0) assertion. SLICE 2 COMPLETE + MERGE-READY. Range cb86d0e70a..HEAD.

## LAKE-BUILD-GREEN (2026-07-02): DONE. lake build green (local 4.12.0 + container). Commits 4103ee2c26 (lean green) + 2b0c40e82a (container/CI).
- VERSION DIVERGENCE (flagged first-check): PR #36614 container = leanprover/lean4:v4.29.1 + Mathlib, bind-mounts the Mz/ semantics project — NOT usable for MirRewrite (pin v4.12.0, Mathlib-free). Aligned the RUNNER to our 4.12.0 pin (proofs verified there; bumping = re-verify + unneeded Mathlib). Built a minimal Mathlib-free 4.12.0 image mirroring the PR's Dockerfile pattern (LEAN_TOOLCHAIN build-arg) at src/transform/lean/Dockerfile + ci/test/lean-mir-rewrite.sh.
- TASK 1 root aggregator: added src/transform/lean/MirRewrite.lean importing Semantics + Generated -> lean_lib target now aggregate-checks the emitted theorems (never built before -> that's why gaps hid).
- TASK 2 green: fixed structural (JoinSpec abbrev Unit for computable Inhabited; defined missing equivsInner/Outer/swapEquivs/swapProjection/flatMapB/TableFunc referenced by relational theorems; denoteSFold @[simp] so simp[denoteS] closes and/or_single; drop_true/empty_false filter proofs now PROVED via simp[filterB,h_p]; flatMapB added to not-modeled-at-bag guard). Emitter (lean.rs) changes flagged: minimal, needed to green the build.
- SORRY TAXONOMY (clean, invariant intact): PERMANENT SORRY = 0 (unchanged); category-3 PRE-EXISTING GAP = 1 (filter_unionAll, distinct marker; negate_unionAll proved outright); category-2 provable-later = 26 (24 pre-existing relational emitter-default sorries: join/map/project/reduce/empty-prop/n-ary + 2 de Morgan simp-fallback). NEWLY PROVED this task: drop_true_filter, empty_false_filter, negate_unionAll (+ and/or_single/not_not already proved).
- RUN COMMAND: `bash ci/test/lean-mir-rewrite.sh` (builds image from lean-toolchain pin, runs lake build in container, fails on error or any PERMANENT SORRY marker). Local: `cd src/transform/lean && lake build` (4.12.0).
- CI RECOMMENDATION: YES wire ci/test/lean-mir-rewrite.sh as a Buildkite step (mirror PR #36614's lean-semantics step: id/label/command + inputs [src/transform/lean, ci/test/lean-mir-rewrite.sh], timeout 15, coverage/sanitizer skip). Cheap (Mathlib-free cold build = apt+elan+toolchain, layer-cached). NOT wired unilaterally (outward-facing CI-config change; pipeline.template.yml on this branch lacks even the PR's lean-semantics step). Without it the aggregate build rots red the moment slice 3 adds theorems.
- REMAINING DEBT (documented, not this task): 24 relational theorems are provable-later sorries (emitter never synthesized their proofs; pre-existing). filter_unionAll provable-later (cond/ih tactic golf, time-boxed out). A future task could prove these or teach the emitter to emit `exact` refs to the Semantics *_unionAll lemmas.

## SDD — SP2b SLICE 3 (analysis-gated If rules)
Plan: docs/superpowers/plans/2026-07-02-eqsat-sp2b-slice3-analysis-gated.md. BASE (pre-slice-3): 2b0c40e82a.
Rule set VERIFIED vs rules.rs: if_true, if_false_or_null, if_same_branches (SIf + per-id analysis-gate conds + non-linear pattern for same-branches). and_or_short_circuit + and_or_drop_unit DEFERRED (short_circuit needs list-quantified cond, drop_unit needs rest-filter template — variadic-list-structural, grouped w/ flatten_assoc for variadic-set slice; USER decision). Behavior-neutral, no --rewrite gate. Old EGraph<ScalarLang> = oracle (delete = slice 7).
KEY: guard() (codegen.rs:473-483) auto-enforces repeated relvars as same-e-class equality -> If(c,x,x) gives then==els free. Lean green = AGGREGATE lake build (slice-2 lesson). Ordering: Lean (Task 5) BEFORE differential (Task 6) — crate red until lean arms land.
## Tasks
- [ ] Task 1: SIf + scalar Cond AST variants + grammar
- [ ] Task 2: scalar view methods (scalar_could_error, scalar_lit_bool_or_null)
- [x] Task 3: codegen SIf arms + scalar-cond emission + child-widen
- [x] Task 4: port 3 If rules to scalar.rewrite
- [x] Task 5: Lean If denotation + 3 theorems (greens crate + aggregate lake build)
- [ ] Task 6: corpus + differential parity (could_error + literal axes)
- [ ] Task 7: slice-3 gate (unit + differential + slt no-rewrite + aggregate lake build)

Task 1: complete (commit 6c75e4de91, review clean — Spec ✅ Quality Approved, 0 findings; cond keyword=where confirmed). Build red on 8 codegen E0004 (Task 3); lean errors masked behind build-script fail (surface after Task 3, Task 5 scope).

Task 2: complete (commit 0d2ccc26b8, review clean — Spec ✅ Quality Approved). scalar_could_error + scalar_lit_bool_or_null on MatchGraph/BaseView; lit_bool_or_null line-for-line match to rules.rs, find() canonicalization present. Deferred test covers true/None only; false/null/error-literal covered by Task 6 differential. Build still red on 8 codegen E0004 (Task 3).

Task 3: complete (commit 9fba4ae476 + style fix — Spec ✅ Quality Approved). Codegen SIf match/build + scalar-cond emission + child-widen + is_scalar_rule; also fixed sym_name + cond() AST-echo exhaustiveness. Codegen E0004 gone; only 3 lean.rs E0004 remain (Pat/Tmpl::SIf, Task 5).
TASK-2 ADDENDUM (commit 39e379f420): ColoredView (colored/view.rs) was missing the 2 scalar view methods (E0046) — Task 2 only added to BaseView; MatchGraph has 2 impls. Found by Task-3 impl, fixed with sound empty stubs (mirror existing ColoredView scalar stubs). PROCESS NOTE: adding a trait method needs an all-impls check; the diff-only review missed the 2nd impl.

Task 4: complete (commit 93bf28585c, review clean — Spec ✅ Quality Approved). 3 If rules; SCALAR_COMPILED_RULES=8. Verified generated guards: if_same_branches = `r2==r3 && !scalar_could_error(r1)` (non-linear x + could_error gate); if_true/if_false_or_null lit guards correct, e-branch direction confirmed. flatten_assoc/short_circuit/drop_unit absent. Build red on 3 lean E0004 (Task 5).

Task 5: complete (commit 51cc876aa9 + comment nits — Spec ✅ Quality Approved). Lean If denotation (ifE, Bool if) + 3 If theorems ALL PROVED outright (no sorry). cargo GREEN + AGGREGATE lake build GREEN (45 theorems, +3) + PERMANENT SORRY=0. cond->hypothesis: lit_true->=true, lit_false_or_null->=false (null folds, documented), no_error->dropped (unconditional in error-free model, documented 3 sites). Caught real is_scalar_rule bug (SIf missing -> unbound env/ill-typed Lean).
TASK-2 ADDENDUM #2 (commit a1c0679bc4): view.rs test had E0502 borrow bug (eg.data_mut().insert(eg.find(...))) -> cargo check --tests failed (hid behind plain check). Hoisted finds into locals. --tests GREEN, deferred view test now RUNS + PASSES. Task 2 had 2 misses (ColoredView + this); under-verified. PROCESS: run cargo check --tests, not just check.

Task 6: complete (commit 08aaf40062, review clean — Spec ✅ Quality Approved, Coverage REAL). Differential parity PASS 5 If (literal true/false/null folds + could_error pos collapse + neg control) + 3 prior; eqsat 340/340. NEG control uses column-division if(#0/#1=0,#2,#2) not literal 1/0 (literal would trip old-engine unported const_fold -> spurious divergence; column div still could_error=true, static per-func property). Reviewer MUTATION-TESTED: disabling old-engine could_error gate makes neg control fail -> real detection power, not vacuous. Gate = literally same shared analysis code both engines.

Task 7: complete (gate-record 327201d29a — GATE GREEN). eqsat 340/340; differential 6/6; relational goldens NO --rewrite 1346/1346 zero diffs (55 files); AGGREGATE lake build clean-rebuild GREEN, PERMANENT SORRY=0; termination converges.
SLICE 3 ALL 7 TASKS DONE. Range 2b0c40e82a..327201d29a. Final whole-slice review next. Task-2 addenda (ColoredView 39e379f420, view-test borrow a1c0679bc4) folded in.

FINAL WHOLE-SLICE REVIEW (opus): READY TO MERGE. 0 Critical/Important. All 6 cross-cutting properties confirmed (relational non-perturbation traced through every shared widen; non-linear If(c,x,x) sound; view methods complete on BaseView+ColoredView; cond->hypothesis soundness; extractor untouched; both gate axes real). 1 Minor fixed: cond_is_color_exact scalar conds true->false (defensive — makes build assertion fire if a scalar rule is ever mistakenly colored, vs silently hitting inert ColoredView stubs). SLICE 3 COMPLETE + MERGE-READY. Range 2b0c40e82a..HEAD.

## SDD — SP2b SLICE 4 (const-eval builtin-applier)
Plan: docs/superpowers/plans/2026-07-02-eqsat-sp2b-slice4-const-eval-builtin.md. BASE (pre-slice-4): 8e169630fb.
READ-FIRST verdict: (1) and_or_empty NOT redundant w/ and_single/or_single (arity 0 vs 1); overlaps const_fold (both fold And([])->true) but both in old engine + same literal = harmless, ship both per spec. Splits into and_empty/or_empty (fixed func). (2) Builtin hatch did NOT exist -> slice 4 introduces it. const_fold=builtin (PERMANENT SORRY), and_or_empty=declarative literal (provable). No semantic delta vs old rules.rs.
DESIGN: Pat::Scalar{binding} matches any scalar CALL node (iterate 4 call syms Unary/Binary/Variadic/If); apply gets b.root=CLASS id (not node) -> const_eval scans class scalar nodes for foldable rep (sound by congruence). apply Err = silently skipped by both saturate loops (mirrors old return vec![]). Tmpl::Builtin{name,args} + Tmpl::SBool(bool). New eqsat/scalar_builtins.rs. Lean: opaque constEval + litB:Bool->ScalarExpr; const_fold thm = permanent sorry (marker `-- PERMANENT SORRY: RHS is a Rust builtin`), and/or_empty proved via simp. Taxonomy after slice: PERMANENT=1, PRE-EXISTING GAP=1 (filter_unionAll), provable-later=26. CI ci/test/lean-mir-rewrite.sh count 0->1.
## Tasks
- [x] Task 1: AST (Pat::Scalar, Tmpl::Builtin, Tmpl::SBool) + grammar
- [x] Task 2: eqsat/scalar_builtins.rs const_eval (class-level port)
- [x] Task 3: codegen Scalar-root iteration + Builtin/SBool emission + is_scalar_rule routing
- [x] Task 4: port const_fold/and_empty/or_empty to scalar.rewrite
- [x] Task 5: Lean opaque constEval+litB + emit arms + permanent sorry marker + CI count 1 (greens crate + AGGREGATE lake)
- [x] Task 6: differential corpus + parity (error-eval 1/0 negative control, mutation-tested)
- [x] Task 7: slice-4 gate (unit + differential + slt no-rewrite + aggregate lake green + PERMANENT=1)

Task 1: complete (commit 08a6d4387e, review clean — Spec ✅ Quality Approved, 0 findings). Pat::Scalar/Tmpl::Builtin/Tmpl::SBool + grammar; reviewer built standalone chumsky harness, 7/7 tests pass, probed shadowing (truething->RelVar, Empty/Negate not shadowed by builtin catch-all). Build RED = expected 5 E0004 codegen (Task 3).

Task 2: complete (commit d399d70db4, review clean — Spec ✅ Quality Approved). scalar_builtins::const_eval class-level port; 3/3 tests (all-literal, 1/0 error-as-data, non-literal-child Err). Reviewer rebuilt+ran+mutation-tested (non-vacuous)+reverted byte-clean. Class-level scan sound by congruence; no borrow held across g.add. MINOR (route to Task 6): add a nested pre-existing-Err-literal-child case e.g. (1/0)+5 two-step fold. Task-3 note: lean.rs needs 3 more E0004 arms (Pat::Scalar/Tmpl::Builtin/Tmpl::SBool) beyond codegen's 5 — that's Task 5.

Task 3: complete (commit 7461b95f5f, review — Spec ✅ Quality Approved, 2 Minor deferred). codegen green; only 3 lean.rs E0004 remain (Task 5). Reviewer inspected generated OUT_DIR: Pat::Scalar iterates 4 call syms + binds e; Builtin emits const_eval(g,ba)? g-first; SBool literal_true/false; is_scalar_rule routes to SCALAR_COMPILED_RULES only. MINOR-1 (final review): Matcher::node Pat::Scalar unreachable! is reachable via nested Scalar(e) child (grammar not root-restricted); no slice-4 rule hits it; improve msg or restrict grammar. MINOR-2 (final review / Task 5 defensive): Tmpl::Builtin pins concrete EGraph -> a colored:true scalar rule wouldn't compile in colored_apply; none colored by design; wants build-time assert.

Task 4: complete (commit 7ed151d606 + comment-style fix). SCALAR_COMPILED_RULES 8->11 (const_fold, and_empty, or_empty). Review Spec ✅; directions verified vs unit_of_and_or (And->literal_true, Or->literal_false); generated find_const_fold iterates 4 syms + apply const_eval; and_empty/or_empty gate len==0 -> literal_true/false; none in relational COMPILED_RULES. 1 Minor (semicolons in comments, from brief text) FIXED directly. Build RED = 3 lean.rs E0004 (Task 5).

Task 5: complete (commit f5890c9997 + comment-style fix). cargo GREEN + AGGREGATE lake GREEN (local + Docker CI script) + PERMANENT SORRY=1. Reviewer kernel-verified via #print axioms: and_empty/or_empty -> [propext] (genuinely proved), const_fold -> [sorryAx] (genuinely permanent). Regeneration parity empty (Generated.lean == emitter output). Inhabited via `deriving Inhabited` on ScalarExpr (minimal, sound). denoteSFold needed no litB arm (matches on List structure not ScalarExpr ctors). CI expected_permanent=1 trip-wire fires on 0 and 2. 3 Minor (semicolons in lean.rs/Semantics.lean comments + stale CI comment) FIXED directly. Taxonomy now: PERMANENT=1, PRE-EXISTING GAP=1, provable-later=26.

Task 6: complete (commit bbe045636e, review clean — Spec ✅ Quality Approved). scalar_parity_const_eval PASS 10 axes (Unary/Binary/If literal folds, 1/0 + overflow error-as-data, nested (1/0)+5, partial-literal no-fold, And()/Or() empty, and_single/and_empty fixpoint); corpus_covers PASS; suite 348 (from 346). Reviewer REPRODUCED mutation live (const_eval skip-Err -> parity FAILS on 1/0 with exact left/right, reverted clean). Subset scoping structural: new engine = SCALAR_COMPILED_RULES (10 ported rules), oracle = separate full-rule ScalarEGraph path; error-literal equality via full MirScalarExpr eq. Staged scalar_saturate.rs + eqsat_scalar_corpus (slice-2/3 precedent 26d9d9320b/08aaf40062). MINOR (final review, no fix req): And(#0) comment "must not fight" overclaims (arity-0 vs arity-1 can't contend); test valid.

Task 7: complete (gate-record 26565a95a2 — GATE GREEN). unit 338 (full --lib 348); parity 4/4 (not_not/if/const_eval/variadic); relational slt NO --rewrite 55 files PASS success=1346 total=1346 zero diffs; AGGREGATE lake clean-rebuild GREEN, PERMANENT SORRY=1 (Generated.lean:266 rule_const_fold); termination no MAX_ITERS.
SLICE 4 ALL 7 TASKS DONE. Range 8e169630fb..26565a95a2. Final whole-slice review next.
DEFERRED MINORS for final review: M1 (codegen.rs:330 Matcher::node Pat::Scalar unreachable! reachable via nested Scalar(e) child; grammar not root-restricted; no slice-4 rule hits it). M2 (Tmpl::Builtin pins concrete EGraph -> colored:true scalar rule wouldn't compile in colored_apply; none colored by design; wants build-time assert like cond_is_color_exact). M3 (scalar_saturate And(#0) comment "must not fight" overclaims; test valid).

FINAL WHOLE-SLICE REVIEW (opus): READY TO MERGE. 0 Critical/Important. All 5 cross-cutting properties CONFIRMED (behavior-neutral additive-only shared surface, relational COMPILED_RULES=37 zero scalar leakage; const_eval class-level sound by congruence + error-as-data; permanent-sorry=1 honest opaque constEval [sorryAx] vs and/or_empty [propext]; routing SCALAR_COMPILED_RULES only; differential non-vacuous + mutation-verified). Minor rulings: M1 acceptable (compile-time panic, no slice-4 rule nests Scalar), M3 leave (comment nit). M2 FIXED (52f5ded769): reviewer found the emit() color-assert only iterates conds so a colored+conditionless scalar rule (esp SBool-RHS and_empty/or_empty) would COMPILE into a silent inert dead rule; added direct assert !(colored && is_scalar_rule) in emit(). Verified colored is explicit grammar opt-in (not vacuous), all scalar rules colored:false, assert passes, cargo GREEN.
SLICE 4 COMPLETE + MERGE-READY. Range 8e169630fb..52f5ded769.

## SDD — SP2b SLICE 5 (type-context builtins + func-metavar binding)
Plan: docs/superpowers/plans/2026-07-02-eqsat-sp2b-slice5-type-context-builtins.md. BASE (pre-slice-5): bf1f8331bd.
READ-FIRST verdict: 5 rules confirmed vs scalar/rules.rs. Split: if_err_cond/null_prop_binary/err_prop_binary = BUILTINS on Scalar(e) catch-all (permanent sorry); isnull_fold = DECLARATIVE `=> false` + new scalar_non_nullable cond (PROVED in Lean); not_binary_negate = FUNC-METAVAR declarative (Pat::SBinaryVar + Tmpl::SBinaryNegate, negate() opaque -> permanent sorry). expected_permanent 1->5 (const_fold+3 builtins+not_binary_negate); provable 26->27; pre-existing gap stays 1. NO LATTICE ADD (nullability on-demand raise+typ+col_types). Typing on combined graph DERISKED: scalar_extract::raise + g.data().scalar.col_types. negate = BinaryFunc::negate() (same table both engines). New surfaces: EBindings func-binding, view scalar_nullable (all-impls incl ColoredView), grammar Binary[f]/Binary[negate(f)], isnull unary keyword.
## Tasks
- [ ] Task 1: DSL AST (Pat::SBinaryVar, Tmpl::SBinaryNegate, Cond::ScalarNonNullable) + grammar
- [ ] Task 2: scalar_builtins if_err_cond/null_prop_binary/err_prop_binary (class-level, combined-graph typing)
- [ ] Task 3: matcher EBindings func-binding + view scalar_nullable (all-impls sweep)
- [ ] Task 4: codegen isnull keyword + ScalarNonNullable emit + SBinaryVar find + SBinaryNegate apply + exhaustiveness
- [ ] Task 5: port 5 rules to scalar.rewrite (11->16)
- [ ] Task 6: Lean isNullE+BinFunc/binaryE/negateFunc opaque + translate arms + proofs + CI count 1->5 + aggregate lake
- [ ] Task 7: differential corpus + parity (all negation pairs x null, MUTATION test; if-err; null/err-prop; isnull) + slice gate

Task 1: complete (commit 1baa15d98d, review clean — Spec ✅ Quality Approved). Pat::SBinaryVar/Tmpl::SBinaryNegate/Cond::ScalarNonNullable + grammar (sbinary_var pat, tsbinary_negate tmpl, scalar_non_nullable cond). Reviewer independently reproduced 10/10 grammar tests + 8 expected E0004 (3 SBinaryVar/3 ScalarNonNullable/2 SBinaryNegate, codegen.rs only) + own negative probes (Binary[f] as Tmpl / Binary[negate(f)] as Pat both correctly rejected -> no real shadow/ambiguity). MINOR (no fix): shadow-test name vacuous (kw() exact-match), real coverage OK. FIX APPLIED to plan: brief/plan example used comma `where c1, c2` but grammar needs repeated `where c1 where c2`; implementer used correct syntax in tests; plan Task-5 isnull_fold rule text corrected.

Task 2: complete (commit 8bcd10b20f, review clean — Spec ✅ Quality Approved). scalar_builtins if_err_cond/null_prop_binary/err_prop_binary + helpers call_scalar_type/scalar_could_error/is_literal_null/literal_err. 15/15 tests (3 const_eval + 12 new adversarial). Reviewer REPRODUCED build+tests (via 8 codegen + 3 lean.rs todo!() stubs, reverted byte-clean) + MUTATION-tested (removing null_prop_binary other-error gate -> null_prop_binary_blocked_when_other_can_error FAILS = non-vacuous) + line-by-line parity vs rules.rs (gates/operand-selection/typing identical; only col_types()->g.data().scalar.col_types + raise::raise->scalar_extract::raise). ArrayRemove used for propagates_nulls==false negative test (real, verified src/expr/.../func.rs:2759). scalar_could_error unwrap_or(false) = brief-prescribed, unreachable (on_add eager-populates). Only scalar_builtins.rs staged. NOTE: lean.rs needs 3 stub arms too (Task 6, known).
