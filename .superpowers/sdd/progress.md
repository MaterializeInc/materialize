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
