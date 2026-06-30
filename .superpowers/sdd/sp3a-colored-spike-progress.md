# SDD ledger — SP3a: colored e-graph measurement spike

Plan: `docs/superpowers/plans/2026-06-27-eqsat-colored-spike-sp3a.md`
Spec: `doc/developer/design/20260627_eqsat_colored_spike.md`
Branch: `claude/mir-equality-optimizer-sodbej`; worktree `.claude/worktrees/mir-equality-optimizer`
BASE (before Task 1): f0b09dca9e
Memory: [[eqsat-shared-core-extraction]] (SP1✅ SP2a✅ → SP3a now; SP2b pending)

SP3a = gated measurement spike: minimal colored e-graph (shared base + flat
per-color equalities + `close_color` congruence kernel) + measurement harness →
findings/gate verdict for SP3b. Congruence-only; conclusions/hierarchy/extraction
deferred to SP3b.

## Tasks
- Task 1: core `uf_len` + colored module skeleton (ToyLang, ColoredUf). STATUS: DONE (893798266a)
- Task 2: `close_color` kernel + ColorMetrics + differential oracle. STATUS: pending
- Task 3: workload generators (Lcg, GenParams, gen_base, gen_colors) + randomized oracle check. STATUS: pending
- Task 4: measurement harness + run sweep + write Findings & Gate Verdict. STATUS: pending
- Final: whole-branch review (opus) + verdict sanity. STATUS: pending

## Minor findings (for final-review triage)
(none yet)

## Log
(none yet)
- Task 1: complete (commits f0b09dca9e..893798266a, review clean — spec ✅, quality Approved)
- Task 2: complete (commits 893798266a..8ca390dee8, review clean — spec ✅, quality Approved [opus])
  Minor (final-review triage):
  - M2.1: fixed-base tests are depth-1 only; multi-round cascade not directly tested. Addressed by Task 3 randomized oracle over generated deeper graphs (gen_base depth 3).
  - M2.2: `iters` counts the terminal non-merging pass (empty eqs ⇒ iters==1). Harmless metric; Task 4 reporting aware.
  - M2.3: `same_partition` does full i×j incl. i==j/both orders (~2× work). Cosmetic, test-only.
- Task 3: complete (commits 8ca390dee8..4cbcf649a2, review clean after 1 fix wave — spec ✅, quality Approved)
  Fix wave: widened ToyLang/ToyNode/ToySym to pub(crate) (private_interfaces was CI-breaking under clippy -Dwarnings) + Debug derives on Lcg/Locality/GenParams. Re-review: RESOLVED.
  Minor (final-review triage):
  - M3.1: gen_base_is_deterministic checks counts not structure (acceptable for spike; PRNG deterministic).
  - M3.2: gen_base frontier.remove(0) is O(n); negligible at spike sizes.
- Task 4: complete (commits 4cbcf649a2..edda4ccedc, review clean after 1 fix wave — spec ✅, quality Approved [opus], VERDICT SOUND)
  Verdict: PROCEED WITH SHARED-DELTA AS-IS. Sharing ratio ≪1 and shrinks with base_size (delta_nodes ~constant ~630 across 100→5000); ~linear in n_colors (~13/color); cascade factor 0.000 for fan_out≥4, ~0.005 at fan_out=2, 1.345 at fan_out=1 (unary chains, structurally absent from relational algebra). No mitigations needed for SP3b.
  Fix wave: corrected broken sweep run command (nextest: --run-ignored ignored-only --no-capture) in colored.rs + spec §6; reconciled §8 cascade wording + "two worst single-axis cells".

## SP3a tasks complete — proceeding to final whole-branch review (opus).
SP3a range for final review: 9bcdd889bc..edda4ccedc (spec + plan + 4 tasks + fixes).

## SP3a COMPLETE (2026-06-27)
All 4 tasks implemented + reviewed clean (Task 3: 1 fix wave for CI-breaking private_interfaces; Task 4: 1 fix wave for run-command/§8 wording). Final whole-branch review (opus): "Ready to merge: YES", gate verdict SOUND, SP3b cleared to build shared-delta. One final-review Important finding fixed post-review: gen_colors nondeterminism (HashMap::keys order) → `roots.sort_unstable()` + `gen_colors_is_deterministic` test + §8 refreshed to reproducible deterministic numbers (12/12 tests pass).

VERDICT: PROCEED WITH SHARED-DELTA AS-IS. Sharing ratio ≪1 and shrinks with base_size (delta_nodes ~constant ~630 across base_size 100→5000); ~linear in n_colors; cascade factor 0.000 for fan_out≥2, 1.345 at fan_out=1 (unary chains, structurally absent from relational algebra). No mitigations required for SP3b.

SP3a range: 9bcdd889bc..7781d552c3 (HEAD). Local ahead of origin — push when user confirms.
NEXT: SP3b (full colored mechanism: sparse delta union-find + hierarchy + colored congruence + color-aware extraction) OR SP2b (declarative DSL port). Each its own spec+plan+SDD cycle.
