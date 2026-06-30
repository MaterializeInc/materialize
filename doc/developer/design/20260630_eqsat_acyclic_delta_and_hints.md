# Eqsat follow-ons: acyclic delta detection + restoring join-impl hints

> **Status:** design notes for the next eqsat sub-project. Two related
> improvements to the cost-model-native join commit landed in SP-B1
> (`docs/superpowers/specs/2026-06-30-eqsat-native-join-commit-design.md`,
> flag `enable_eqsat_native_join_commit`). Both live in the **commit path**
> (`raise.rs` / `eqsat/join_commit.rs`), not the e-graph cost, which is what
> makes them tractable. Hand off to a brainstorm → spec → plan → SDD cycle.

## Background: what SP-B1 left on the table

SP-B1 makes the eqsat physical pass commit acyclic `Rel::Join`s to a
cost-model-chosen `JoinImplementation::Differential` at raise time, so
`JoinImplementation` (JI) no-ops on eqsat-produced joins. Two known gaps remain,
both visible in the corpus diff:

1. **Acyclic joins are *always* committed as `Differential`.** SP-B1
   deliberately did not give acyclic joins a delta alternative, because relaxing
   the `join_to_wcoj` gate to mint a `WcoJoin` (delta) alternative in the
   *e-graph* makes the AGM `Cost` over-pick delta (delta streams its output, so
   it carries no intermediate-arrangement memory term, and the keyed-ness axis is
   intentionally not in `Cost`). The consequence: on indexed multi-way joins,
   eqsat emits a single differential chain where JI would have produced a
   **delta query** for free (its `delta_new_arrangements == 0` rule). Delta is
   generally better for incremental maintenance (one path per input → low update
   latency for a change to any input), so this is a real, if narrow, regression
   (~13 transform goldens flipped delta → differential).

2. **The emitted plan drops JI's join-input hints.** `commit_differential` sets
   `JoinInputCharacteristics = None` for every order element, so EXPLAIN shows
   bare keys (`%1:bar[#0]`) instead of JI's `K` / `UK` / `A` / `ef` markers.
   They are EXPLAIN-only, but they hurt readability and inflate golden diffs vs
   JI's format.

Both move eqsat toward parity with JI — i.e. toward the north-star of removing
JI from the eqsat path (`doc/developer/design/20260630_joinimplementation_internals.md`).

---

## Design 1 — detecting acyclic delta joins (in the commit path)

### Key idea

JI's delta decision was never an AGM-cost comparison; it is a **structural
arrangement-reuse rule**: commit delta iff `delta_new_arrangements == 0`
(or `delta_new ≤ diff_new` under `enable_eager_delta_joins`). That decision
belongs at **commit time**, where the `available` per-input arrangements are in
hand and can be counted — *not* in the e-graph `Cost`. Deciding it in the commit
sidesteps the AGM over-pick that forced "acyclic → differential" in SP-B1, and
is the resolution to that deferred decision.

### Decision rule

At commit, compute both candidate orders and commit a `DeltaQuery` (else fall
through to the SP-B1 `Differential`) iff **both** hold:

```
(a) crosses == 0      — from CostModel::delta_join_terms: no forced cross on
                        ANY per-driver path (already computed today)
(b) delta_new == 0    — every per-path arrangement is already in `available`
                        (or delta_new ≤ diff_new when enable_eager_delta_joins)
```

- **(b) is the load-bearing condition.** It recovers exactly the plans SP-B1
  regressed: indexed multi-way joins (see
  `test/sqllogictest/transform/join_index.slt`) where every probe arrangement
  already exists, so delta is free. This is JI's `delta_new_arrangements == 0`
  rule replicated structurally.
- **(a) is a cheap structural guard, not independently load-bearing.** A forced
  cross is counted only when *neither* side is constant (cost.rs:795-796), so a
  non-constant cross needs an empty-key broadcast arrangement that is never in
  `available` — meaning (b) `delta_new == 0` already rejects it. We keep (a)
  anyway as a near-free early-out (it falls out of `delta_join_terms`, which we
  walk regardless) and because it is the keyed-ness signal SP-B1 already trusts.
  Do not justify (a) as "what keeps the VOJ correct": (b) does that. The VOJ
  flagship is acyclic and its delta path cross-products `t1`; the cross needs a
  broadcast arrangement absent from `available`, so (b) rejects delta and it
  stays `Differential`. (a) merely rejects it one step earlier.

#### Interaction with the spelling selector

`crosses` and `delta_new` are computed over the **canonicalized** equivalences,
the same ones SP-B1's `commit_differential` orders over (raise.rs canonicalizes
before both ordering and, here, delta detection). The earlier delta-join-cost
spelling selector (`enable_eqsat_delta_join_cost`, `select_join_spelling`) does
**not** feed this decision: under native commit it is moot, because canonicalize
overrides its local spelling (#2 → #0) before the delta test runs. So the two
sub-projects do not disagree about the VOJ. The selector never reaches the delta
decision, and the VOJ's cross is a property of its canonical equivalence
structure, independent of which spelling flag is set. State this in the spec so
the next implementer does not try to reconcile the selector's #2 output with the
delta test's #0 input.

### Mechanics

- **`cost.rs`:** add `delta_join_order(inputs, equivalences) -> Vec<JoinPath>`
  that surfaces the per-driver left-deep paths `delta_join_terms` already walks
  (each path = driver + ordered lookups with per-step local keys), mirroring how
  `binary_join_order` surfaces the differential order.
- **`eqsat/join_commit.rs`:** add `commit_delta_query` — turn the per-driver
  paths into the `JoinImplementation::DeltaQuery(Vec<Vec<(usize, key, _)>>)`
  shape and reuse `implement_arrangements` + `permute_order` +
  `install_lifted_mfp`, mirroring `delta_queries::plan`
  (`join_implementation.rs`). Reuse the SP-B1 start-key alignment helper for each
  path's first edge (delta paths are also left-deep, so each driver is a stream
  side whose key must align with its first lookup — the same C1 hazard applies
  per path; derive via `JoinInputMapper::find_bound_expr`).
- **`raise.rs` `Rel::Join` arm:** after canonicalizing equivalences, compute the
  differential order (today) *and* the delta paths; count `delta_new` against
  `available`; apply the (a)+(b) test; commit the winner. Read
  `enable_eager_delta_joins` for the `≤ diff_new` variant.
- **Does not touch the e-graph `Cost`** — so the AGM over-pick never arises.

### Risks / tests

- Per-path start-key alignment is the same wrong-results hazard as SP-B1's C1
  (over-wide stream key → empty results). Every delta path's driver key must be
  `find_bound_expr`-derived and length-checked; fall back to `Differential` on
  any mismatch.
- Verify by **execution** (not just EXPLAIN): a representative indexed star/chain
  must (i) commit `type=delta`, (ii) return rows identical to flag-off. The VOJ
  must stay `type=differential`.
- Corpus: expect ~13 indexed joins to flip differential → delta (recovering the
  SP-B1 regression); confirm 0 result-row changes vs base via the result-row
  gate.

### Architectural seam: commit-time vs cost-time

The delta decision is **decoupled** from cost-time selection: extraction picks
the relational plan and the arrangement set under `ArrangementCount` (a
differential-shaped cost), and only then does commit pick delta over
differential. The arrangement set extraction minimized is not necessarily the
one the delta paths want, so the committed plan is not provably jointly
co-optimal. This is the same "moved it out of `Cost`, now it is decoupled"
tension that runs through the project (the AGM cost cannot see keyed-ness, so
the structural decision lives downstream).

We accept it because we bound delta commit to the **acyclic** case (star and
chain), where the input set a left-deep order touches is the same regardless of
delta-vs-differential — only the per-driver path direction differs, and delta
reuses arrangements differential already required. So for acyclic joins the
decoupling is benign: delta does not need arrangements differential did not
already select. A cyclic (WcoJoin) plan would not have this property, which is a
further reason to keep delta commit gated to acyclic joins. Name this bound in
the spec.

---

## Design 2 — restore the join-input hints

### Key idea

Populate the `JoinInputCharacteristics` the cost model already knows, in the
emitter, instead of `None`. `JoinInputCharacteristics::new(unique_key,
key_length, arranged, cardinality, filters, input, prioritize_arranged)` — SP-B1
has everything except the two SP-B2/B3 axes:

| field | source at commit time |
|---|---|
| `key_length` | `key_cols.len()` |
| `arranged` | `key ∈ available[input]` (per-input arrangements) |
| `unique_key` | `inputs[i].typ().keys` — any unique key ⊆ the step key (same as JI) |
| `input` | the input index |
| `prioritize_arranged` | the `enable_join_prioritize_arranged` flag (selects V1 vs V2) |
| `cardinality` | `None` until SP-B2 (cardinality axis) |
| `filters` | `FilterCharacteristics::none()` until SP-B3 (selectivity axis) |

This restores the **K / UK / A** markers immediately (the structurally-derived
ones); the cardinality/filter-derived suffixes fill in when SP-B2/B3 land.

### Mechanics

- A shared `step_characteristics(input, key, available, inputs, features)` helper
  in `eqsat/join_commit.rs`, used by both `commit_differential` and (Design 1)
  `commit_delta_query`, applied to the start key (computed on its
  `find_bound_expr` result) and every lookup.
- Thread `enable_join_prioritize_arranged` (and the `inputs` types) into the
  emitter. `arranged` must be computed from the **original** `available`
  (pre-`implement_arrangements`), matching JI.
- Pure EXPLAIN/readability change; regenerate the affected transform goldens.

### Why this is display-only (and what to verify)

In JI, `JoinInputCharacteristics` drive **order selection** (the greedy max-min
scorer). Here the cost model has already chosen the order; we set the
characteristics post-hoc, after the decision. They are display-only iff nothing
downstream re-reads them for a decision. That holds because eqsat commits a
concrete `JoinImplementation` and JI no-ops on an already-implemented join, so
rendering/LIR consume the committed order, not the characteristics. The spec
must still confirm this: a partial characteristic (`cardinality = None`,
`filters = none()`) is a silent correctness bug, not a cosmetic one, if any
consumer reads it as "zero rows" or "no filter". Grep the consumers of
`JoinInputCharacteristics` after the renderer boundary and confirm none make a
plan decision from a committed (eqsat-emitted) join's characteristics. If one
does, the partial fields must be deferred until SP-B2/B3 fill them, not emitted
as `None`/`none()`.

---

## Sequencing

Do **hints first** (the shared `step_characteristics` helper), then **delta
detection** reuses it for the `DeltaQuery` tuples. Together they (a) erase the
delta → differential regression on indexed joins and (b) bring eqsat's EXPLAIN
to near-parity with JI — both straight toward removing JI from the eqsat path.

## Pointers

- Commit path: `src/transform/src/eqsat/raise.rs` (`Rel::Join` arm),
  `src/transform/src/eqsat/join_commit.rs` (`commit_differential`).
- Cost model: `src/transform/src/eqsat/cost.rs` —
  `binary_join_order`, `delta_join_terms` (keyed-ness `(crosses, degrees)`),
  `join_key_cols_for_input`, `frontier_key_cols`.
- JI reference: `src/transform/src/join_implementation.rs` —
  `plan_join_min_arrangements`, `delta_queries::plan`, `differential::plan`,
  the start-key derivation at `~1305-1318`, `implement_arrangements`,
  `install_lifted_mfp`, `permute_order`.
- `JoinInputCharacteristics`: `src/expr/src/relation.rs` (V1/V2).
- Related: `doc/developer/design/20260630_joinimplementation_internals.md`
  (the JI subsume map), `doc/developer/design/20260629_eqsat_join_cost_findings.md`.
