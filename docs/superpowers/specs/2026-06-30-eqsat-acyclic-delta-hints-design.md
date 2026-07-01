# Eqsat acyclic delta commit + join-input hints — design spec

> **Status:** approved design, ready for an implementation plan.
> Follow-on to SP-B1 (`docs/superpowers/specs/2026-06-30-eqsat-native-join-commit-design.md`).
> Supersedes the relevant parts of the design notes in
> `doc/developer/design/20260624_eqsat/20260630_eqsat_acyclic_delta_and_hints.md`.

## Goal

Make eqsat's native join commit (a) emit `JoinInputCharacteristics` (the K / UK /
A markers) on committed joins, and (b) commit a `JoinImplementation::DeltaQuery`
instead of `Differential` when delta is free, recovering the free-delta plans on
indexed multi-way joins that SP-B1 regressed differential.

Both changes live in the **commit path** (`raise.rs` / `eqsat/join_commit.rs`),
sourced from the cost model. The e-graph `Cost` is untouched, so the AGM
over-pick that forced "acyclic always differential" in SP-B1 never arises.

## Background: what SP-B1 left on the table

SP-B1 commits every acyclic `Rel::Join` to a cost-model-chosen `Differential`,
so `JoinImplementation` (JI) no-ops on eqsat joins. Two gaps remain, both visible
in the corpus diff:

1. **Acyclic joins are always `Differential`.** On indexed multi-way joins where
   every probe arrangement already exists, JI would have produced a `DeltaQuery`
   for free (its `delta_new_arrangements == 0` rule). Delta is generally better
   for incremental maintenance (one path per input → low update latency for a
   change to any input). ~13 transform goldens flipped delta → differential.

2. **The emitted plan drops JI's join-input hints.** `commit_differential` sets
   `JoinInputCharacteristics = None` for every order element, so EXPLAIN shows
   bare keys (`%1:bar[#0]`) instead of `K` / `UK` / `A` markers. They are
   EXPLAIN-only, but they hurt readability and inflate golden diffs vs JI.

Both move eqsat toward parity with JI, i.e. toward removing JI from the eqsat
path (`doc/developer/design/20260624_eqsat/20260630_joinimplementation_internals.md`).

## Architecture

The delta-vs-differential decision is a **structural arrangement-reuse rule**,
exactly as it is in JI: commit delta iff its plan needs no new arrangements
(strict) or no more than differential needs (eager). That decision belongs at
commit time, where the per-input `available` arrangements are in hand. It is not
an AGM-cost comparison, and it is not in the e-graph.

The commit path gains one decision point in `raise.rs`'s `Rel::Join` arm: after
canonicalizing the equivalences (as SP-B1 already does), compute the differential
order and, separately, the per-driver delta paths; if a delta plan exists and is
free enough, commit `DeltaQuery`; otherwise commit the SP-B1 `Differential`.

### The viability invariant (corrected from the design notes)

A `DeltaQuery` stores **lookups only**: `DeltaQuery(Vec<Vec<(usize,
Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>>)`, one inner vector per
driver, the driver implicit by path position (relation.rs:3247; JI's
`delta_queries::plan` emits `o.into_iter().skip(1)`, dropping the start element).
There is **no stored start/driver key**. The driver's own arrangement key
(`source_key`) is derived in lowering as the smallest lookup key over all paths
(delta_join.rs:95-110), and each lookup's stream-side key is derived in lowering
via `find_bound_expr` (delta_join.rs:185-195).

Consequences:

- **No per-path start-key alignment is needed.** SP-B1's C1 hazard (an over-wide
  *stored* start key silently producing empty results) is specific to
  `Differential`, whose `differential::plan` extracts and emits `order[0]` as an
  explicit start tuple. `commit_delta_query` mirrors JI's lookups-only emission
  and carries no start key, so the hazard structurally cannot occur.

- **`crosses == 0` is a representability/viability precondition, not a panic
  guard.** A non-keyed (cross) step has no arrangement key to put in its lookup
  tuple, so a disconnected join simply has no delta plan. This is why
  `delta_join_order` returns `None` on a cross. (Do not justify it as preventing
  a `find_bound_expr` render panic: `find_bound_expr` is in the differential and
  render paths, never in the delta commit path, and the viability guard upstream
  means the bad tuple is never constructed in the first place.)

For a connected acyclic join graph every step has a keyed extension, so a delta
plan exists exactly when the join is connected.

### Precondition: acyclic joins only

`delta_join_order` is reached only from the `Rel::Join` arm, which carries
acyclic joins. Cyclic joins lower to `Rel::WcoJoin` (the `join_to_wcoj` gate) and
take a different raise arm, so the left-deep per-driver walk is never applied to a
cyclic graph. This matters: on a *connected cyclic* graph `delta_join_order`
would happily return `Some` with left-deep paths that are not the right shape for
a worst-case-optimal join. The caller must only invoke it on `Rel::Join`.

### Parity with JI is "correct-and-delta", not "identical plan"

`delta_join_order` walks eqsat's greedy paths (prefer keyed, then lowest AGM);
JI's `delta_queries::plan` uses `optimize_orders`. These are different
order-selection algorithms, so two things follow, neither a correctness bug (any
valid delta plan returns correct rows; the execution gate covers that):

- eqsat's `delta_new` can differ from JI's. The set that flips to delta need not
  be exactly the set SP-B1 regressed: it could be more, fewer, or different
  joins.
- Even where both choose delta, eqsat's EXPLAIN may not byte-match JI's (different
  path orders or keys).

So the "~13" figure below is an **estimate** from SP-B1's observed regression, not
a guarantee. The flipped-golden set and their EXPLAIN need case-by-case review
(the corpus A/B gate does exactly this), not a byte-match against JI. This is the
same decoupling tension as the rest of the project: the decision moved out of
`Cost`, so eqsat's plans are not co-optimal with JI's chooser.

## Part A — Join-input hints (implement first; shared helper)

### `step_characteristics` helper

Add to `eqsat/join_commit.rs`:

```rust
/// Build the `JoinInputCharacteristics` for one order step (start or lookup),
/// from the structurally-known fields. `available` is the ORIGINAL per-input
/// arrangement keys (before `implement_arrangements` wraps inputs in ArrangeBy),
/// matching JI's `arranged` computation. `cardinality`/`filters` are left at
/// their neutral values until the cardinality/selectivity axes land.
fn step_characteristics(
    input: usize,
    key: &[MirScalarExpr],
    available: &[Vec<Vec<MirScalarExpr>>],
    inputs: &[MirRelationExpr],
    enable_join_prioritize_arranged: bool,
) -> JoinInputCharacteristics
```

Field sources (constructor:
`JoinInputCharacteristics::new(unique_key, key_length, arranged, cardinality,
filters, input, enable_join_prioritize_arranged)`):

| field | source |
|---|---|
| `key_length` | `key.len()` |
| `arranged` | `available[input].iter().any(|k| k == key)` — original `available` |
| `unique_key` | some unique key in `inputs[input].typ().keys` is a subset of `key`'s columns (same test as JI) |
| `input` | `input` |
| `cardinality` | `None` |
| `filters` | `FilterCharacteristics::none()` |
| `enable_join_prioritize_arranged` | the flag |

`unique_key` is computed over `key`'s plain-column members. A `key` element that
is a non-column expression cannot match a unique-key column, so it never
contributes; this matches JI, which derives `unique_key` from column keys.

### Wiring into `commit_differential`

`commit_differential` stops emitting `None`:

- The start element's characteristics are computed on its `find_bound_expr`-
  aligned start key (the key as finalized at join_commit.rs:85, before the
  `implement_arrangements` projection compensation, so `arranged` is tested
  against the original `available`).
- Each lookup element's characteristics are computed on its local key.

Thread `inputs: &[MirRelationExpr]` (the join's inputs, already available via the
`JoinInputMapper` construction) and `enable_join_prioritize_arranged: bool` into
`commit_differential`. The flag reaches `raise.rs` the same way
`native_join_commit` does (a `bool` threaded through `raise`/`raise_inner`, fed
from `OptimizerFeatures::enable_join_prioritize_arranged`).

### Consumer audit (must do before relying on "EXPLAIN-only")

Characteristics here are set **after** the cost model has already chosen the
order; in JI they instead *drive* order selection. The precise hazard is the
transition this introduces: SP-B1 emits `None` for the whole
`Option<JoinInputCharacteristics>`; Part A changes that to `Some(chars)` with
`cardinality = None` and `filters = none()`. The danger is not "a consumer reads
chars" but "a consumer that correctly handled *absent* chars now gets
*present-but-placeholder* chars and treats `Some` as authoritative." The fields
that would bite are `arranged` and `unique_key` if read as complete (they are
real and correct here, so this is safe), not `cardinality`/`filters` (those were
already neutral).

Required audit task: grep every reader of `JoinInputCharacteristics`, and for
each, check what it does on an already committed (eqsat-emitted) join when the
`Option` goes from `None` to `Some(partial)`. Confirm none make a plan decision
that depends on `cardinality`/`filters` being populated, and none treat the mere
presence of `Some` as "fully specified". Expectation from the SP-B1 finding (JI
no-ops on committed joins, rendering consumes the committed order): this is
display-only. But the audit is a required task, not an assumption; if a consumer
fails the check, defer the partial fields rather than emit neutral values.

## Part B — Acyclic delta detection

### `cost.rs::delta_join_order`

```rust
/// Surface the per-driver left-deep delta paths: one path per driver (indexed by
/// driver position, to match the renderer's `join_orders[source_relation]`),
/// each a sequence of keyed lookups. Returns `None` if any path has a non-keyed
/// (cross) step, i.e. the join is disconnected and has no delta plan.
pub(crate) fn delta_join_order(
    &self,
    inputs: &[Rel],
    equivalences: &[Vec<EScalar>],
) -> Option<Vec<Vec<JoinStep>>>
```

Walk the same greedy per-driver paths `delta_join_terms` walks (prefer a keyed
extension, then the lowest resulting AGM degree, deterministic). For each driver,
the returned `Vec<JoinStep>` is the lookups after the driver (the driver itself
is not a step), each step's `key_cols` from `frontier_key_cols` against the
accumulating per-path frontier (driver + prior lookups).

"Keyed" is decided by `frontier_key_cols` being non-empty — the **same predicate
`binary_join_order` uses** for differential non-start steps. This makes the keys
`delta_join_order` emits exactly the keyed steps, and keeps it consistent with
the differential surfacing. If the chosen next step's `frontier_key_cols` is
empty, the path has a cross and `delta_join_order` returns `None`.

`delta_join_terms` (the `(crosses, degrees)` cost axis) is **not** consulted at
commit; it serves the e-graph spelling selector only. The commit decision routes
solely through `delta_join_order`, so the coarser mask-based `keys_step` in
`delta_join_terms` (and its constant-side gate) cannot drift the commit decision.

### `join_commit.rs::commit_delta_query`

```rust
/// Commit `join` to a `DeltaQuery` following `paths` (one lookup sequence per
/// driver). `available` gives each input's existing arrangement keys. Returns
/// `None` (caller keeps the differential / bare join) if `join` is not a `Join`
/// or the paths are degenerate.
pub(crate) fn commit_delta_query(
    join: MirRelationExpr,
    paths: Vec<Vec<JoinStep>>,
    available: &[Vec<Vec<MirScalarExpr>>],
    enable_join_prioritize_arranged: bool,
) -> Option<MirRelationExpr>
```

Mirror `delta_queries::plan` (join_implementation.rs:698-772):

1. Build `orders: Vec<Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>>`
   from `paths`: each step → `(step.input, key_from(step.key_cols), Some(chars))`,
   with `chars` from the Part-A `step_characteristics` helper computed against the
   original `available`. No start key, no start element (paths already exclude the
   driver).
2. `let (lifted_mfp, lifted_projections) = implement_arrangements(inputs,
   available, orders.iter().flatten());`
3. `orders.iter_mut().for_each(|o| permute_order(o, &lifted_projections));`
4. `*implementation = JoinImplementation::DeltaQuery(orders);`
5. `install_lifted_mfp(&mut join, lifted_mfp);`

`implement_arrangements`, `permute_order`, `install_lifted_mfp` are already
`pub(crate)` (made so in SP-B1).

### `raise.rs` `Rel::Join` arm — the decision

Extend the SP-B1 commit path (after canonicalize, after computing the
differential `order`):

```text
let diff_order = binary_join_order(...);            // SP-B1, computed as today
if let Some(paths) = delta_join_order(inputs, &canon_escalars) {
    let delta_new = count_new_arrangements(&paths, &per_input);
    let commit_delta = if enable_eager_delta_joins {
        let diff_new = differential_new_arrangements(&diff_order, &per_input);
        delta_new <= diff_new
    } else {
        delta_new == 0
    };
    if commit_delta {
        if let Some(j) = commit_delta_query(canon_join, paths, &per_input, prioritize) {
            return j;
        }
    }
}
// fall through to SP-B1 differential commit
```

- **(a) viability** = `delta_join_order(..).is_some()` (no separate `crosses`
  check).
- **(b) free enough** = `delta_new == 0` (strict) or `delta_new <= diff_new`
  when `enable_eager_delta_joins` is on. Matches JI's `plan_join_min_arrangements`.

`delta_new` = number of **distinct** non-`available` `(input, key)` lookup
arrangements over all paths, deduplicated via a set (identical to
`delta_queries::plan:727-739`). `diff_new` is computed only in the eager branch,
mirroring `differential::plan`'s count (the start's new input arrangement plus
the per-step lookups), against `per_input`.

The existing SP-B1 abort ("any non-start step has empty key_cols → keep bare
join") stays for the differential fall-through. For delta the equivalent is
folded into `delta_join_order` returning `None`.

## Flag

Reuse `enable_eqsat_native_join_commit`: delta detection and hints are intrinsic
to "commit joins natively," not a separable user knob, so they ship together
under the existing flag. The eager variant reads the existing
`enable_eager_delta_joins`. No new flag.

Because the flag is reused, the ~13 differential → delta flips land atomically
with this change. Isolation is recovered at **review time** (see Testing): tag
the flipped goldens as delta-recovery and confirm 0 result-row changes on each.

## Sequencing

1. **Part A first** (the `step_characteristics` helper + `commit_differential`
   wiring + consumer audit). Self-contained; its only corpus effect is K/UK/A
   markers appearing on the existing differential commits.
2. **Part B** (`delta_join_order`, `commit_delta_query`, `raise.rs` decision).
   `commit_delta_query` reuses the Part-A helper.

## Testing

### Unit

- `cost.rs`: `delta_join_order` returns the expected per-driver paths on a 3-input
  star and on a chain (keys local, driver excluded); returns `None` on a
  disconnected (cross-product) join.
- `join_commit.rs`: `commit_delta_query` builds the expected `DeltaQuery` shape
  (one path per driver, lookups only); a lookup on an arranged, unique-keyed
  input carries `arranged = true` and `unique_key = true`.
- `join_commit.rs`: `step_characteristics` yields `arranged`/`unique_key`/
  `key_length` correctly against a crafted `available` + input `keys`.

### Execution gate (the C1 lesson — required)

EXPLAIN-only golden checks miss wrong-results regressions. For a representative
indexed star and chain (e.g. in `test/sqllogictest/transform/join_index.slt`):

1. assert the committed plan is `type=delta`,
2. assert returned rows are **identical to flag-off**, via a result-row diff vs
   base (both added and removed rows), not just EXPLAIN structure.

The flagship variable-outer-join must stay `type=differential` (its delta path
crosses `t1`, so `delta_join_order` returns `None`).

### Corpus

- Expect roughly the ~13 indexed joins SP-B1 regressed to flip differential →
  delta, but treat ~13 as an **estimate**: eqsat's greedy order may pick correct
  delta plans on a different set of joins than JI's `optimize_orders` would, and
  the EXPLAIN need not byte-match JI. Review the flipped set case by case rather
  than asserting an exact count or a JI byte-match. Regenerate affected transform
  goldens per-file in isolation (the shared-catalog batch-rewrite GlobalId drift
  from SP-B1 still applies).
- Confirm **0 result-row changes vs base** across the corpus.
- **Reviewable A/B (required):** in the landing PR, explicitly tag the goldens
  that flip differential → delta as "delta-recovery", separate from any other
  diff, and record the confirmed 0 result-row delta on each. This recovers the
  isolation the reused flag does not provide.

## Risks

- **Drift between `delta_join_order` and `delta_join_terms`.** Mitigated by
  routing the commit decision solely through `delta_join_order` and deciding
  "keyed" via `frontier_key_cols` (shared with `binary_join_order`).
  `delta_join_terms` stays the selector-only cost axis.
- **Partial characteristics misread as decisions.** Mitigated by the consumer
  audit in Part A; defer partial fields if any consumer makes a decision from a
  committed join's chars.
- **Eager-mode `diff_new` miscount.** The eager branch compares two counts that
  must be computed the same way JI does; mirror `differential::plan` /
  `delta_queries::plan` exactly and cover with a unit test on a join where
  `delta_new == diff_new`.

## File map

- `src/transform/src/eqsat/cost.rs` — add `delta_join_order` (sibling of
  `binary_join_order`); reuse `frontier_key_cols`, the greedy walk from
  `delta_join_terms`.
- `src/transform/src/eqsat/join_commit.rs` — add `step_characteristics` and
  `commit_delta_query`; wire characteristics into `commit_differential`.
- `src/transform/src/eqsat/raise.rs` — `Rel::Join` arm: the delta decision and
  the `prioritize_arranged` / `eager_delta` threading.
- `src/transform/src/eqsat.rs` — thread the two new flags through
  `optimize_with_availability` / `optimize_inner` to `raise`.
- `src/transform/src/join_implementation.rs` — reference only
  (`delta_queries::plan`, `differential::plan`, `implement_arrangements`,
  `permute_order`, `install_lifted_mfp`); helpers already `pub(crate)`.
- `src/expr/src/relation.rs` — `JoinInputCharacteristics::new` (V1/V2),
  `JoinImplementation::DeltaQuery` (reference).

## Pointers

- SP-B1 spec: `docs/superpowers/specs/2026-06-30-eqsat-native-join-commit-design.md`
- Design notes (superseded in part): `doc/developer/design/20260624_eqsat/20260630_eqsat_acyclic_delta_and_hints.md`
- JI subsume map: `doc/developer/design/20260624_eqsat/20260630_joinimplementation_internals.md`
- Delta lowering (the viability evidence): `src/compute-types/src/plan/join/delta_join.rs`
