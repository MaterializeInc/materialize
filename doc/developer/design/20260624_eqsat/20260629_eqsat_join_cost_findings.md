# Eqsat join-cost findings: the cost model is the bottleneck

> **Status:** investigation findings + de-risking spike. NOT yet a spec. This
> document hands off to a future session to run the
> brainstorm → spec → plan → SDD cycle for a delta-aware join cost. All probes
> described here were throwaway and have been reverted; the branch is clean.

## Question

Where can equality saturation beat the production term-graph optimizer? The
production optimizer is a **fixpoint rule loop**: directional canonicalizations
(predicate pushdown, fusion, literal lifting, `canonicalize_equivalences`, …)
re-run to a fixed point. It reaches a *normal form*; it never un-pushes,
un-fuses, or un-canonicalizes. Eqsat instead keeps all rewrites and extracts the
*cheapest reachable* form.

So the local-vs-global gap is non-empty only when **both**:
1. a directional rule moves *away* from the cost optimum, **and**
2. the cost model can **see** the difference.

MZ's algebra is largely confluent by design (the loop converges to one normal
form), so the gap concentrates where rewrites are *arrangement-visible* and
*non-confluent*.

## Case taxonomy

Examples examined (customer references anonymized intentionally — record the
pattern, not who hit it):

**Not eqsat problems** (don't let these muddy the thesis):
- *Partial-arrangement / MFP reuse*: the optimum (lift MFP for reuse keys, keep
  it inline for newly-created keys) is not **representable** in MIR. An
  IR-expressiveness gap; fix at LIR (per-key `mfp_before`), orthogonal to eqsat.
- *Redundant-join elimination needing FK/derivation facts*: MZ has no FK
  constraints, so neither legacy nor eqsat can prove the join redundant.
- *Semijoin reduction / magic-sets*: deriving a new predicate on one relation
  from another's scoping. MZ doesn't do it; eqsat doesn't add it.
- A merged console PR (#37323) bundled three of these (predicate-pushdown into a
  shared CTE + a non-FK redundant-join drop + a semijoin reduction) as a hand
  workaround. Eqsat would **not** have produced that plan.

**Scalar canonicalization**: `WHERE x*2 = 6` misses an index on `2*x`
(commutativity). Index keys and predicates must share **one** canonical scalar
form — the unification thesis (scalars first-class, one canonical form
everywhere).

**The flagship — variable-outer-join (VOJ) cross join** (next section).

## The flagship: VOJ cross join

```sql
CREATE TABLE t1(f0 int, f1 int);
CREATE TABLE t2(f2 int, f3 int);
CREATE TABLE t3(f4 int, f5 int);
-- second join on f2 = f4:
SELECT * FROM t1 LEFT JOIN t2 ON (f0 = f2) LEFT JOIN t3 ON (f2 = f4);
```

This delta-plans a **cross join**: the join key is
`#5 = case when (#4) IS NULL then null else #0 end`, and the t3-driven delta path
is `%2 » %0:t1[×] » %1[#0]K` — t3 must cross-product the full `t1`.

Root cause: a **shared** transform (`canonicalize_equivalences` /
`EquivalencePropagation`, used by both the legacy and eqsat paths) canonicalizes
the `{f0, f2}` equivalence class to the **foreign** representative `#0` (t1). The
semantically-identical **local** spelling `#2` (t2) — or writing the query as
`... ON (f3 = f4)` — lets the planner build a local arrangement and avoid the
cross.

### Ground truth (captured via `EXPLAIN ... WITH(join implementations)`)

- **eqsat ON ≡ eqsat OFF — byte-identical.** Eqsat does not help today: the `#0`
  spelling is baked in upstream, and SP4b deliberately excluded Join keys from
  colored resolution.
- **`enable_eager_delta_joins` OFF** → `type=differential`, single path
  `%0:t1[#0]K » %1[#0]K » %2[#0]K`, no cross. (Confirms the bug is
  delta-path-specific; eager delta exposes it because a delta join maintains a
  path per input.)
- **Spelling variants** confirm the trigger is purely the key spelling:
  `ON (f0 = f4)` collapses to `#0 = #2 = #5` (no cross); `ON (f3 = f4)` keeps a
  local spelling `…else #3 end` and the planner builds a local arrangement
  `%1[case when (#2) IS NULL then null else #1 end]K` (no cross).

## Three walls — all in the cost model, not the search

`CostModel` (`src/transform/src/eqsat/cost.rs`) compares
`Cost { arrangements, memory: Vec<f64>, time: Vec<f64>, nodes }` lexicographically
(`cmp_memory_first`: memory → time → nodes). The degrees are AGM (worst-case
output-size) bounds.

1. **Predicate-simp folds are arrangement-neutral.** They tie on `memory`/`time`
   and differ only on the `nodes` tiebreaker, which downstream normalization
   erases. (This is why the SP4d colored saturation + a nullability spike both
   came back inert — see `eqsat-local-vs-global` notes.)

2. **The scalar colored extractor can't discriminate the VOJ spelling**, and the
   binary join cost prefers the wrong one. `#0` and `#2` are both single column
   refs with identical `scalar_expr_cost`, so `resolve_scalar_colored`'s
   `(scalar_expr_cost, name_key)` tiebreak can't pick `#2`. Worse, the current
   `binary_join_terms` cost rates the `#0` (cross) spelling **cheaper** — more
   cross-input equality constraints → tighter AGM → looks better. The cost isn't
   even spelling-invariant (two semantically identical joins get different
   costs).

3. **AGM cannot tell a non-key equi-join from a cross product.** Both have
   worst-case output N² (degree 2). A cross product's real cost (stream the full
   other side per delta, via an empty-key arrangement) is constant-factor /
   arrangement-structural, invisible to a worst-case output-size bound.

The recurring lesson: the bottleneck is the cost model's **axes** (worst-case AGM
output size + logical arrangement count), not the rewrite search.

## The lever: a delta-aware join cost (probe-validated)

`binary_join_terms` costs a join as a **single best left-deep order**; it roots
away from the cross and never sees it. A **delta** join maintains **one path per
input**. A delta-aware cost = sum over per-driver-rooted left-deep paths, with:

- **(a) correct delta-connectivity** — a class can *key* a step extending
  `frontier` by input `j` only if **all** its inputs are within
  `frontier ∪ {j}` (a class also touching a not-yet-joined input can't be applied
  yet; plain hypergraph connectivity over-counts and misses this);
- **(b) a keyed-ness axis** — count of forced cross-product steps, compared
  **lexicographically before** the degree vector (NOT a magic penalty, NOT a
  tiebreaker);
- **(c) drop each path's final full-join term** (the streamed output, identical
  across spellings — mirrors what `collect_memory_into` already does with
  `terms.pop()`).

### Probe results (3 inputs `t1(2), t2b(3), t3b(3)`; `class1 = {#0,#2}`; `class2 = {#5, col(X) Eq col(4)}`, X=0 = cross, X=2 = local)

| cost variant | `#0` (cross) | `#2` (local) | `#2 < #0`? |
|---|---|---|---|
| current binary | `[2, 1]` | `[3, 2]` | **NO** (wrong) |
| delta + magic `+1.0` penalty | `[3, 1, 1]` | `[2, 2, 2]` | yes |
| delta + keyed-ness axis (no penalty) | `(crosses=1, [2,1,1])` | `(crosses=0, [2,2,2])` | **YES** |

**Crucial nuance:** on the degree vector *alone*, `#0=[2,1,1] < #2=[2,2,2]` — the
degrees point the **wrong way**. So keyed-ness must be **primary**
(lexicographically ahead of degree), not a tiebreaker. A single degree (even
degree+penalty) is fundamentally fragile; the cross/keyed distinction is a
separate, dominant axis.

### Design conclusion

- Cost becomes a **total** lexicographic order `(keyed-ness, memory, time,
  nodes)`. A genuine **partial/Pareto** order (size ↔ selectivity trade-off) is
  the conceptually honest structure but is **not needed** for this case and would
  break ILP extraction's scalar objective. The integer cross-count folds into the
  ILP objective as a large-weight term, so a total order stays ILP-compatible.
- Conceptual basis: keyed-ness (access selectivity) and AGM degree (worst-case
  output size) are **incommensurable** under worst-case analysis. Key length is a
  sound, monotone selectivity proxy; the discontinuity that matters is `K: 0 → 1`
  (cross → lookup; broadcast → index in differential-dataflow terms), not finer
  key-length refinements. Uniqueness is *already* folded into the degree (the
  `key_covered` / private-vertex AGM refinement: a key-covered input drops to
  degree 1 — see `key_covered_join_input_drops_private_vertex`), so the only
  missing piece is the `K=0` empty-key delta cross.

### Reproduction recipe

Add a unit test to `cost.rs`'s `mod tests` that builds the two joins above with
`get("t1",2), get("t2b",3), get("t3b",3)` and the two `equivalences` spellings,
then computes per-driver-rooted paths over `agm_degree_subset_memo`, counting
forced-cross steps (a step is keyed iff some equivalence class's input-mask is a
subset of `frontier ∪ {j}` and spans the boundary). Compare `(crosses, degrees)`
lexicographically.

## Scope for the real project (open questions a spec must resolve)

Goal framing (per project owner): **remove `JoinImplementation` from the eqsat
optimizer** and let cost-driven extraction subsume it. The delta-aware cost is
the enabling change.

1. **Corpus risk (the big one).** Keyed-ness as a *primary* axis overrides
   degree; it will move plans wherever the current degree-based choice was right.
   Requires the alongside-build-and-differential discipline (as in SP4b/SP4c)
   before flipping anything.
2. **Candidate generation.** The cost can now *rank* `#2` below `#0`, but
   extraction only picks `#2` if it is *present* in the e-graph. Lift SP4b's
   join-key exclusion via the colored extractor, **or** add a
   localize-equivalences rule, to mint the alternative spellings.
3. **Faithful cost wiring.** Real `delta_join_terms` in `cost()` for `Join` *and*
   `WcoJoin`; the cross also costs a **memory** term (empty-key arrangement), not
   just keyed-ness/time; AGM-with-FD interaction; per-input arrangement dedup.
4. **Removing `JoinImplementation`.** What does it still decide that the cost
   model must absorb (delta vs differential, arrangement commitment,
   `WcoJoin → DeltaQuery`)?

## Pointers

- Cost model: `src/transform/src/eqsat/cost.rs` (`cost`, `binary_join_terms`,
  `join_degree`, `join_key_cols_for_input`, `JoinGraph`, `Hypergraph`,
  `agm_degree_subset_memo`, the `JoinOrdererKind` orderers).
- Join-key resolution at extraction: `egraph/build.rs` `resolve_equivalences`
  (the non-colored path Join uses today) vs `resolve_scalars_in_color` (the
  colored path Filter/Map use; SP4b excluded Join keys).
- Prior eqsat design docs: `20260628_eqsat_colored_contextual_sp4b.md` (why Join
  keys were excluded from colored resolution), `20260629_eqsat_colored_*.md`.
- Personal/agent memory: `eqsat-local-vs-global-cost-model`,
  `eqsat-shared-core-extraction`.
