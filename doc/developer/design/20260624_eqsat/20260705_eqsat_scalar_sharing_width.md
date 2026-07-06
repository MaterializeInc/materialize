# WS2: MFP-part sharing and the width cost axis

Status: WS2a-c BUILT, reviewed, and CERTIFIED. The acceptance is
`test/sqllogictest/eqsat_scalar_sharing.slt`. Builds on WS0 + WS1 (landed,
`20260704_eqsat_sharing_aware_cost.md`). Scope: WS2a (Map-split rule,
peel-first) + WS2c (width cost axis) + WS2d (post-eqsat reconcile) built; WS2b
(factoring extractor) and Project-splitting designed and deferred.

The Map-split rule, the width-aware `(degree, arity)` cost memory, and the ILP
arity tier are implemented, each two-stage reviewed, and byte-identical
flag-off. The acceptance blocker that held WS2 inert has been removed: the
join-shaped acceptance triggered a join-commutativity cycle that tripped the ILP
cycle-guard, silently falling back to the DAG-blind greedy extractor where the
scalar-aware and width tiers do not apply. The cycle-aware ILP extraction
(`20260705_eqsat_ilp_cycle_aware_extraction.md`, landed) makes the ILP run on
cyclic join SCCs, so the extractor now reaches the shared form. Under
`enable_eqsat_scalar_sharing` the acceptance query computes the shared jsonb
extraction once in a `Map` `Let` consumed by both join inputs (verified in the
acceptance slt, flag on versus off).

The acceptance shape is a join of two subqueries, not `UNION`. WS2 needs two
consumers that share a `Map` prefix and may DIFFER in arity. `UNION` cannot
express that (equal-arity, padding breaks the prefix into a diverging share), so
the acceptance uses the arity-free join-of-two-subqueries shape. That shape was
what tripped the commutativity cycle, which the cycle-aware ILP resolved.

## Motivation

An MFP (`MapFilterProject`) is the fused linear operator MIR canonicalizes
toward. The eqsat IR has no fused MFP node: only `Get/Project/Map/Filter/Reduce`
primitives (node.rs:17-54), raised into canonical Map-then-Filter-then-Project
(raise.rs:18). So inside the e-graph an MFP is already decomposed into shareable
sub-nodes. WS1 shared a `Filter`. WS2 shares a `Map`.

The value case is jsonb multi-extract. `data->'obj'` navigates and deserializes
a jsonb blob, real per-row CPU. Extracting the same path in more than one
consumer recomputes the parse. Materialize already does intra-Map scalar CSE
(the #2409 spike confirmed two `abs(f1)` in one Map compile to one computation),
so a path extracted twice inside one Map is already shared. The gap WS2a fills is
CROSS-CONSUMER `Map`-prefix sharing that whole-stage CSE does not reach: the same
extraction in two consumers, one consumer's `Map` a prefix the other extends.

The two consumers must be allowed to DIFFER in arity, so `UNION ALL` does not
work: `UNION` requires equal arity, and padding the shorter branch to match adds
a `Map` element, which breaks the exact-prefix shape and makes it a diverging
share (see the scope section). Maps add columns, so unlike WS1's filters they
cannot express an exact subset-prefix under `UNION`. An arity-free two-consumer
shape does work, for example a join of two subqueries. The acceptance query uses
that shape, not `UNION`, and not a shared extraction inside one Map.

Cross-OPERATOR sharing (the extraction in a `Filter` predicate and a downstream
`Map`) is NOT reachable by Map-split: an inline predicate can only be shared by a
predicate-hoisting rewrite `Filter[p(e)](r) => Filter[p(#c)](Map[e](r))`, which is
the factoring-extractor work in WS2b, not a `Map` peel. WS2a is cross-branch
only. Cross-operator moves to WS2b.

## The one thing that makes WS2 harder than "WS1 for Map"

Filter-sharing is memory-free: a shared `Filter` adds no column and streams, so
WS1's cost lever lived in the ILP objective and `cost.rs` stayed untouched. A
shared `Map` ADDS a materialized column that rides downstream and costs
arranged-row WIDTH wherever it crosses a `Join` or `Reduce`. The width cost is
the defining WS2 work, and it breaks the `cost.rs`-untouched invariant WS0 + WS1
held.

### Where the compute-once credit lives, and what WS2 actually adds

The sharing WIN in WS2 does not come from a new time credit. It comes from the
scalar-aware node tier WS0 already built, and the new width term is purely a
DECLINE gate. Getting this right is load-bearing.

`collect_work` (cost.rs::collect_work, the `Let` arm) charges a `Let`'s shared
value once. That is the GREEDY path (`cost.rs`), used as the ILP's
solver-failure fallback. But the operative extractor is the ILP, and the ILP
objective's time tier is `node_work_degree` per selected node (extract.rs:672),
which for a `Map` is `child(input)`, the input's output degree, SCALAR-BLIND.
`Map[s](r)` and `Map[s, g](r)` both cost `deg(r)`. So the ILP time tier does NOT
see computing `s` once versus twice.

Work the subset-prefix acceptance shape (branch 1 needs `s`, branch 2 needs
`s` and `g`):

* Fused: branch 1 `Map[s](r)`, branch 2 `Map[s, g](r)`. Two `Map` nodes, time
  `2 * deg(r)`.
* Shared: `l0 = Map[s](r)`, branch 1 `Get l0`, branch 2 `Map[g'](l0)`. Two `Map`
  nodes, time `2 * deg(r)`.

Arrangement count ties, time ties, the flat node count ties (two `Map` nodes
each). The ONLY tier that discriminates is the scalar-aware node tier: fused
scalar mass `= |s| + (|s| + |g|) = 2|s| + |g|`, shared `= |s| + |g|`, so shared
wins by `|s|` (the recomputed scalar's expr node count). That tier is exactly
WS0's `weight_scalar_nodes` term, gated today behind `enable_eqsat_filter_sharing`
(eqsat.rs:251), a DIFFERENT flag.

Consequence, and a required dependency: WS2's Map-share pick depends on the
scalar-aware node tier being on. So `enable_eqsat_scalar_sharing` MUST also
enable `weight_scalar_nodes` (`weight_scalar_nodes = filter_sharing ||
scalar_sharing`). The compute-once credit is not new machinery, it is WS0's tier,
and WS2 must turn it on. The NEW machinery WS2 adds is only the width term, whose
sole job is to DECLINE a widening share, never to create the win.

### Width is a hard memory gate, not a cross-axis tradeoff

The cost order is memory-primary over time (cost.rs::cmp_memory_first). Nesting
width into the memory axis makes the width charge a HARD GATE: any arity
increase on an arrangement is weighed before the compute-once time saving. So
WS2 shares if and only if the share is arity-neutral, and declines a
widening share even when the CPU saving is large.

This is correct for incremental view maintenance. An arrangement's width is
persistent maintained memory. The compute-once saving is transient per-update
CPU. Materialize's cost model is deliberately memory-first. Growing a maintained
arrangement forever to save recompute is usually the wrong trade in a
long-running dataflow. A true CPU-versus-memory tradeoff (a big parse saving
justifying memory growth) would need comparable, scalarized axes, a much larger
cost-model change that is out of WS2 scope. WS2 does not fake it with epsilons.

Sharing never narrows. Against the per-consumer-optimal plan, the non-shared
form already projects away any unneeded wide column per consumer, so a shared
prefix can only be arity-NEUTRAL (both consumers need the shared columns anyway)
or WIDER (a materialized column carried into an arrangement it need not cross).
There is no narrowing-win case. So the width term is purely a DECLINE gate against
over-sharing, never the win mechanism. The win is the scalar-aware node tier (a
memory-neutral share resolves the count/time tie toward sharing by fewer scalar
evaluations). The width term binds only to reject a widening share.

Consequence for acceptance (see Validation): the positive case is an
arity-NEUTRAL subset-prefix share, won on the count/time tie by the scalar-aware
node tier. The anti-case is a widening carry (a cheap scalar carried into a large
arrangement it need not cross), DECLINED by the width term. Declining a widening
share is by design, not a bug.

### Scope: subset-prefix shares only

WS2a realizes SUBSET-PREFIX shares: one consumer's `Map` is a prefix the other
extends. When both consumers diverge (each adds its own scalar, `Map[s, f]` and
`Map[s, g]`), the shared form is three `Map` nodes (`Map[s]`, `Map[f']`,
`Map[g']`) versus the fused two (`Map[s, f]`, `Map[s, g]`), so it loses on the
time tier (`3 * deg` versus `2 * deg`), which sits above the node tier, and
nothing below rescues it. So sharing only wins when one branch is exactly the
shared prefix. This is the same at-least-two-consumers-subset lesson as WS1, and
the acceptance query must have that shape.

## Width is measured in columns (arity), not bytes

Only `Rel::Constant` carries `col_types` and it is usually `None`, so byte-level
row width is not available at cost time. `Rel::arity()` (ir.rs:343) is cheap on
every node. So the width unit is column count (arity). A shared `Map` that adds
K columns widens by K the arranged rows it crosses.

## WS2c: the width cost axis, in two coordinated places

The width term lives in TWO places. The reason is not that arrangement count is
"backwards". The reason is that the ILP objective is a SEPARATE scalarized proxy
(arrangement count, then time, then nodes, the extract.rs objective) that NEVER
consults `cost.rs`'s memory-degree vector. NOTE: the two places are now gated
differently. The ILP-objective place (the arity term) is promoted and always on
(a cost-model correctness property, see Flag), because the ILP is the extractor
that makes the width decision and it now runs on joins. The `cost.rs` place stays
flag-gated (it feeds greedy and recommendation, no regression there). A width term in `cost.rs` is therefore invisible to the ILP's decision,
and the ILP is the extractor that makes the share decision (greedy is DAG-blind
and does not pick shared forms, the same reason WS1 forced the ILP). So width
must be injected into the ILP objective to be operative. This is the WS0 lesson
restated: the operative term rides in the ILP objective.

* (a) `cost.rs` width-aware memory. Model coherence, and the greedy path used as
  the ILP's solver-failure fallback. Each arranged collection's memory entry
  becomes an ordered `(degree, arity)` pair, compared lexicographically with the
  cardinality `size_degree` primary and arity a strict secondary. A wider
  arrangement of the same cardinality costs more, but width never outweighs a
  real cardinality-degree difference (a wide small arrangement never beats a
  narrow huge one). Prefer the pair representation over `degree + arity * eps`,
  which is float-fragile.
* (b) ILP objective arity term. Operative. The ILP weights each selected
  arrangement by the arity of the arranged collection, ranked BELOW the
  arrangement-count primary and ABOVE the time tier (memory-like, parallel to
  `cost.rs`'s memory-over-time order). Then a widening carry (arity up on an
  arrangement that exists either way) is weighed before the compute-once time
  saving, and is declined.

### Where the width gate actually binds: the count-tie carry

The gate binds on a count-TIE, not a count-reduction. A materialized column
carried into an arrangement that exists either way (a `Join` or `Reduce` present
in both the shared and non-shared forms) leaves the arrangement COUNT unchanged
and changes only the arrangement's WIDTH: sharing pushes the column upstream of
the arrangement (wide), the non-shared form computes it downstream, post
arrangement, streaming (narrow). The ILP ties on its count primary, falls
through to the time tier which favors compute-once, and without a width term
over-shares the widening carry. The arity term at (b) is what makes that tie
resolve against the widening share.

Two things the arity term must NOT do:

1. Do not fold arity into the count tier (make a wide arrangement "count as more
   than one"). That is the dimensionally-unsound multiplicative move, and worse,
   it would penalize a genuine arrangement dedup: a deduped wide arrangement
   would look like more than one and a real memory win would be wrongly
   declined.
2. Do not try to decline count-reducing shares. Collapsing two identical
   `(collection, key)` arrangements to one is a dedup win, already credited by
   `cost.rs`'s `ArrId` dedup and the ILP's distinct-`(class, key)` count, for
   free, pre-WS2. Leave those to the count primary.

### Verification: no count-reduction is memory-worse (so the below-count ranking is sound)

`ArrId` is `Node(Rel)` or `JoinInput { input: Rel, key }` (cost.rs:80-83); the
ILP's `arr_set` keys on `(class, key)` (extract.rs:202-208). Both identities
include the full collection. Dedup therefore collapses only IDENTICAL
collections, which have identical arity, hence identical width. So a
count-reduction is a same-width collapse of N arrangements into one, strictly
less memory, never memory-worse. Conversely any width difference means the
collections differ, so different `ArrId` and different `(class, key)`, so no
dedup, so the count is a tie. Width differences live only in count-ties, and no
count-reduction is memory-worse. The below-count ranking binds exactly where it
should, and the dimensional question stays closed. This is verified from the
`ArrId` and `(class, key)` structure, not assumed. If implementation surfaces a
real count-reducing share that is genuinely memory-worse, stop: that reopens the
dimensional question and the below-count ranking would not bind.

### Known limitation (documented, not buried)

Injecting arity gives the ILP a width signal but the ILP still proxies
cardinality by arrangement COUNT and ignores cardinality DEGREE (a huge
arrangement counts the same as a tiny one, a pre-existing coarseness of the ILP
objective distinct from the greedy path's degree-aware `cost.rs` memory). So
after WS2 the ILP models arrangement width but not arrangement cardinality
degree. This is the minimal correct change for the width gate. The
count-not-degree coarseness is a separate, pre-existing ILP asymmetry, recorded
here, not fixed by WS2.

## WS0 is orthogonal to the width term, not its general form

WS0's tie-break is scalar-node count in the ILP objective, a work discriminator
on the nodes tier. It breaks a MEMORY-FREE filter tie (fused `Filter[a,b]`
versus split `Filter[b](Filter[a])`) by preferring fewer scalar evaluations. It
has no memory content. WS2's width term is arity on arranged collections, a
MEMORY discriminator. Different quantities, different tiers. WS2's width term is
NOT the general form of WS0 and cannot do WS0's job (a memory-free filter tie has
no arity to charge).

They compose with no conflict under the memory-over-time-over-nodes order. On a
filter-only query the width term is inert (no arrangement to widen) and WS0
operates alone, so filter-split still works. On a Map-materialization query the
memory/width tier decides share-versus-carry first, and WS0's node tier only
breaks residual ties below it. Orthogonal tiers, they stack.

WS0's actual general form is a `cost.rs` TIME-axis scalar-work term
(predicate/scalar count times degree), a separate potential `cost.rs` change
that would unify WS0's ILP-objective node tie-break with the cost model. That is
a low-priority future cleanup, unrelated to the width term. WS0 is left
untouched by this spec.

## Flag

A dedicated `enable_eqsat_scalar_sharing` (default off), distinct from WS1's
`enable_eqsat_filter_sharing`. This decouples the higher-blast `cost.rs` width
axis (shared by greedy and the ILP fallback) from WS1's memory-free
extract.rs-only path, so WS1 can graduate to default-on independently of the
riskier width change. The flag gates THREE things: the Map-split rule, the
`cost.rs` `(degree, arity)` memory pair, and the scalar-aware node tier
(`weight_scalar_nodes = filter_sharing || scalar_sharing`, because the Map-share
pick is won by that tier, see the compute-once section). It forces the ILP for
its run (the greedy extractor cannot realize the share), mirroring
`filter_sharing`.

The ILP objective arity term is NO LONGER gated by this flag. It was built here
but it is not a sharing feature, it is a cost-model correctness property: among
arrangements that tie on count, width is a cost, so the ILP must prefer the
narrower one. Its flag-off-byte-identical rationale held only while the ILP never
ran on joins. The cycle-aware ILP now runs on joins, so a width-blind objective
became a live plan-quality regression against greedy (measured: 7 of 70 net-wider
golden files, lost projection pushdowns). The tier is therefore promoted to the
production objective, always on for every ILP solve (`extract.rs`,
`width_aware: true` set at the `use_ilp` extractor construction in `eqsat.rs`).
See `20260705_eqsat_extraction_determinism.md`. The remaining two gated pieces
(`cost.rs` `(degree, arity)`, the scalar-node tier) stay byte-identical off by
construction: their flag-off arm is the verbatim width-blind original. They feed
the greedy and recommendation paths, where no regression was demonstrated, so
dragging them along would churn greedy plans to fix nothing.

## WS2a: the Map-split rule

The WS1 pattern applied to `Map { input, scalars: Vec<Id> }` (node.rs:36-40, the
same opaque-list situation as Filter predicates). A hand-written `CompiledRule`
(the DSL cannot destructure the scalar list, and `Tmpl::Builtin` is scalar-only),
mirroring `filter_split.rs`. The mechanism is peel-FIRST, the reverse of
`fuse_maps`: peel ONLY the first scalar (index 0) into an upstream `Map`,
`Map[s0, rest](r)` becomes `Map[rest](Map[s0](r))`. Length cap `K`, record
declines. Iterated peel-first plus `fuse_maps` reaches every common POSITIONAL,
in-order prefix, so restricting to the first scalar loses no positional-prefix
share.

### Peel-first needs no guard and no index rewrite

The forward rule `fuse_maps` is `Map[s2](Map[s1](r)) = Map[s1 ++ s2](r)`, so its
reverse at the first scalar is unconditional:

* No independence guard. The first appended scalar reads only input columns by
  construction (nothing precedes it to reference), so it is always the
  provably-sound input-only case. This covers the jsonb value case (`data->'obj'`
  reads only the input `data` column) whenever that scalar is the positional
  first.
* No column-index rewrite. A position-1 prefix split places `s0` at exactly the
  column index it already occupies in the fused Map. `Map[rest](Map[s0](r))`
  preserves the exact output column layout and arity, so no restoring `Project`
  and no reindex are needed. `rest` keeps its ORIGINAL order, since Map appends
  columns positionally and reordering would change the layout. This is the
  opposite of filter-split, which sorts to canonicalize an order-insensitive
  predicate set.

### LIMITATION: order-sensitivity

A share is MISSED when the shared scalar is not a positional prefix of the
extending Map. `Map[g, s](r)` alongside a sibling `Map[s](r)` does not share,
because `s` is not first and Map scalars are order-sensitive (unlike filter
predicates there is no reordering to canonicalize this away). The follow-up for
that reach is an any-input-only-peel with a column-index rewrite (a corrective
`Project` for a middle peel), deferred if that reach matters. Peel-first is the
unconditional, layout-preserving core that needs neither.

### Shared peel helper: not factored

`filter_split` and `map_split` share only a thin skeleton (the `nodes_by_sym`
loop, the `K` cap, the `n < 2` skip, the `DECLINED` counter). Their cores
DIVERGE: filter emits one match per predicate index and sorts `rest` by
`g.find`, map emits one match and peels index 0 preserving order. Factoring the
skeleton behind closures for the differing parts contorts both rules for a
handful of shared lines, so the two are kept separate. The Lean theorems stay
separate regardless (filter-composition versus map-composition are different
statements).

### Lean: a sorry symmetric to fuse_maps

Hand-authored in a new `MirRewrite/MapSplit.lean`, imported into `MirRewrite.lean`
(invisible to gen-lean, like `FilterSplit.lean`). The statement is
`mapB (catRows s1 s2) r = mapB s2 (mapB s1 r)`, the reverse orientation of
`rule_fuse_maps`. Map acts on row/column structure, which the bag model
(`Row -> Int` multiplicity) does not represent, so this is a SORRY, the same
established Map-rule modeling boundary that `fuse_maps`, `fuse_projects`, and
`push_filter_through_map` sit behind. It is NOT a discharged precondition-carrying
theorem: peel-first is unconditional, so there is no independence precondition to
state, and no precondition to discharge. Consistent with `rule_fuse_maps`, the
sorry carries no `-- PERMANENT SORRY` marker, so the permanent-sorry guard count
is unchanged.

* Drift test. A test that asserts the rule's emitted shape (`apply_map_split` on
  `Map[s0, s1]` produces `Map[[s1]](Map[[s0]](input))`) matches the shape the Lean
  theorem states. This is a TRIPWIRE, not a correspondence proof: it pins the
  rule's output so a future change to the Rust rule breaks the test and forces
  re-review of the hand-authored theorem. It does not verify the theorem is
  semantically about the rule.

## WS2d: reconcile with the post-eqsat pipeline

`CanonicalizeMfp` (canonicalize_mfp.rs) and MFP fusion run after eqsat and would
re-fuse a split `Map` back into one MFP. `Demand` (demand.rs) and
`ProjectionPushdown` (movement/projection_pushdown.rs) prune the materialized
column. The shared `Map` is a `Rel::Let` with two consumers, so by the same
survival argument as WS1's shared `Filter` `Let` it should survive fusion
(cannot fuse into either consumer without duplicating) and survive pruning (it is
demanded by both). VERIFY this end to end on the acceptance query. If a
post-eqsat pass undoes the share, that is the likely failure mode, and the fix is
ordering or a share-preserving guard, not the eqsat rule.

## WS2b: the factoring extractor (deferred, design-only)

Fine-grained scalar hoisting, where consumers have otherwise-incompatible MFPs
and share only the scalar `f(x)`, needs a factoring extractor that hoists just
the scalar into an upstream `Map` and rewrites downstream column references,
because scalars are inlined on output today (scalar_extract.rs:110-135 clones the
`MirScalarExpr` per parent, no scalar `Let`). This is deeper and separable. It is
sketched here as the frame WS2 fits into and its implementation is deferred to a
later slice. Do not build the factoring extractor in this spec.

Project-splitting is likewise deferred (design-note only), for scope, not on a
settled value or difficulty judgment. Revisit after the Map-prefix slice lands.

## Validation

* Acceptance positive (CERTIFIED, `test/sqllogictest/eqsat_scalar_sharing.slt`):
  a CROSS-CONSUMER subset-prefix jsonb query in an ARITY-FREE two-consumer shape
  (a join of two subqueries, NOT `UNION`, which cannot express the exact prefix,
  and NOT intra-Map, which prod already CSEs). Consumer 1 is `Project(Map[s](r))`
  extracting `data->'obj'`, consumer 2 is `Project(Map[s, g](r))` extracting
  `data->'obj'` plus `data->'foo'`, so the share is exact-prefix and
  arity-neutral. Flag off, `data->'obj'` is extracted in both join inputs. Under
  `enable_eqsat_scalar_sharing` it is computed once in a shared `Map` `Let`
  (`l0`) consumed by both inputs, won on the count/time tie by the scalar-aware
  node tier. Verified by inspecting the actual EXPLAIN plans flag on versus off,
  Projects included.
* Acceptance anti-case, in two parts. The width-decline gate (a shared column
  carried into an arrangement it need not cross is NOT taken, because the arity
  term declines the widening carry) is certified in isolation by the unit test
  `width_aware_arity_tier_prefers_narrower_arrangement` in `eqsat/extract.rs`:
  two arrangement forms tying on every tier above arity, the width-aware ILP
  picks the narrow one. The arity term is always on (promoted, see Flag), so
  this gate is live corpus-wide, independent of the flag. The acceptance slt
  adds a plan-level negative: diverging consumers (`data->'f1'` versus
  `data->'f2'`) share no `l0`, because a non-prefix common scalar loses on the
  time tier (three Map nodes versus two). Together they show the extractor
  shares only an arity-neutral subset prefix and declines both a widening carry
  and a diverging share.
* Corpus no-regression, flag off: full sqllogictest and the eqsat corpus
  byte-identical for the flag-gated pieces, which REQUIRES confirming the
  `cost.rs` width term is truly gated (flag-off runs the verbatim width-blind
  path). Stricter than WS1 because `cost.rs` is shared with greedy. The ILP arity
  term is NOT part of this byte-identical-off expectation: it is promoted and
  always on, so its plan changes are the reviewed
  `20260705_eqsat_extraction_determinism.md` regen, not a flag toggle.
* Unit tests. The `cost.rs` `(degree, arity)` memory comparison (a wider
  arrangement costs more than a narrower one of equal cardinality degree, and a
  higher degree still dominates any arity). The ILP arity term (a widening carry
  is declined, an arity-neutral share is taken). The Map-split rule (peel-first
  exposes the shared prefix, `rest` keeps original order, cap declines a wide
  Map and records it). The drift test pinning `Map[[s1]](Map[[s0]](input))`.
* Use the `mz-test` skill for canonical commands (`bin/sqllogictest --optimized`).

## Scope boundaries

* IN: WS2a Map-split rule, WS2c the two-place width axis (gated), WS2d post-eqsat
  reconcile, the shared-Map-prefix case.
* OUT this spec: the factoring extractor for fine-grained scalar hoisting (WS2b,
  design-only), Project-splitting (design-note only), a CPU-versus-memory
  scalarized tradeoff (a larger separate cost-model change), the ILP
  count-not-degree coarseness (pre-existing, documented), arrangement sharing
  (already handled by `ArrId` dedup), HIR rewrites, sub-join sharing, cardinality
  estimation.

## Known width residuals, accepted with fix vectors named

After the arity-tier promotion (always on for the ILP), a corpus regen against
merge-base `4637c747` narrowed or held five of the seven previously net-wider
files (tpch_select, tpch_create_materialized_view, tpch_create_index each -6;
aoc_1212 -4; mir_arity 0). Three residual widenings remain. Each is OUTSIDE the
arity tier's reach, and each has a fix vector already on the schedule, so all
three are accepted and documented here rather than chased now. Chasing any would
open new work to shave bounded arity off a few files while the pipeline holds.

1. **chbench +66 (greedy-gated join).** The 7-way TPC-CH join has more than
   `MAX_CYCLIC_SCCS` (6) non-trivial SCCs, so extraction sheds it to the greedy
   fallback, which is width-blind. The merge-base plan was also greedy here (the
   cycle-guard bailed before the size gate existed), so the delta is
   determinism-canonicalization drift in a GREEDY plan, not a width regression:
   the arity tier never runs on this query. Fix vector: the HiGHS solver swap
   (scheduled after the cycle-aware ILP Tasks 3 and 4). A real solver makes the
   7-way affordable, `MAX_CYCLIC_SCCS` rises or is retired, and the join returns
   to width-aware ILP where the tier narrows it. Gate-shed, recovers with the
   solver swap. NOTE: accept-and-document does not waive the execution audit. The
   greedy drift still needs a worse-to-execute check on the final regen.

2. **aggregation_nullability +10 (CSE-representative width). RESOLVED, moot.**
   The residual was a determinism-canonical extraction sharing a wider `l0`
   (arity 2, unprojected) where the merge-base shared the narrow projected form
   (arity 1). The WS2 certification free experiment (Task 4) settled it. The
   current committed plan already binds the narrow `l0` (arity 1) at flag off,
   and flipping `enable_eqsat_scalar_sharing` on produces a byte-identical file,
   so the gated `cost.rs` `(degree, arity)` pair is inert here. The wide-`l0`
   form no longer reproduces: it was an artifact of the pre-HiGHS regen, undone
   by the HiGHS solver swap and the determinism sweep upstream, not by the width
   pair. No residual widening remains on this file and no new work is owed.

3. **all_parts_essential (saturation coverage gap).** The projected
   broadcast-customer form is never an e-graph candidate, so no extractor choice
   (greedy or ILP, width-blind or width-aware) can select it. Filed as a
   saturation coverage gap, not a cost or extraction problem. No fix vector on the
   near schedule: closing it means adding the missing rewrite to saturation.

The `MAX_CYCLIC_SCCS = 6` size gate is confirmed doing its job: chbench's
`PhysicalEqSatTransform` fell from 54s (ungated cyclic ILP) to 1.775s (greedy
shed). That is the deterministic-tail-shed working as designed, pending the
HiGHS-era recalibration named in residual 1.

## Decisions recorded

1. Scope: WS2a cross-BRANCH subset-prefix Map sharing only. Diverging consumers
   lose on the time tier, so one branch must be exactly the shared prefix.
   Cross-operator sharing is WS2b. Project-splitting deferred (design-note), not
   on difficulty, revisit after this slice.
2. Width unit: arity (columns), byte-width unavailable at cost time. Composition:
   `(degree, arity)` lexicographic pair in `cost.rs` memory (degree primary,
   arity secondary), and an arity term in the ILP objective ranked below the
   arrangement-count primary and above the time tier. A HARD memory gate, not a
   cross-axis tradeoff. The arity term is purely a DECLINE gate against a
   widening share. The sharing win comes from the scalar-aware node tier, not the
   width term.
3. Flag: dedicated `enable_eqsat_scalar_sharing`, default off, gating THREE pieces
   (the Map-split rule, the `cost.rs` `(degree, arity)` memory, and the
   scalar-aware node tier via `weight_scalar_nodes = filter_sharing ||
   scalar_sharing`), forcing the ILP, flag-off verbatim width-blind for those
   three. The scalar-aware node tier must be on because it is what wins the share
   pick. The ILP objective arity term is NOT gated: it is a cost-model
   correctness property (width is a cost), promoted to the production objective
   and always on for the ILP, because the cycle-aware ILP now runs on joins where
   width-blindness is a live regression. Certification toggles the rule and the
   `cost.rs` pair only, the tier is already on.
4. WS0 left untouched, orthogonal (work tier versus memory tier), composes with
   the width term. WS0's real future subsumption is a `cost.rs` time-axis
   scalar-work term, unrelated to the width axis.
5. Map-split hand-written as peel-FIRST (reverse of `fuse_maps`), NOT a general
   single-input-only-scalar peel with an index rewrite. Peeling only the first
   scalar needs no independence guard (the first scalar is always input-only) and
   no column-index rewrite (a position-1 prefix split preserves the layout), and
   iterated peel-first plus `fuse_maps` reaches all positional in-order common
   prefixes. The order-sensitivity LIMITATION is accepted: a share is missed when
   the shared scalar is not a positional prefix (`Map[g, s]` versus sibling
   `Map[s]` does not share), and the any-input-only-peel with index rewrite is the
   follow-up if that reach matters. The Lean theorem is a SORRY symmetric to
   `rule_fuse_maps` (Map acts on row/column structure, not bag-modeled), paired
   with a drift-detection tripwire test. The peel skeleton is NOT factored with
   `filter_split`: their cores diverge (filter sorts `rest`, map preserves order),
   so the handful of shared lines does not justify a contorting shared helper. The
   DSL destructuring primitive is deferred until a third split rule
   (Project-split) would justify it.
