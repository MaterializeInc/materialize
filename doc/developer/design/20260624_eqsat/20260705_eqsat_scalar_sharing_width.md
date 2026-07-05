# WS2: MFP-part sharing and the width cost axis

Status: design, pending implementation. Builds on WS0 + WS1 (landed,
`20260704_eqsat_sharing_aware_cost.md`). Scope: build WS2a (Map-split rule) +
WS2c (width cost axis) + WS2d (post-eqsat reconcile) now, design and defer WS2b
(factoring extractor) and Project-splitting.

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

The width term lives in TWO places, both gated, both flag-off verbatim. The
reason is not that arrangement count is "backwards". The reason is that the ILP
objective is a SEPARATE scalarized proxy (arrangement count, then time, then
nodes, the extract.rs objective) that NEVER consults `cost.rs`'s memory-degree
vector. A width term in `cost.rs` is therefore invisible to the ILP's decision,
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
riskier width change. The flag gates FOUR things: the Map-split rule, the
`cost.rs` `(degree, arity)` memory pair, the ILP objective arity term, and the
scalar-aware node tier (`weight_scalar_nodes = filter_sharing || scalar_sharing`,
because the Map-share pick is won by that tier, see the compute-once section). It
forces the ILP for its run (the greedy extractor cannot realize the share),
mirroring `filter_sharing`.

Byte-identical off, by construction. Both the `cost.rs` memory computation and
the ILP objective branch on the flag, and the flag-off arm is the verbatim
width-blind original (a branch, not an algebraic reduction), so flag-off is
byte-identical corpus-wide. This is stricter than WS1 because `cost.rs` is shared
with the greedy extractor.

## WS2a: the Map-split rule

The WS1 pattern applied to `Map { input, scalars: Vec<Id> }` (node.rs:36-40, the
same opaque-list situation as Filter predicates). A hand-written `CompiledRule`
(the DSL cannot destructure the scalar list, and `Tmpl::Builtin` is scalar-only),
mirroring `filter_split.rs`: peel one scalar into an upstream `Map`, single-scalar
peel, length cap, canonical-`find` ordering, record declines.

### Shared peel helper

`filter_split` and `map_split` both do single-element peel, length cap,
canonical-`g.find(p)` sort, and decline counting. Factor that into one Rust
helper, with two thin wrappers for the `Filter` and `Map` node types. One peel
implementation to keep aligned instead of two divergent copies. The Lean
theorems stay separate (filter-composition versus map-composition are different
statements).

### The correctness crux: map-split is not a trivial reflection

Filter predicates are order-independent (conjunction commutes), so filter-split
was a clean reflection of `merge_filters`. Map scalars are NOT order-independent:
`Map[f, g]` where `g` reads `f`'s appended output column cannot be reordered, and
peeling shifts column indices. Map-split is therefore sound only under a
condition, and both the rule and its Lean theorem must carry it.

* Independence guard. WS2a peels only an INPUT-ONLY scalar, one that reads only
  columns of the Map's input `r`, not any sibling appended scalar. This is the
  provably-sound subset and it covers the jsonb value case (`data->'obj'` reads
  only the input `data` column). A scalar that references a sibling is not
  peeled. Dependent-scalar hoisting is out of scope.
* Column-index rewrite. Hoisting scalar `s` at position `|r| + i` into an
  upstream `Map[s](r)` places `s` at position `|r|` in the shared prefix, so the
  outer `Map` (the remaining scalars) and a restoring `Project` must reindex so
  the overall output columns and arity are preserved. This index rewrite is the
  subtle part of WS2a and the plan must specify it precisely.
* Lean theorem. Hand-authored in a new `MirRewrite/MapSplit.lean`, imported into
  `MirRewrite.lean` (invisible to gen-lean, like `FilterSplit.lean`). The
  statement is map-composition WITH the independence precondition, not a bare
  reflection: it must state that `s` reads only `r`'s columns.
* Drift test. A test that asserts the rule's emitted shape matches the shape its
  Lean theorem states. This is a TRIPWIRE, not a correspondence proof: it pins
  the rule's output so a future change to the Rust rule breaks the test and
  forces re-review of the hand-authored theorem. It does not verify the theorem
  is semantically about the rule. Given map-split's precondition subtlety the
  tripwire matters more here than for filter-split.

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

* Acceptance positive: a CROSS-CONSUMER subset-prefix jsonb query in an
  ARITY-FREE two-consumer shape (a join of two subqueries, NOT `UNION`, which
  cannot express the exact prefix, and NOT intra-Map, which prod already CSEs).
  Consumer 1 is `Project(Map[s](r))` extracting `data->'obj'`, consumer 2 is
  `Project(Map[s, g](r))` extracting `data->'obj'` plus one more field, so the
  share is exact-prefix and arity-neutral. Under `enable_eqsat_scalar_sharing`
  the extraction is computed once in a shared `Map` `Let`, won on the count/time
  tie by the scalar-aware node tier. The plan MUST construct the concrete query,
  generate the EXPLAIN, and VERIFY the node/time tie of the actual plan (Projects
  included) before relying on it, treating this as an inspect-first step (a
  likely failure mode is UNION-arity padding or any construction that breaks the
  exact prefix into a diverging share, alongside post-eqsat re-fusion and
  pruning). EXPLAIN OPTIMIZED PLAN, flag on versus off. Consumer 2's predicate
  must NOT reference the extraction inline (that is cross-operator, WS2b).
* Acceptance anti-case (as important as the positive): a subset-prefix share
  whose shared column is carried into an arrangement it need not cross (a
  widening carry) is NOT taken, because the arity term declines it. This proves
  the width gate binds. A pure-widening share is declined by design.
* Corpus no-regression, flag off: full sqllogictest and the eqsat corpus
  byte-identical, which REQUIRES confirming the `cost.rs` width term and the ILP
  arity term are truly gated (flag-off runs the verbatim width-blind path).
  Stricter than WS1 because `cost.rs` is shared with greedy.
* Unit tests. The `cost.rs` `(degree, arity)` memory comparison (a wider
  arrangement costs more than a narrower one of equal cardinality degree, and a
  higher degree still dominates any arity). The ILP arity term (a widening carry
  is declined, an arity-neutral share is taken). The Map-split rule (single
  peel, cap, canonical order, independence guard rejects a dependent scalar,
  index-reference preservation, declines recorded). The drift test.
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
3. Flag: dedicated `enable_eqsat_scalar_sharing`, default off, gating FOUR pieces
   (the Map-split rule, the `cost.rs` `(degree, arity)` memory, the ILP arity
   term, and the scalar-aware node tier via `weight_scalar_nodes = filter_sharing
   || scalar_sharing`), forcing the ILP, flag-off verbatim width-blind. The
   scalar-aware node tier must be on because it is what wins the share pick.
4. WS0 left untouched, orthogonal (work tier versus memory tier), composes with
   the width term. WS0's real future subsumption is a `cost.rs` time-axis
   scalar-work term, unrelated to the width axis.
5. Map-split hand-written with a shared peel helper factored across
   `filter_split`, an input-only independence guard, a column-index rewrite, a
   precondition-carrying hand-authored Lean theorem, and a drift-detection
   tripwire test. The DSL destructuring primitive is deferred until a third split
   rule (Project-split) would justify it.
