# Finding: the ILP extractor silently bails to greedy on a join-commutativity cycle

Status: open bug, filed. Discovered while certifying WS2 (scalar sharing), but
the impact is broader than WS2.

## Severity: a live default-path degradation, not a WS2-only issue

`enable_eqsat_ilp_extraction` is default ON, so the ILP extractor is the
production extractor today. When the reachable e-graph for a query contains a
directed cycle, `IlpExtractor::solve` hits its `reachable_has_cycle` guard
(extract.rs:182), returns `None`, and `IlpExtractor::extract` silently falls
back to the DAG-blind `GreedyExtractor` (extract.rs:148). No error, no warning,
no plan marker. So for every query whose saturated e-graph develops such a
cycle, the ILP's whole cost-based selection is silently skipped and the plan is
whatever greedy picks. This is happening on the default path now, corpus-wide,
wherever the cycle arises. WS2 did not create this. WS2 made it observable by
constructing a query that reliably triggers it.

The first investigation step is therefore NOT to fix WS2's acceptance. It is to
measure how widespread the bail already is: instrument the fallback at
extract.rs:148 (count guard trips, log the root query or plan shape) and run the
corpus. That count is the real severity signal.

## The mechanism: commutativity with a reordering Project self-references its own class

The concrete cycle observed on the WS2 acceptance query (a join of two
subqueries that both extend a shared `Map` prefix, so map-split unifies both
join inputs into one shared-prefix class):

```
class=18 pos=0 node=Project { input: 18, outputs: [0,1,2,3,4,5,6] } children=[18]
class=18 pos=1 node=Project { input: 19, outputs: [3,4,5,6,0,1,2] } children=[19]
class=18 pos=2 node=Join { inputs: [14, 4], equivalences: [[11, 8]] } children=[14, 4]
class=19 pos=0 node=Project { input: 18, outputs: [4,5,6,0,1,2,3] } children=[18]
class=19 pos=1 node=Project { input: 19, outputs: [0,1,2,3,4,5,6] } children=[19]
class=19 pos=2 node=Join { inputs: [4, 14], equivalences: [[8, 9]] } children=[4, 14]
```

Class 18 is a join `Join(14, 4)`, class 19 is its commuted counterpart
`Join(4, 14)`, reached by a column-reordering `Project`. Once binary join
commutativity unifies both orders into ONE class, a rule of the shape
`Join(A, B) => Project[reorder](Join(B, A))` produces a `Project` whose input
class is the same class the `Project` now lives in. That is a self-loop
(`Project{input: 18}` inside class 18), plus a mutual 18 to 19 edge. The
reachable subgraph is cyclic.

`reachable_has_cycle`'s own doc comment (extract.rs:90-92) states this should be
impossible: "Within a Let-free fragment the e-graph is acyclic by construction
... so a cycle here means an upstream invariant was violated." The invariant is
being violated by commutativity-with-a-reordering-Project once both join orders
share a class. This is inherent to modeling join commutativity plus column
reordering in an e-graph, so it is likely reachable well beyond the WS2 shape.

## Two candidate fix paths (decide during the investigation, not here)

1. Guard-side (probably the right first fix, smaller blast radius): the ILP
   need not bail the WHOLE solve on any cycle. A self-referential or otherwise
   cyclic candidate node cannot appear in an acyclic extracted plan anyway, so
   exclude such nodes from the ILP's node set (the `arr_set`/`node_vars`
   construction) and solve over the acyclic remainder, rather than returning
   `None`. This keeps the ILP running (and its cost model applied) for every
   query, cyclic or not, and only drops the un-selectable nodes.
2. Rule-side: stop the commutativity/reordering rule from emitting a `Project`
   whose input resolves to its own class (a trivial self-loop), or from firing
   when it would recreate an existing class's own commuted form. Higher risk: it
   touches saturation output for every join query and interacts with the
   join-order/delta cost model, so it needs careful no-regression work.

Both belong in the investigation. Neither is decided by WS2.

## Minimal repro

```sql
CREATE TABLE t(id int, data jsonb, k jsonb);

SELECT c1.obj, c2.obj2, c2.g
FROM (SELECT data->'obj' AS obj, id FROM t) c1
JOIN (SELECT id, data->'obj' AS obj2, data->'g' AS g FROM t) c2 ON c1.id = c2.id;
```

Under `enable_eqsat_scalar_sharing = true` (which forces the ILP and adds the
map-split rule so both join inputs share the `Map[data->'obj']` prefix class),
`IlpExtractor::solve` trips `reachable_has_cycle` and falls back to greedy. The
same underlying cycle can likely be reached without WS2 on other join shapes, so
a broader repro is part of the investigation.

## Consequence for WS2

WS2a-c (the Map-split rule, the width-aware cost memory, the ILP arity tier) are
built and reviewed and are byte-identical flag-off. But their share cannot be
realized end to end on the target shape, because that shape trips this cycle and
the ILP bails to greedy (where the scalar-aware and width tiers, being
`IlpExtractor`-only, do not apply). WS2 is therefore landed inert. Its Task 5
acceptance re-runs once this finding is fixed.
