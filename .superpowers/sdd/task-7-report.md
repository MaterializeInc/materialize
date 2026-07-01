# Task 7 report: SQL correctness differential and arrangement measurement

Status: DONE. Both parts implemented, tested, committed.

* Part 1 commit: `35856716f15e59a2d7bab13662fd24f572540eb9`
* Part 2 commit: `41a1a8f5e037efe8cd4f44d44be4c65e39499338`

## Part 1: SQL-level correctness differential

File: `test/sqllogictest/transform/eqsat_wmr_lift.slt`.
Run: `bin/sqllogictest --optimized -- test/sqllogictest/transform/eqsat_wmr_lift.slt` => PASS 7/7.

### Final WMR query

```sql
WITH MUTUALLY RECURSIVE
  r(x int) AS (
    SELECT src FROM edges
    UNION SELECT x FROM a
    UNION SELECT x FROM b
  ),
  a(x int) AS (
    SELECT e.dst FROM edges e JOIN r ON e.src = r.x
    EXCEPT ALL
    SELECT e.dst FROM edges e JOIN r ON e.src = r.x WHERE e.dst > 4
  ),
  b(x int) AS (
    SELECT e.dst FROM edges e JOIN r ON e.src = r.x
    INTERSECT ALL
    SELECT x FROM r
  )
SELECT * FROM r ORDER BY 1
```

Table `edges`: `(1,2),(2,3),(3,1),(3,4),(4,5)`.

The shared subterm `SELECT e.dst FROM edges e JOIN r ON e.src = r.x` is built in
BOTH the `a` and `b` binding values over the recursive binding `r`. The body is
non-monotonic (`EXCEPT ALL`, `INTERSECT ALL`). The `EXCEPT ALL ... WHERE e.dst > 4`
arm in `a` keeps node 5 out of the fixpoint, so the result is the proper subset
`{1,2,3,4}` rather than the full vertex set. The query is run with
`enable_eqsat_wmr_lift` OFF and then ON; both blocks assert the same expected
rows.

### Result rows (identical under flag OFF and flag ON)

```
1
2
3
4
```

### Evidence the hoist fires

I temporarily instrumented `hoist_shared_letrec_subterms` in
`src/transform/src/eqsat/cse.rs` with an `eprintln!` and ran the flag-ON query.
The probe (since removed; `cse.rs` is back to its committed state) reported, for
each flag-ON query block:

```
WMRLIFT_PROBE: hoist_shared_letrec_subterms shared_count=1 versioned=1 invariant=0
```

So the lift's cross-binding hoist fires on exactly one shared subterm, and that
subterm reads a versioned recursive leaf (`versioned=1`) -> it is hoisted into a
new recursive `LetRec` binding at a version-valid slot. With the flag OFF the
hoist function is never called (the probe printed nothing), confirming the
flag-OFF block is the unmodified baseline.

`EXPLAIN OPTIMIZED PLAN` (flag ON) shows the shared subterm as a single recursive
binding `cte l2`, referenced by both the `a` binding (`cte l3`) and the `b`
binding (`cte l4`):

```
With Mutually Recursive
  cte l0 = Distinct project=[#0] Union( ReadStorage edges , Get l3 , Get l4 )  -- r
  cte l1 = ArrangeBy keys=[[#0]] Filter (#0 IS NOT NULL) Get l0
  cte l2 = Project (#1) Join on=(#0 = #2) (ArrangeBy[[#0]] Filter edges, Get l1)  -- shared join over r
  cte l3 = Threshold Union( Get l2 , Negate(Join(... Get l1) where dst>4) )       -- a
  cte l4 = Union( Get l2 , Negate(Threshold Union( Get l2 , Negate(Get l0) )) )   -- b
Return Get l0
```

### Honest caveat on the EXPLAIN

The final raised plan is identical with the flag OFF, because the production
`RelationCSE` pass (which runs AFTER eqsat at `src/transform/src/lib.rs:907,988`)
shares the same structurally identical subterm. The pure-legacy pipeline
(`enable_eqsat_optimizer=false`) produces the same `cte` sharing too. So the
EXPLAIN cannot, by itself, distinguish the lift from RelationCSE in the final
plan. What the lift uniquely does is run the version-aware cross-binding hoist
INSIDE the eqsat search (proven by the probe: `versioned=1` only with the flag
on), and the correctness gate is that this version-aware path produces the SAME
results as the untagged baseline. That is what the differential proves. This
nuance is documented in the SLT file's header comment.

## Part 2: arrangement-count measurement

File: `src/transform/tests/eqsat_wmr_lift.rs` (extended).
Run: `bin/cargo-test -p mz-transform --test eqsat_wmr_lift -- --nocapture` => PASS
(all 6 tests, including the 4 pre-existing).

### Finding: the `ArrangementCount` metric is already reuse-aware

`CostModel::cost(&rel).arrangements` (the exact field the `ArrangementCount`
objective compares, `src/transform/src/eqsat/objective.rs`) deduplicates
structurally identical arrangements across the whole plan via the `seen: BTreeSet<ArrId>`
set in `collect_memory_into` (`src/transform/src/eqsat/cost.rs`). It therefore
ALREADY credits cross-binding sharing: two structurally identical joins in two
bindings count the same as one shared join. As a result the lift never raises
this count and, on these fixtures, leaves it unchanged. The `<=` contract holds
(with equality), but a STRICT reduction is impossible on the dedup-based metric.

### Measured numbers (`--nocapture`)

Reuse-aware count (`ArrangementCount` objective metric), with/without lift:

```
shared_letrec (Task 5):        reuse-aware arrangements with-lift=0 without=0
rel_invariant_letrec (Task 6): reuse-aware arrangements with-lift=0 without=0
join_sharing_letrec:           reuse-aware arrangements with-lift=2 without=2
```

(The Task 5/6 fixtures use only Filter/Map/Negate/Threshold/Union, which the cost
model does not charge arrangements for, so they score 0.)

Physical (non-deduplicated) arrangement count -- the number of arrangements the
dataflow builds per operator before CSE collapses shared operators -- on the
join-bearing fixture:

```
join_sharing_letrec: physical arrangements with-lift=2 without=4   <- STRICT reduction
```

Without the lift the arrangement-bearing join sits inlined in BOTH the `b1` and
`b2` binding values (2 joins x 2 inputs each = 4 input arrangements). With the
lift the join becomes one shared recursive binding (1 join x 2 inputs = 2). This
is the resource win the reuse-aware metric predicts but does not display.

### What the tests assert

* `wmr_lift_does_not_increase_arrangement_count`: over all three fixtures
  (Task 5 lowered, Task 6, the join fixture), the reuse-aware
  `ArrangementCount` metric satisfies `with_lift <= without`.
* `wmr_lift_reduces_physical_arrangement_count`: on `join_sharing_letrec` the
  physical arrangement count strictly drops (4 -> 2), and the reuse-aware count
  stays equal (2 == 2), documenting that the dedup-based metric already credits
  the sharing.

Both reuse the crate's existing lowering (`lower::lower_with`) and CSE
(`eliminate_common_subexpressions`) path and the `CostModel`/`Cost::arrangements`
the objective consumes. No new scoring path was invented.

## Concerns

* The brief assumed the `ArrangementCount` objective metric would show a strict
  reduction under the lift. It does not, because that metric is reuse-aware
  (dedups across the plan), so it already credits sharing. I measured it anyway
  (it satisfies `<=` with equality) and added a physical-count measurement to
  exhibit the strict reduction the lift actually delivers. This is a deviation
  from the brief's literal wording but, I judge, a faithful realization of its
  intent (show the sharing win); flagging for review.
* The lift's effect is invisible in the final SQL EXPLAIN because post-eqsat
  RelationCSE reproduces the same sharing. The correctness gate therefore rests
  on (a) the probe-confirmed fact that the version-aware hoist fires only with
  the flag on, and (b) identical results. If a reviewer wants the EXPLAIN itself
  to differ, that is not reachable through SQL given RelationCSE, and would
  require disabling post-eqsat RelationCSE (no such flag exists).
