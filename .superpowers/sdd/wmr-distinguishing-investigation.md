# Does `enable_eqsat_wmr_lift` ever produce a strictly better plan than legacy?

Worktree `/home/moritz/dev/repos/materialize/.claude/worktrees/mir-equality-optimizer`,
branch `claude/mir-equality-optimizer-sodbej`, revision `41a1a8f5e0`.
Read + empirical only, no tracked-source edits.

## Conclusion: B (conclusive negative)

No SQL-reachable WMR query distinguishes `enable_eqsat_wmr_lift = false` from
`= true` (eqsat otherwise on).
The mechanism is that the production `RelationCSE` pass, which runs *after* the
eqsat pass in both flag settings, already hoists a recursive-`Get`-containing
subterm shared across sibling `LetRec` bindings into a single shared binding, and
it does so version-consistently.
The lift is therefore redundant on every query tried, including ones constructed
specifically to defeat it.
No soundness concern was found in production `RelationCSE`'s WMR handling. The
version-blind worry in case (b) does not materialize because production ANF is in
fact version-aware via its before/after id scheme.

## 1. Code: production RelationCSE policy on recursive subterms

### Pipeline placement (the load-bearing fact)

`src/transform/src/lib.rs`:

* Line 824: `transforms.push(Box::new(eqsat::EqSatTransform))` under
  `if ctx.features.enable_eqsat_optimizer`.
* Lines 907, 988: `cse::relation_cse::RelationCSE::new(...)` run *after* eqsat,
  unconditionally. Line 988 is the last `RelationCSE::new(true)` before
  `JoinImplementation`.

So with `enable_eqsat_wmr_lift = false` the eqsat pass leaves cross-binding
recursive subterms duplicated (see below), and the production `RelationCSE` that
follows is what gets the chance to share them.

### `RelationCSE` = ANF + NormalizeLets

`src/transform/src/cse/relation_cse.rs:53-67`: `RelationCSE` runs `ANF`
(`anf.transform_without_trace`) then `NormalizeLets`.

`src/transform/src/cse/anf.rs:129-255` is the `LetRec` handler. It does perform
cross-binding sharing of recursive-`Get`-containing subterms, and it is
version-aware:

* Lines 162-187, the "before"/"after" id scheme. Each `LetRec` id gets a
  temporary "before" id and a final "after" id. While processing binding
  `index`, references to `ids[index]` resolve to the "before" id (lines 171-173);
  after the binding is processed the rebinding switches to the "after" id (lines
  182-186). The doc comment at lines 156-160 states the invariant directly: "We
  can equate two 'before' references and two 'after' references, but we must not
  equate a 'before' and an 'after' reference." This is exactly version tracking:
  two occurrences reading the same iterate of a recursive binding get the same
  rebound id and CSE together; two occurrences reading different iterates get
  distinct rebound ids and are never equated.
* Lines 204-211: every distinct subterm discovered inside the `LetRec` scope
  (filtered by `id_boundary`), including subterms that contain recursive `Get`s,
  is collected and emitted as a new `LetRec` binding, sorted into dependency
  order. There is no guard that refuses a recursive-`Get`-containing subterm. The
  only structural restriction is the `body` carve-out (lines 189-201): the body
  is processed in a separate `Bindings` so it cannot acquire references to
  in-scope arranged terms (lines 124-128 explain this is a `JoinImplementation`
  limitation, not a recursion-scope refusal).

Definitive statement: production `RelationCSE` **does** perform cross-`LetRec`-
binding sharing of recursive subterms, and it does so only for version-consistent
occurrences (the before/after id scheme makes the distinction).

### What the eqsat pass itself does with the flag off

`src/transform/src/eqsat/lower.rs:84-89`: a recursive `Get(Id::Local)` lowers to
`Rel::LocalGet { get: Some(Box(original)), version }`. The `get` field is `Some`
regardless of the flag; `version` is `None` when the flag is off
(`lower.rs:77-81`). `LetRec` lowers to a first-class `Rel::LetRec` in both cases
(`lower.rs:155, 204`).

`src/transform/src/eqsat/cse.rs`:

* Lines 58-62: `hoist_letrec_subterms_everywhere` runs only when
  `enable_wmr_lift` is true. With the flag off it is skipped entirely.
* Lines 117-123 `worth_binding` requires `is_closed`. Lines 161-171 `is_closed`
  returns `false` for `LocalGet { get: Some(_), .. }`. So the general (non-LetRec)
  outer CSE refuses to hoist any subterm containing a recursive reference out of
  the `LetRec` scope. This is the eqsat-side soundness guard.

Net: with the flag off the eqsat pass emits a `Rel::LetRec` with the shared
subterm duplicated in each binding; `raise` reconstructs a real
`MirRelationExpr::LetRec` with the duplication; the downstream production
`RelationCSE` then shares it. With the flag on, eqsat's own
`hoist_shared_letrec_subterms` (`cse.rs:324-485`) shares it earlier, inside the
search. Both arrive at the same shared structure.

### Cost model already credits the sharing (why the lift cannot win on cost)

The team's own Rust test encodes this, `src/transform/tests/eqsat_wmr_lift.rs:466-525`:

* `wmr_lift_does_not_increase_arrangement_count`: the reuse-aware arrangement
  count (the `ArrangementCount` objective's metric, `Cost::arrangements`)
  deduplicates structurally identical arrangements across the whole plan, so it is
  unchanged by the lift (`with_lift <= without`, equal on the fixtures).
* `wmr_lift_reduces_physical_arrangement_count`: the lift reduces a *physical*
  count that charges each operator occurrence with no cross-tree dedup. But that
  is precisely the duplication that production `RelationCSE` removes downstream, so
  it does not survive to the emitted plan.

## 2. Empirical: EXPLAIN comparisons

`bin/sqllogictest --optimized`, scratch files in `misc/scratch/`,
`enable_eqsat_optimizer = true` throughout, toggling `enable_eqsat_wmr_lift`.

### Case A: `edges JOIN r` shared across bindings `a` and `b`, `r` ordered first (both reads Cur(r))

Query (the `eqsat_wmr_lift.slt` shape):

```sql
WITH MUTUALLY RECURSIVE
  r(x int) AS (SELECT src FROM edges UNION SELECT x FROM a UNION SELECT x FROM b),
  a(x int) AS (
    SELECT e.dst FROM edges e JOIN r ON e.src = r.x
    EXCEPT ALL
    SELECT e.dst FROM edges e JOIN r ON e.src = r.x WHERE e.dst > 4),
  b(x int) AS (
    SELECT e.dst FROM edges e JOIN r ON e.src = r.x
    INTERSECT ALL
    SELECT x FROM r)
SELECT * FROM r ORDER BY 1
```

`EXPLAIN OPTIMIZED PLAN` and `EXPLAIN PHYSICAL PLAN`, flag off vs on: **byte-identical**
(`diff` empty). The flag-off optimized plan already hoists the shared subterm:

```
cte l1 = ArrangeBy keys=[[#0{x}]] Filter (#0{x}) IS NOT NULL  Get l0      -- r, arranged
cte l2 = Project (#1) Join on=(#0{src} = #2{x}) ... ArrangeBy[edges] ... Get l1   -- edges JOIN r
cte l3 = Threshold (Union (Get l2) (Negate ...))            -- binding a (EXCEPT ALL), reads l2
cte l4 = Union (Get l2) (Negate (Threshold (Union (Get l2) (Negate (Get l0)))))  -- binding b, reads l2
```

`cte l2` is the shared `edges JOIN r` subterm, containing the recursive `Get l1`.
It is bound once and read by both `l3` (binding `a`) and `l4` (binding `b`). This
is produced with the lift OFF. The lift ON produces the identical plan.

### Case B: `r` ordered in the middle, so `a` reads Prev(r) and `b` reads Cur(r) of the same subterm

```sql
WITH MUTUALLY RECURSIVE
  a(x int) AS ( ... edges JOIN r ... ),   -- before r: Prev(r)
  r(x int) AS ( ... ),
  b(x int) AS ( ... edges JOIN r ... )    -- after r: Cur(r)
SELECT * FROM r ORDER BY 1
```

`EXPLAIN OPTIMIZED PLAN`, flag off vs on: **byte-identical**. In this plan the two
`edges JOIN r` occurrences are NOT merged into one shared binding: the `a`-arm
join (folded into `cte l2`) reads `r` through `cte l1 = ArrangeBy(Get l2)`, while
the `b`-arm join (`cte l3`) builds its own `ArrangeBy(Filter(Get l2))` inline. The
two reads denote different iterates of `r`, and production `RelationCSE` correctly
refused to equate them. Result rows are `{1,2,3,4}` under both flags
(`wmr_b_results.slt`), no divergence.

This is the soundness probe. Production `RelationCSE` is **version-aware** and did
not wrongly share the Prev/Cur occurrences. No soundness concern.

### Case C: self-join `r JOIN r` shared across `a` and `b`

`EXPLAIN PHYSICAL PLAN`, flag off vs on: **byte-identical** (only the
`ALTER SYSTEM SET` line differs). RedundantJoin collapses the self-equijoin to a
single `r`, so this fixture degenerates, but it still shows no flag effect.

## 3. Summary table

| Query | Plan kind | off vs on |
|-------|-----------|-----------|
| A (Cur/Cur share, r first) | OPTIMIZED | identical |
| A | PHYSICAL | identical |
| B (Prev/Cur, r middle) | OPTIMIZED | identical |
| B | result rows | identical `{1,2,3,4}` |
| C (self-join share) | PHYSICAL | identical |

## Soundness note

No soundness concern in production `RelationCSE`. The version-blind worry posed in
case (b) does not materialize: production ANF distinguishes iterates via the
before/after id scheme (`anf.rs:162-187`), so it shares only version-consistent
occurrences and refuses to equate a Prev read with a Cur read. The empirical
case-B plan confirms the two differently-versioned `edges JOIN r` reads stay
separate. The eqsat pass adds its own guard (`cse.rs is_closed` rejects
`LocalGet { get: Some }`), so even the flag-off eqsat CSE never lifts a recursive
reference out of scope.

## Bottom line

The WMR lift makes more distinctions than version-blind matching, never fewer, so
it is sound. But it has no distinctive *value*: the production `RelationCSE` pass
that runs after eqsat already performs the same version-consistent cross-binding
sharing, and the cost model already credits sharing in its reuse-aware
arrangement count. Every SQL query tried, including ones built to favor the lift,
yields byte-identical optimized and physical plans with the flag off and on. The
lift is redundant with the existing pipeline.
