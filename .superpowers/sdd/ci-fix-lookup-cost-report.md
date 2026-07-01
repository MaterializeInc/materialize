# eqsat lookup-vs-full-scan: root cause is seeding/CSE structure, not the cost model

Status: BLOCKED (with a precise, verified root cause and a candidate fix that is larger than a cost tweak).

The instructed fix (penalize full scans and cross joins, make `IndexedFilter` cheaper) does not apply: the cost model is already correct, and penalizing full scans would be a no-op here because the lookup form is never an extraction candidate for the failing query.
The tree was returned to a clean state; no code changes are committed.

## Confirmed symptom

`test/sqllogictest/transform/relation_cse.slt` fails at lines 782 and 820 (and 561 is adjacent).
For the line-820 query the binary emits a shared `ReadIndex i1=[*** full scan ***]` feeding two `Filter` branches, where production keeps two point lookups (`value=(1)`, `value=(2)`).

## Mechanism (instrumented, not guessed)

I instrumented the engine (`extract_with` cost dump, seed-count and matcher dumps) and ran the failing query through `bin/sqllogictest`.

1. The default extractor is `GreedyExtractor` (engine.rs), which orders by the full `ArrangementCount` comparator: `arrangements -> time -> nodes`.
   At the fragment level the cost model already prefers the lookup: `Filter(Get)` costs `time:[1.0]` (a full scan, degree 1), while `IndexedFilter` costs `time:[0.0]` (constant, dropped).
   So if both forms were candidates in the same e-class, greedy would pick the lookup.

2. The lookup form is never a candidate. `collect_indexed_filter_seeds` (transform.rs) only seeds when it finds a bare `Filter(Get global)` subtree in the input MIR.
   For the line-820 query the relation reaching `PhysicalEqSatTransform` is:

   ```
   CrossJoin
     Project ()
       Filter (#0{f1} = 1)
         Project (#0)
           Get u1
     Filter (#0{f1} = 1)
       Project (#0)
         Get u1
     Project ()
       Filter (#0{f1} = 2)
         Project (#0)
           Get u1
     Filter (#0{f1} = 2)
       Project (#0)
         Get u1
   ```

   The filter sits over `Project(#0, Get u1)`, not over the global `Get` directly, so the seeder produces **0 seeds** and no `IndexedFilter` enters the e-graph.
   The greedy extractor then can only choose the full-scan `Filter`.

3. Production (eqsat OFF) gets the lookup because its standalone `LiteralConstraints` pass uses `MapFilterProject::extract_non_errors_from_expr_mut`, which sees through the `Project` to the `Get`.
   The same `LiteralConstraints` also runs after `PhysicalEqSatTransform` (lib.rs:899), but by then the filter sits over a CTE local get (`Filter(#0=1, Get l0)` with `l0 = Project(Get u1)`), and `LiteralConstraints`'s MFP extraction stops at the local `Get l0`, so it cannot convert it.

### Why this is not the cost model

The instructed cost change (full-scan / cross-join penalty, keep `IndexedFilter` cheapest) cannot help, because the e-graph never contains the lookup alternative to choose.
The cost-axis ordering for the two complete forms already favors the lookup (fewer/0 time terms), and arrangement counts are equal; the only thing missing is the candidate node.

## Candidate fix (larger than a cost tweak)

Two layers are needed, and even together they did not fix the target query in my experiments:

1. Seeding must recognize an MFP (Map/Project) between the filter and the global `Get`, matching what `LiteralConstraints` already detects, instead of only a bare `Filter(Get)`.
   I prototyped this with `MapFilterProject::extract_from_expression`, synthesizing a bare `Filter[mfp.predicates](Get)` subtree.
   With it, seeds were produced (2 seeds for the line-820 query).

2. Seeding must run **after** saturation (or be repeated post-saturation).
   The rule `push_filter_past_project` (`Filter[p](Project[o] r) => Project[o](Filter[remap(p,o)] r)`, relational.rewrite line 196) exposes the bare `Filter[remap](Get)` form, but only during saturation, while seeding currently runs once before saturation.
   Re-seeding after saturation does attach `IndexedFilter`s (I verified `filter_over_get=4 pred_eq=2 unions=2` for the line-820 query, so the `predicates.as_slice()` equality matches fine; scalar modeling in the e-graph is not required).

Even with both changes the extracted plan still used full scans for the line-820 query.
The remaining blocker is structural: `RelationCSE` has already hoisted `Project(Get t1)` into a shared CTE `l0` **before** eqsat runs, and the engine optimizes each Let-bound fragment in isolation.
The filter binding `l1 = Filter(#0=1, Get l0)` is permanently separated from the global `Get` by the `l0` CTE boundary, so neither `push_filter_past_project` (the Project is in a different binding) nor the seeder (its input is a `LocalGet`, not a global `Get`) can act.
Production avoids this because `LiteralConstraints` runs over the whole plan and un-shares the filters into per-value lookups; eqsat's fragment-at-a-time saturation cannot.

So the complete fix touches: (a) MFP-aware seeding, (b) seeding timing relative to saturation, and (c) the interaction between RelationCSE-introduced CTE boundaries and eqsat's per-fragment optimization (e.g. inlining the `l0 = Project(Get)` definition into the filter fragment via the existing `optimize_body_with_let_union` mechanism, extended to sibling bindings, or moving the seed/lookup decision to operate across the CTE boundary).
This is well beyond the scoped cost-model change and warrants a deliberate design decision.

## Repro (executable)

`bin/sqllogictest -- <file>` with:

```sql
CREATE TABLE t1 (f1 INTEGER, f2 INTEGER);
CREATE INDEX i1 ON t1 (f1);
EXPLAIN OPTIMIZED PLAN WITH(humanized expressions, arity, join implementations) AS VERBOSE TEXT FOR
SELECT * FROM
(SELECT a2.f1 AS f1 FROM t1 AS a1 LEFT JOIN t1 AS a2 USING (f1)) AS s1,
(SELECT a2.f1 AS f1 FROM t1 AS a1 LEFT JOIN t1 AS a2 USING (f1)) AS s2
WHERE s1.f1 = 1 AND s2.f1 = 2;
```

The `src/transform/tests/eqsat_indexed_filter.rs` integration test is the natural home for a focused regression once the fix lands (it runs `PhysicalEqSatTransform` in isolation, so an `IndexedFilter` in the output can only come from the eqsat seeding path).
A new test there should feed `Filter[#0=5](Project[#0](Get global))` and assert an `IndexedFilter` is produced.

## Verification performed

* Confirmed the failure: `bin/sqllogictest --optimized -- test/sqllogictest/transform/relation_cse.slt` -> `FAIL: output-failure=2` (lines 782, 820), actual output uses `*** full scan ***` where the golden expects lookups.
* Confirmed 0 seeds collected for the failing query (instrumented `collect_indexed_filter_seeds`).
* Confirmed the fragment-level cost prefers `IndexedFilter` over `Filter` (`time:[0.0]` vs `time:[1.0]`).
* Confirmed `push_filter_past_project` exists and the post-saturation re-seed attaches `IndexedFilter`s (`unions=2`), yet the final plan still uses full scans due to the CTE-boundary separation.
* No goldens were rewritten. The tree is clean (no committed changes).

## Recommendation

Do not apply a cost-model hack; it would mis-model the problem (the lookup form is absent from the e-graph, not mis-costed).
Decide on the seeding/CSE-boundary fix as a separate, scoped change.
