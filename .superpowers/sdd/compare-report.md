# compare_real harness: run report (Get-based sources)

**Date:** 2026-06-19
**Branch:** claude/mir-equality-optimizer-sodbej
**Corpus:** 20 cases, Get-based sources (GlobalId::Transient), not empty constants

## Full table

| label | c\_in | c\_real | c\_eq | verdict |
|---|---|---|---|---|
| filter\_single | degrees=[1.00] nodes=2 | degrees=[] nodes=1 | degrees=[1.00] nodes=2 | LOSS (real cheaper) |
| nested\_filter\_outer\_0\_inner\_1 | degrees=[1.00] nodes=2 | degrees=[] nodes=1 | degrees=[1.00] nodes=2 | LOSS (real cheaper) |
| nested\_filter\_outer\_1\_inner\_0 | degrees=[1.00] nodes=2 | degrees=[] nodes=1 | degrees=[1.00] nodes=2 | LOSS (real cheaper) |
| filter\_false | degrees=[1.00] nodes=2 | degrees=[] nodes=1 | degrees=[] nodes=1 | TIE |
| filter\_over\_union\_with\_branch\_filters | degrees=[1.00,1.00,1.00,1.00] nodes=6 | degrees=[] nodes=1 | degrees=[1.00,1.00,1.00] nodes=5 | LOSS (real cheaper) |
| filter\_union\_branch\_filters\_map\_project | degrees=[1.00,1.00,1.00,1.00,1.00,1.00] nodes=8 | degrees=[] nodes=1 | degrees=[1.00,1.00,1.00,1.00] nodes=6 | LOSS (real cheaper) |
| double\_negation | degrees=[] nodes=1 | degrees=[] nodes=1 | degrees=[] nodes=1 | TIE |
| double\_negation\_under\_filter | degrees=[1.00] nodes=2 | degrees=[] nodes=1 | degrees=[1.00] nodes=2 | LOSS (real cheaper) |
| union\_cancel | degrees=[1.00,1.00] nodes=4 | degrees=[] nodes=1 | degrees=[1.00,1.00] nodes=4 | LOSS (real cheaper) |
| union\_cancel\_under\_filter\_map | degrees=[1.00,1.00,1.00,1.00] nodes=6 | degrees=[] nodes=1 | degrees=[1.00,1.00,1.00,1.00] nodes=6 | LOSS (real cheaper) |
| double\_threshold | degrees=[1.00] nodes=2 | degrees=[] nodes=1 | degrees=[] nodes=1 | TIE |
| threshold\_over\_union\_cancel | degrees=[1.00,1.00,1.00] nodes=5 | degrees=[] nodes=1 | degrees=[1.00,1.00,1.00] nodes=5 | LOSS (real cheaper) |
| threshold\_over\_union\_filtered\_inputs | degrees=[1.00,1.00,1.00,1.00] nodes=6 | degrees=[] nodes=1 | degrees=[1.00,1.00,1.00] nodes=5 | LOSS (real cheaper) |
| triangle\_join | degrees=[2.00,1.50] nodes=4 | degrees=[2.00,1.50,1.50] nodes=5 | degrees=[2.00,1.50] nodes=4 | **WIN** |
| four\_cycle\_join | degrees=[2.00,2.00,2.00] nodes=5 | degrees=[2.00,2.00,2.00,2.00] nodes=6 | degrees=[2.00,2.00,2.00] nodes=5 | **WIN** |
| triangle\_join\_filtered | degrees=[2.00,1.50,1.50] nodes=5 | degrees=[] nodes=1 | degrees=[2.00,1.50,1.50] nodes=5 | LOSS (real cheaper) |
| join\_with\_reduce\_branch | degrees=[1.00] nodes=3 | degrees=[1.00,1.00] nodes=4 | degrees=[1.00] nodes=3 | **WIN** |
| topk\_over\_filter | degrees=[] nodes=1 | degrees=[] nodes=1 | degrees=[] nodes=1 | TIE |
| project\_then\_filter | degrees=[1.00,1.00] nodes=3 | degrees=[] nodes=1 | degrees=[1.00,1.00] nodes=3 | LOSS (real cheaper) |
| filter\_then\_project | degrees=[1.00,1.00] nodes=3 | degrees=[] nodes=1 | degrees=[1.00,1.00] nodes=3 | LOSS (real cheaper) |

**SUMMARY: 3 wins / 13 losses / 4 ties / 0 skips**

## WIN cases: input, real, and eqsat plans

### WIN: triangle\_join

Input: `Join(Get t1, Get t2, Get t3)` with equivalences `[#0=#4, #1=#2, #3=#5]`.

Real optimizer output: wraps the unchanged join in `Project([0,1,1,3,0,3])`.
Eqsat output: same join node, no project wrapper.

Real cost: degrees=[2.00,1.50,1.50] nodes=5 (join + project node).
Eqsat cost: degrees=[2.00,1.50] nodes=4 (join only).

### WIN: four\_cycle\_join

Input: `Join(Get t1, Get t2, Get t3, Get t4)` with equivalences `[#0=#6, #1=#2, #3=#4, #5=#7]`.

Real optimizer output: wraps the unchanged join in `Project([0,1,1,3,3,5,0,5])`.
Eqsat output: same join node, no project wrapper.

Real cost: degrees=[2.00,2.00,2.00,2.00] nodes=6 (join + project node).
Eqsat cost: degrees=[2.00,2.00,2.00] nodes=5 (join only).

### WIN: join\_with\_reduce\_branch

Input: `Join(Reduce(Get t1, key=[#0]), Get t2)` with equivalences `[#0=#1]`.

Real optimizer output: wraps the unchanged join in `Project([0,0,2])`.
Eqsat output: same join node, no project wrapper.

Real cost: degrees=[1.00,1.00] nodes=4 (join + project node).
Eqsat cost: degrees=[1.00] nodes=3 (join only).

## Interpretation

### Are the wins genuine phase-ordering wins?

**No.** All three wins are cost-model artifacts, not genuine ordering wins.

In every win case the real optimizer's output is structurally identical to eqsat's except for a `Project` node.
The real optimizer inserts this project to canonicalize join output: when equivalences link column pairs (e.g., `#0=#4`), the output has duplicate values and the pipeline emits one canonical copy per equivalence class via an output projection.
Eqsat's `lower` pass does not generate this canonicalization project; it keeps the raw join.
The cost model counts the `Project` node, so eqsat appears cheaper.
This is not a real execution advantage: the project is O(1) per tuple and is required for downstream consumers that expect deduplicated equivalent columns.
A fair comparison would either charge both sides for the canonicalization project or charge neither.

### Are the ties true parity?

**Yes.**
The four ties (`filter_false`, `double_negation`, `double_threshold`, `topk_over_filter`) reflect rules both pipelines share:
`double_negation` and `double_threshold` are handled by eqsat's own rewrite rules and by the real optimizer's passes.
`filter_false` reduces to empty on both sides.
`topk_over_filter` is entirely a bail leaf in eqsat; both sides score identically.
These are genuine ties.

### Do losses indicate eqsat's rule set is a strict subset?

**Yes, with a clear explanation.**
The 13 losses share two root causes:

1. **Semantic folding on NOT NULL predicates.**
   `IS NULL(col)` on a column declared `NOT NULL` is always false.
   The real optimizer's `NonNullRequirements` and `LiteralConstraints` passes exploit this and collapse filters to empty constants, which then propagate upward through union, threshold, negate, and join.
   Eqsat lacks this semantic rule; it treats `IS NULL(col)` as an opaque predicate regardless of nullability.
   This drives losses in cases 1-3, 5-6, 8, 10, 12-13, 16, 19-20.

2. **Empty-result propagation.**
   Once a filter collapses to empty, the real optimizer propagates that through Union, Threshold, Negate, and Join to reduce the whole tree.
   Eqsat does not implement this chain; the unfolded subtree remains at its original cost.
   This amplifies losses in cases 9-10, 12-13, 16.

Adding `IS NULL(NOT NULL col) -> false` (a constant-folding rule) and empty-result propagation rules to the eqsat ruleset would convert most losses to ties.
No loss indicates a deficiency in eqsat's join-ordering logic or core structural rewrites.

## Commit hash

`e01b799a35` — "transform-egraph: harness corpus uses Get-based sources (survive fold)"
