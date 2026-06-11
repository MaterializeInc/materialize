# output_consistency

Differential SQL consistency testing framework. Runs the same randomly
generated SQL expressions against two evaluation strategies (dataflow rendering
vs. constant folding) and verifies they produce identical output.

## Subtree (≈ 15,248 LOC)

| Path | LOC | What it owns |
|---|---|---|
| `input_data/` | 6,916 | Type/operation catalogue (see its CONTEXT.md) |
| `generators/` | ~1,200 | `ExpressionGenerator`, `QueryGenerator` — random query construction |
| `execution/` | ~1,200 | `EvaluationStrategy`, `SqlExecutor`, `QueryExecutionManager` |
| `validation/` | ~1,100 | `ResultComparator`, `ErrorMessageNormalizer` |
| `ignore_filter/` | ~850 | `InconsistencyIgnoreFilter`, expression matchers for known divergences |
| `operation/` | ~350 | `DbOperationOrFunction`, operation descriptors |
| `query/` | ~450 | `QueryTemplate` |
| `runner/` | ~250 | `ConsistencyTestRunner` — top-level loop |
| `output_consistency_test.py` | 394 | Entry point `OutputConsistencyTest.run_output_consistency_tests()` |

## Purpose

Generates random SQL expressions from a type/operation catalogue, submits them
to both `DataFlowRenderingEvaluation` and `ConstantFoldingEvaluation`, and
flags divergences. The ignore filter suppresses known-acceptable differences.

## Key interfaces

- `OutputConsistencyTest.run_output_consistency_tests()` — entry point called from mzcompose workflows
- `EvaluationStrategy` — abstract; two concrete implementations: `DataFlowRenderingEvaluation`, `ConstantFoldingEvaluation`
- `ResultComparator` — compares strategy outputs; the heart of the diff logic
- `GenericInconsistencyIgnoreFilter` — pluggable filter for known divergences
- `ConsistencyTestRunner` — orchestrates generate → execute → compare → report loop

## Evaluation strategies

`EvaluationStrategyKey` is an `Enum` (`DATAFLOW_RENDERING`, `CONSTANT_FOLDING`).
An inner expression is evaluated once per strategy; results are compared
pairwise. New strategies require: a new enum value + a new `EvaluationStrategy`
subclass + registration in the test entry point.

## Bubbled findings for materialize/CONTEXT.md

- **`all_operations_provider.py` is a manual append-only list**: adding a new
  SQL type requires editing the `itertools.chain(...)` call; auto-discovery
  would be lower friction.
- **Ignore filter is a seam with high churn risk**: `InternalOutputInconsistencyIgnoreFilter`
  at 316 LOC holds known-divergence rules; these silently become stale when
  bugs are fixed.
- **`EvaluationStrategyKey` enum is the right pattern**: clean seam for adding
  new backends (e.g., Postgres cross-validation).
