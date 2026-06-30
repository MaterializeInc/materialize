# CLU-137 Temporal Factoring Verification Report

## Status: DONE

The temporal predicate `mz_now() < cast(ts)` IS extracted at the top level with the current size-only cost.

## Exact canonical MirScalarExpr (Debug form)

```
CallVariadic(And(And), [
  Column(2),
  Column(3),
  Column(4),
  CallBinary(Lt(Lt),
    CallUnmaterializable(MzNow),
    CallUnary(CastTimestampTzToMzTimestamp(CastTimestampTzToMzTimestamp), Column(0))
  ),
  CallVariadic(Or(Or), [
    CallUnary(IsNull(IsNull), Column(1)),
    CallBinary(Eq(Eq), Column(1), Literal(Ok(Row{[String("")]}), ReprColumnType { scalar_type: String, nullable: false }))
  ])
])
```

## Analysis

The `factor_and_or` rule fires for the CLU-137 predicate:

**Input** (two-branch DNF, top-level OR):
```
OR(
  AND(col2, col3, col4, is_null(col1), lt(mz_now(), cast_tz(col0))),
  AND(col2, col3, col4, eq(col1, ""), lt(mz_now(), cast_tz(col0)))
)
```

**Why the rule fires:**
- Intersection: `{col2, col3, col4, lt(mz_now(), cast_tz(col0))}` (shared across both branches).
- Residuals: `{is_null(col1)}` and `{eq(col1, "")}` (one per branch, non-empty).
- Residual-error gate: `IsNull` is infallible, `Eq` is infallible, both children are columns (not could_error). Gate passes.
- The common factor `lt(mz_now(), cast_tz(col0))` has `could_error: true` but is exempt from the gate.

**Extracted form** (size-only cost prefers factored):
```
AND(col2, col3, col4, lt(mz_now(), cast_tz(col0)), OR(is_null(col1), eq(col1, "")))
```

Cost comparison (tree nodes):
- Original OR form: 22 nodes
- Factored AND form: 14 nodes

`mz_now() < cast(ts)` appears at position 3 of the outer AND's operand list, as a direct operand (not nested in an OR).

## Test added

`test_clu137_temporal_factors_to_top_level` in
`src/transform/src/eqsat/scalar/rules.rs`, at the CLU-137 regression section
between `factor_and_or` and `absorb_and_or` tests.

The test:
1. Builds the exact CLU-137 predicate with real `UnmaterializableFunc::MzNow`, `UnaryFunc::CastTimestampTzToMzTimestamp`, and `BinaryFunc::Lt`.
2. Calls `canonicalize` with the five-column type context.
3. Asserts the outer form is `And`.
4. Asserts `mz_now() < cast(ts)` (or `cast(ts) > mz_now()`) is a direct AND operand.
5. Asserts the residual `OR(is_null(col1), eq(col1, ""))` is a direct AND operand.

All 71 scalar eqsat tests pass, including `test_canonicalize_eval_differential`.
`bin/fmt` and `cargo check` are clean.
