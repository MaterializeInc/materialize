# Strangler-fig cutover 1: skip logical CanonicalizeMfp when eqsat on

## Verdict: CLEAN CUTOVER COMMITTED

Commit: `5e28accfdc`

## Gating change

In `src/transform/src/lib.rs`, `logical_cleanup_pass`, the inner `vec![]` of the
`fixpoint_logical_cleanup_pass_01` `Fixpoint` was changed to a `transforms![]`
macro, and the `CanonicalizeMfp` entry was gated with:

```rust
Box::new(CanonicalizeMfp);
    if !ctx.features.enable_eqsat_optimizer,
```

With a comment explaining the strangler-fig intent.
The safeguard path (eqsat off) is preserved.

## SLT results

| Suite | Success | Total | Multiplicity errors | Panics |
|---|---|---|---|---|
| AoC 2023 (`advent-of-code/2023/*.slt`) | 125 | 125 | 0 | 0 |
| LDBC BI + eager (`ldbc_bi.slt`, `ldbc_bi_eager.slt`) | 205 | 205 | 0 | 0 |
| Arithmetic (`arithmetic.slt`) | 206 | 206 | 0 | 0 |

All three suites match the committed goldens exactly.
Zero non-positive multiplicity errors.
Zero panics.

## Diagnosis

No coverage gaps detected.
The eqsat pass (`EqSatTransform`) fully subsumes the logical-cleanup
`CanonicalizeMfp` for all tested query shapes.

## Concerns

None.
The `CanonicalizeMfp` inside `physical_optimizer` (~line 894, 905) and
`fast_path_optimizer` (~line 988) were intentionally left untouched; those
phases are not covered by the logical eqsat pass.
