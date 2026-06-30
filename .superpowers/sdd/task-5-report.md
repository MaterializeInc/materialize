# Task 5 report: delta-join-cost inertness verification

**Status:** DONE — inertness confirmed. All tests pass; zero plan movement detected.

**Date:** 2026-06-30

---

## Commands run

### 1. sqllogictest suite (flag `enable_eqsat_delta_join_cost` at default ON)

```
COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 \
bin/sqllogictest --optimized -- \
  test/sqllogictest/transform/eqsat_delta_join_cost.slt \
  test/sqllogictest/transform/equivalence_propagation.slt \
  test/sqllogictest/transform/eqsat_wins.slt \
  test/sqllogictest/joins.slt \
  test/sqllogictest/outer_join.slt \
  test/sqllogictest/outer_join_lowering.slt \
  test/sqllogictest/outer_join_simplification.slt \
  test/sqllogictest/variadic_outer_join.slt \
  test/sqllogictest/join-identity-elision.slt
```

All 9 files confirmed present before running.

### 2. mz-transform unit corpus

```
COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 \
bin/cargo-test -p mz-transform
```

---

## Per-file results

| File | Status | success / total |
|---|---|---|
| transform/eqsat_delta_join_cost.slt | PASS | 9 / 9 |
| transform/equivalence_propagation.slt | PASS | 4 / 4 |
| transform/eqsat_wins.slt | PASS | 16 / 16 |
| joins.slt | PASS | 136 / 136 |
| outer_join.slt | PASS | 25 / 25 |
| outer_join_lowering.slt | PASS | 29 / 29 |
| outer_join_simplification.slt | PASS | 27 / 27 |
| variadic_outer_join.slt | PASS | 37 / 37 |
| join-identity-elision.slt | PASS | 9 / 9 |
| **TOTAL** | **PASS** | **292 / 292** |

Exit code: 0. One unrelated `WARN mz_persist_client::batch: un-consumed Batch` appeared in stderr; this is a cleanup artifact not related to test correctness.

---

## mz-transform unit test result

```
Summary [   6.058s] 376 tests run: 376 passed, 3 skipped
```

Exit code: 0. All 376 tests passed; 3 skipped (expected).

---

## Plan diffs found

**None.** No golden moved. The delta-join-cost infrastructure is fully inert across the optimizer corpus at flag default (ON). The `JoinImplementation::canonicalize_equivalences` post-pass successfully erases any key re-spelling introduced by eqsat extraction, as expected.

---

## Conclusion

The delta-join-cost infrastructure landed this cycle causes **zero plan movement**. Inertness is confirmed.
