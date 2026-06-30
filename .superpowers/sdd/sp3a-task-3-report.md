# SP3a Task 3 Report

## What was done

Implemented Task 3 of SP3a verbatim from the brief: seeded workload generators
for the colored e-graph measurement spike.

### TDD order followed

1. Added all 5 new tests to `mod tests` in `colored.rs` (failing).
2. Confirmed compile failure (21 errors: undefined `GenParams`, `Locality`,
   `gen_base`, `gen_colors`, `indegree`).
3. Added implementation before `#[cfg(test)]`: `Lcg` (SplitMix64), `Locality`,
   `GenParams`, `indegree`, `gen_base`, `gen_colors`.
4. All 11 tests pass.

### Test command and full output (summary)

```
bin/cargo-test -p mz-transform eqsat::colored
```

```
PASS [   0.005s] ( 1/11) mz-transform eqsat::colored::tests::close_color_propagates_congruence
PASS [   0.006s] ( 2/11) mz-transform eqsat::colored::tests::close_color_leaf_equality_no_cascade
PASS [   0.006s] ( 3/11) mz-transform eqsat::colored::tests::gen_base_is_deterministic
PASS [   0.006s] ( 4/11) mz-transform eqsat::colored::tests::close_color_no_equalities_is_base_partition
PASS [   0.006s] ( 5/11) mz-transform eqsat::colored::tests::gen_base_reaches_target_size
PASS [   0.006s] ( 6/11) mz-transform eqsat::colored::tests::locality_steers_toward_shared_classes
PASS [   0.006s] ( 7/11) mz-transform eqsat::colored::tests::gen_colors_shape
PASS [   0.006s] ( 8/11) mz-transform eqsat::colored::tests::colored_uf_starts_equal_to_base
PASS [   0.006s] ( 9/11) mz-transform eqsat::colored::tests::colored_uf_union_merges_classes
PASS [  0.006s] (10/11) mz-transform eqsat::colored::tests::close_color_matches_oracle_fixed
PASS [  0.010s] (11/11) mz-transform eqsat::colored::tests::close_color_matches_oracle_random
Summary [  0.012s] 11 tests run: 11 passed, 297 skipped
```

### Compiler warnings (non-blocking)

Three `dead_code` / privacy mismatch warnings were emitted:
- `ToyLang` is `pub(self)` while `gen_base`/`gen_colors`/`indegree` are
  `pub(crate)` — the functions reference `ToyLang` in their signatures. These
  are suppressed by the module-level `#![allow(dead_code)]` and don't affect
  correctness. They are expected for a spike; SP3b will likely make `ToyLang`
  `pub(crate)` or restructure.

### Deviations

None. Code transcribed verbatim from the brief. Commit message matches brief exactly.

## Commit

`3e8fd7e831` — "eqsat: SP3a workload generators (Lcg, GenParams, gen_base, gen_colors)"

---

## SP3a Task 3 Review-Fixes Report

### What was changed

In `src/transform/src/eqsat/colored.rs` only:

1. **Visibility widened** (fixes `private_interfaces` CI-breaking lint):
   - `enum ToyNode` → `pub(crate) enum ToyNode`
   - `enum ToySym` → `pub(crate) enum ToySym`
   - `struct ToyLang` → `pub(crate) struct ToyLang`

2. **`Debug` derives added** (fixes `missing_debug_implementations` lint):
   - `ToySym`: added `Debug` to existing derive list
   - `Lcg`: added `#[derive(Debug)]` (no prior derive line)
   - `Locality`: added `Debug` to existing derive list
   - `GenParams`: added `Debug` to existing derive list

No logic, signatures, test code, or algorithm was changed. The module-level `#![allow(dead_code)]` and its SP3b note are intact.

### Test command and output

```
bin/cargo-test -p mz-transform eqsat::colored
```

```
   Compiling mz-transform v0.0.0 (.../src/transform)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 28.48s
────────────
 Nextest run ID 3066b3b6-7cbb-4b0d-a372-28758adf0b96 with nextest profile: default
    Starting 11 tests across 10 binaries (297 tests skipped)
        PASS [   0.005s] ( 1/11) mz-transform eqsat::colored::tests::close_color_leaf_equality_no_cascade
        PASS [   0.005s] ( 2/11) mz-transform eqsat::colored::tests::gen_colors_shape
        PASS [   0.005s] ( 3/11) mz-transform eqsat::colored::tests::close_color_no_equalities_is_base_partition
        PASS [   0.005s] ( 4/11) mz-transform eqsat::colored::tests::gen_base_reaches_target_size
        PASS [   0.006s] ( 5/11) mz-transform eqsat::colored::tests::close_color_matches_oracle_fixed
        PASS [   0.006s] ( 6/11) mz-transform eqsat::colored::tests::gen_base_is_deterministic
        PASS [   0.006s] ( 7/11) mz-transform eqsat::colored::tests::colored_uf_starts_equal_to_base
        PASS [   0.006s] ( 8/11) mz-transform eqsat::colored::tests::colored_uf_union_merges_classes
        PASS [   0.006s] ( 9/11) mz-transform eqsat::colored::tests::close_color_propagates_congruence
        PASS [   0.006s] (10/11) mz-transform eqsat::colored::tests::locality_steers_toward_shared_classes
        PASS [   0.010s] (11/11) mz-transform eqsat::colored::tests::close_color_matches_oracle_random
────────────
     Summary [   0.011s] 11 tests run: 11 passed, 297 skipped
```

No `warning:` lines mentioning `private_interfaces` or `colored.rs` in the build output. Clean compile.

### Commit

`4cbcf649a2` — "eqsat: SP3a fix colored spike visibility + Debug derives"

---

## Notes for Task 4

- All public(crate) interfaces are in place: `Lcg`, `Locality`, `GenParams`,
  `indegree`, `gen_base`, `gen_colors`.
- `close_color_matches_oracle_random` passes across seeds 0–7 with depth=3,
  fan_out=2, confirming multi-round congruence cascade correctness.
- The `locality_steers_toward_shared_classes` test empirically validates that
  `SharedHot` selects higher-indegree classes than `LeafOnly` — the workload
  differentiation is working as intended.
- Task 4 (the harness / measurement loop) can call `gen_base` + `gen_colors` +
  `close_color` directly; all are `pub(crate)`.
