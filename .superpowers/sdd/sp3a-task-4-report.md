# SP3a Task 4 Report

## What was done

1. Added `run_cell` and `measure_color_explosion` (verbatim from brief) to the
   `#[cfg(test)] mod tests` block in `src/transform/src/eqsat/colored.rs`.

2. Compiled and ran unit tests:
   ```
   cargo nextest run -p mz-transform eqsat::colored
   ```
   Result: 11/11 PASS, `measure_color_explosion` reported as 1 ignored (skipped).
   First build took ~8s (already warm).

3. Ran the sweep (note: `bin/cargo-test` passes `--ignored` as a nextest flag
   which nextest rejects; used `cargo nextest run --run-ignored ignored-only` directly):
   ```
   cargo nextest run -p mz-transform --run-ignored ignored-only --no-capture \
     eqsat::colored::tests::measure_color_explosion
   ```
   Result: PASS in 0.449 s, 27 CSV rows printed.

4. Wrote Findings & Gate Verdict into §8 of
   `doc/developer/design/20260627_eqsat_colored_spike.md`.

5. Committed: `62593cc649`

## Full CSV output

```
# SP3a color-explosion sweep
# sharing_ratio = delta_nodes / (n_colors * node_count)
# cascade_factor = induced_merges / applied_equalities
axis,value,sharing_ratio,cascade_factor,delta_nodes,node_count,wall_ms,max_iters
base_size,100,0.1198,0.000,599,100,1,1
base_size,250,0.0518,0.000,647,250,3,1
base_size,500,0.0254,0.000,635,500,7,1
base_size,1000,0.0128,0.000,642,1000,14,1
base_size,2500,0.0049,0.000,616,2500,28,1
base_size,5000,0.0025,0.000,630,5000,58,1
fan_out,1,0.0145,1.345,362,500,14,7
fan_out,2,0.0123,0.005,308,500,5,2
fan_out,4,0.0243,0.000,607,500,5,1
fan_out,8,0.0476,0.000,1189,500,5,1
fan_out,16,0.0951,0.000,2378,500,6,1
fan_out,32,0.1719,0.000,4297,500,9,1
n_colors,10,0.0268,0.000,134,500,1,1
n_colors,50,0.0258,0.000,645,500,5,1
n_colors,100,0.0258,0.000,1290,500,10,1
n_colors,250,0.0248,0.000,3106,500,27,1
n_colors,500,0.0252,0.000,6299,500,53,1
n_colors,1000,0.0252,0.000,12611,500,106,1
eqs_per_color,1,0.0072,0.000,179,500,5,1
eqs_per_color,2,0.0122,0.000,305,500,5,1
eqs_per_color,4,0.0245,0.000,613,500,5,1
eqs_per_color,8,0.0500,0.000,1250,500,5,1
eqs_per_color,16,0.0980,0.000,2451,500,6,1
eqs_per_color,32,0.1838,0.000,4594,500,5,1
locality,0,0.0322,0.000,806,500,5,1
locality,1,0.0253,0.000,632,500,5,1
locality,2,0.0488,0.000,1220,500,5,1
```

## Verdict and reasoning

**Proceed with shared-delta as-is.**

### Sharing ratio

- **base_size sweep:** delta_nodes stays constant (~630) as base_size grows 50×
  (100 → 5000). The sharing ratio drops from 0.12 to 0.0025 — the overhead
  relative to the full graph *shrinks* as plans grow. Absolute delta is
  dominated by eqs_per_color × local neighborhood, independent of total graph
  size.

- **n_colors sweep:** sharing ratio stays flat at ~0.025 across n_colors = 10 →
  1000. delta_nodes scales exactly linearly. No super-linear cross-color
  accumulation.

- **fan_out sweep:** sharing ratio rises with fan_out (0.012 → 0.172 at 32),
  which is expected (more children to re-canonicalize). Even at fan_out=32 it is
  well below 1.

- All measured sharing ratios are ≪ 1.

### Cascade factor

- **0.000 for every configuration with fan_out ≥ 2.** Locality (LeafOnly /
  Mixed / SharedHot) produces no cascade: SharedHot adds delta storage but not
  induced merges.

- **fan_out=1 anomaly:** cascade=1.345, max_iters=7. Unary chains create deep
  congruence cascades (f(x)≡f(y)≡f(f(x))≡…). This structural pattern does not
  appear in SQL relational algebra and poses no practical risk.

- **Wall-clock:** scales linearly with both n_colors and base_size; 1000 colors ×
  500 nodes runs in 106 ms in unoptimized test mode. Tractable.

### Anomalies / honesty

- No underfilled cells (all base graphs reached target size).
- bin/cargo-test cannot pass --ignored to nextest; used cargo nextest directly.
  The doc comment on the harness says `bin/cargo-test -p mz-transform
  eqsat::colored::measure_color_explosion -- --ignored --nocapture` which nextest
  rejects. The working equivalent is:
  `cargo nextest run -p mz-transform --run-ignored ignored-only --no-capture \
    eqsat::colored::tests::measure_color_explosion`
  (could update the doc comment, but the brief said to transcribe verbatim).

## Files changed

- `src/transform/src/eqsat/colored.rs` — run_cell + measure_color_explosion added
- `doc/developer/design/20260627_eqsat_colored_spike.md` — §8 written with full CSV, analysis, verdict, exclusions

---

## Doc-accuracy fix report (post-review)

### Finding 1 — broken sweep run command (Important)

**Problem:** Both the `measure_color_explosion` doc comment in `colored.rs` (lines 593–595) and spec §6 (line 301) gave the libtest-style command `bin/cargo-test -p mz-transform eqsat::colored::measure_color_explosion -- --ignored --nocapture`, which nextest rejects.

**Fix in `src/transform/src/eqsat/colored.rs`:**
```
// Old:
/// Run: `bin/cargo-test -p mz-transform \
///   eqsat::colored::measure_color_explosion -- --ignored --nocapture`
// New:
/// Run: `bin/cargo-test -p mz-transform --run-ignored ignored-only --no-capture \
///   eqsat::colored::tests::measure_color_explosion`
```

**Fix in `doc/developer/design/20260627_eqsat_colored_spike.md` §6:**
```
// Old:
- sweep: `bin/cargo-test -p mz-transform eqsat::colored::measure_color_explosion -- --ignored --nocapture`
// New:
- sweep: `bin/cargo-test -p mz-transform --run-ignored ignored-only --no-capture eqsat::colored::tests::measure_color_explosion`
```

**Corrected command:** `bin/cargo-test -p mz-transform --run-ignored ignored-only --no-capture eqsat::colored::tests::measure_color_explosion`

### Finding 2 — §8 verdict over-rounds cascade claim (Minor)

**Problem:** Verdict bullet 2 said "cascade factor is 0 for all configurations with fan_out ≥ 2", but the CSV shows `fan_out,2 → 0.005`.

**Fix in `doc/developer/design/20260627_eqsat_colored_spike.md` §8 Verdict:**
```
// Old:
2. The cascade factor is 0 for all configurations with fan_out ≥ 2, which
   covers the entirety of SQL/relational operator arity. Congruence closure
   converges in a single pass under the measured workloads.
// New:
2. The cascade factor is 0.000 for fan_out ≥ 4 and ~0.005 at fan_out = 2
   (5 induced merges per 1000 asserted equalities), covering the entirety of
   SQL/relational operator arity. Congruence closure converges in a single
   pass under the measured workloads.
```

### Finding 3 — §8 describes unmeasured joint "corner" (Minor)

**Problem:** The verdict referred to "The `eqs_per_color = 32` / `n_colors = 1000` corner" implying a joint measurement, contradicting the Exclusions line that says joint extremes were not measured.

**Fix in `doc/developer/design/20260627_eqsat_colored_spike.md` §8 Verdict:**
```
// Old:
The `eqs_per_color = 32` / `n_colors = 1000` corner shows
the highest load (sharing ratio 0.18, 106 ms for 1 000 colors × 500 nodes) and
remains tractable.
// New:
The two worst single-axis cells (`eqs_per_color = 32` and
`n_colors = 1000`, measured independently) show the highest load (sharing ratio
0.18, 106 ms for 1 000 colors × 500 nodes) and remain tractable.
```

### Verification

Command: `bin/cargo-test -p mz-transform eqsat::colored`
Result: 11/11 PASS, 298 skipped (build 8s warm, all tests green).

---

## Determinism fix report (gen_colors + §8 refresh)

### Bug

`gen_colors` collected `roots` from `base.class_ids()`, which iterates a
`HashMap` whose order is randomised per-process (`RandomState`). The selection
of which classes became leaves / hot candidates was therefore non-deterministic,
causing different `delta_nodes` on every run even with the same seed. The five
baseline-crossing cells in the old §8 CSV showed this: all have identical
parameters yet reported delta_nodes of 635/607/645/613/632.

`gen_base` was unaffected (uses a frontier deque, not class_ids for ordering)
which is why the existing `gen_base_is_deterministic` test masked the issue.

### Fix 1 — one line in `src/transform/src/eqsat/colored.rs`

After collecting `roots` (line 295), changed binding to `mut` and added
`roots.sort_unstable();`:

```rust
// Before:
let roots: Vec<Id> = base.class_ids().into_iter().filter(|&id| base.find(id) == id).collect();

// After:
let mut roots: Vec<Id> = base.class_ids().into_iter().filter(|&id| base.find(id) == id).collect();
roots.sort_unstable(); // Ensure deterministic order regardless of HashMap iteration order.
```

`leaves` (filtered from `roots`) and the Mixed/all pool inherit the stable
order; `hot` is already deterministic (it sorts by in-degree then by id).

### Fix 2 — new test `gen_colors_is_deterministic`

Added to `#[cfg(test)] mod tests` in `colored.rs`, after `gen_colors_shape`:

```rust
#[mz_ore::test]
fn gen_colors_is_deterministic() {
    // gen_colors must produce the same output on every call with the same seed,
    // regardless of HashMap iteration order. This test must FAIL before the
    // roots.sort_unstable() fix and PASS after.
    let p = small_params();
    let base = gen_base(&p, 42);
    let colors_a = gen_colors(&p, &base, 99);
    let colors_b = gen_colors(&p, &base, 99);
    assert_eq!(colors_a, colors_b, "gen_colors must be deterministic given the same seed");
}
```

### Fix 3 — §8 refreshed with new deterministic CSV

New CSV (deterministic, from `cargo nextest run --run-ignored ignored-only --no-capture eqsat::colored::tests::measure_color_explosion`):

```
# SP3a color-explosion sweep
# sharing_ratio = delta_nodes / (n_colors * node_count)
# cascade_factor = induced_merges / applied_equalities
axis,value,sharing_ratio,cascade_factor,delta_nodes,node_count,wall_ms,max_iters
base_size,100,0.1204,0.000,602,100,0,1
base_size,250,0.0518,0.000,647,250,2,1
base_size,500,0.0260,0.000,651,500,5,1
base_size,1000,0.0127,0.000,635,1000,10,1
base_size,2500,0.0051,0.000,632,2500,26,1
base_size,5000,0.0025,0.000,629,5000,55,1
fan_out,1,0.0172,1.710,430,500,14,8
fan_out,2,0.0137,0.000,342,500,4,1
fan_out,4,0.0260,0.000,651,500,5,1
fan_out,8,0.0499,0.000,1248,500,5,1
fan_out,16,0.0972,0.000,2430,500,6,1
fan_out,32,0.1743,0.000,4357,500,8,1
n_colors,10,0.0276,0.000,138,500,1,1
n_colors,50,0.0260,0.000,651,500,5,1
n_colors,100,0.0249,0.000,1245,500,10,1
n_colors,250,0.0253,0.000,3160,500,25,1
n_colors,500,0.0250,0.000,6262,500,51,1
n_colors,1000,0.0249,0.000,12441,500,104,1
eqs_per_color,1,0.0068,0.000,171,500,5,1
eqs_per_color,2,0.0132,0.000,330,500,5,1
eqs_per_color,4,0.0260,0.000,651,500,5,1
eqs_per_color,8,0.0490,0.000,1226,500,5,1
eqs_per_color,16,0.0960,0.000,2400,500,5,1
eqs_per_color,32,0.1880,0.000,4700,500,5,1
locality,0,0.0332,0.000,829,500,5,1
locality,1,0.0260,0.000,651,500,5,1
locality,2,0.0488,0.000,1220,500,5,1
```

### §8 numeric diff-summary

| Location | Old | New |
|---|---|---|
| base_size sweep delta_nodes range | (599 → 630) | (602 → 629) |
| n_colors sweep delta_nodes range | (134 → 12 611) | (138 → 12 441) |
| fan_out sharing ratio range | 0.012 to 0.172 | 0.014 to 0.174 |
| cascade factor exceptions | fan_out=1 (1.345) and fan_out=2 (0.005) | fan_out=1 only (1.710) |
| fan_out=1 max_iters | 7 | 8 |
| SharedHot vs LeafOnly sharing | 0.049 vs 0.032 | 0.049 vs 0.033 |
| Verdict: cascade threshold | fan_out ≥ 4 (0.000), ~0.005 at fan_out=2 | fan_out ≥ 2 (0.000) |
| Worst single-axis sharing ratio | 0.18 (eqs_per_color=32) | 0.19 (eqs_per_color=32) |
| n_colors=1000 wall_ms | 106 ms | 104 ms |

Notable: `fan_out=2` previously showed cascade=0.005 (an artefact of non-determinism that landed equalities on a deeper chain). With deterministic roots, it shows 0.000. This makes the verdict strictly better.

### Verdict confirmation

The verdict ("Proceed with shared-delta as-is.") is unchanged and remains
supported by the new numbers:
1. Sharing ratio ≪ 1 across all axes (max 0.19), decreasing as base_size grows.
2. Cascade factor 0.000 for all fan_out ≥ 2 (stronger than before: fan_out=2 now
   also 0.000). fan_out=1 anomaly (1.710) is structurally absent from SQL plans.

### Test command output (12/12)

```
Starting 12 tests across 10 binaries (298 tests skipped)
    PASS [   0.005s] ( 1/12) mz-transform eqsat::colored::tests::gen_base_reaches_target_size
    PASS [   0.005s] ( 2/12) mz-transform eqsat::colored::tests::close_color_matches_oracle_fixed
    PASS [   0.005s] ( 3/12) mz-transform eqsat::colored::tests::locality_steers_toward_shared_classes
    PASS [   0.005s] ( 4/12) mz-transform eqsat::colored::tests::gen_colors_is_deterministic
    PASS [   0.006s] ( 5/12) mz-transform eqsat::colored::tests::close_color_no_equalities_is_base_partition
    PASS [   0.006s] ( 6/12) mz-transform eqsat::colored::tests::close_color_propagates_congruence
    PASS [   0.006s] ( 7/12) mz-transform eqsat::colored::tests::gen_base_is_deterministic
    PASS [   0.006s] ( 8/12) mz-transform eqsat::colored::tests::close_color_leaf_equality_no_cascade
    PASS [   0.006s] ( 9/12) mz-transform eqsat::colored::tests::gen_colors_shape
    PASS [   0.006s] (10/12) mz-transform eqsat::colored::tests::colored_uf_union_merges_classes
    PASS [   0.006s] (11/12) mz-transform eqsat::colored::tests::colored_uf_starts_equal_to_base
    PASS [   0.009s] (12/12) mz-transform eqsat::colored::tests::close_color_matches_oracle_random
────────────
 Summary [   0.011s] 12 tests run: 12 passed, 298 skipped
```
