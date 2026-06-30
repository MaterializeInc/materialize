## Task 11: Wire `colored_saturate` into the engine (alongside; differential capture)

**Files:**
- Modify: `src/transform/src/eqsat/engine.rs` (3 sites: ~234, ~300, ~450; insert after `build_colored_layer`, before `extract`)
- Test: `engine.rs` `#[cfg(test)]` differential helper

**Interfaces:**
- Consumes: `colored_saturate` (Task 6), `ColoredLayer` (now hierarchical), the extended `extract_with` (Task 10).
- Produces: each site runs `colored_saturate(&mut layer, &eg, self.rules.colored_rules_static())` after `build_colored_layer` and before `extract`; close-all already ran in build_colored_layer (Task 7) or is invoked here.

At each site replace:
```rust
let mut layer = build_colored_layer(&eg, scopes);
```
with:
```rust
let mut layer = build_colored_layer(&eg, scopes);
colored_saturate(&mut layer, &eg, self.rules.colored_rules_static());
```
(`colored_rules_static()` returns `&'static [ColoredRule]` filtered to tagged, from Task 6; reachable via `self.rules` like the base rule set.)

- [ ] **Step 1: Capture the corpus differential (alongside, before goldens move)**

Add a test/debug path or a one-off harness that, for the eqsat datadriven corpus, records the extracted plan with colored saturation ON vs OFF (e.g. a `MZ_EQSAT_COLORED_SAT=0/1` env switch read once at engine construction). Run `REWRITE=0 bin/cargo-test -p mz-transform run_tests` with both settings and diff. **Do not regenerate yet.**

Run: `bin/cargo-test -p mz-transform run_tests 2>&1 | tail -40`
Expected: a list of `.spec` cases whose output changes (the differential to justify in Task 12).

- [ ] **Step 2: Implement the wiring** at all 3 sites.

- [ ] **Step 3: Run unit + colored tests (not goldens yet)**

Run: `bin/cargo-test -p mz-transform --lib`
Expected: PASS (unit tests; golden `.spec`/slt addressed in Task 12).

- [ ] **Step 4: Clippy + commit (goldens deliberately not yet regenerated)**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/engine.rs
git commit -m "SP4d P5: wire colored_saturate into the engine (alongside)"
```

