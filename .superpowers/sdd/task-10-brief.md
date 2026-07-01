## Task 10: Colored relational candidates in extraction

**Files:**
- Modify: `src/transform/src/eqsat/extract.rs` (`build_rel` — where `resolve_scalar_colored` is called)
- Modify: `src/transform/src/eqsat/colored_derive.rs` (add a per-class colored-conclusion enumerator)
- Test: `extract.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `ColoredEGraph::extract_colored<C: colored::CostModel<L>>` (colored/extract.rs:29); the real `cost::CostModel` (cost.rs:338).
- Produces: in `build_rel`, when expanding a relational class `id` under `color_of[id]`, the candidate set includes the **cheapest colored conclusion** for that class (a colored delta e-node), costed in the final choice by the real `cost::CostModel` on the built subtree.

Approach (mirrors SP4b `resolve_scalar_colored`, lifted to relational):
- Add `fn colored_rel_conclusion(layer, base, color, id) -> Option<ENode>` that runs `layer.ceg.extract_colored(color, &TreeSizeColoredCost)` once per color (cache per color), returning the lowest-tree-size colored e-node for `id`'s colored class if it differs from the base nodes. `TreeSizeColoredCost` is a `colored::CostModel<CombinedLang>` scoring `node_count`-style (relational nodes count 1 + children), the colored analogue of SP4b's scalar `scalar_expr_cost` selection.
- In `build_rel`, when choosing the best e-node for `id`, append the colored conclusion (if any) to the candidate e-nodes; the existing real-cost selection then picks base-or-colored by the true cost. This retires `extract_colored` (now production-called) while keeping the final decision on `cost::CostModel`.

- [ ] **Step 1: Write the failing test** — extraction prefers a colored conclusion when it is cheaper:

```rust
#[mz_ore::test]
fn extraction_picks_colored_conclusion_when_cheaper() {
    // Filter(#0 = #1)(leaf) with a color #0 ≅ #1 and colored_saturate run so the
    // filter's class has a colored conclusion == leaf (filter removed). Extraction
    // under that color must return the bare leaf (cheaper) not the Filter.
    // ... build eg + layer, run colored_saturate, then extract_with(root,...,Some(&mut layer)) ...
    let rel = eg.extract_with(root, &model, &ArrangementCount, Some(&mut layer)).unwrap();
    assert!(matches!(rel, Rel::Constant { .. })); // filter elided under context
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform extraction_picks_colored_conclusion_when_cheaper`
Expected: FAIL.

- [ ] **Step 3: Implement** `TreeSizeColoredCost`, `colored_rel_conclusion`, and the `build_rel` candidate extension.

- [ ] **Step 4: Run to verify it passes**

Run: `bin/cargo-test -p mz-transform extraction_picks_colored`
Expected: PASS.

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/extract.rs src/transform/src/eqsat/colored_derive.rs
git commit -m "SP4d P5: colored relational conclusions as extraction candidates"
```

