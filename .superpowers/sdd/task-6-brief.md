## Task 6: Colored rule driver `colored_saturate`

**Files:**
- Create: `src/transform/src/eqsat/colored/saturate.rs`
- Modify: `src/transform/src/eqsat/colored.rs` (`mod saturate;`), `src/transform/src/eqsat/egraph.rs` (define `COLORED_MAX_ITERS`)
- Modify: `src/transform/build/codegen.rs` (emit `find_NAME_colored`/`apply_NAME_colored` for tagged rules; add `find_colored`/`apply_colored` to a `ColoredRule` table)
- Modify: `src/transform/src/eqsat/rules.rs` (`ColoredRule { name, find_colored, apply_colored }`; `colored_rules()` returns `&'static ColoredRule`)
- Test: `colored/saturate.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `ColoredView` (Task 5), `colored_rules()` (Task 4), the emitted colored fn pointers.
- Produces: `pub(crate) fn colored_saturate(layer: &mut ColoredLayer<'_>, base: &EGraph, rules: &[ColoredRule]) -> usize` running each color's tagged subset to a bounded fixpoint; and `CompiledRuleSet::colored_rules_static(&self) -> &'static [ColoredRule]` returning the tagged colored-fn-pointer table (distinct from Task 4's `colored_rules()`, which returns `&'static CompiledRule`s for identification/tests).

Driver structure (per color, parent-first via `layer.ceg` color order):
```rust
pub(crate) fn colored_saturate(layer: &mut ColoredLayer<'_>, base: &EGraph,
                               rules: &[ColoredRule]) -> usize {
    let mut total = 0;
    for &color in &layer.all_colors_parent_first() {       // Task 7 supplies order; flat = decl order
        for _ in 0..COLORED_MAX_ITERS {
            let mut view = ColoredView::new(&mut layer.ceg, color, base); // builds the per-round index
            let mut pending: Vec<(usize, EBindings)> = Vec::new();
            let an = Analyses::default();                  // colored rules read no analyses
            for (qi, rule) in rules.iter().enumerate() {
                let (ms, _hit) = (rule.find_colored)(&mut view, &an, MATCH_LIMIT + 1);
                for b in ms.into_iter().take(MATCH_LIMIT) { pending.push((qi, b)); }
            }
            let mut changed = false;
            let mut equalities: Vec<(Id, Id)> = Vec::new();
            for (qi, b) in pending {
                if let Ok(new_id) = (rules[qi].apply_colored)(&mut view, &b) {
                    equalities.push((new_id, b.root));
                }
                // delta-node budget guard
                if view.ceg.colored_node_count(color) > MAX_ENODES { break; }
            }
            // Apply unions + close congruence in the color.
            let metrics = layer.ceg.close(color, &equalities);
            if metrics.induced_merges > 0 || metrics.applied_equalities > 0 { changed = true; }
            total += 1;
            if !changed { break; }
        }
    }
    total
}
```
Notes: `find_colored` takes `&mut ColoredView` because building matches may need `ceg.find`/`canon` (which are `&mut self` on `ColoredEGraph`). `apply_colored` adds colored nodes (mutates). The match-then-apply split avoids borrow conflicts within a round. `colored_node_count(color)` is a small accessor to add on `ColoredEGraph` (count of the color's delta nodes); `all_colors_parent_first()` returns flat-color order in P2 (replaced by forest pre-order in P3).

Codegen for colored variants: emit `fn find_NAME_colored(g: &mut ColoredView, an: &Analyses, limit) -> (Vec<EBindings>, bool)` and `fn apply_NAME_colored(g: &mut ColoredView, b) -> Result<Id, String>` using the **same body generator** as the base variants but with the colored graph type and `&mut` receiver for find (since colored reads mutate the layered UF). Only emit these for `r.colored` rules.

- [ ] **Step 1: Write the failing test** — a contextual simplification produces a colored conclusion (`drop_true_filter` under a context that makes a predicate true):

```rust
#[mz_ore::test]
fn colored_saturate_simplifies_under_context() {
    // Build Filter(#0 = #1)(leaf arity 2); under color #0 ≅ #1 the predicate
    // (#0 = #1) reduces; drop_true_filter then removes the filter in the color.
    let mut eg = EGraph::new();
    let leaf = eg.add(CNode::Rel(ENode::Constant { card: 1, arity: 2, col_types: None }));
    let eqp = eg.intern_scalar(&EScalar::plain(
        MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), mz_expr::BinaryFunc::Eq)));
    let filt = eg.add(CNode::Rel(ENode::Filter { predicates: vec![eqp], input: leaf }));
    eg.rebuild();
    // One flat color asserting #0 ≅ #1.
    let mut layer = /* build a ColoredLayer with one color unioning escalar(#0),escalar(#1) */;
    let n = colored_saturate(&mut layer, &eg, &CompiledRuleSet::all().colored_rules_static());
    assert!(n > 0);
    // Under the color, `leaf` is reachable as a colored conclusion of `filt`'s class.
    let members = layer.ceg.colored_class_members(layer.color_of[&eg.find(filt)], eg.find(filt));
    assert!(members.contains(&eg.find(leaf)));
}
```
(Adjust helper construction to the real `ColoredLayer` builder; the assertion is that the filter's class colored-merges with its input under the context.)

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform colored_saturate_simplifies_under_context`
Expected: FAIL — `colored_saturate` not found.

- [ ] **Step 3: Implement** the driver, the colored codegen variants, `ColoredRule` table, `colored_node_count`, `COLORED_MAX_ITERS`.

- [ ] **Step 4: Run to verify it passes**

Run: `bin/cargo-test -p mz-transform colored_saturate`
Expected: PASS.

- [ ] **Step 5: Full suite still byte-identical** (driver not yet wired into engine)

Run: `bin/cargo-test -p mz-transform`
Expected: PASS, same count, zero golden diffs.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/colored/saturate.rs src/transform/src/eqsat/colored.rs src/transform/src/eqsat/egraph.rs src/transform/build/codegen.rs src/transform/src/eqsat/rules.rs
git commit -m "SP4d P2: colored_saturate driver + colored rule codegen variants"
```

---

# Phase P3 — hierarchical colors + remove-in-close

