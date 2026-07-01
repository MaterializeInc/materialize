## Task 7b: Commutativity via a column-order-restoring Project

**Why:** Task 7's binarization re-associates the fixed input order but cannot
reorder inputs, so it is near-inert under the arrangement-count objective (it
only adds an intermediate join arrangement). The join-order value the spec wants
comes from commutativity. A naive `Join(a, b) = Join(b, a)` is unsound in an
e-graph: the two forms have different output column orders (`[a|b]` vs `[b|a]`),
so unioning them into one e-class lets the extractor return the commuted form to
a context whose Project/Map/Filter reference columns positionally, corrupting
them (confirmed by a real `Can't union scalar types` panic on the `mz_indexes`
catalog view). The fix wraps the commuted join in a projection that restores the
original column order, so both members of the e-class share one schema.

**Files:**
* Modify: `src/transform/src/eqsat/matcher.rs` (`swap_projection` helper; remove the `#[allow(dead_code)]` on `swap_equivs`)
* Modify: `src/transform/src/eqsat/dsl.rs`, `src/transform/build/grammar.rs`, `src/transform/build/codegen.rs`, `src/transform/src/eqsat/lean.rs` (wire `swap_projection` PExpr + the `is_binary_join` condition)
* Modify: `src/transform/src/eqsat/egraph.rs` (`is_binary_join` condition method)
* Modify: `src/transform/src/eqsat/rules/relational.rewrite` (`commute_binary_join` rule)
* Test: `src/transform/tests/wcoj_decision.rs`

**Interfaces:**
* Consumes: `swap_equivs` (Task 6), `Payload::Outputs`, `arity(..)`, `is_binary_join`.
* Produces:
  ```rust
  /// The projection that restores the original column order after swapping the
  /// first two join inputs (arities a, b). The commuted `Join(b, a)` outputs
  /// columns [b | a]; this maps them back to [a | b], so the projected result
  /// has the same schema as the original `Join(a, b)`.
  pub(crate) fn swap_projection(arity_a: i64, arity_b: i64) -> Result<Payload, String>;
  // returns Payload::Outputs((b..b+a).chain(0..b).collect())
  ```

- [ ] **Step 1: Write the failing unit test for `swap_projection`**

```rust
#[mz_ore::test]
fn swap_projection_restores_column_order() {
    // a-arity 2, b-arity 1: commuted [b | a] = [#0_b, #0_a, #1_a]; restore maps
    // back to [a | b] = positions [1, 2, 0].
    assert_eq!(
        swap_projection(2, 1).unwrap(),
        Payload::Outputs(vec![1, 2, 0]),
    );
}
```

Run `bin/cargo-test -p mz-transform eqsat::matcher::tests::swap_projection`, confirm FAIL, implement `swap_projection`, confirm PASS.

- [ ] **Step 2: Wire `swap_projection` (PExpr) and `is_binary_join` (Cond)**

`swap_projection(n, m)` is a two-IxExpr-arg template helper returning an `Outputs` payload, wired exactly like `swap_equivs` from Task 6 (dsl.rs PExpr variant, grammar parse, codegen dispatch, lean.rs opaque arm). `is_binary_join()` is a no-arg condition wired like `has_three_or_more_inputs` from Task 7 (it checks the matched join root's e-class for a Join node with exactly 2 inputs).

- [ ] **Step 3: Add the `commute_binary_join` rule**

```
rule commute_binary_join {
    doc "join(a, b) = project([restore], join(b, a)): reorder inputs, restore column order"
    Join[e](a, b)
        => Project[swap_projection(arity(a), arity(b))](
               Join[swap_equivs(e, arity(a), arity(b))](b, a))
    where is_binary_join()
}
```

Remove the `#[allow(dead_code)]` (and its comment) from `swap_equivs` in matcher.rs; it is now used.

- [ ] **Step 4: Verify termination**

`commute_binary_join` is its own inverse. Double-application gives `Project[P1](Project[P2](Join(a,b)))`, which `fuse_projects` collapses to `Project[compose(P2,P1)](Join(a,b)) = Project[identity](Join(a,b))`. `commute_binary_join` does not fire on a Project node and `fuse_projects` needs a nested Project, so no further nodes are generated; saturation converges (hash-consing + the `MAX_ENODES` cap). Add a test that an 8-way join (and a self-join) saturate without hitting the cap (assert iterations < max_iters or node count under cap). If the e-graph bloats, optionally add an identity-projection elimination rule (`Project[p] r => r` when `p` is the full identity projection), but this is not required for correctness.

- [ ] **Step 5: Regression: the panic must be gone**

The previous panic was on the `mz_indexes` catalog view at startup. Run the catalog explain golden, which exercises catalog views:

Run: `bin/sqllogictest --optimized -- test/sqllogictest/catalog_server_explain.slt`
Expected: PASS, no panic. If it panics or regresses, the Project-restore schema is wrong; debug the `swap_projection` column mapping before proceeding.

- [ ] **Step 6: Positive-utility test (addresses the Task 7 review gap)**

Build a join where reordering inputs aligns an available index so the commuted (Project-wrapped) form maintains fewer arrangements than the original order, and assert the optimizer extracts the reordered form. The assertion must inspect plan structure (the reordered join is chosen), not be a tautology. Use the `CostModel::with_available` + MIR/Rel builders the other `wcoj_decision.rs` tests use.

- [ ] **Step 7: Goldens + lint + commit**

Run the explain goldens (`optimized_plan_as_text.slt`, `ldbc_bi.slt`, `ldbc_bi_eager.slt`); accept only reuse-gain / arrangement-reduction diffs, listing each. `cargo fmt`, `bin/lint`, `cargo clippy -p mz-transform`. Commit:

```bash
git commit -m "transform: add schema-safe join commutativity to eqsat via a restoring projection"
```

---

