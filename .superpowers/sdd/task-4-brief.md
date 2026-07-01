## Task 4: Add the `colored` rule attribute (DSL → codegen → rules table)

**Files:**
- Modify: `src/transform/build/grammar.rs` (rule header ~583-600, after the `phase` parse)
- Modify: `src/transform/src/eqsat/dsl.rs` (`Rule` struct ~344-353: add `pub colored: bool`)
- Modify: `src/transform/build/codegen.rs` (derive + emit `colored` into the table; build-time assertion)
- Modify: `src/transform/src/eqsat/rules.rs` (`CompiledRule` gains `pub(crate) colored: bool`; add `CompiledRuleSet::colored_rules()`)
- Modify: `src/transform/src/eqsat/rules/relational.rewrite` (tag the targeted subset)
- Test: `rules.rs` `#[cfg(test)]`

**Interfaces:**
- Produces: `CompiledRuleSet::colored_rules(&self) -> Vec<&'static CompiledRule>` returning the tagged subset; build-time guarantee that a colored rule's conds are color-exact.

The targeted subset to tag (`relational.rewrite`, names verbatim): predicate-simplification — `drop_true_filter`, `empty_false_filter`; filter-structural — `merge_filters`, `push_filter_through_map`, `push_filter_past_flatmap`, `push_filter_through_negate`, `push_filter_through_threshold`, `push_filter_past_project`, `push_filter_into_join_first`, `push_filter_into_join_second`, `distribute_filter_union`, `distribute_filter_union_nary`. (Confirm each exists by name; tag only those whose conds are color-exact.)

Color-exact cond set (from the EGraph-surface map): all free-fn conds (`all_true`/`any_false`/`no_false`/`no_error`/`all_columns`/`uses_only_input`/`cols_in_range`/`identity_projection`), plus `not_rel_empty`/`is_rel_empty`/`has_three_or_more_inputs`/`is_binary_join`/`reads_indexed_global`/`has_inner_equiv`/`produces_key`/`join_is_cyclic`. **Excluded** (analysis-gated): `non_negative`, `monotonic`, `is_unique_key`.

- [ ] **Step 1: Write the failing test** (`rules.rs`)

```rust
#[mz_ore::test]
fn colored_rules_are_tagged_subset() {
    let set = all();
    let names: std::collections::BTreeSet<_> =
        set.colored_rules().iter().map(|r| r.name).collect();
    assert!(names.contains("drop_true_filter"));
    assert!(names.contains("merge_filters"));
    assert!(names.contains("push_filter_through_map"));
    // An analysis-gated rule must never be colored.
    assert!(!names.iter().any(|n| *n == "some_key_rule")); // replace with a real key/monotonic rule name
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform colored_rules_are_tagged_subset`
Expected: FAIL — `colored_rules` not found.

- [ ] **Step 3: Implement the attribute and the build-time assertion**

Grammar (`grammar.rs`): after `.then(kw("phase").ignore_then(phasekw).or_not())` add `.then(kw("colored").or_not().map(|o| o.is_some()))` and thread `colored` into the `Rule { .. }` map. (Order the parse so `colored` follows `phase`.)

`dsl.rs`: add `pub colored: bool` to `Rule`; default `false` everywhere a `Rule` is constructed in tests.

`codegen.rs`: in the table emission, add `colored: {}` from `r.colored`. Add a build-time assertion loop:
```rust
const COLOR_EXACT: &[&str] = &["all_true","any_false","no_false","no_error","all_columns",
  "uses_only_input","cols_in_range","identity_projection","not_rel_empty","is_rel_empty",
  "has_three_or_more_inputs","is_binary_join","reads_indexed_global","has_inner_equiv",
  "produces_key","join_is_cyclic"];
for r in rules {
    if r.colored {
        for c in &r.conds {
            assert!(cond_is_color_exact(c),
                "colored rule `{}` uses non-color-exact condition {:?}", r.name, c);
        }
    }
}
```
where `cond_is_color_exact` matches the `Cond::*` variants against the excluded analysis trio (`NonNegative`/`Monotonic`/`IsUniqueKey`) → false, all else → true.

`rules.rs`: add `pub(crate) colored: bool` to `CompiledRule`; add:
```rust
pub(crate) fn colored_rules(&self) -> Vec<&'static CompiledRule> {
    self.rules.iter().copied().filter(|r| r.colored).collect()
}
```

`relational.rewrite`: add the `colored` keyword to each tagged rule header (after `phase` if present, else after `doc`/before the pattern).

- [ ] **Step 4: Run to verify it passes**

Run: `bin/cargo-test -p mz-transform colored_rules_are_tagged_subset`
Expected: PASS.

- [ ] **Step 5: Full suite stays byte-identical** (tagging adds a field, no behavior change yet)

Run: `bin/cargo-test -p mz-transform`
Expected: PASS, same count; zero golden diffs.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/build/grammar.rs src/transform/build/codegen.rs src/transform/src/eqsat/dsl.rs src/transform/src/eqsat/rules.rs src/transform/src/eqsat/rules/relational.rewrite
git commit -m "SP4d P2: colored rule attribute + colored_rules() + build-time color-exact assertion"
```

