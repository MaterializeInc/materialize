### Task 3: Flag + threading + `raise` routing

**Files:**
- Modify: `src/sql/src/session/vars/definitions.rs` (system var + binding)
- Modify: `src/repr/src/optimize.rs` (`OptimizerFeatures` field + macro)
- Modify: `src/sql/src/plan/statement/dml.rs` (exhaustive override)
- Modify: `src/sql/src/plan/statement/ddl.rs` (exhaustive destructure)
- Modify: `src/transform/src/eqsat/transform.rs` (read flag, pass it)
- Modify: `src/transform/src/eqsat.rs` (`optimize_with_availability` /
  `optimize_inner` thread the flag; update the other entry points + callers)
- Modify: `src/transform/src/eqsat/raise.rs` (`raise` / `raise_inner` thread the
  flag; `Rel::Join` arm routes through the emitter)
- Modify: `src/transform/src/eqsat/validation.rs` (caller of
  `optimize_with_availability`)
- Modify: `test/sqllogictest/transform/eqsat_delta_join_cost.slt`

**Interfaces:**
- Consumes: `CostModel::binary_join_order` (Task 1), `commit_differential`
  (Task 2), existing `per_input_available` (used in `raise.rs`'s WcoJoin arm).
- Produces: the `enable_eqsat_native_join_commit` optimizer feature flag and the
  wired routing. Consumed by Task 4's JI guard.

- [ ] **Step 1: Add the system variable**

In `src/sql/src/session/vars/definitions.rs`, immediately after the
`enable_eqsat_delta_join_cost` definition block (ends ~line 2140), add:

```rust
    {
        name: enable_eqsat_native_join_commit,
        // Defaulted on (in-branch): routes acyclic Rel::Join through a
        // cost-model-native Differential commit at eqsat raise time, so
        // JoinImplementation no-ops on eqsat joins and the spelling selector
        // survives. Primary use is easy A/B testing; flag-off is byte-identical
        // to the prior behavior.
        desc: "commit eqsat acyclic joins to a cost-model-chosen Differential at raise time",
        default: true,
        enable_for_item_parsing: false,
    },
```

- [ ] **Step 2: Add the `SystemVars -> OptimizerFeatures` binding**

In `src/sql/src/session/vars/definitions.rs`, next to line 2376
(`enable_eqsat_delta_join_cost: vars.enable_eqsat_delta_join_cost(),`), add:

```rust
            enable_eqsat_native_join_commit: vars.enable_eqsat_native_join_commit(),
```

- [ ] **Step 3: Add the `OptimizerFeatures` field**

In `src/repr/src/optimize.rs`, next to the `enable_eqsat_delta_join_cost` field
(line 162), add — matching the surrounding `optimizer_feature_flags!` macro
syntax (copy the exact shape of the neighbouring entry, including any comment
and trailing comma):

```rust
    /// Bound from `SystemVars::enable_eqsat_native_join_commit`. Commit eqsat
    /// acyclic joins to a Differential at raise time so JoinImplementation
    /// no-ops on them.
    enable_eqsat_native_join_commit: bool,
```

- [ ] **Step 4: Add the exhaustive override sites**

In `src/sql/src/plan/statement/dml.rs`, next to line 646
(`enable_eqsat_delta_join_cost: Default::default(),`), add:

```rust
                enable_eqsat_native_join_commit: Default::default(),
```

In `src/sql/src/plan/statement/ddl.rs`, next to line 4961
(`enable_eqsat_delta_join_cost: _,`), add:

```rust
                enable_eqsat_native_join_commit: _,
```

- [ ] **Step 5: Verify the flag compiles end-to-end (no behavior yet)**

Run: `cargo check -p mz-repr -p mz-sql`
Expected: compiles. (If `cargo check` complains about `METADATA_BACKEND_URL`,
prefix with the env vars from the mz-test skill, or skip to Step 11's build.)

- [ ] **Step 6: Thread the flag through `eqsat.rs`**

In `src/transform/src/eqsat.rs`:

`optimize_with_availability` (line 126) — add a parameter and forward it:

```rust
pub fn optimize_with_availability(
    expr: MirRelationExpr,
    available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    seeds: Vec<egraph::IndexedFilterSeed>,
    use_ilp: bool,
    use_delta: bool,
    native_join_commit: bool,
) -> MirRelationExpr {
    let rules = default_ruleset().for_phase(dsl::Phase::Physical);
    optimize_inner(
        expr, true, available, rules, true, seeds, use_ilp, false, use_delta, native_join_commit,
    )
}
```

`optimize_inner` (line 181) — add the parameter and pass it to `raise::raise`:

```rust
fn optimize_inner(
    expr: MirRelationExpr,
    commit_wcoj: bool,
    available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    rules: rules::CompiledRuleSet,
    union_let_defs: bool,
    seeds: Vec<egraph::IndexedFilterSeed>,
    use_ilp: bool,
    enable_wmr_lift: bool,
    use_delta: bool,
    native_join_commit: bool,
) -> MirRelationExpr {
```

and change the raise call (line 225):

```rust
    let mut raised = raise::raise(&best, commit_wcoj, &available_for_raise, native_join_commit);
```

Update the other `optimize_inner` callers in this file to pass `false` (the
logical phase never commits): `optimize` (line 72 body), `optimize_logical`
(line 147), `optimize_with_wmr_lift` (line 168). Each currently ends its
argument list with `..., false)` for `use_delta`; append `, false`.

- [ ] **Step 7: Thread the flag through `raise.rs`**

In `src/transform/src/eqsat/raise.rs`:

`raise` (line 66) — add the parameter and forward it:

```rust
pub fn raise(
    rel: &Rel,
    commit_wcoj: bool,
    available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    native_join_commit: bool,
) -> MirRelationExpr {
    let mut scope = BTreeMap::new();
    let mut raised = raise_inner(rel, commit_wcoj, available, native_join_commit, &mut scope);
    // ... rest unchanged
```

`raise_inner` (line 85) — add the parameter; the local `raise` closure
(line 91-93) captures it automatically, so update only its body call:

```rust
fn raise_inner(
    rel: &Rel,
    commit_wcoj: bool,
    available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    native_join_commit: bool,
    scope: &mut BTreeMap<usize, ReprRelationType>,
) -> MirRelationExpr {
    let raise = |r: &Rel, scope: &mut BTreeMap<usize, ReprRelationType>| {
        raise_inner(r, commit_wcoj, available, native_join_commit, scope)
    };
```

- [ ] **Step 8: Route `Rel::Join` through the emitter**

Replace the `Rel::Join` arm (`raise.rs:134-146`) with:

```rust
        Rel::Join {
            inputs,
            equivalences,
        } => {
            // `join_scalars` intentionally drops constant-singleton (arity-0,
            // 1-row) inputs that act as join identities, so a round-trip is not
            // byte-identical for such inputs but is arity- and
            // semantics-preserving.
            let raised_inputs: Vec<MirRelationExpr> =
                inputs.iter().map(|r| raise(r, scope)).collect();
            let join = MirRelationExpr::join_scalars(
                raised_inputs.clone(),
                equivalences.iter().map(|class| resolve(class)).collect(),
            );
            // Physical phase only: commit acyclic joins to a cost-model-chosen
            // Differential so JoinImplementation no-ops on them and the spelling
            // selector survives. On any failure, fall back to the bare
            // Unimplemented join and let JoinImplementation handle it.
            if !commit_wcoj || !native_join_commit {
                return join;
            }
            let model = if available.is_empty() {
                crate::eqsat::cost::CostModel::new()
            } else {
                crate::eqsat::cost::CostModel::with_available(available.clone())
            };
            let Some(order) = model.binary_join_order(inputs, equivalences) else {
                return join;
            };
            let per_input = per_input_available(&raised_inputs, available);
            crate::eqsat::join_commit::commit_differential(join.clone(), order, &per_input)
                .unwrap_or(join)
        }
```

> `per_input_available` is already imported/used by the `WcoJoin` arm in this
> file; reuse it as-is. If the `Rel::Join` arm is reached via the local `raise`
> closure that returns the arm value (not via early `return`), the early
> `return`s above still return from `raise_inner` — matching the `WcoJoin`
> arm's `return join;` at line 269.

- [ ] **Step 9: Update `transform.rs` to pass the real flag**

In `src/transform/src/eqsat/transform.rs`, line 170, change the call to:

```rust
        let out = crate::eqsat::optimize_with_availability(
            relation.clone(),
            available,
            seeds,
            use_ilp,
            use_delta,
            ctx.features.enable_eqsat_native_join_commit,
        );
```

(Adjust to the exact existing binding/format at that site; the new last argument
is `ctx.features.enable_eqsat_native_join_commit`.)

- [ ] **Step 10: Update the remaining `optimize_with_availability` callers**

Add a trailing `, false` to every other call (test/util callers keep the
feature off): `src/transform/src/eqsat/transform.rs` lines 514, 537, 560, 595,
628, and any call in `src/transform/src/eqsat/validation.rs`. Find them with:

```bash
grep -rn "optimize_with_availability(" src/transform/src/eqsat/
```

- [ ] **Step 11: Build the transform crate**

Run: `bin/cargo-test -p mz-transform --no-run`
Expected: compiles (no unused-parameter or arity errors).

- [ ] **Step 12: Update the slt and capture the new plan**

In `test/sqllogictest/transform/eqsat_delta_join_cost.slt`:
1. Replace the header comment block (lines 3-20) with a short note that the
   flag-on plan now commits the VOJ as a **differential** with no cross, while
   flag-off (the prior delta cross) is unchanged.
2. Before the "flag ON" `EXPLAIN`, also set the new flag on:

```
simple conn=mz_system,user=mz_system
ALTER SYSTEM SET enable_eqsat_native_join_commit = true
----
COMPLETE 0
```

3. For the "flag OFF" section, also turn the new flag off so it asserts the
   prior cross behavior:

```
simple conn=mz_system,user=mz_system
ALTER SYSTEM SET enable_eqsat_native_join_commit = false
----
COMPLETE 0
```

4. Regenerate the expected EXPLAIN output:

Run: `bin/sqllogictest -- --rewrite-results test/sqllogictest/transform/eqsat_delta_join_cost.slt`
Expected: the file is rewritten with actual plans.

- [ ] **Step 13: Verify the cross is gone (flag on) and present (flag off)**

Run:
```bash
bin/sqllogictest -- test/sqllogictest/transform/eqsat_delta_join_cost.slt
grep -n "type=differential\|type=delta\|:t1\[×\]" test/sqllogictest/transform/eqsat_delta_join_cost.slt
```
Expected: the slt PASSES; the **flag-on** EXPLAIN shows `type=differential` and
contains **no** `:t1[×]` cross; the **flag-off** EXPLAIN still shows
`type=delta` with `%2 » %0:t1[×] » %1[#0]K`. If the flag-on plan still contains
`:t1[×]`, STOP — the commit did not take effect (check that all three flags —
`enable_eqsat_physical_optimizer`, `enable_eqsat_delta_join_cost`,
`enable_eqsat_native_join_commit` — are on for that query).

- [ ] **Step 14: Commit**

```bash
git add src/sql/src/session/vars/definitions.rs src/repr/src/optimize.rs \
  src/sql/src/plan/statement/dml.rs src/sql/src/plan/statement/ddl.rs \
  src/transform/src/eqsat.rs src/transform/src/eqsat/raise.rs \
  src/transform/src/eqsat/transform.rs src/transform/src/eqsat/validation.rs \
  test/sqllogictest/transform/eqsat_delta_join_cost.slt
git commit -m "eqsat: route Rel::Join through cost-model-native Differential commit (flag enable_eqsat_native_join_commit)"
```

---

