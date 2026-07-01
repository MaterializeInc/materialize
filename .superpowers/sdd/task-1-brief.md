### Task 1: Surface the cost-model join order (`binary_join_order`)

**Files:**
- Modify: `src/transform/src/eqsat/cost.rs` (add `JoinOrder`, `JoinStep`,
  `CostModel::binary_join_order`, free fns `best_left_deep_sequence`,
  `frontier_key_cols`; add tests to the existing `mod tests`).

**Interfaces:**
- Consumes: existing `CostModel` internals — `size_degree` (`cost.rs:301`),
  `Hypergraph::build` (`1194`), `intern_hg` (`643`), `agm_degree_subset_memo`
  (`665`), `MAX_EXACT_JOIN_INPUTS` (`71`), `terms_cost`, `join_key_cols_for_input`
  (`877`), `Rel::arity` (`ir.rs:343`), `EScalar::cols`.
- Produces: `pub(crate) struct JoinOrder { pub steps: Vec<JoinStep> }`,
  `pub(crate) struct JoinStep { pub input: usize, pub key_cols: BTreeSet<usize> }`,
  and `pub(crate) fn CostModel::binary_join_order(&self, inputs: &[Rel],
  equivalences: &[Vec<EScalar>]) -> Option<JoinOrder>` (`steps[0]` is the start;
  each `key_cols` is local to its input). Consumed by Task 2's emitter and
  Task 3's routing.

- [ ] **Step 1: Write the failing tests**

Add to the `mod tests` block in `src/transform/src/eqsat/cost.rs`. If `get`,
`col`, `eq_expr` helpers already exist in that module, reuse them and drop the
duplicates; otherwise add these:

```rust
#[mz_ore::test]
fn binary_join_order_voj_local_is_keyed() {
    use mz_expr::{func, BinaryFunc};
    let get = |name: &str, arity: usize| Rel::Get { name: name.into(), arity };
    let col = |c: usize| EScalar::plain(MirScalarExpr::column(c));
    let eq = |a: usize, b: usize| {
        EScalar::plain(
            MirScalarExpr::column(a).call_binary(MirScalarExpr::column(b), BinaryFunc::Eq(func::Eq)),
        )
    };
    let model = CostModel::new();
    // VOJ shape: t1(2) cols 0-1, t2(3) cols 2-4, t3(3) cols 5-7.
    let inputs = vec![get("t1", 2), get("t2", 3), get("t3", 3)];
    // class {#0,#2}: t1.f0 = t2.f2 ; class {#5, eq(#2,#4)}: LOCAL spelling.
    let equivalences = vec![vec![col(0), col(2)], vec![col(5), eq(2, 4)]];

    let order = model
        .binary_join_order(&inputs, &equivalences)
        .expect("connected join has an order");

    assert_eq!(order.steps.len(), 3);
    // Every step after the start probes the frontier with a non-empty key:
    // a fully keyed left-deep order, no forced cross.
    for step in &order.steps[1..] {
        assert!(
            !step.key_cols.is_empty(),
            "step for input {} should be keyed, got empty key",
            step.input
        );
    }
}

#[mz_ore::test]
fn binary_join_order_disconnected_returns_order_with_cross() {
    let get = |name: &str, arity: usize| Rel::Get { name: name.into(), arity };
    let model = CostModel::new();
    let inputs = vec![get("a", 2), get("b", 2)];
    // No equivalences: the two inputs are unconnected -> the second step is a
    // cross (empty key). The order is still produced (does not panic / None).
    let equivalences: Vec<Vec<EScalar>> = vec![];

    let order = model
        .binary_join_order(&inputs, &equivalences)
        .expect("two inputs always yield an order");
    assert_eq!(order.steps.len(), 2);
    assert!(
        order.steps[1].key_cols.is_empty(),
        "disconnected step must be a cross (empty key)"
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bin/cargo-test -p mz-transform binary_join_order`
Expected: FAIL — `binary_join_order` / `JoinOrder` not found (does not compile).

- [ ] **Step 3: Add the `JoinOrder` / `JoinStep` types and helpers**

Add near the other free functions in `src/transform/src/eqsat/cost.rs` (after
`join_key_cols_for_input`, ~line 902). Ensure `use std::collections::BTreeSet;`
is in scope (it already is — `join_key_cols_for_input` uses it).

```rust
/// A chosen left-deep join order with per-step arrangement keys, surfaced from
/// the cost model so the eqsat emitter can build a `JoinImplementation::Differential`.
/// `steps[0]` is the starting input; `key_cols` are local to each step's input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinStep {
    /// Global input index (position in the join's `inputs`).
    pub input: usize,
    /// Column indices local to `input` forming this step's arrangement key.
    pub key_cols: BTreeSet<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinOrder {
    pub steps: Vec<JoinStep>,
}

/// Local key columns of the input at `[offset, offset+arity)` that are equated,
/// via some class in `equivalences`, to a column of an already-placed (frontier)
/// input. `placed` holds the `(offset, arity)` ranges of inputs placed so far.
fn frontier_key_cols(
    offset: usize,
    arity: usize,
    placed: &[(usize, usize)],
    equivalences: &[Vec<EScalar>],
) -> BTreeSet<usize> {
    let in_frontier = |c: usize| placed.iter().any(|&(o, a)| c >= o && c < o + a);
    let mut key_cols = BTreeSet::new();
    for class in equivalences {
        let mut local = Vec::new();
        let mut touches_frontier = false;
        for escalar in class {
            for col in escalar.cols() {
                if col >= offset && col < offset + arity {
                    local.push(col - offset);
                } else if in_frontier(col) {
                    touches_frontier = true;
                }
            }
        }
        if touches_frontier {
            key_cols.extend(local);
        }
    }
    key_cols
}

/// The input sequence (global indices) of the cheapest left-deep order under the
/// AGM-degree `terms_cost` objective — the same objective `DpSub` minimizes,
/// extended with backpointers so the order can be reconstructed. Mirrors the
/// `DpSub::work_terms` recurrence. Returns `None` only when `n == 0`.
fn best_left_deep_sequence(n: usize, agm: &dyn Fn(u32) -> f64) -> Option<Vec<usize>> {
    if n == 0 {
        return None;
    }
    let full = (1u32 << n) - 1;
    // best[S] = (cost terms, predecessor subset, last input added).
    let mut best: Vec<Option<(Vec<f64>, u32, usize)>> = vec![None; 1 << n];
    for i in 0..n {
        best[1 << i] = Some((vec![], 0, i)); // single input: no work, no predecessor
    }
    for s in 1..=full {
        if s.count_ones() < 2 {
            continue;
        }
        let agm_s = agm(s);
        let mut sub = s;
        while sub > 0 {
            let i = sub.trailing_zeros() as usize;
            let rest = s & !(1 << i);
            if rest != 0 {
                if let Some((rest_terms, _, _)) = &best[rest as usize] {
                    let mut cand = rest_terms.clone();
                    cand.push(agm_s);
                    let better = best[s as usize]
                        .as_ref()
                        .is_none_or(|(c, _, _)| terms_cost(&cand).lt(&terms_cost(c)));
                    if better {
                        best[s as usize] = Some((cand, rest, i));
                    }
                }
            }
            sub &= sub - 1;
        }
    }
    // Walk predecessors from the full set back to a singleton.
    let mut seq = Vec::with_capacity(n);
    let mut s = full;
    loop {
        let (_, pred, last) = best[s as usize].clone()?;
        seq.push(last);
        if pred == 0 {
            break;
        }
        s = pred;
    }
    seq.reverse();
    Some(seq)
}
```

- [ ] **Step 4: Add the `binary_join_order` method**

Add inside `impl CostModel` in `src/transform/src/eqsat/cost.rs` (next to
`binary_join_terms`, ~line 696):

```rust
/// Surface the cost-model-chosen left-deep join order with per-step local
/// arrangement keys, so the eqsat emitter can commit a `Differential` plan.
/// The order minimizes the same AGM-degree objective as `binary_join_terms`.
pub(crate) fn binary_join_order(
    &self,
    inputs: &[Rel],
    equivalences: &[Vec<EScalar>],
) -> Option<JoinOrder> {
    let n = inputs.len();
    if n == 0 {
        return None;
    }
    let arities: Vec<usize> = inputs.iter().map(|r| r.arity()).collect();
    let mut offsets = Vec::with_capacity(n);
    let mut acc = 0;
    for &a in &arities {
        offsets.push(acc);
        acc += a;
    }

    let seq: Vec<usize> = if n == 1 {
        vec![0]
    } else if n > MAX_EXACT_JOIN_INPUTS {
        // Wide-join fallback: a left-deep chain in input order, mirroring
        // `binary_join_terms`'s own wide-join fallback.
        (0..n).collect()
    } else {
        let degs: Vec<f64> = inputs.iter().map(|r| self.size_degree(r)).collect();
        let hg = Hypergraph::build(inputs, equivalences);
        let hg_id = self.intern_hg(&hg, &degs);
        let agm = |subset: u32| self.agm_degree_subset_memo(hg_id, &hg, &degs, subset);
        best_left_deep_sequence(n, &agm)?
    };

    let mut placed: Vec<(usize, usize)> = Vec::new();
    let mut steps = Vec::with_capacity(n);
    for (k, &i) in seq.iter().enumerate() {
        let key_cols = if k == 0 {
            // The start is arranged by its full join key (cols equated to any
            // other input); the first edge keys into it.
            join_key_cols_for_input(offsets[i], arities[i], equivalences)
        } else {
            frontier_key_cols(offsets[i], arities[i], &placed, equivalences)
        };
        steps.push(JoinStep { input: i, key_cols });
        placed.push((offsets[i], arities[i]));
    }
    Some(JoinOrder { steps })
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `bin/cargo-test -p mz-transform binary_join_order`
Expected: PASS (2 tests).

- [ ] **Step 6: Verify no existing cost tests regressed**

Run: `bin/cargo-test -p mz-transform eqsat::cost`
Expected: PASS (all cost-model tests, including the pre-existing ones).

- [ ] **Step 7: Commit**

```bash
git add src/transform/src/eqsat/cost.rs
git commit -m "eqsat: surface cost-model join order via binary_join_order"
```

---

