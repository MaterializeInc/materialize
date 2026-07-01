# Task 1h: rule contract refactor to enable nested-node construction

Part of Phase 1 of the scalar equality-saturation canonicalizer. This is a PURE
REFACTOR: it changes the rewrite-rule contract so rules can build nested new
nodes, and migrates all existing rules to the new contract WITHOUT changing their
behavior. No new rewrite rule. The De Morgan rule (next task) and Phase 2
factoring/absorption need this capability.

## The problem

The current contract is `pub type Rule = fn(eg: &ScalarEGraph, node: &SNode) -> Vec<SNode>`.
Rules are read-only and return one-level `SNode`s whose children are existing
class `Id`s; the saturate apply step does `add` + `union`. This cannot express a
rewrite whose result contains NEW intermediate nodes (De Morgan:
`NOT(AND(a,b)) -> OR(NOT a, NOT b)` needs to build the two `NOT` nodes).

## The new contract

Change to: `pub type Rule = fn(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id>`.

A rule now constructs whatever nodes it needs by calling `eg.add(..)` (which
hash-conses, so re-adding an existing node is a no-op returning its class) and
returns the e-class `Id`s that should be unioned with the source node's class.
A rule that unions with an existing class returns that class's id directly
(`eg.find(child)`), with no re-add.

Rules still read analyses (`eg.analysis`) and class contents (`eg.nodes`) before
mutating. The borrow discipline: read what you need into owned values first (drop
the immutable borrow), then call `eg.add`/`eg.find`.

## Saturate apply phase (egraph.rs)

The collect/apply split stays, but the apply phase now calls rules with `&mut
self`. Collect read-only `(Id, SNode)` snapshots first (clone the nodes), bounded
by `MATCH_LIMIT` exactly as now, then apply:

```rust
// Collect read-only snapshots of (class id, node).
let mut work: Vec<(Id, SNode)> = Vec::new();
'collect: for id in self.class_ids() {
    for node in self.nodes(id) {
        work.push((id, node));
        if work.len() >= MATCH_LIMIT {
            break 'collect;
        }
    }
}
// Apply: rules may now mutate (add nested nodes). Union each returned class
// into the source class.
let mut changed = false;
for (id, node) in work {
    for rule in rules::rules() {
        for new_id in rule(self, &node) {
            if self.union(id, new_id) {
                changed = true;
            }
        }
    }
}
```

NOTE the snapshot is taken BEFORE any mutation, so a snapshotted node's child ids
may become non-canonical as earlier rules union during this pass. That is fine:
`add` canonicalizes children via `find`, and `union(id, new_id)` canonicalizes
both ends. The top-of-loop `rebuild` restores full congruence before the next
pass, unchanged. Keep the `MAX_ENODES`/`MAX_ITERS` bounds and the
`if !changed break` exactly as now. Re-confirm the exact current saturate body
before editing.

Previously `MATCH_LIMIT` bounded the number of (id, new_node) rewrites; now it
bounds the number of (id, node) work items collected per pass. Update its doc
comment to match if needed.

## Migrating the existing rules (mechanical, behavior-preserving)

Every rule in `rules.rs` changes signature to `fn(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id>`.
The body changes are mechanical:

* A rule that returned `vec![]` still returns `vec![]`.
* A rule that returned `vec![some_snode]` now returns `vec![eg.add(some_snode)]`.
  Build `some_snode` (reading analyses first), then `eg.add` it. Affects:
  `const_fold`, `and_or_dedup`, `and_or_empty`, `and_or_short_circuit`,
  `and_or_drop_unit`, `not_binary_negate`, `null_prop_binary`,
  `null_prop_variadic`, `err_prop_binary`, `err_prop_variadic`, `if_err_cond`.
* A rule that returned `eg.nodes(child)` to union with an existing class now
  returns `vec![eg.find(child)]` directly (no re-add needed). Affects:
  `and_or_single`, `not_not`, `if_true`, `if_false_or_null`, `if_same_branches`.
  Confirm each: the returned id is the class you intend to union the source with.

Watch the borrow checker: where a rule reads `eg.analysis(..)`/`eg.nodes(..)` and
then calls `eg.add(..)`, finish the reads (clone out what you need) before the
mutable call. `call_scalar_type` currently takes `&ScalarEGraph`; if a caller now
holds `&mut`, a reborrow as `&*eg` or restructuring the call order resolves it.
Keep `call_scalar_type` taking `&ScalarEGraph`.

Helpers `lit_bool`, `lit_bool_or_null`, `is_literal_null`, `literal_err`,
`is_and_or`, `unit_node`, `destructure_literal`, `call_scalar_type` stay
`&ScalarEGraph` (read-only); only the `Rule` fns become `&mut`.

## Constraints (binding)

* PURE REFACTOR: no behavior change, no new rewrite rule. Every existing scalar
  test must pass UNCHANGED (same assertions). If a test needs editing beyond the
  rule-call mechanics, you changed behavior - stop and reconsider.
* Separate scalar engine; no relational code.
* No `as` conversions.
* Comments: no em-dashes, no clause-joining semicolons; doc states the contract.
* Keep the saturate bounds and fixpoint behavior identical.

## Tests

No new behavior, so no new tests. The gate is: ALL existing `eqsat::scalar` tests
pass unchanged (56 tests as of HEAD). Run the full scalar suite and confirm the
count and that none were modified except for any unavoidable rule-signature
mechanics in test helpers (there should be none, since tests drive `canonicalize`,
not rules directly).

If any test calls a rule function directly (grep for the rule names in the test
module), update only its call mechanics, not its assertions.

## Done criteria

* `cargo check -p mz-transform --tests` clean.
* `cargo clippy -p mz-transform --tests` clean.
* `cargo fmt -p mz-transform` applied.
* All prior scalar tests pass, unchanged in assertions.

## Commit

On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: rule contract returns class ids so rules can build nested nodes (Phase 1)`.
End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report

Write your full report to `.superpowers/sdd/scalar-task-1h-infra-report.md` and
return only: status, commit sha, one-line test summary (test count unchanged),
concerns.
