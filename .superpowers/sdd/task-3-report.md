# Task 3 Report: Codegen scalar match arms + `SCALAR_COMPILED_RULES` split

## Summary
Taught the build-time codegen (`src/transform/build/codegen.rs`) to emit a
scalar-unary matcher and to split scalar-root rules into a separate
`SCALAR_COMPILED_RULES` static, and closed out the two `lean.rs` match sites that
Task 2's new `Pat::SUnary` variant made non-exhaustive. Crate is GREEN.

## The five non-exhaustive `Pat` match sites handled
1. `codegen.rs::sym_name` (~L100) — added
   `Pat::SUnary { .. } => unreachable!("scalar patterns have no relational operator symbol")`.
   Scalar roots never reach `sym_name`; `find_stmts` routes them through the new
   scalar branch first, mirroring the existing `Pat::RelVar` unreachable.
2. `codegen.rs::Matcher::node` (~L142) — added the scalar-unary arm per brief:
   emits `let SNode::CallUnary { func: mz_expr::UnaryFunc::Not(_), expr: e{c} } = {node} else { continue };`
   then `self.child(input, "e{c}")`.
3. `codegen.rs::pat()` (~L1049) — the third codegen `Pat` match (AST-literal
   emitter for `rules_ast()`). Added a `Pat::SUnary { func, input }` arm emitting
   `crate::eqsat::dsl::Pat::SUnary { func: <str>, input: Box::new(<pat>) }`,
   consistent with the surrounding operator arms.
4. `lean.rs::collect_binders` (~L150) — added
   `Pat::SUnary { input, .. } => collect_binders(input, out, seen)`. A fixed
   scalar func binds no metavariable, so only its input contributes binders.
   This is correct and permanent.
5. `lean.rs::translate_pat` (~L218) — added
   `Pat::SUnary { .. } => todo!("scalar Lean translation lands in SP2b slice-1 Task 9")`
   with a comment noting `gen-lean` is not run until Task 9, so the stub is never
   reached before then.

## Other brief items implemented (codegen.rs)
- `unary_func_pat("not") -> "mz_expr::UnaryFunc::Not(_)"` helper next to
  `sym_name`, panicking on unknown keywords.
- Scalar root enumeration branch in `find_stmts`: `Pat::SUnary { .. }` iterates
  `g.nodes_by_scalar_sym(crate::eqsat::scalar::lang::ScalarSym::Unary)`, added
  before the relational `_ =>` arm.
- `Matcher::child` scalar branch: when the child pattern is `Pat::SUnary { .. }`
  it scans `g.scalar_class_nodes({class})`, otherwise `g.rel_class_nodes({class})`
  unchanged.
- `is_scalar_rule(r)` helper (`matches!(r.lhs, Pat::SUnary { .. })`).
- `SCALAR_COMPILED_RULES` split in `emit_compiled`: `COMPILED_RULES` now emits
  from `rules.iter().filter(|r| !is_scalar_rule(r))`, followed by a new
  `SCALAR_COMPILED_RULES` static from `rules.iter().filter(|r| is_scalar_rule(r))`.
  Both share the identical per-rule literal via the extracted
  `compiled_rule_literal(r)` helper. No `Tmpl` changes (a bare-metavar scalar RHS
  reuses the existing `Tmpl::RelVar` path).

## Byte-identity of relational `COMPILED_RULES`
The original inline per-rule `format!` was extracted verbatim into
`compiled_rule_literal(r)` (same format string, same argument order). The
`COMPILED_RULES` header text is unchanged, and the `!is_scalar_rule` filter
passes every existing relational rule through unchanged.

Confirmed against the freshly generated
`target/debug/build/mz-transform-*/out/eqsat_rules.rs`:
- `COMPILED_RULES` contains all 37 rules (unchanged count, all relational).
- `SCALAR_COMPILED_RULES` is present and EMPTY (`&[\n];`), as expected since no
  scalar rule exists yet (Task 7 adds `not_not`).
So the only additions to the generated output are the partition boundary and the
new empty second table. The relational block is byte-identical by construction.

## Green-build evidence
- `bin/fmt` — all tasks successful.
- `cargo check -p mz-transform` — `Finished dev profile`, no errors/warnings.

## Testing note
No runtime test is added here: no scalar rule exists yet, so
`SCALAR_COMPILED_RULES` is a valid empty table and the generated
`find_not_not_base`/`apply_not_not_base` do not exist until Task 7. Acceptance
for this task is the green build. Task 7 owns the first scalar rule and its
behavioral exercise. I did not invent a rule to test with.

## Self-review
- The scalar `find`/`child`/`node` string-emitting branches are never invoked at
  build time in this task (no rule has an `SUnary` lhs), so they cannot regress
  existing output. They compile as ordinary Rust string formatting. Their emitted
  Rust is first exercised by Task 7's build.
- Followed the brief's `Matcher::node` snippet verbatim, including passing
  `e{c}` (not `*e{c}`) to `self.child`. Since no scalar rule is compiled yet, the
  generated code for this path is not produced in this task; Task 7's build is the
  first to compile it.

## Concerns
- Potential latent `&Id` vs `Id` mismatch in the emitted scalar child loop: the
  `SNode::CallUnary { expr: e{c} }` binding over a `&SNode` may yield `&Id`, while
  `g.scalar_class_nodes(id)` takes `Id` by value. Match ergonomics may or may not
  coerce this. This is not observable until Task 7 compiles a scalar rule; the
  brief's verbatim snippet was used, and Task 7's exercising build will surface any
  fixup (deref to `*e{c}`) if needed. Flagging so Task 7 is aware.

## Review fix (Critical + Minor, applied post-review)

The concern flagged above was confirmed as a real Critical defect during joint
review of Tasks 2+3, plus one Minor cleanup. Both were in the `Pat::SUnary`
arm of `Matcher::node` in `src/transform/build/codegen.rs`. Fixed as follows.

### Critical: missing deref on the scalar child binding

Before:
```rust
Pat::SUnary { func, input } => {
    let c = self.fresh.id();
    let fpat = unary_func_pat(func);
    self.stmts.push(format!(
        "let SNode::CallUnary {{ func: {fpat}, expr: e{c} }} = {node} else {{ continue }};"
    ));
    self.child(input, &format!("e{c}"));
}
```

After:
```rust
Pat::SUnary { func, input } => {
    let fpat = unary_func_pat(func);
    self.stmts.push(format!(
        "let SNode::CallUnary {{ func: {fpat}, expr: e{c} }} = {node} else {{ continue }};"
    ));
    self.child(input, &format!("*e{c}"));
}
```

`{node}` is a `&SNode`, so match ergonomics bind `e{c}` as `&Id` even though
`Id` is `Copy`. `self.child` was passing that name straight through to
`g.scalar_class_nodes(<name>)` (takes `Id` by value) and to
`b.rels.insert(name, <name>)` (map value type `Id`), both of which would fail
to compile against a `&Id` once Task 7 emits a scalar rule and the scalar find
fn is actually generated. Every relational arm already dereferences its child
binding (`*in{c}`); the scalar arm was the only one missing the `*`. Changing
`e{c}` to `*e{c}` brings it in line with the relational convention.

### Minor: redundant fresh id shadowing the outer `c`

The same arm allocated its own `let c = self.fresh.id();`, shadowing the `c`
already bound at the top of `Matcher::node` (`let c = self.fresh.id();` before
the `match pat`). Every relational arm reuses that outer `c`; the SUnary arm
was the only one re-allocating its own. Removed the redundant allocation (see
diff above, the `let c = self.fresh.id();` line inside the arm is gone) so
`e{c}` now refers to the function-level `c`. Uniqueness is preserved: `c` is
freshly allocated once per `node()` call (i.e. once per e-node), and `node()`
is only re-entered for a nested pattern via `child()`, which allocates its own
new `c` for the `for n{c} in ...` loop and recurses into `node()` on that new
node string, so no two arms active in the same generated function body can
ever share a `c` value.

### `cargo check -p mz-transform` result

```
Compiling mz-transform v0.0.0 (.../src/transform)
Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.92s
```
Green, no errors or warnings. `bin/fmt` also reported all tasks successful.
This only verifies the relational path and that the (still-unused) scalar
string-emitting code compiles as ordinary Rust; no scalar rule exists yet, so
`SCALAR_COMPILED_RULES` stays empty and the generated `find_not_not_base` /
scalar matcher body is not produced until Task 7 adds the `not_not` rule. Full
compile-verification of the *emitted* scalar matcher remains Task 7's gate.

### Trace: why the emitted scalar matcher will compile at Task 7

When Task 7 adds a scalar rule, `Matcher::node`'s `SUnary` arm will emit
(with `c` bound to whatever fresh id `node()` allocated for this e-node):
```rust
let SNode::CallUnary { func: mz_expr::UnaryFunc::Not(_), expr: e3 } = n2 else { continue };
```
Here `n2: &SNode`, so match ergonomics bind `e3: &Id`. The call site now emits
`self.child(input, "*e3")`, i.e. the generated matcher writes `*e3` wherever
the child class is referenced. Since `Id` is `Copy`, `*e3` evaluates to a
plain `Id` by value. That `Id` flows into `g.scalar_class_nodes(*e3)` (matches
its `Id`-by-value signature) if the child pattern is itself scalar, or
`g.rel_class_nodes(*e3)` if relational, and, for a leaf `RelVar` child, into
`b.rels.insert(name, *e3)` where the map's value type is `Id`. All three sites
now receive `Id`, matching exactly how every relational arm's `*in{c}`
already behaves, so the generated scalar matcher will type-check the same way
the relational one does.
