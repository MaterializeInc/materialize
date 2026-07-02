# SP2b Slice 2: variadic scalar rules Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the variadic scalar pattern/template capability (`Pat::SVariadic`, `Tmpl::SVariadic`, `Tmpl::SUnary`, `rest…` splice + `map(F[_], list)`) to the proven slice-1 seam, and port the variadic-only rules `and_single`, `or_single`, `not_demorgan_and`, `not_demorgan_or` through it. Behavior-neutral: the combined path must equal the old standalone `EGraph<ScalarLang>` engine on a corpus that exercises multi-operand And/Or.

**Architecture:** Extend the one relational rewrite-DSL grammar and its codegen with scalar-variadic match and build arms, reusing the existing `ListPat`/`ListTmpl` `rest…` splice and `TElem::MapSplice` that relational `Join`/`Union` already use. Scalar rules continue to compile into the separate `SCALAR_COMPILED_RULES` static and run only in the scalar saturate pass. The determinism-parity extractor is unchanged (it already ports the full `raise.rs`, including the And/Or operand `sort()`), so variadic extraction already works. The old engine stays as the A/B oracle (delete-last is slice 7).

**Tech Stack:** Rust, the `mz-transform` crate, a `chumsky`-based build-time grammar, datadriven tests, Lean 4 (theorem emission, `sorry`-stubbed).

## Global Constraints

- **Behavior-neutral, strict gate.** No `--rewrite`, no `cargo insta accept`. The combined path must reproduce the old engine's output exactly on the corpus.
- **`ScalarLang` the type stays.** This slice deletes nothing (delete-last is slice 7). The old engine `crate::eqsat::scalar::canonicalize` remains the differential oracle.
- **Production untouched.** The three production callers of `crate::eqsat::scalar::canonicalize_predicates` stay on the old `EGraph<ScalarLang>` engine. This slice only grows the NEW path used by the differential test.
- **Relational grammar byte-unchanged.** `relational.rewrite` is not edited. All codegen changes are additive scalar arms; relational `find`/`apply` emission is unchanged.
- **Fixed-func rules only.** No func-metavar binding, no func-switch/`switch_and_or`. Each And/Or rule is split into an `and` variant and an `or` variant with a fixed function keyword, matching the `SUnary` precedent. `[and|or]` alternation is NOT added.
- **`flatten_assoc` is out of scope** (deferred by decision to its own increment: it needs middle/two-sided splice matching that `ListPat` cannot express, plus the imperative one-hop cycle guard and `FLATTEN_MAX_OPERANDS` cap that a declarative pattern cannot carry). Do NOT port it, and do NOT put nested same-func-associativity inputs in the corpus (the old engine flattens them via `flatten_assoc` while the combined engine, lacking it, does not, so such an input fails parity by construction).
- **Scalar saturate bounds unchanged:** `MAX_ENODES = 600`, `MATCH_LIMIT = 1_000`, `MAX_ITERS = 100` (already in `scalar_saturate.rs`).
- **Cheap checks before every commit:** `bin/fmt` and `cargo check -p mz-transform`.
- **Test command (mz-test skill):** unit tests via `bin/cargo-test -p mz-transform <filter>`; slt via `bin/sqllogictest --optimized -- test/sqllogictest/transform/`.

---

## Verified slice-2 rule set (vs `src/transform/src/eqsat/scalar/rules.rs`)

The source has 20 rules (`rules()` at `rules.rs:34-58`). Slice 1 ported `not_not`. Slice 2 ports the variadic-only subset that needs exactly the new machinery below:

| Concrete rule | reduce-parity source | Shape |
|---|---|---|
| `and_single` | `and_or_single` (And arm) | `Variadic[and](x) => x` |
| `or_single`  | `and_or_single` (Or arm)  | `Variadic[or](x) => x` |
| `not_demorgan_and` | `not_demorgan` (And arm) | `Unary[not](Variadic[and](xs...)) => Variadic[or](map(Unary[not](_), xs))` |
| `not_demorgan_or`  | `not_demorgan` (Or arm)  | `Unary[not](Variadic[or](xs...)) => Variadic[and](map(Unary[not](_), xs))` |

`and_or_single` and `not_demorgan` each split into two fixed-func rules because the grammar's func keyword (`bracket_ident`) admits no alternation and func-switch is a later-slice capability. Both splits are exact reduce-parity: `and_or_single` fires per connective; `not_demorgan` maps `Not` over operands and flips the connective, which the two fixed-func directions cover.

`flatten_assoc` from the spec's slice-2 row is deferred (see Global Constraints). Multi-operand variadic parity (the real risk carried from slice 1) is still exercised, via `not_demorgan_*` over `And`/`Or` with three-plus operands.

---

## File Structure

**New files:** none.

**Modified files:**
- `src/transform/src/eqsat/dsl.rs`: add `Pat::SVariadic { func, inputs: ListPat }`, `Tmpl::SVariadic { func, inputs: ListTmpl }`, `Tmpl::SUnary { func, input }`.
- `src/transform/build/grammar.rs`: parse `Variadic[<func>]( … )` into `Pat::SVariadic` (pattern) and `Tmpl::SVariadic` (template), and `Unary[<func>](child)` into `Tmpl::SUnary` (template side; the pattern side already exists).
- `src/transform/build/codegen.rs`: `variadic_func_pat` (match-pattern helper); `unary_func_value`/`variadic_func_value` (construction-value helpers); `Matcher::node` arm for `Pat::SVariadic`; scalar-variadic root branch in `find_stmts`; widen `Matcher::child`'s scalar detection to `SUnary | SVariadic`; `tmpl_stmts` arms for `Tmpl::SVariadic` and `Tmpl::SUnary`; widen `is_scalar_rule` to `SUnary | SVariadic`; `sym_name`/AST-echo (`pat`/`tmpl`) arms for the new variants.
- `src/transform/src/eqsat/rules/scalar.rewrite`: add the four rules above.
- `src/transform/src/eqsat/scalar_saturate.rs`: grow the in-crate differential harness with variadic cases (slice-2 + slice-1) and the corpus-coverage assertion.
- `src/transform/tests/testdata/eqsat_scalar_corpus`: grow the fixture with variadic cases.
- `src/transform/lean/MirRewrite/Semantics.lean`: extend the scalar denotation with `And`/`Or` over a list.
- `src/transform/src/eqsat/lean.rs`: `translate_pat`/`translate_tmpl`/`choose_proof` arms for `SVariadic`/`Tmpl::SUnary`.
- `src/transform/lean/MirRewrite/Generated.lean`: regenerated (theorems for the four rules).

---

## Interfaces (cross-task contract)

- `dsl::Pat::SVariadic { func: String, inputs: ListPat }` — a scalar variadic call with a FIXED function keyword (`"and"`/`"or"`), operands captured via `ListPat` (`items` + optional trailing `rest`). Mirrors `Pat::Union`.
- `dsl::Tmpl::SVariadic { func: String, inputs: ListTmpl }` — builds a scalar variadic; `inputs` is an ordered `ListTmpl` (supports `Item`, `Splice`, `MapSplice`). Mirrors `Tmpl::Union`.
- `dsl::Tmpl::SUnary { func: String, input: Box<Tmpl> }` — builds a scalar unary; used inside `map(Unary[not](_), xs)`.
- Codegen classifies a rule as scalar (→ `SCALAR_COMPILED_RULES`) iff its `lhs` root is `Pat::SUnary` or `Pat::SVariadic`.
- Emitted matchers bind a scalar variadic's operands as `ins{c}: &Vec<Id>` (from `SNode::CallVariadic { exprs: ins{c} }`) and reuse `Matcher::variadic`, exactly as the relational `Union`/`Join` path binds `ins{c}` from `ENode`.
- Emitted builders add scalar nodes via `g.add(CNode::Scalar(SNode::CallVariadic { .. }))` / `CallUnary`, reusing `listtmpl_stmts` (which already handles `MapSplice`) for the operand list.
- No change to `scalar_extract::raise`, `scalar_saturate::{saturate, canonicalize_combined}`, `rules::scalar_all`, or the `BaseView`/`ScalarIndex` surface (slice 1 already ships them; `ScalarSym::Variadic` already exists at `scalar/lang.rs:73`).

---

### Task 1: Scalar variadic/unary AST variants + grammar

**Files:**
- Modify: `src/transform/src/eqsat/dsl.rs`
- Modify: `src/transform/build/grammar.rs`
- Test: `src/transform/build/grammar.rs` (`#[cfg(test)]`, mirroring the existing `parses_scalar_unary_nested` test at grammar.rs ~626)

**Interfaces:**
- Produces: `Pat::SVariadic { func: String, inputs: ListPat }`, `Tmpl::SVariadic { func: String, inputs: ListTmpl }`, `Tmpl::SUnary { func: String, input: Box<Tmpl> }`.
- Consumes: existing `Pat`, `Tmpl`, `ListPat`, `ListTmpl`, and the grammar combinators `kw`, `bracket_ident`, `listpat`, `listtmpl`, `tmpl`, `pat`.

- [ ] **Step 1: Add the `Pat::SVariadic` variant**

In `dsl.rs`, inside `pub enum Pat`, after the `SUnary { .. }` variant, add:

```rust
    /// A scalar variadic call with a FIXED function, e.g. `Variadic[and](x)` or
    /// `Variadic[or](xs...)`. `func` is the scalar-func keyword text, resolved to
    /// a concrete `VariadicFunc` by codegen. Operands are captured via `ListPat`
    /// (`items` plus an optional trailing `rest`), mirroring `Union`. Func-metavar
    /// binding and func-switching are later-slice capabilities.
    SVariadic { func: String, inputs: ListPat },
```

- [ ] **Step 2: Add the `Tmpl::SVariadic` and `Tmpl::SUnary` variants**

In `dsl.rs`, inside `pub enum Tmpl`, add (near the other operator templates):

```rust
    /// Build a scalar unary call with a FIXED function, e.g. `Unary[not](_)`
    /// inside a `map(...)`. `func` is the scalar-func keyword text.
    SUnary { func: String, input: Box<Tmpl> },
    /// Build a scalar variadic call with a FIXED function, e.g.
    /// `Variadic[or](map(Unary[not](_), xs))`. `inputs` is an ordered `ListTmpl`
    /// (supports `Item`, `Splice`, and `MapSplice`), mirroring `Union`.
    SVariadic { func: String, inputs: ListTmpl },
```

- [ ] **Step 3: Parse `Pat::SVariadic` in the grammar**

In `grammar.rs`, in the `pat` recursive parser (alongside `sunary`, grammar.rs ~382), add:

```rust
        let svariadic = kw("Variadic")
            .ignore_then(bracket_ident())
            .then(listpat.clone())
            .map(|(func, inputs)| Pat::SVariadic { func, inputs });
```

Add `svariadic` to the `pat` `choice((...))` list (grammar.rs ~391), before `sunary`.

- [ ] **Step 4: Parse `Tmpl::SUnary` and `Tmpl::SVariadic` in the grammar**

In `grammar.rs`, in the `tmpl` recursive parser (alongside `union`, grammar.rs ~494), add:

```rust
        let tsunary = kw("Unary")
            .ignore_then(bracket_ident())
            .then(tmpl.clone())
            .map(|(func, input)| Tmpl::SUnary {
                func,
                input: Box::new(input),
            });
        let tsvariadic = kw("Variadic")
            .ignore_then(bracket_ident())
            .then(listtmpl.clone())
            .map(|(func, inputs)| Tmpl::SVariadic { func, inputs });
```

Add `tsvariadic` and `tsunary` to the `tmpl` `choice((...))` list (grammar.rs ~504), before `hole_or_relvar`.

- [ ] **Step 5: Write the failing parse test**

In `grammar.rs`'s `#[cfg(test)] mod tests` (where `parses_scalar_unary_nested` lives), add:

```rust
    #[test]
    fn parses_scalar_variadic_single() {
        let src = "rule and_single { Variadic[and](x) => x }";
        let rules = crate::grammar::parse(src).expect("parses");
        match &rules[0].lhs {
            crate::dsl::Pat::SVariadic { func, inputs } => {
                assert_eq!(func, "and");
                assert_eq!(inputs.items.len(), 1);
                assert!(inputs.rest.is_none());
            }
            other => panic!("expected SVariadic, got {other:?}"),
        }
    }

    #[test]
    fn parses_scalar_demorgan_mapsplice() {
        let src =
            "rule not_demorgan_and { Unary[not](Variadic[and](xs...)) => Variadic[or](map(Unary[not](_), xs)) }";
        let rules = crate::grammar::parse(src).expect("parses");
        // LHS: Not(And(xs...))
        match &rules[0].lhs {
            crate::dsl::Pat::SUnary { input, .. } => match &**input {
                crate::dsl::Pat::SVariadic { func, inputs } => {
                    assert_eq!(func, "and");
                    assert!(inputs.items.is_empty());
                    assert_eq!(inputs.rest.as_deref(), Some("xs"));
                }
                other => panic!("expected SVariadic input, got {other:?}"),
            },
            other => panic!("expected SUnary root, got {other:?}"),
        }
        // RHS: Or(map(Not(_), xs))
        match &rules[0].rhs {
            crate::dsl::Tmpl::SVariadic { func, inputs } => {
                assert_eq!(func, "or");
                assert_eq!(inputs.elems.len(), 1);
                assert!(matches!(
                    inputs.elems[0],
                    crate::dsl::TElem::MapSplice { .. }
                ));
            }
            other => panic!("expected SVariadic template, got {other:?}"),
        }
    }
```

- [ ] **Step 6: Run the tests**

The grammar module is a build-script module. If its `#[cfg(test)]` tests run under `cargo test -p mz-transform` (the slice-1 `parses_scalar_unary_nested` test is the precedent — run it to confirm the harness), run:

Run: `bin/cargo-test -p mz-transform parses_scalar_variadic_single parses_scalar_demorgan_mapsplice`
Expected: PASS. If the build-script tests are not reachable from that target (as slice-1 Task 2 noted for its grammar test), instead confirm via `cargo check -p mz-transform` that the grammar compiles, and rely on Task 4's build to exercise parsing. State which path applied in the task report.

- [ ] **Step 7: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/src/eqsat/dsl.rs src/transform/build/grammar.rs
git commit -m "eqsat dsl: scalar variadic pattern/template + scalar unary template"
```

---

### Task 2: Codegen scalar-variadic match arms + child widening

**Files:**
- Modify: `src/transform/build/codegen.rs`
- Test: exercised by Task 4's build (codegen has no standalone test target); the differential harness in Task 5 is the behavioral gate.

**Interfaces:**
- Produces: a `variadic_func_pat` helper; a `Matcher::node` arm for `Pat::SVariadic`; a scalar-variadic root branch in `find_stmts`; `Matcher::child` scalar detection widened to `SUnary | SVariadic`; `is_scalar_rule` widened to `SUnary | SVariadic`; a `sym_name` arm and an AST-echo (`pat`) arm for `Pat::SVariadic`.
- Consumes: `Pat::SVariadic`, the existing `unary_func_pat`, `Matcher::variadic`, `Matcher::child`, `find_stmts` scalar-root precedent (the `Pat::SUnary` branch), `ScalarSym::Variadic`.

- [ ] **Step 1: Add the variadic func match-pattern helper**

Next to `unary_func_pat` in `codegen.rs`, add:

```rust
/// The Rust pattern text matching a fixed scalar `VariadicFunc` by keyword.
/// Extend this table as scalar rules reference more variadic functions.
fn variadic_func_pat(func: &str) -> String {
    match func {
        "and" => "mz_expr::VariadicFunc::And(_)".to_string(),
        "or" => "mz_expr::VariadicFunc::Or(_)".to_string(),
        other => panic!("unknown scalar variadic func keyword: {other}"),
    }
}
```

- [ ] **Step 2: `Matcher::node` arm for `Pat::SVariadic`**

In `Matcher::node` (codegen.rs ~150), add before the `Pat::RelVar(_) => unreachable!(...)` arm:

```rust
            Pat::SVariadic { func, inputs } => {
                let fpat = variadic_func_pat(func);
                self.stmts.push(format!(
                    "let crate::eqsat::scalar::node::SNode::CallVariadic {{ func: {fpat}, exprs: ins{c} }} = {node} else {{ continue }};"
                ));
                self.variadic(inputs, c);
            }
```

`self.variadic(inputs, c)` is reused verbatim: it enforces the arity/`rest` shape and calls `self.child(item, "ins{c}[i]")` for each fixed item and `self.rest(rest)` + `let rest{n}: Vec<Id> = ins{c}[k..].to_vec();` for the trailing rest. `ins{c}` is `&Vec<Id>` here (bound from `&SNode`), exactly as the relational `Union` path binds it from `&ENode`, so `.len()`, `[i]`, and `[k..].to_vec()` all typecheck unchanged.

- [ ] **Step 3: Scalar-variadic root branch in `find_stmts`**

In `find_stmts` (codegen.rs ~496), the `match &rule.lhs` has a `Pat::SUnary { .. } =>` branch that enumerates `g.nodes_by_scalar_sym(ScalarSym::Unary)`. Add a sibling branch that mirrors it exactly, differing only in the symbol:

```rust
        Pat::SVariadic { .. } => {
            s.push_str(
                "for (root_id, root_node) in g.nodes_by_scalar_sym(crate::eqsat::scalar::lang::ScalarSym::Variadic) {\n",
            );
            s.push_str("let root_id = root_id;\n");
            s.push_str("let root_node = &root_node;\n");
            m.node(&rule.lhs, "root_node");
            for stmt in &m.stmts {
                s.push_str(stmt);
                s.push('\n');
            }
            s.push_str(&body(rule, &m, mode));
            for _ in 0..m.open_braces {
                s.push_str("}\n");
            }
            s.push_str("}\n");
        }
```

Copy the exact body from the current `Pat::SUnary` branch (it may differ slightly from the snippet above if slice-1 review adjusted it); the only intended edit is `ScalarSym::Unary` → `ScalarSym::Variadic`.

- [ ] **Step 4: Widen `Matcher::child` scalar detection**

In `Matcher::child` (codegen.rs ~312), change the scalar detection from SUnary-only to include SVariadic:

```rust
                let scalar = matches!(pat, Pat::SUnary { .. } | Pat::SVariadic { .. });
```

This is required by `not_demorgan_*`: the `Variadic[and](xs...)` is the child of `Unary[not]`, so `child()` must scan `g.scalar_class_nodes` (not `g.rel_class_nodes`) when the child pattern is a scalar variadic. (The slice-1 self-review flagged this widening as due when SVariadic lands.)

- [ ] **Step 5: Widen `is_scalar_rule`**

In `codegen.rs`, change the classifier so a scalar-variadic-rooted rule is emitted into `SCALAR_COMPILED_RULES`:

```rust
fn is_scalar_rule(r: &Rule) -> bool {
    matches!(r.lhs, Pat::SUnary { .. } | Pat::SVariadic { .. })
}
```

- [ ] **Step 6: Add `sym_name` and AST-echo arms for `Pat::SVariadic`**

`sym_name` (codegen.rs ~99) and the AST-echo `pat` function (codegen.rs ~1176, the `Pat::Union` line is the model) must be exhaustive over `Pat`. Add:

In `sym_name`, a scalar root has no relational `Sym`; the scalar root branch in `find_stmts` (Step 3) bypasses `sym_name`, so give `Pat::SVariadic` (like `Pat::SUnary`) whatever the current `Pat::SUnary` arm does (either a dedicated arm or an `unreachable!`). Mirror the `Pat::SUnary` treatment exactly.

In the AST-echo `pat` function, add:

```rust
        Pat::SVariadic { func, inputs } => {
            format!("{P}::Pat::SVariadic {{ func: {}, inputs: {} }}", s(func), listpat(inputs))
        }
```

(`s(..)` string-quotes; `listpat(..)` already echoes a `ListPat`. Mirror the exact style of the neighboring `Pat::Union` / `Pat::SUnary` arms — the `{P}` prefix and helper names must match what those arms use.)

- [ ] **Step 7: Verify via build after Task 4**

Codegen cannot be exercised until Task 4 adds the rules. Note in the commit that Task 4's `cargo check` is the exercising build. Do NOT hand-write a codegen unit test; the generated `find`/`apply` are gated by Task 4's build and Task 5's differential.

- [ ] **Step 8: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/build/codegen.rs
git commit -m "eqsat codegen: scalar-variadic match arms + child scalar-detection widen"
```

---

### Task 3: Codegen scalar-variadic/unary template build arms

**Files:**
- Modify: `src/transform/build/codegen.rs`
- Test: exercised by Task 4's build; behavior gated by Task 5.

**Interfaces:**
- Produces: `unary_func_value`/`variadic_func_value` (construction-value helpers); `tmpl_stmts` arms for `Tmpl::SVariadic` and `Tmpl::SUnary`; AST-echo (`tmpl`) arms for both.
- Consumes: existing `tmpl_stmts`, `listtmpl_stmts` (already handles `TElem::MapSplice`), `Tmpl::Hole` handling.

- [ ] **Step 1: Add construction-value helpers**

Next to `variadic_func_pat` (Task 2), add value-form helpers (the concrete func VALUE to construct, distinct from the match pattern):

```rust
/// The Rust expression constructing a fixed scalar `UnaryFunc` by keyword.
fn unary_func_value(func: &str) -> String {
    match func {
        "not" => "mz_expr::UnaryFunc::Not(mz_expr::func::Not)".to_string(),
        other => panic!("unknown scalar unary func keyword: {other}"),
    }
}

/// The Rust expression constructing a fixed scalar `VariadicFunc` by keyword.
fn variadic_func_value(func: &str) -> String {
    match func {
        "and" => "mz_expr::VariadicFunc::And(mz_expr::func::variadic::And)".to_string(),
        "or" => "mz_expr::VariadicFunc::Or(mz_expr::func::variadic::Or)".to_string(),
        other => panic!("unknown scalar variadic func keyword: {other}"),
    }
}
```

- [ ] **Step 2: `tmpl_stmts` arm for `Tmpl::SUnary`**

In `tmpl_stmts` (codegen.rs ~675), add an arm:

```rust
        Tmpl::SUnary { func, input } => {
            let vin = tmpl_stmts(input, hole, out, fresh);
            let c = fresh.id();
            let v = format!("id{c}");
            let fval = unary_func_value(func);
            out.push_str(&format!(
                "let {v} = g.add(CNode::Scalar(crate::eqsat::scalar::node::SNode::CallUnary {{ func: {fval}, expr: {vin} }}));\n"
            ));
            v
        }
```

Note `input` is built with the SAME `hole` in scope, so `Unary[not](_)` inside a `map(..)` resolves `_` to the current element (via `Tmpl::Hole`, already handled at tmpl_stmts ~685).

- [ ] **Step 3: `tmpl_stmts` arm for `Tmpl::SVariadic`**

Add an arm:

```rust
        Tmpl::SVariadic { func, inputs } => {
            let vins = listtmpl_stmts(inputs, hole, out, fresh);
            let c = fresh.id();
            let v = format!("id{c}");
            let fval = variadic_func_value(func);
            out.push_str(&format!(
                "let {v} = g.add(CNode::Scalar(crate::eqsat::scalar::node::SNode::CallVariadic {{ func: {fval}, exprs: {vins} }}));\n"
            ));
            v
        }
```

`listtmpl_stmts` already emits the `Vec<Id>` for `Item`/`Splice`/`MapSplice`. For `not_demorgan_and`, the single element is `MapSplice { func: Tmpl::SUnary{"not", Hole}, list: "xs" }`, so `listtmpl_stmts` emits a loop over `b.rests["xs"]` that builds `Not(hole)` per operand (via the Step 2 arm) and pushes it. No new list machinery is needed.

- [ ] **Step 4: AST-echo `tmpl` arms**

In the AST-echo `tmpl` function (codegen.rs ~1200, model on the `Tmpl::Union` line), add exhaustive arms:

```rust
        Tmpl::SUnary { func, input } => {
            format!("{P}::Tmpl::SUnary {{ func: {}, input: Box::new({}) }}", s(func), tmpl(input))
        }
        Tmpl::SVariadic { func, inputs } => {
            format!("{P}::Tmpl::SVariadic {{ func: {}, inputs: {} }}", s(func), listtmpl(inputs))
        }
```

(Match the exact `{P}` prefix and helper names used by the neighboring arms.)

- [ ] **Step 5: Verify via build after Task 4**

As Task 2: the arms are exercised by Task 4's build and Task 5's differential. Do not add a codegen unit test.

- [ ] **Step 6: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/build/codegen.rs
git commit -m "eqsat codegen: scalar variadic/unary template build arms"
```

---

### Task 4: Port the four rules to `scalar.rewrite`

**Files:**
- Modify: `src/transform/src/eqsat/rules/scalar.rewrite`
- Test: build compiles the four `find_*`/`apply_*` and `SCALAR_COMPILED_RULES` grows to five entries; behavior in Task 5.

**Interfaces:**
- Produces: `and_single`, `or_single`, `not_demorgan_and`, `not_demorgan_or` compiled into `SCALAR_COMPILED_RULES`.
- Consumes: grammar (Task 1), codegen match arms (Task 2) and build arms (Task 3).

- [ ] **Step 1: Add the four rules**

Append to `scalar.rewrite` (after `not_not`):

```
# A one-argument AND/OR equals its sole argument.
rule and_single {
    doc "And(x) = x"
    Variadic[and](x) => x
}

rule or_single {
    doc "Or(x) = x"
    Variadic[or](x) => x
}

# De Morgan: push a NOT through a variadic AND/OR, flipping the connective and
# wrapping each operand. Operand order is preserved; the And/Or error is the
# order-independent max over operands, so the flip is exact under three-valued
# logic and errors (no could_error gate).
rule not_demorgan_and {
    doc "Not(And(xs...)) = Or(Not(xs)...)"
    Unary[not](Variadic[and](xs...)) => Variadic[or](map(Unary[not](_), xs))
}

rule not_demorgan_or {
    doc "Not(Or(xs...)) = And(Not(xs)...)"
    Unary[not](Variadic[or](xs...)) => Variadic[and](map(Unary[not](_), xs))
}
```

- [ ] **Step 2: Build and confirm the generated code compiles**

Run: `cargo check -p mz-transform`
Expected: builds. `$OUT_DIR/eqsat_rules.rs` now defines `find_and_single_base`/`apply_and_single_base` (and the other three), and `SCALAR_COMPILED_RULES` has five entries (not_not + four).

- [ ] **Step 3: Assert each rule rewrites (integration smoke tests)**

Add to the `#[cfg(test)] mod tests` in `scalar_saturate.rs`:

```rust
    #[mz_ore::test]
    fn variadic_rules_rewrite_via_combined() {
        use mz_expr::{MirScalarExpr, UnaryFunc, VariadicFunc};
        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let c = MirScalarExpr::column;

        // and_single / or_single collapse a one-operand connective.
        assert_eq!(canonicalize_combined(&and(vec![c(0)]), &[]), c(0));
        assert_eq!(canonicalize_combined(&or(vec![c(0)]), &[]), c(0));

        // De Morgan over >2 operands (the multi-operand variadic-parity case).
        let got = canonicalize_combined(&not(and(vec![c(0), c(1), c(2)])), &[]);
        // The old engine is the oracle; assert equality there in Task 5. Here,
        // assert the head shape flipped to Or and no double-negation remains.
        assert!(matches!(
            got,
            MirScalarExpr::CallVariadic { func: VariadicFunc::Or(_), .. }
        ));
    }
```

Run: `bin/cargo-test -p mz-transform variadic_rules_rewrite_via_combined`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/src/eqsat/rules/scalar.rewrite src/transform/src/eqsat/scalar_saturate.rs
git commit -m "eqsat: and/or single + de Morgan as declarative scalar rules"
```

---

### Task 5: Grow corpus + differential parity (slice-2 + slice-1)

**Files:**
- Modify: `src/transform/tests/testdata/eqsat_scalar_corpus`
- Modify: `src/transform/src/eqsat/scalar_saturate.rs` (the in-crate `#[cfg(test)]` harness that slice 1 placed here; it reads the corpus via `include_str!`)
- Test: the harness itself.

**Interfaces:**
- Consumes: `canonicalize_combined` (combined path), `crate::eqsat::scalar::canonicalize` (old oracle). Both are `pub(crate)`, so the harness stays in-crate (slice-1 precedent).

- [ ] **Step 1: Grow the corpus fixture**

Append variadic records to `eqsat_scalar_corpus`. Every input must be shaped so the old engine's UNPORTED rules do not fire (no literals → no const_fold/short_circuit/drop_unit/if/null-prop/err-prop/isnull; no duplicate operands → no dedup; no nested same-func → no flatten_assoc; no binary child under Not → no not_binary_negate). Operands are distinct bare boolean columns.

```
# --- slice 2: variadic (and/or single, de Morgan) ---
# and_single / or_single
expr: and(#0)
types: bool

expr: or(#0)
types: bool

# de Morgan, both directions, >2 operands (multi-operand variadic parity)
expr: not(and(#0, #1, #2))
types: bool,bool,bool

expr: not(or(#0, #1, #2, #3))
types: bool,bool,bool,bool

# de Morgan feeding single-collapse: Not(And(#0)) -> Or(Not #0) -> Not #0
expr: not(and(#0))
types: bool
```

Keep the exact serialization the slice-1 harness parses. If the harness builds `MirScalarExpr` values in Rust (slice-1 seed did), add the corresponding constructor cases in Step 2 instead of relying on a text parser.

- [ ] **Step 2: Add the variadic differential case set**

In the in-crate harness (the `scalar_parity_*` test in `scalar_saturate.rs`), add a variadic parity test that differentials the combined path against the old oracle. Keep the slice-1 `not_not` cases intact (no regression):

```rust
    #[mz_ore::test]
    fn scalar_parity_variadic() {
        use mz_expr::{MirScalarExpr, UnaryFunc, VariadicFunc};
        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let c = MirScalarExpr::column;

        let cases = vec![
            and(vec![c(0)]),
            or(vec![c(0)]),
            not(and(vec![c(0), c(1), c(2)])),
            not(or(vec![c(0), c(1), c(2), c(3)])),
            not(and(vec![c(0)])),
            // slice-1 shapes still hold under the grown rule set:
            not(not(c(0))),
        ];
        for e in cases {
            // Boolean, type-agnostic rules: `&[]` col_types is sufficient (the
            // rules ported here never read a column type).
            let new = canonicalize_combined(&e, &[]);
            let old = crate::eqsat::scalar::canonicalize(&e, &[]);
            assert_eq!(new, old, "parity failed for {e:?}");
        }
    }
```

A failure here is the slice-2 finding: do NOT adjust the assertion or the output. Report which case diverged and how (it means either a ported rule's shape differs from reduce or an unported old-engine rule fired on the corpus).

- [ ] **Step 3: Grow the corpus-coverage assertion**

Extend the slice-1 `corpus_covers_slice1` assertion (or add `corpus_covers_slice2`) so the fixture provably exercises the new capability:

```rust
    #[mz_ore::test]
    fn corpus_covers_slice2() {
        assert!(CORPUS.contains("and(#0)"), "corpus must exercise and/or single");
        assert!(
            CORPUS.contains("not(and(#0, #1, #2))"),
            "corpus must exercise multi-operand de Morgan"
        );
    }
```

- [ ] **Step 4: Run the harness, expect PASS**

Run: `bin/cargo-test -p mz-transform scalar_parity_variadic scalar_parity_not_not corpus_covers`
Expected: PASS on every case. A divergence is the slice-2 go/no-go finding, not something to paper over.

- [ ] **Step 5: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/tests/testdata/eqsat_scalar_corpus src/transform/src/eqsat/scalar_saturate.rs
git commit -m "eqsat: differential corpus + parity for variadic scalar rules"
```

---

### Task 6: Lean denotation for And/Or over a list + theorems

**Files:**
- Modify: `src/transform/lean/MirRewrite/Semantics.lean`
- Modify: `src/transform/src/eqsat/lean.rs`
- Modify: `src/transform/lean/MirRewrite/Generated.lean` (regenerated)
- Test: `cargo run -p mz-transform --example gen-lean` regenerates the four theorems; `grep -c "PERMANENT SORRY"` stays `0`.

**Interfaces:**
- Consumes: the DSL `Rule` with `Pat::SVariadic`/`Tmpl::SVariadic`/`Tmpl::SUnary`, and `lean.rs`'s `translate_pat`/`translate_tmpl`/`choose_proof`/`emit_rule`.
- Produces: an `And`/`Or`-over-list scalar denotation in `Semantics.lean` and `rule_and_single`, `rule_or_single`, `rule_not_demorgan_and`, `rule_not_demorgan_or` in `Generated.lean`.

- [ ] **Step 1: Extend the scalar denotation with And/Or over a list**

In `Semantics.lean`, extend the `ScalarExpr` inductive and `denoteS` (added in slice 1) with variadic And/Or as a list fold over the connective unit:

```lean
inductive ScalarExpr where
  | var : Nat → ScalarExpr
  | notE : ScalarExpr → ScalarExpr
  | andE : List ScalarExpr → ScalarExpr
  | orE  : List ScalarExpr → ScalarExpr

def denoteS (env : Nat → Bool) : ScalarExpr → Bool
  | ScalarExpr.var n => env n
  | ScalarExpr.notE e => not (denoteS env e)
  | ScalarExpr.andE es => es.foldr (fun e acc => (denoteS env e) && acc) true
  | ScalarExpr.orE es => es.foldr (fun e acc => (denoteS env e) || acc) false
```

(Two-valued `Bool` denotation matches the slice-1 skeleton. Full three-valued/error denotation is a later-slice deepening; the slice-1 `not_not` theorem is likewise two-valued. Keep the same fidelity, do not expand scope.)

- [ ] **Step 2: Emit theorems for the four rules**

In `lean.rs`, extend `translate_pat`/`translate_tmpl` with arms for `Pat::SVariadic`/`Tmpl::SVariadic` (emit `ScalarExpr.andE [...]` / `ScalarExpr.orE [...]`, translating `items` and splicing `rest` as a Lean list variable) and `Tmpl::SUnary` (emit `ScalarExpr.notE (...)`), plus the `map(Not(_), xs)` combinator (emit `xs.map (fun h => ScalarExpr.notE h)`). Extend `choose_proof`:

- `and_single` / `or_single`: `andE [x]` unfolds to `denoteS x && true = denoteS x` (dually `orE [x]`), provable by `simp [denoteS]`.
- `not_demorgan_and` / `not_demorgan_or`: `not (foldr && true xs) = foldr || false (map not xs)`, a list induction. Attempt `simp [denoteS, List.foldr, List.map]`; if it does not close, emit a plain `sorry` (NON-permanent). A plain `sorry` is acceptable here (slice-1's contract is "sorry-stubbed-or-proved"); it must NOT carry the `-- PERMANENT SORRY` marker, which is reserved for builtin appliers (none exist until slice 4).

Theorem shapes (illustrative):

```lean
theorem rule_and_single : ∀ (env : Nat → Bool) (x : ScalarExpr),
    denoteS env (ScalarExpr.andE [x]) = denoteS env x := by
  intro env x; simp [denoteS]
```

- [ ] **Step 3: Regenerate and confirm**

Run: `cargo run -p mz-transform --example gen-lean`
Expected: `Generated.lean` gains `rule_and_single`, `rule_or_single`, `rule_not_demorgan_and`, `rule_not_demorgan_or`, and still contains `rule_not_not` and all relational theorems.

- [ ] **Step 4: Confirm no permanent-sorry regression**

Run: `grep -c "PERMANENT SORRY" src/transform/lean/MirRewrite/Generated.lean`
Expected: `0` (unchanged from slice 1; none of slice 2's rules are builtin appliers).

- [ ] **Step 5: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/lean/MirRewrite/Semantics.lean src/transform/lean/MirRewrite/Generated.lean src/transform/src/eqsat/lean.rs
git commit -m "eqsat lean: And/Or list denotation + variadic-rule theorems"
```

---

### Task 7: Slice-2 gate + regression sweep

**Files:**
- Test only.

- [ ] **Step 1: Full mz-transform eqsat unit suite green**

Run: `bin/cargo-test -p mz-transform eqsat`
Expected: all eqsat tests PASS (relational + slice-1 scalar + slice-2 scalar). Confirms the additive codegen did not perturb the relational engine or the slice-1 path.

- [ ] **Step 2: Differential parity (the slice-2 criterion)**

Run: `bin/cargo-test -p mz-transform scalar_parity_variadic scalar_parity_not_not`
Expected: PASS. `canonicalize_combined == old canonicalize` on every case, slice-2 and slice-1. A failure is the go/no-go trigger: STOP, do not adjust output, report the diverging case.

- [ ] **Step 3: Relational golden regression (no rewrite)**

Run: `bin/sqllogictest --optimized -- test/sqllogictest/transform/`
Expected: no diffs. Confirms the codegen additions (new match/build arms, `is_scalar_rule` widening, `child` scalar-detection widening) did not change the relational engine's output. Do NOT pass `--rewrite`.

- [ ] **Step 4: Termination check**

Confirm `canonicalize_combined` terminates within the copied bounds on the variadic corpus (no hang; de Morgan + single-collapse converge). Implicitly covered by Step 2 completing; note it explicitly in the task review. In particular confirm `not(and(#0))` reaches the same fixpoint as the old engine (de Morgan then `or_single`), i.e. the two ported rule families compose without oscillation.

- [ ] **Step 5: Final commit (gate record)**

```bash
git commit --allow-empty -m "eqsat: slice-2 gate PASS (variadic parity + relational goldens clean)"
```

---

## Self-Review

**Spec coverage (against the slice-2 brief and the SP2b design):**
- Scalar variadic `Pat`/`Tmpl` + `rest…` splice + `map(F[_], …)` → Task 1 (AST/grammar), Tasks 2–3 (codegen). Reuses `ListPat`/`ListTmpl`/`MapSplice`, not a reimplementation.
- Codegen scalar arms additive, mirroring slice-1 SUnary → Tasks 2–3.
- `Matcher::child` scalar-detection widening (SUnary → SUnary|SVariadic), the slice-1 self-review's flagged item → Task 2 Step 4.
- Verified rule set (and_single, or_single, not_demorgan_and, not_demorgan_or); `flatten_assoc` deferred with reason → header + Global Constraints.
- Differential parity, slice-2 + slice-1, corpus grown with multi-operand And/Or → Task 5. Old engine is the oracle, not deleted.
- Lean And/Or list denotation + 1:1 theorems, no permanent-sorry regression → Task 6.
- Production/relational goldens untouched (no `--rewrite`) → Task 7 Step 3; grammar byte-unchanged for `relational.rewrite` (Global Constraints).

**Placeholder scan:** No TBD/TODO-as-work. Every code step shows code. Codegen tasks (2, 3) are exercised by Task 4's build and Task 5's differential, stated explicitly (build-script modules have no standalone test target), matching slice-1's approach.

**Type consistency:** `Pat::SVariadic { func, inputs: ListPat }` and `Tmpl::SVariadic { func, inputs: ListTmpl }` are echoed by the AST-echo `pat`/`tmpl` functions (Task 2 Step 6, Task 3 Step 4) with the same field names, matched in the grammar (Task 1) and consumed in `Matcher::node`/`tmpl_stmts` (Tasks 2–3). `is_scalar_rule` (Task 2 Step 5) and the `find_stmts` scalar-root branch (Task 2 Step 3) agree on `SUnary | SVariadic`. `variadic_func_pat` (match) vs `variadic_func_value` (construct) are used in the match arm vs the build arm respectively, never crossed.

**Open risk to verify during execution:** the multi-operand de Morgan parity is the carried-forward risk. The corpus must actually exercise `>2` operands (it does: `not(and(#0,#1,#2))`, `not(or(#0,#1,#2,#3))`) and the de-Morgan→single-collapse interaction (`not(and(#0))`). Nested same-func associativity is deliberately absent (it belongs to the deferred `flatten_assoc` slice and would fail parity by construction here). If Task 5 diverges on a case, that is the finding to report, not to silence.
