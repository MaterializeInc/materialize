# sqlfunc canonicalization implementation plan, part 1

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the `#[sqlfunc]` macro's three independent generator arms into one
shape-parameterized code path, add stateful and arena support for unary and binary
functions on top of it, and prove the result by converting one source file.

**Architecture:** A `Shape` enum names the three arities and answers narrow questions
about each: trait path, associated-type construction, output-method name and
signature, nullability formula, and which modifiers are legal with which return
type. One `generate` function drives emission for all three. Modifier legality and
override-method emission become table driven, so the 19 duplicated `quote!` blocks
and 18 hand-written rejections collapse to one helper and one loop. Capabilities then
arrive as data in a `Shape`, not as code copied per arm.

**Tech Stack:** Rust, `syn` 3.x, `quote`, `proc-macro2`, `darling` for attribute
parsing, `insta` for snapshot tests of generated code, `prettyplease` for formatting
the snapshots.

**Spec:** `doc/developer/design/20260917_sqlfunc_canonicalization.md`

## Global Constraints

* Base every branch on `upstream/main`, never `origin/main` or `main_empty`. Push to
  `origin`. Pull requests target `upstream`.
* Work in a git worktree under `.claude/worktrees/`.
* `bin/fmt` and `bin/lint` before every commit. `bin/fmt` takes no path arguments.
  Never `cargo fmt --check`.
* Never edit a `*.snap` file by hand. Run `cargo test`, then `cargo insta accept`.
* No `std::collections::HashMap` or `HashSet`. Use `BTreeMap`, `BTreeSet`, or
  `mz_ore::collections::HashMap`.
* No side effects inside `debug_assert!`.
* No em-dashes in comments, docs, or commit messages. No semicolons used to join
  independent clauses in prose.
* Comments state contracts and non-obvious reasoning. No comments narrating what the
  code plainly does, and no comments referring to previous or future states of the
  code.
* Each pull request description ends with the line
  `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. Each commit
  message ends with `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`.
* The snapshot suite is the acceptance gate for every macro change. It lives in
  `src/expr-derive-impl/src/lib.rs` under `#[cfg(test)] mod test` and holds 13 tests
  named `insta_test_*`. New tests go in that same module, following the local
  pattern.
* `src/expr-derive-impl` gates `insta` and `prettyplease` behind the `test` feature,
  which `[dev-dependencies]` enables, so `cargo test -p mz-expr-derive-impl` works
  with no extra flags.

## Scope of this plan

This plan covers PR1 through PR4 of the 23 in the spec:

| PR | Content | Tasks |
|---|---|---|
| PR1 | Canonicalize the three arms. Generated output byte identical. | 1 to 6 |
| PR2 | Arena on unary, stateful unary and binary, `skip_display`. | 7 to 11 |
| PR3 | Direct-unwrap `LazyBinaryFunc`, behind a measurement gate. | 12 |
| PR4 | Convert `src/expr/src/scalar/func/impls/int16.rs`. | 13 |

PR5 through PR23, the remaining 19 conversions, get a second plan written after PR4
lands. Their steps depend on what PR2's generated output actually looks like, and PR4
is what establishes the recipe. Writing them now would be guesswork.

## File structure

**New files:**

* `src/expr-derive-impl/src/shape.rs`: the `Shape` enum, the `Modifier` enum, the
  `ReturnTy` enum, and the per-shape modifier tables. Pure data and small token
  fragments, no emission logic. This is the file a future capability edits.
* `src/expr-derive-impl/src/generate.rs`: the single `generate` function plus the
  shared emission helpers (`optional_method`, `emit_display`, `emit_funcname`,
  `emit_struct_or_impl`).

**Modified files:**

* `src/expr-derive-impl/src/sqlfunc.rs`: keeps attribute parsing (`Modifiers`,
  `SqlName`), arity detection, and the type helpers (`arg_type`, `output_type`,
  `erase_all_generic_params`, `classify_generic_usage`, `derive_output_type_for_generics`,
  `non_nullable_position_checks`, `camel_case`). Loses `unary_func`, `binary_func`,
  and `variadic_func`, which become three-line calls into `generate`.
* `src/expr-derive-impl/src/lib.rs`: declares the two new modules, gains new
  snapshot tests.
* `src/expr/src/scalar/func/unary.rs`: `EagerUnaryFunc::call` signature, blanket
  `LazyUnaryFunc` impl (PR2).
* `src/expr/src/scalar/func/binary.rs`: blanket `LazyBinaryFunc` impl removal (PR3).
* `src/expr/src/scalar/func.rs`: `func_name!` entries (PR2 for `RangeCreate`, PR4
  onward for conversions).
* `src/expr-derive/src/lib.rs`: stale rustdoc (PR2).
* `doc/developer/sqlfunc.md`: arity table, `skip_display`, uncovered shapes (PR2).

Splitting `shape.rs` out of `sqlfunc.rs` is deliberate: `sqlfunc.rs` is 1627 lines
today, and the point of the refactor is that capabilities are declared in one small
place. Leaving the tables inside a 1200-line file would undercut that.

---

## Task 1: The shape and modifier tables

**Files:**
- Create: `src/expr-derive-impl/src/shape.rs`
- Modify: `src/expr-derive-impl/src/lib.rs:16` (add `mod shape;`)
- Test: `src/expr-derive-impl/src/shape.rs` (unit tests in a `#[cfg(test)] mod tests` at the bottom)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `pub(crate) enum Shape { Unary, Binary, Variadic }`
  - `pub(crate) enum Modifier` with variants `CouldError`, `IntroducesNulls`,
    `IsMonotone`, `IsInfixOp`, `PropagatesNulls`, `Inverse`, `PreservesUniqueness`,
    `IsEliminableCast`, `Negate`, `IsInfinityMonotone`, `IsAssociative`
  - `pub(crate) enum ReturnTy { Bool, BoolPair, OptUnaryFunc, OptBinaryFunc }`
  - `impl Modifier { pub(crate) fn name(&self) -> &'static str }`
  - `impl ReturnTy { pub(crate) fn to_tokens(&self) -> proc_macro2::TokenStream }`
  - `impl Shape { pub(crate) fn modifiers(&self) -> &'static [(Modifier, ReturnTy)] }`
  - `impl Shape { pub(crate) fn label(&self) -> &'static str }` returning `"unary"`,
    `"binary"`, `"variadic"` for error messages

- [ ] **Step 1: Write the failing test**

Create `src/expr-derive-impl/src/shape.rs` containing only the test module, so the
test names the API before it exists:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Per-arity descriptions of the three scalar function shapes.
//!
//! A [`Shape`] answers the questions that differ between `EagerUnaryFunc`,
//! `EagerBinaryFunc`, and `EagerVariadicFunc`. Everything that does not differ lives
//! in `crate::generate`. Adding a modifier to an arity means adding a row to that
//! arity's table here, not writing emission code.

#[cfg(test)]
mod tests {
    use super::{Modifier, ReturnTy, Shape};

    fn has(shape: Shape, m: Modifier) -> bool {
        shape.modifiers().iter().any(|(candidate, _)| *candidate == m)
    }

    #[mz_ore::test]
    fn unary_accepts_inverse_and_rejects_negate() {
        assert!(has(Shape::Unary, Modifier::Inverse));
        assert!(has(Shape::Unary, Modifier::PreservesUniqueness));
        assert!(has(Shape::Unary, Modifier::IsEliminableCast));
        assert!(!has(Shape::Unary, Modifier::Negate));
        assert!(!has(Shape::Unary, Modifier::IsInfixOp));
        assert!(!has(Shape::Unary, Modifier::PropagatesNulls));
        assert!(!has(Shape::Unary, Modifier::IsAssociative));
    }

    #[mz_ore::test]
    fn binary_accepts_negate_and_infinity_monotone() {
        assert!(has(Shape::Binary, Modifier::Negate));
        assert!(has(Shape::Binary, Modifier::IsInfinityMonotone));
        assert!(has(Shape::Binary, Modifier::IsInfixOp));
        assert!(!has(Shape::Binary, Modifier::Inverse));
        assert!(!has(Shape::Binary, Modifier::IsAssociative));
    }

    #[mz_ore::test]
    fn variadic_accepts_associative_only_among_the_exclusives() {
        assert!(has(Shape::Variadic, Modifier::IsAssociative));
        assert!(has(Shape::Variadic, Modifier::IsInfixOp));
        assert!(!has(Shape::Variadic, Modifier::Negate));
        assert!(!has(Shape::Variadic, Modifier::Inverse));
        assert!(!has(Shape::Variadic, Modifier::IsInfinityMonotone));
    }

    #[mz_ore::test]
    fn could_error_introduces_nulls_and_is_monotone_are_universal() {
        for shape in [Shape::Unary, Shape::Binary, Shape::Variadic] {
            assert!(has(shape, Modifier::CouldError), "{}", shape.label());
            assert!(has(shape, Modifier::IntroducesNulls), "{}", shape.label());
            assert!(has(shape, Modifier::IsMonotone), "{}", shape.label());
        }
    }

    #[mz_ore::test]
    fn is_monotone_returns_a_pair_only_for_binary() {
        let ret = |shape: Shape| {
            shape
                .modifiers()
                .iter()
                .find(|(m, _)| *m == Modifier::IsMonotone)
                .map(|(_, r)| *r)
                .expect("is_monotone is universal")
        };
        assert_eq!(ret(Shape::Unary), ReturnTy::Bool);
        assert_eq!(ret(Shape::Binary), ReturnTy::BoolPair);
        assert_eq!(ret(Shape::Variadic), ReturnTy::Bool);
    }
}
```

Add the module declaration next to the existing one at
`src/expr-derive-impl/src/lib.rs:16`:

```rust
mod shape;
mod sqlfunc;
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p mz-expr-derive-impl shape:: 2>&1 | tail -20`

Expected: compile failure, `cannot find type 'Shape' in this scope` and the same for
`Modifier` and `ReturnTy`.

- [ ] **Step 3: Write the minimal implementation**

Insert above the `mod tests` block in `src/expr-derive-impl/src/shape.rs`:

```rust
use proc_macro2::TokenStream;
use quote::quote;

/// The three scalar function arities the macro generates for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shape {
    Unary,
    Binary,
    Variadic,
}

/// A modifier that maps directly onto one optional trait method.
///
/// Modifiers that do not produce a trait method, such as `sqlname`, `output_type`,
/// `output_type_expr`, `test`, and `skip_display`, are absent: `crate::generate`
/// handles those explicitly because they feed `Display`, the output-type body, or the
/// emission decision instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Modifier {
    CouldError,
    IntroducesNulls,
    IsMonotone,
    IsInfixOp,
    PropagatesNulls,
    Inverse,
    PreservesUniqueness,
    IsEliminableCast,
    Negate,
    IsInfinityMonotone,
    IsAssociative,
}

impl Modifier {
    /// The attribute key, which is also the generated method name.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Modifier::CouldError => "could_error",
            Modifier::IntroducesNulls => "introduces_nulls",
            Modifier::IsMonotone => "is_monotone",
            Modifier::IsInfixOp => "is_infix_op",
            Modifier::PropagatesNulls => "propagates_nulls",
            Modifier::Inverse => "inverse",
            Modifier::PreservesUniqueness => "preserves_uniqueness",
            Modifier::IsEliminableCast => "is_eliminable_cast",
            Modifier::Negate => "negate",
            Modifier::IsInfinityMonotone => "is_infinity_monotone",
            Modifier::IsAssociative => "is_associative",
        }
    }
}

/// The return type of a generated override method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReturnTy {
    Bool,
    BoolPair,
    OptUnaryFunc,
    OptBinaryFunc,
}

impl ReturnTy {
    pub(crate) fn to_tokens(&self) -> TokenStream {
        match self {
            ReturnTy::Bool => quote! { bool },
            ReturnTy::BoolPair => quote! { (bool, bool) },
            ReturnTy::OptUnaryFunc => quote! { Option<crate::UnaryFunc> },
            ReturnTy::OptBinaryFunc => quote! { Option<crate::BinaryFunc> },
        }
    }
}

const UNARY_MODIFIERS: &[(Modifier, ReturnTy)] = &[
    (Modifier::CouldError, ReturnTy::Bool),
    (Modifier::IntroducesNulls, ReturnTy::Bool),
    (Modifier::IsMonotone, ReturnTy::Bool),
    (Modifier::Inverse, ReturnTy::OptUnaryFunc),
    (Modifier::PreservesUniqueness, ReturnTy::Bool),
    (Modifier::IsEliminableCast, ReturnTy::Bool),
];

const BINARY_MODIFIERS: &[(Modifier, ReturnTy)] = &[
    (Modifier::CouldError, ReturnTy::Bool),
    (Modifier::IntroducesNulls, ReturnTy::Bool),
    (Modifier::IsMonotone, ReturnTy::BoolPair),
    (Modifier::IsInfixOp, ReturnTy::Bool),
    (Modifier::PropagatesNulls, ReturnTy::Bool),
    (Modifier::Negate, ReturnTy::OptBinaryFunc),
    (Modifier::IsInfinityMonotone, ReturnTy::Bool),
];

const VARIADIC_MODIFIERS: &[(Modifier, ReturnTy)] = &[
    (Modifier::CouldError, ReturnTy::Bool),
    (Modifier::IntroducesNulls, ReturnTy::Bool),
    (Modifier::IsMonotone, ReturnTy::Bool),
    (Modifier::IsInfixOp, ReturnTy::Bool),
    (Modifier::PropagatesNulls, ReturnTy::Bool),
    (Modifier::IsAssociative, ReturnTy::Bool),
];

impl Shape {
    /// The modifiers this arity accepts, with the return type of each generated
    /// method. A modifier absent from this table is rejected.
    pub(crate) fn modifiers(&self) -> &'static [(Modifier, ReturnTy)] {
        match self {
            Shape::Unary => UNARY_MODIFIERS,
            Shape::Binary => BINARY_MODIFIERS,
            Shape::Variadic => VARIADIC_MODIFIERS,
        }
    }

    pub(crate) fn label(&self) -> &'static str {
        match self {
            Shape::Unary => "unary",
            Shape::Binary => "binary",
            Shape::Variadic => "variadic",
        }
    }
}
```

The `Modifier` and `ReturnTy` variants are read only by the tests at this point, so
add `#![allow(dead_code)]` at the top of `shape.rs` if the workspace lint config
rejects unused items. Remove it in Task 3, when `generate` consumes them.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p mz-expr-derive-impl shape:: 2>&1 | tail -10`

Expected: `5 passed`.

- [ ] **Step 5: Verify no snapshot moved**

Run:
```bash
cargo test -p mz-expr-derive-impl
git status --short src/expr-derive-impl/src/snapshots/
```

Expected: all tests pass, and `git status` prints nothing for the snapshots
directory. Nothing is wired into the macro yet, so this is the baseline reading of
the gate that Tasks 2 through 5 must keep clean.

- [ ] **Step 6: Commit**

```bash
bin/fmt
bin/lint
git add src/expr-derive-impl/src/shape.rs src/expr-derive-impl/src/lib.rs
git commit -m "$(cat <<'EOF'
expr-derive-impl: Add per-arity shape and modifier tables

Declares the three scalar function arities and, for each, which modifiers it
accepts and the return type of the method each modifier generates. Nothing
consumes the tables yet.

The tables replace legality rules that are currently spread across 18
separately written rejections in the three generator arms.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Route modifier validation through the table

**Files:**
- Modify: `src/expr-derive-impl/src/sqlfunc.rs` (add `Modifiers::iter`, replace the 18 rejection blocks)
- Modify: `src/expr-derive-impl/src/shape.rs` (add `Shape::reject_inapplicable`)
- Test: `src/expr-derive-impl/src/lib.rs` (`mod test`)

**Interfaces:**
- Consumes: `Shape`, `Modifier`, `ReturnTy` from Task 1.
- Produces:
  - `impl Modifiers { pub(crate) fn iter(&self) -> impl Iterator<Item = (Modifier, &syn::Expr)> + '_ }`
  - `impl Shape { pub(crate) fn reject_inapplicable(&self, mods: &Modifiers) -> darling::Result<()> }`

- [ ] **Step 1: Write the failing test**

Add to `#[cfg(test)] mod test` in `src/expr-derive-impl/src/lib.rs`:

```rust
#[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
#[mz_ore::test]
fn unary_rejects_negate_by_name() {
    let (output, _input) = crate::test_sqlfunc(
        quote! { negate = to_unary!(super::Foo) },
        quote! {
            fn some_unary<'a>(a: i32) -> i32 { a }
        },
    );
    assert!(
        output.contains("negate") && output.contains("unary"),
        "expected an error naming the modifier and the arity, got:\n{output}"
    );
}

#[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
#[mz_ore::test]
fn unary_rejects_is_infinity_monotone() {
    let (output, _input) = crate::test_sqlfunc(
        quote! { is_infinity_monotone = false },
        quote! {
            fn some_unary<'a>(a: i32) -> i32 { a }
        },
    );
    assert!(
        output.contains("is_infinity_monotone"),
        "is_infinity_monotone must be rejected on unary rather than ignored, got:\n{output}"
    );
}
```

The second test is the behavior change the spec calls out: today
`is_infinity_monotone` on a unary function is destructured as
`is_infinity_monotone: _` and silently discarded.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p mz-expr-derive-impl unary_rejects 2>&1 | tail -25`

Expected: `unary_rejects_negate_by_name` fails because the current message is
`"negate not supported for unary functions"` from
`darling::Error::unknown_field`, which does contain both words, so this one may
pass already. `unary_rejects_is_infinity_monotone` must FAIL, because the current
code produces a successful expansion with no mention of the modifier. If the first
test passes at this step, that is fine, it is a regression guard. The second failing
is the signal to proceed.

- [ ] **Step 3: Write the minimal implementation**

Add to `src/expr-derive-impl/src/sqlfunc.rs`, next to the `Modifiers` definition:

```rust
impl Modifiers {
    /// The method-producing modifiers that are present, in table order.
    ///
    /// Modifiers that do not produce a trait method are excluded, because
    /// `crate::generate` consumes those by name.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (Modifier, &Expr)> + '_ {
        [
            (Modifier::CouldError, self.could_error.as_ref()),
            (Modifier::IntroducesNulls, self.introduces_nulls.as_ref()),
            (Modifier::IsMonotone, self.is_monotone.as_ref()),
            (Modifier::IsInfixOp, self.is_infix_op.as_ref()),
            (Modifier::PropagatesNulls, self.propagates_nulls.as_ref()),
            (Modifier::Inverse, self.inverse.as_ref()),
            (Modifier::PreservesUniqueness, self.preserves_uniqueness.as_ref()),
            (Modifier::IsEliminableCast, self.is_eliminable_cast.as_ref()),
            (Modifier::Negate, self.negate.as_ref()),
            (Modifier::IsInfinityMonotone, self.is_infinity_monotone.as_ref()),
            (Modifier::IsAssociative, self.is_associative.as_ref()),
        ]
        .into_iter()
        .filter_map(|(modifier, expr)| expr.map(|expr| (modifier, expr)))
    }
}
```

Add to `src/expr-derive-impl/src/shape.rs`:

```rust
impl Shape {
    /// Errors if `mods` carries a method-producing modifier this arity does not accept.
    pub(crate) fn reject_inapplicable(&self, mods: &crate::sqlfunc::Modifiers) -> darling::Result<()> {
        for (modifier, _) in mods.iter() {
            let accepted = self
                .modifiers()
                .iter()
                .any(|(candidate, _)| *candidate == modifier);
            if !accepted {
                return Err(darling::Error::custom(format!(
                    "`{}` is not supported for {} functions",
                    modifier.name(),
                    self.label(),
                )));
            }
        }
        Ok(())
    }
}
```

`Modifiers` and its fields need `pub(crate)` visibility for `shape.rs` to read them
through `Modifiers::iter`. Change `pub(crate) struct Modifiers` to keep its fields
private and rely on `iter` alone, which is why `iter` lives in `sqlfunc.rs`.

Then, in each of `unary_func`, `binary_func`, and `variadic_func`, replace the
per-modifier rejection blocks with a single call before the destructuring:

```rust
Shape::Unary.reject_inapplicable(&modifiers)?;
```

Delete the six `if is_infix_op.is_some() { return Err(...) }` style blocks from
`unary_func`, the six from `binary_func`, and the six from `variadic_func`. Keep the
three checks that are not about modifier legality: `output_type` conflicting with
`output_type_expr`, `output_type_expr` requiring `introduces_nulls`, and variadic's
"must have at least one input parameter".

Change the destructuring in all three arms so it no longer needs the `: _` escape
hatches. In `unary_func` and `variadic_func` that means `is_infinity_monotone` is
simply not bound.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p mz-expr-derive-impl 2>&1 | tail -10`

Expected: all tests pass, including both new ones.

- [ ] **Step 5: Verify no snapshot moved**

Run: `git status --short src/expr-derive-impl/src/snapshots/`

Expected: no output. The error text changed from darling's `unknown_field` wording to
the new `custom` wording, so if any of the 13 snapshots records a rejection message
this step will catch it. `mz_expr_derive_impl__test__unary_arena_fn.snap` records a
`compile_error!`, but from `Error::custom` in the arity dispatch, which this task does
not touch.

- [ ] **Step 6: Commit**

```bash
bin/fmt
bin/lint
git add src/expr-derive-impl/src/
git commit -m "$(cat <<'EOF'
expr-derive-impl: Validate modifiers against the shape tables

Replaces 18 separately written rejections across the three generator arms with
one loop over the modifiers that are present, checked against the arity's
table.

Passing `is_infinity_monotone` to a unary or variadic function is now an error.
Both arms previously destructured it as `is_infinity_monotone: _` and discarded
it without complaint. All six uses in the tree are on binary `mul_*` and `div_*`
functions, so nothing in the tree changes.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: One helper for override-method emission

**Files:**
- Create: `src/expr-derive-impl/src/generate.rs`
- Modify: `src/expr-derive-impl/src/lib.rs` (add `mod generate;`)
- Modify: `src/expr-derive-impl/src/sqlfunc.rs` (replace the 19 `quote!` blocks)
- Modify: `src/expr-derive-impl/src/shape.rs` (drop the `allow(dead_code)` from Task 1)

**Interfaces:**
- Consumes: `Shape`, `Modifier`, `ReturnTy`, `Modifiers::iter` from Tasks 1 and 2.
- Produces:
  - `pub(crate) fn override_methods(shape: Shape, mods: &Modifiers) -> Vec<TokenStream>`

- [ ] **Step 1: Write the failing test**

Add to `src/expr-derive-impl/src/generate.rs`:

```rust
#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::shape::Shape;

    #[mz_ore::test]
    fn binary_is_monotone_emits_a_pair_return() {
        let mods = crate::sqlfunc::Modifiers::from_tokens(quote! {
            is_monotone = (true, true),
            could_error = false,
        })
        .expect("parses");
        let methods = super::override_methods(Shape::Binary, &mods);
        let rendered = methods
            .iter()
            .map(|m| m.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            rendered.contains("fn is_monotone (& self) -> (bool , bool)"),
            "got:\n{rendered}"
        );
        assert!(
            rendered.contains("fn could_error (& self) -> bool"),
            "got:\n{rendered}"
        );
    }

    #[mz_ore::test]
    fn unary_is_monotone_emits_a_bool_return() {
        let mods = crate::sqlfunc::Modifiers::from_tokens(quote! { is_monotone = true })
            .expect("parses");
        let methods = super::override_methods(Shape::Unary, &mods);
        let rendered = methods[0].to_string();
        assert!(rendered.contains("fn is_monotone (& self) -> bool"), "got:\n{rendered}");
    }

    #[mz_ore::test]
    fn absent_modifiers_emit_nothing() {
        let mods = crate::sqlfunc::Modifiers::from_tokens(quote! {}).expect("parses");
        assert!(super::override_methods(Shape::Unary, &mods).is_empty());
    }
}
```

This needs a test constructor on `Modifiers`. Add it in `sqlfunc.rs`:

```rust
impl Modifiers {
    /// Parses modifiers from attribute tokens. Test helper for `crate::generate`.
    #[cfg(test)]
    pub(crate) fn from_tokens(tokens: TokenStream) -> darling::Result<Self> {
        let args = darling::ast::NestedMeta::parse_meta_list(tokens)?;
        <Self as FromMeta>::from_list(&args)
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p mz-expr-derive-impl generate:: 2>&1 | tail -20`

Expected: compile failure, `cannot find function 'override_methods'`.

- [ ] **Step 3: Write the minimal implementation**

Add above the test module in `src/expr-derive-impl/src/generate.rs`:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Emission shared by every scalar function arity.

use proc_macro2::{Ident, TokenStream};
use quote::quote;

use crate::shape::Shape;
use crate::sqlfunc::Modifiers;

/// One `fn name(&self) -> Ret { expr }` per modifier present in `mods`.
///
/// Emits in the arity table's order rather than the attribute's order, so the
/// generated code is stable against how a call site happens to spell its
/// modifiers.
pub(crate) fn override_methods(shape: Shape, mods: &Modifiers) -> Vec<TokenStream> {
    let present: Vec<_> = mods.iter().collect();
    shape
        .modifiers()
        .iter()
        .filter_map(|(modifier, ret)| {
            let expr = present
                .iter()
                .find(|(candidate, _)| candidate == modifier)
                .map(|(_, expr)| *expr)?;
            let name = Ident::new(modifier.name(), proc_macro2::Span::call_site());
            let ret = ret.to_tokens();
            Some(quote! {
                fn #name(&self) -> #ret {
                    #expr
                }
            })
        })
        .collect()
}
```

Declare the module in `src/expr-derive-impl/src/lib.rs`:

```rust
mod generate;
mod shape;
mod sqlfunc;
```

In `sqlfunc.rs`, delete the 19 `let *_fn = ...map(|x| quote! { ... })` blocks across
the three arms and replace each arm's use of them with:

```rust
let override_methods = crate::generate::override_methods(Shape::Unary, &modifiers);
```

then splice `#(#override_methods)*` into the trait impl where the individual
`#could_error_fn #is_monotone_fn ...` interpolations were.

Two interpolations are not plain overrides and must stay hand-built:
`introduces_nulls`, because the arms synthesize it from `output_type` when the
modifier is absent, and the output-type method itself. Keep both as they are and
exclude `Modifier::IntroducesNulls` from `override_methods`' output when the arm has
already synthesized one, by clearing `introduces_nulls` on the `Modifiers` copy
passed in.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p mz-expr-derive-impl 2>&1 | tail -10`

Expected: all tests pass.

- [ ] **Step 5: Verify no snapshot moved**

Run: `git status --short src/expr-derive-impl/src/snapshots/`

Expected: no output.

This is the step most likely to fail in this task, because method ordering inside the
generated `impl` block is visible in every snapshot. If a snapshot moves, compare the
diff: if the only change is the order of the override methods, reorder the arity
tables in `shape.rs` to match the emission order the arms used, rather than accepting
the snapshot. The arms emitted in the order `could_error`, `introduces_nulls`,
`inverse`, `is_monotone`, `preserves_uniqueness`, `is_eliminable_cast` for unary, and
`could_error`, `introduces_nulls`, `is_infix_op`, `is_monotone`,
`is_infinity_monotone`, `negate`, `propagates_nulls` for binary. Set the tables to
those orders.

- [ ] **Step 6: Commit**

```bash
bin/fmt
bin/lint
git add src/expr-derive-impl/src/
git commit -m "$(cat <<'EOF'
expr-derive-impl: Emit override methods from one helper

The three generator arms each built the same `fn name(&self) -> Ret { expr }`
shape, once per modifier, for 19 near-identical blocks. One helper now walks the
arity's table and emits them, taking the return type from the table so binary's
`is_monotone` keeps its `(bool, bool)`.

Emission follows table order rather than attribute order, so generated code does
not depend on how a call site spells its modifiers. The tables are ordered to
reproduce the previous output exactly.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Shared struct, Display, and FuncName emission

**Files:**
- Modify: `src/expr-derive-impl/src/generate.rs`
- Modify: `src/expr-derive-impl/src/sqlfunc.rs`

**Interfaces:**
- Consumes: Task 3's `override_methods`.
- Produces:
  - `pub(crate) struct Emission { pub struct_name: Ident, pub has_self: bool, pub sqlname: TokenStream, pub fn_name: Ident }`
  - `pub(crate) fn emit(e: &Emission, func: &syn::ItemFn, trait_impl: TokenStream) -> TokenStream`

- [ ] **Step 1: Write the failing test**

Add to `generate.rs`'s `mod tests`:

```rust
#[mz_ore::test]
fn unit_struct_emission_defines_the_struct_and_keeps_the_function() {
    let func: syn::ItemFn = syn::parse_quote! {
        fn some_fn(a: i32) -> i32 { a }
    };
    let e = super::Emission {
        struct_name: syn::parse_quote!(SomeFn),
        has_self: false,
        sqlname: quote! { "some_fn" },
        fn_name: syn::parse_quote!(some_fn),
    };
    let out = super::emit(&e, &func, quote! { impl Marker for SomeFn {} }).to_string();
    assert!(out.contains("pub struct SomeFn"), "got:\n{out}");
    assert!(out.contains("impl std :: fmt :: Display for SomeFn"), "got:\n{out}");
    assert!(out.contains("impl crate :: func :: FuncName for SomeFn"), "got:\n{out}");
    assert!(out.contains("fn some_fn"), "got:\n{out}");
}

#[mz_ore::test]
fn external_struct_emission_attaches_a_method_and_defines_no_struct() {
    let func: syn::ItemFn = syn::parse_quote! {
        fn some_fn(&self, a: i32) -> i32 { a }
    };
    let e = super::Emission {
        struct_name: syn::parse_quote!(SomeFn),
        has_self: true,
        sqlname: quote! { "some_fn" },
        fn_name: syn::parse_quote!(some_fn),
    };
    let out = super::emit(&e, &func, quote! { impl Marker for SomeFn {} }).to_string();
    assert!(!out.contains("pub struct SomeFn"), "got:\n{out}");
    assert!(out.contains("impl SomeFn"), "got:\n{out}");
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p mz-expr-derive-impl generate:: 2>&1 | tail -20`

Expected: compile failure, `cannot find struct 'Emission'`.

- [ ] **Step 3: Write the minimal implementation**

Add to `generate.rs`:

```rust
/// What the shared emission needs that is not arity specific.
pub(crate) struct Emission {
    pub struct_name: Ident,
    /// True when the struct is defined at the call site, so emission attaches an
    /// inherent method instead of defining a unit struct.
    pub has_self: bool,
    pub sqlname: TokenStream,
    pub fn_name: Ident,
}

/// Wraps an arity's trait impl with the parts every arity shares.
pub(crate) fn emit(e: &Emission, func: &syn::ItemFn, trait_impl: TokenStream) -> TokenStream {
    let Emission { struct_name, has_self, sqlname, fn_name } = e;

    let display_impl = quote! {
        impl std::fmt::Display for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(#sqlname)
            }
        }
    };

    let funcname_impl = quote! {
        impl crate::func::FuncName for #struct_name {
            const NAME: &'static str = stringify!(#fn_name);
        }
    };

    if *has_self {
        quote! {
            impl #struct_name {
                #func
            }
            #trait_impl
            #display_impl
            #funcname_impl
        }
    } else {
        quote! {
            #[derive(
                Ord, PartialOrd, Clone,
                Debug, Eq, PartialEq, serde::Serialize,
                serde::Deserialize, Hash,
            )]
            #[cfg_attr(any(test, feature = "proptest"), derive(proptest_derive::Arbitrary))]
            pub struct #struct_name;

            #trait_impl
            #display_impl
            #funcname_impl

            #func
        }
    }
}
```

Rewrite the tail of each arm in `sqlfunc.rs` to build an `Emission` and call `emit`,
deleting the three copies of the struct derives, the `Display` impl, and the
`FuncName` impl.

Note the ordering difference to preserve: `unary_func` and `binary_func` currently
emit `trait_impl`, `Display`, `FuncName`, then `#func`, while `variadic_func` emits
`trait_impl`, `Display`, `FuncName` with `#func` last in the unit-struct branch and
inside `impl #struct_name` in the `has_self` branch. `emit` above matches both. Check
the snapshots in Step 5 rather than assuming.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p mz-expr-derive-impl 2>&1 | tail -10`

Expected: all tests pass.

- [ ] **Step 5: Verify no snapshot moved**

Run: `git status --short src/expr-derive-impl/src/snapshots/`

Expected: no output. If a snapshot moves here it is almost certainly item ordering
inside the generated module. Adjust `emit` to match the snapshot, never the reverse.

- [ ] **Step 6: Commit**

```bash
bin/fmt
bin/lint
git add src/expr-derive-impl/src/
git commit -m "$(cat <<'EOF'
expr-derive-impl: Emit struct, Display, and FuncName once

The unit-struct definition, the `Display` impl, the `FuncName` impl, and
re-emitting the annotated function are identical across the three arities. They
now live in one function that wraps whatever trait impl an arity produced.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Collapse the three arms into `generate`

**Files:**
- Modify: `src/expr-derive-impl/src/generate.rs`
- Modify: `src/expr-derive-impl/src/shape.rs`
- Modify: `src/expr-derive-impl/src/sqlfunc.rs`

**Interfaces:**
- Consumes: everything from Tasks 1 to 4.
- Produces:
  - `impl Shape { pub(crate) fn trait_path(&self) -> TokenStream }`
  - `impl Shape { pub(crate) fn output_method(&self) -> (Ident, TokenStream) }` returning the method name and its parameter list
  - `impl Shape { pub(crate) fn takes_arena(&self) -> bool }`
  - `pub(crate) fn generate(shape: Shape, func: &syn::ItemFn, mods: Modifiers, struct_ty: Option<syn::Path>, has_self: bool) -> darling::Result<TokenStream>`
  - `unary_func`, `binary_func`, `variadic_func` reduced to calls into `generate`

- [ ] **Step 1: Write the failing test**

Add to `shape.rs`'s `mod tests`, encoding the naming divergence the spec calls out:

```rust
#[mz_ore::test]
fn variadic_output_method_is_named_output_type() {
    let (unary, _) = Shape::Unary.output_method();
    let (binary, _) = Shape::Binary.output_method();
    let (variadic, _) = Shape::Variadic.output_method();
    assert_eq!(unary.to_string(), "output_sql_type");
    assert_eq!(binary.to_string(), "output_sql_type");
    assert_eq!(variadic.to_string(), "output_type");
}

#[mz_ore::test]
fn only_unary_takes_a_single_column_type() {
    let (_, unary) = Shape::Unary.output_method();
    let (_, binary) = Shape::Binary.output_method();
    assert!(unary.to_string().contains("SqlColumnType"));
    assert!(!unary.to_string().contains("["));
    assert!(binary.to_string().contains("["));
}

#[mz_ore::test]
fn unary_is_the_only_shape_without_an_arena() {
    assert!(!Shape::Unary.takes_arena());
    assert!(Shape::Binary.takes_arena());
    assert!(Shape::Variadic.takes_arena());
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p mz-expr-derive-impl shape:: 2>&1 | tail -20`

Expected: compile failure, `no method named 'output_method'`.

- [ ] **Step 3: Write the minimal implementation**

Add to `shape.rs`:

```rust
impl Shape {
    pub(crate) fn trait_path(&self) -> TokenStream {
        match self {
            Shape::Unary => quote! { crate::func::EagerUnaryFunc },
            Shape::Binary => quote! { crate::func::binary::EagerBinaryFunc },
            Shape::Variadic => quote! { crate::func::variadic::EagerVariadicFunc },
        }
    }

    /// The name and parameter list of the trait's output-type method.
    ///
    /// `EagerVariadicFunc` has no `output_sql_type`. Its core method is named
    /// `output_type` and takes the column types directly, where the unary and binary
    /// traits declare `output_sql_type` and carry a separate `output_type` wrapper
    /// over `ReprColumnType`.
    pub(crate) fn output_method(&self) -> (Ident, TokenStream) {
        let span = proc_macro2::Span::call_site();
        match self {
            Shape::Unary => (
                Ident::new("output_sql_type", span),
                quote! { input_type: mz_repr::SqlColumnType },
            ),
            Shape::Binary => (
                Ident::new("output_sql_type", span),
                quote! { input_types: &[mz_repr::SqlColumnType] },
            ),
            Shape::Variadic => (
                Ident::new("output_type", span),
                quote! { input_types: &[mz_repr::SqlColumnType] },
            ),
        }
    }

    /// Whether `call` receives a `&'a RowArena`.
    pub(crate) fn takes_arena(&self) -> bool {
        match self {
            Shape::Unary => false,
            Shape::Binary | Shape::Variadic => true,
        }
    }
}
```

Add `use proc_macro2::Ident;` to `shape.rs`.

Then write `generate` in `generate.rs`, moving the body that the three arms share:
generic-parameter erasure, `output_type_expr` derivation, the output-type method
body with its per-shape nullability formula, `call`, and the `emit` call. Keep the
per-shape nullability difference behind `Shape::nullability`, which returns the
`is_null` expression: unary's
`nullable || (propagates_nulls && input_type.nullable)` against binary and
variadic's version carrying the `non_nullable_position_checks` term.

Reduce the three entry points to:

```rust
fn unary_func(
    func: &syn::ItemFn,
    modifiers: Modifiers,
    struct_ty: Option<syn::Path>,
    has_self: bool,
) -> darling::Result<TokenStream> {
    crate::generate::generate(Shape::Unary, func, modifiers, struct_ty, has_self)
}
```

with `binary_func` and `variadic_func` differing only in the `Shape`. The `arena`
parameter the arms currently take comes from `Shape::takes_arena` instead, except
that `Arity::Unary { arena: true }` must keep returning today's
`compile_error!("Unary functions do not yet support RowArena.")` until PR2. Keep that
rejection in the arity dispatch in `sqlfunc.rs`, not in `generate`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p mz-expr-derive-impl 2>&1 | tail -15`

Expected: all tests pass.

- [ ] **Step 5: Verify no snapshot moved and the consumer still builds**

Run:
```bash
git status --short src/expr-derive-impl/src/snapshots/
cargo check -p mz-expr 2>&1 | tail -5
```

Expected: no snapshot output, and `mz-expr` checks clean. `mz-expr` is the real test
of this task: it expands `#[sqlfunc]` 535 times, so a shape mismatch the 13 snapshots
miss will surface here.

- [ ] **Step 6: Confirm the tripwire snapshot is intact**

Run: `cat src/expr-derive-impl/src/snapshots/mz_expr_derive_impl__test__unary_arena_fn.snap`

Expected: still contains
`::core::compile_error! { "Unary functions do not yet support RowArena." }`. This
proves PR1 added no capability.

- [ ] **Step 7: Commit**

```bash
bin/fmt
bin/lint
git add src/expr-derive-impl/src/
git commit -m "$(cat <<'EOF'
expr-derive-impl: Drive all three arities from one generator

`unary_func`, `binary_func`, and `variadic_func` were three independent 200 to
320 line paths that agreed on almost everything. They are now calls into one
`generate`, parameterized by a `Shape` that answers the six questions where the
traits genuinely differ: trait path, input associated type, whether `call` takes
an arena, the output-type method's name, its parameter, and the nullability
formula.

The name is a real divergence rather than a wart to paper over.
`EagerVariadicFunc` has no `output_sql_type`, its core method is `output_type`,
so `Shape::output_method` returns the name alongside the signature and emission
uses whichever it gets.

Generated output is unchanged. All 13 snapshots are byte identical, including the
one asserting that unary functions still reject a `RowArena`.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Open PR1

**Files:**
- Modify: `doc/developer/sqlfunc.md` (note where modifier legality now lives)

- [ ] **Step 1: Document where capabilities are declared**

Append to the Modifiers section of `doc/developer/sqlfunc.md`:

```markdown
Which modifiers apply to which arity is declared in
`src/expr-derive-impl/src/shape.rs`, one table per arity. A modifier absent from an
arity's table is rejected with an error naming both the modifier and the arity.
```

- [ ] **Step 2: Verify the whole gate**

Run:
```bash
cargo test -p mz-expr-derive-impl
cargo test -p mz-expr
cargo check --workspace 2>&1 | tail -5
git status --short src/expr-derive-impl/src/snapshots/
bin/lint
```

Expected: tests pass, workspace checks clean, no snapshot changed, lint clean.

- [ ] **Step 3: Commit and push**

```bash
bin/fmt
git add doc/developer/sqlfunc.md
git commit -m "$(cat <<'EOF'
doc: Point at where sqlfunc modifier legality is declared

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
git push -u origin HEAD
```

- [ ] **Step 4: Open the pull request**

Target `upstream/main`. Body:

```markdown
Collapses the `#[sqlfunc]` macro's three generator arms into one
shape-parameterized path. `unary_func`, `binary_func`, and `variadic_func` were
762 lines across three paths that agreed on almost everything: they each built the
same eleven optional override methods with 19 near-identical `quote!` blocks,
enforced modifier legality with 18 separately written rejections, and each repeated
the emission of the struct, `Display`, `FuncName`, and the annotated function.

A `Shape` enum now answers the six questions where the arities genuinely differ.
One of those is a naming divergence rather than a signature difference:
`EagerVariadicFunc` has no `output_sql_type`, its core method is `output_type`, so
`Shape::output_method` carries the name alongside the parameter list.

No capability is added and no generated output changes. All 13 snapshots are byte
identical, including the one asserting that unary functions still reject a
`RowArena`, which is the tripwire proving this PR smuggled nothing in.

Adding a modifier to an arity is now a row in a table in
`src/expr-derive-impl/src/shape.rs`.

Design: `doc/developer/design/20260917_sqlfunc_canonicalization.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

- [ ] **Step 5: Close the superseded drafts**

Close #36697 and #36705, each with a comment naming this PR and the design document,
and stating that their conversions predate #37961's MIR and LIR separation so the
`Box<MirScalarExpr>` hardcoding in them is no longer correct.

---

## Task 7: Arena on `EagerUnaryFunc::call`

**Files:**
- Modify: `src/expr/src/scalar/func/unary.rs:114` (trait method), `:159` onward (blanket impl)
- Modify: 15 files under `src/expr/src/scalar/func/impls/` holding the 40 hand-written `EagerUnaryFunc` impls
- Modify: `src/expr-derive-impl/src/shape.rs` (`takes_arena` for unary), `src/expr-derive-impl/src/sqlfunc.rs` (drop the arity rejection)
- Test: `src/expr-derive-impl/src/lib.rs`

**Interfaces:**
- Consumes: `Shape::takes_arena` from Task 5.
- Produces: `EagerUnaryFunc::call(&self, input: Self::Input<'a>, temp_storage: &'a RowArena) -> Self::Output<'a>`

Note the scale: the trait change forces a mechanical re-signing of all 40
hand-written unary `call` methods, which PR4 onward then delete. That waste is
unavoidable if the arena lands before the conversions, and landing it after would
mean the conversions could not use it.

- [ ] **Step 1: Write the failing test**

Change the existing `insta_test_unary_arena` in `src/expr-derive-impl/src/lib.rs`
from asserting a rejection to asserting real output. The test body already passes a
`temp_storage: &RowArena`, so only the snapshot needs to change, which Step 4
handles. Add a second test for an arena the function actually uses:

```rust
#[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
#[mz_ore::test]
fn insta_test_unary_arena_used() {
    let attr = quote! { sqlname = "to_arena_string", test = true };
    let item = quote! {
        fn to_arena_string<'a>(a: i32, temp_storage: &'a RowArena) -> &'a str {
            temp_storage.push_string(a.to_string())
        }
    };
    let (output, input) = crate::test_sqlfunc(attr, item);
    insta::assert_snapshot!("unary_arena_used", output, &input);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p mz-expr-derive-impl unary_arena 2>&1 | tail -20`

Expected: `insta_test_unary_arena_used` fails on a new snapshot whose content is
`compile_error!("Unary functions do not yet support RowArena.")`.

- [ ] **Step 3: Write the minimal implementation**

In `src/expr/src/scalar/func/unary.rs`, change the trait method:

```rust
fn call<'a>(&self, input: Self::Input<'a>, temp_storage: &'a RowArena) -> Self::Output<'a>;
```

and update the blanket `impl<T: EagerUnaryFunc> LazyUnaryFunc for T` to pass
`temp_storage` through.

In `src/expr-derive-impl/src/shape.rs`, change `takes_arena` so `Shape::Unary`
returns `true`. In `src/expr-derive-impl/src/sqlfunc.rs`, delete the
`Arity::Unary { arena: true } => Err(...)` rejection so both arena states route to
`generate`. `generate` binds the parameter as `_temp_storage` when the annotated
function does not take one, matching what `binary_func` already does.

For the 40 hand-written impls, add the parameter and prefix it with an underscore.
Find them with:

```bash
grep -rn "fn call<'a>(&self, a: Self::Input<'a>) -> Self::Output<'a>" \
  src/expr/src/scalar/func/impls/
```

- [ ] **Step 4: Run the tests and accept the snapshots**

Run:
```bash
cargo test -p mz-expr-derive-impl 2>&1 | tail -10
cargo insta accept
git diff --stat src/expr-derive-impl/src/snapshots/
```

Expected: two snapshots change. `unary_arena_fn.snap` loses its `compile_error!` and
gains a real impl. `unary_arena_used.snap` is created. Every other unary snapshot
gains a `_temp_storage` parameter on `call`, so expect roughly six files touched.
Read the diff and confirm each change is exactly the new parameter, nothing else.

- [ ] **Step 5: Verify the consumer builds**

Run: `cargo test -p mz-expr 2>&1 | tail -10`

Expected: pass.

- [ ] **Step 6: Commit**

```bash
bin/fmt
bin/lint
git add src/expr-derive-impl/ src/expr/src/scalar/func/
git commit -m "$(cat <<'EOF'
expr: Give EagerUnaryFunc::call a RowArena

`EagerBinaryFunc::call` and `EagerVariadicFunc::call` already take an arena.
Unary did not, which blocked the compound-type casts that allocate their output
and forced them to stay hand-written `LazyUnaryFunc` impls.

The 40 hand-written unary impls take the parameter and ignore it. Later commits
in this stack convert them, at which point the macro supplies the signature.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Stateful unary and binary

**Files:**
- Modify: `src/expr-derive-impl/src/sqlfunc.rs` (arity, argument offsets, entry points)
- Modify: `src/expr-derive-impl/src/generate.rs` (`self.` call expression)
- Test: `src/expr-derive-impl/src/lib.rs`

**Interfaces:**
- Consumes: Task 5's `generate`, Task 7's arena.
- Produces: `#[sqlfunc(StructName, ...)] fn f(&self, ..)` accepted for unary and binary.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
#[mz_ore::test]
fn insta_test_unary_self() {
    let attr = quote! { PadTo, sqlname = "pad_to", test = true };
    let item = quote! {
        fn pad_to<'a>(&self, a: &'a str, temp_storage: &'a RowArena) -> &'a str {
            temp_storage.push_string(format!("{a:width$}", width = self.width))
        }
    };
    let (output, input) = crate::test_sqlfunc(attr, item);
    insta::assert_snapshot!("unary_self", output, &input);
}

#[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
#[mz_ore::test]
fn insta_test_binary_self() {
    let attr = quote! { ClampAt, sqlname = "clamp_at", is_infix_op = false, test = true };
    let item = quote! {
        fn clamp_at<'a>(&self, a: i64, b: i64) -> i64 {
            a.clamp(self.lower, b)
        }
    };
    let (output, input) = crate::test_sqlfunc(attr, item);
    insta::assert_snapshot!("binary_self", output, &input);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p mz-expr-derive-impl _self 2>&1 | tail -20`

Expected: both fail with new snapshots containing
`compile_error!("Unsupported argument type")`, which is `arg_type` hitting
`FnArg::Receiver`.

- [ ] **Step 3: Write the minimal implementation**

In `sqlfunc.rs`, carry `has_self` through the unary and binary arity variants:

```rust
enum Arity {
    Nullary,
    Unary { arena: bool, has_self: bool },
    Binary { arena: bool, has_self: bool },
    Variadic { arena: bool, has_self: bool },
}
```

and pass `struct_ty` and `has_self` from the dispatch into `unary_func` and
`binary_func`, which already forward to `generate` after Task 5.

In `generate`, derive the struct name from `struct_ty` when present, offset argument
indices by one when `has_self`, and build the call expression as
`self.#fn_name(..)` rather than `#fn_name(..)`. `variadic_func` already does all
three, so move that logic into `generate` rather than writing it again.

- [ ] **Step 4: Run the tests and accept the snapshots**

Run:
```bash
cargo test -p mz-expr-derive-impl 2>&1 | tail -10
cargo insta accept
```

Expected: two new snapshots. Read both. Each must define no struct, attach
`impl PadTo { fn pad_to(..) }` or the `ClampAt` equivalent, and call through
`self.pad_to(a, temp_storage)`.

- [ ] **Step 5: Verify the consumer builds**

Run: `cargo test -p mz-expr 2>&1 | tail -10`

Expected: pass, with all previously generated code unchanged, since no existing call
site passes a struct name to a unary or binary function.

- [ ] **Step 6: Commit**

```bash
bin/fmt
bin/lint
git add src/expr-derive-impl/
git commit -m "$(cat <<'EOF'
expr-derive-impl: Accept stateful unary and binary functions

`#[sqlfunc(StructName, ..)]` on a function with a `&self` receiver now works for
unary and binary, as it already did for variadic. The macro attaches an inherent
method and the trait impl to the existing struct instead of defining a unit
struct.

Both arms previously indexed arguments from position zero, so the receiver reached
`arg_type` and produced `compile_error!("Unsupported argument type")`. Because the
arities share one generator, this is an offset and a call expression rather than
two copies of the variadic logic.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: The `skip_display` modifier

**Files:**
- Modify: `src/expr-derive-impl/src/sqlfunc.rs` (`Modifiers`), `src/expr-derive-impl/src/generate.rs` (`Emission`, `emit`)
- Test: `src/expr-derive-impl/src/lib.rs`

**Interfaces:**
- Consumes: Task 4's `Emission`.
- Produces: `skip_display: Option<bool>` on `Modifiers`, `skip_display: bool` on `Emission`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
#[mz_ore::test]
fn insta_test_skip_display() {
    let attr = quote! { ExtractThing, skip_display = true, test = true };
    let item = quote! {
        fn extract_thing<'a>(&self, a: i64) -> i64 {
            a + self.offset
        }
    };
    let (output, input) = crate::test_sqlfunc(attr, item);
    assert!(
        !output.contains("impl std :: fmt :: Display"),
        "skip_display must suppress the Display impl, got:\n{output}"
    );
    insta::assert_snapshot!("skip_display", output, &input);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p mz-expr-derive-impl skip_display 2>&1 | tail -20`

Expected: failure from darling, `Unknown field: 'skip_display'`.

- [ ] **Step 3: Write the minimal implementation**

Add to `Modifiers` in `sqlfunc.rs`:

```rust
/// Suppresses the generated `fmt::Display`. Set this when the struct's name depends
/// on its state, so the call site keeps a hand-written impl.
skip_display: Option<bool>,
```

Add `skip_display: bool` to `Emission` and gate the `display_impl` in `emit`:

```rust
let display_impl = if *skip_display {
    quote! {}
} else {
    quote! { /* as before */ }
};
```

`skip_display` is not a method-producing modifier, so it must not appear in
`Modifier` and must not be yielded by `Modifiers::iter`, or Task 2's validation will
reject it on every arity.

- [ ] **Step 4: Run the tests and accept the snapshot**

Run:
```bash
cargo test -p mz-expr-derive-impl 2>&1 | tail -10
cargo insta accept
```

Expected: one new snapshot, containing no `Display` impl.

- [ ] **Step 5: Commit**

```bash
bin/fmt
bin/lint
git add src/expr-derive-impl/
git commit -m "$(cat <<'EOF'
expr-derive-impl: Add a skip_display modifier

Twenty-four of the hand-written scalar functions format their name from struct
state, for example `extract_{unit}_ts`. Suppressing the generated `Display` lets
them keep the impl they already have, which is simpler than teaching `sqlname` to
evaluate an expression with `self` in scope.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Convert `RangeCreate` and fix the stale docs

**Files:**
- Modify: `src/expr/src/scalar/func/variadic.rs:583-660` (`RangeCreate`)
- Modify: `src/expr/src/scalar/func.rs` (`func_name!`, add a `RangeCreate` entry if the generated one collides)
- Modify: `src/expr-derive/src/lib.rs:28-51` (rustdoc)
- Modify: `doc/developer/sqlfunc.md`

- [ ] **Step 1: Convert `RangeCreate`**

It is the only hand-written `EagerVariadicFunc`, it is stateful, and it is blocked
solely on `Display` matching `self.elem_type`. Replace its `impl EagerVariadicFunc`
block with:

```rust
#[sqlfunc(RangeCreate, skip_display = true, introduces_nulls = false)]
fn range_create<'a>(
    &self,
    lower: Datum<'a>,
    upper: Datum<'a>,
    flags_datum: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    // body moved verbatim from the deleted `call`
}
```

Keep its `impl fmt::Display for RangeCreate` exactly as it is. Its hand-written
`output_type` becomes `output_type_expr`, and because the hand-written body ignores
its input (`fn output_type(&self, _input_types: &[SqlColumnType])`), the expression
does not read `input_types`.

- [ ] **Step 2: Verify it compiles and behaves**

Run:
```bash
cargo check -p mz-expr 2>&1 | tail -5
cargo test -p mz-expr 2>&1 | tail -10
```

Expected: clean. A duplicate-impl error on `FuncName` means `RangeCreate` does have a
`func_name!` entry after all, contrary to the survey. Delete the entry if so.

- [ ] **Step 3: Fix the stale rustdoc**

In `src/expr-derive/src/lib.rs`, delete the Limitations bullet
"Unary functions cannot yet receive a `&RowArena` as an argument" (line 51), and
correct the `output_type_expr` entry, which claims the modifier "Applies to binary
and variadic functions" although `unary_func` has always handled it. Add
`skip_display` to the modifier list.

- [ ] **Step 4: Update the developer doc**

In `doc/developer/sqlfunc.md`, change the arity table so rows 1 and 2 read
"Supports `&RowArena` and `&self`", document `skip_display`, and add:

```markdown
## Shapes the macro does not cover

Two shapes stay hand-written by design.

* Functions generic over an `Eval` implementor, which hold sub-expressions in
  `Box<E>` and evaluate them per element. The macro would have to emit an impl
  generic over a struct type parameter with a trait bound, which is a different
  mechanism from the erasure it applies to function type parameters. The nine
  compound-type casts in `src/expr/src/scalar/func/impls/` are these.
* Functions that do not evaluate every operand. The macro emits `Eager*` impls,
  which evaluate all arguments before dispatch. `And`, `Or`, `Coalesce`, `Greatest`,
  `Least`, `ErrorIfNull`, and `CaseLiteral` are these.
```

- [ ] **Step 5: Verify the full gate and push**

Run:
```bash
cargo test -p mz-expr-derive-impl
cargo test -p mz-expr
bin/sqllogictest --optimized 2>&1 | tail -20
cargo check --workspace 2>&1 | tail -5
bin/lint
```

Expected: all pass. `bin/sqllogictest --optimized` is the first point in the stack
where a real semantic change could surface, because `RangeCreate` is a real
conversion.

- [ ] **Step 6: Commit and open PR2**

```bash
bin/fmt
git add src/expr/ src/expr-derive/ doc/developer/sqlfunc.md
git commit -m "$(cat <<'EOF'
expr: Convert RangeCreate to the sqlfunc macro

The only hand-written `EagerVariadicFunc`, blocked solely on a `Display` impl
that matches on `self.elem_type`. `skip_display` keeps that impl and the macro
supplies the rest, which makes this the first real user of the modifier.

Also corrects the macro's own rustdoc, which claimed unary functions cannot
receive a `RowArena` and that `output_type_expr` does not apply to them.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
git push -u origin HEAD
```

PR2 body:

```markdown
Stacked on #<PR1>.

Teaches `#[sqlfunc]` the three things that kept 60 stateful scalar functions
hand-written:

* `EagerUnaryFunc::call` takes a `&'a RowArena`, which binary and variadic already
  did. The 40 hand-written unary impls take the parameter and ignore it until this
  stack converts them.
* `#[sqlfunc(StructName, ..)]` with a `&self` receiver works for unary and binary,
  as it already did for variadic.
* `skip_display = true` suppresses the generated `Display`, for the 24 functions
  whose name is formatted from struct state.

Because the arities share one generator after #<PR1>, these are flags and an
argument offset rather than three capabilities written three times.

`RangeCreate` converts here as the first real user of `skip_display`. It is the
only hand-written `EagerVariadicFunc` and it was blocked on nothing else.

Receivers stay as plain `&self`. No `Eager*Func::call` in the tree ties the
receiver to `'a`, and doing so is a variance change rather than an additive one, so
it waits until a conversion actually needs it.

Design: `doc/developer/design/20260917_sqlfunc_canonicalization.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

---

## Task 11: Answer the binary receiver question

**Files:**
- Read only: `src/expr/src/scalar/func/impls/list.rs:312`, `src/expr/src/scalar/func/impls/string.rs:1363`

This task produces a decision, not code. The spec leaves it open and it is cheap to
close before conversions begin.

- [ ] **Step 1: Inspect the two stateful binary functions**

Read `ListLengthMax` and `RegexpReplace`. For each, determine whether its output
borrows from `self`. `ListLengthMax` returns `Result<Option<i32>, EvalError>` and
`RegexpReplace` returns a string built into the arena, so neither should need the
receiver tied to `'a`.

- [ ] **Step 2: Record the answer**

If neither needs it, append to the spec's Open questions section that the question is
closed and receivers stay `&self`, and move the item into the Solution Proposal as a
settled decision. If either does need it, stop and escalate: changing
`EagerBinaryFunc::call`'s receiver affects every binary implementor and belongs in
its own pull request, not folded into a conversion.

- [ ] **Step 3: Commit the spec update**

```bash
git add doc/developer/design/20260917_sqlfunc_canonicalization.md
git commit -m "$(cat <<'EOF'
doc: Close the binary receiver question in the sqlfunc design

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: PR3, direct-unwrap `LazyBinaryFunc`, behind a measurement gate

**Files:**
- Modify: `src/expr/src/scalar/func/binary.rs:144` (remove the blanket impl)
- Modify: `src/expr-derive-impl/src/generate.rs` (emit an explicit `LazyBinaryFunc` impl for binary)
- Modify: `src/expr/src/scalar/func/macros.rs` (add `lazy_via_eager_binary!`)
- Modify: `src/expr/src/scalar/func/impls/list.rs`, `src/expr/src/scalar/func/impls/string.rs` (use the new macro)

- [ ] **Step 1: Take the baseline measurement**

Run:
```bash
cargo llvm-lines -p mz-expr 2>&1 | tail -3 > /tmp/llvm-lines-before.txt
cargo llvm-lines -p mz-expr 2>&1 | grep "try_from_iter" | head -5
cat /tmp/llvm-lines-before.txt
```

Record the total and the `<(T0, T1)>::try_from_iter` line count. The superseded
#36705 measured 1,289,072 total and 93,259 lines across 218 copies against a May 2026
tree, before #37961 reshaped the dispatch path. Do not reuse those numbers.

- [ ] **Step 2: Apply the gate**

If the baseline shows `try_from_iter` contributing less than roughly 2% of the total,
stop. Per the spec's decision rule, PR3 still lands on the grounds that per-struct
emission is more explicit than a blanket impl, but the PR body must report the
measured number rather than claiming a performance win. If the baseline shows a
contribution comparable to the May figures, proceed as a performance change.

- [ ] **Step 3: Write the failing test**

```rust
#[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
#[mz_ore::test]
fn insta_test_binary_direct_unwrap() {
    let attr = quote! { sqlname = "+", is_infix_op = true, propagates_nulls = true, test = true };
    let item = quote! {
        fn add_thing<'a>(a: i32, b: i32) -> i32 { a + b }
    };
    let (output, input) = crate::test_sqlfunc(attr, item);
    assert!(
        output.contains("impl crate :: func :: binary :: LazyBinaryFunc"),
        "binary structs must get an explicit LazyBinaryFunc impl, got:\n{output}"
    );
    insta::assert_snapshot!("binary_direct_unwrap", output, &input);
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cargo test -p mz-expr-derive-impl binary_direct_unwrap 2>&1 | tail -20`

Expected: failure, because the generated output has only an `EagerBinaryFunc` impl
and relies on the blanket impl for the lazy side.

- [ ] **Step 5: Implement**

In `generate.rs`, for `Shape::Binary`, emit a `LazyBinaryFunc` impl whose `eval`
calls `try_from_result` per argument rather than
`<(T0, T1) as InputDatumType>::try_from_iter`. Remove the blanket
`impl<T: EagerBinaryFunc> LazyBinaryFunc for T` from `binary.rs`. Add
`lazy_via_eager_binary!` to `macros.rs`, a declarative macro producing the old
tuple-based forwarding, and apply it to `ListLengthMax` and `RegexpReplace`, which
are the only two hand-written `EagerBinaryFunc` impls in the tree.

- [ ] **Step 6: Run the tests, accept snapshots, re-measure**

Run:
```bash
cargo test -p mz-expr-derive-impl 2>&1 | tail -10
cargo insta accept
cargo test -p mz-expr 2>&1 | tail -10
cargo llvm-lines -p mz-expr 2>&1 | tail -3
```

Expected: tests pass. Every binary snapshot gains a `LazyBinaryFunc` impl. The
`llvm-lines` total is lower than the Step 1 baseline. Record both numbers for the PR
body.

- [ ] **Step 7: Verify semantics and commit**

Run:
```bash
bin/sqllogictest --optimized 2>&1 | tail -20
cargo check --workspace 2>&1 | tail -5
bin/lint
```

```bash
bin/fmt
git add src/expr/ src/expr-derive-impl/
git commit -m "$(cat <<'EOF'
expr: Emit LazyBinaryFunc directly for generated binary structs

Generated binary structs get an explicit `LazyBinaryFunc` impl that unwraps each
argument with `try_from_result`, instead of inheriting a blanket impl that routes
through the tuple `InputDatumType::try_from_iter` and its iterator state machine.

`ListLengthMax` and `RegexpReplace`, the only two hand-written `EagerBinaryFunc`
impls, keep the tuple path through `lazy_via_eager_binary!` until this stack
converts them.

Measured on this tree rather than reusing the figures from the superseded #36705,
which predate #37961: <before> to <after> lines from `cargo llvm-lines -p mz-expr`.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
git push -u origin HEAD
```

Replace `<before>` and `<after>` with the Step 1 and Step 6 numbers. Do not commit
the placeholder.

---

## Task 13: PR4, convert `impls/int16.rs`

**Files:**
- Modify: `src/expr/src/scalar/func/impls/int16.rs:140-190` (`CastInt16ToNumeric`)
- Modify: `src/expr/src/scalar/func.rs` (delete the `CastInt16ToNumeric` `func_name!` entry)

This is the pattern-setter for PR5 through PR23. `int16.rs` holds exactly one
convertible function and that function overrides five methods, so it exercises the
modifier path without being large.

- [ ] **Step 1: Record what the hand-written impl asserts**

Read `src/expr/src/scalar/func/impls/int16.rs:140-190` and write down every method it
overrides. For `CastInt16ToNumeric` that is `call`, `output_sql_type`, `could_error`,
`inverse`, and `is_monotone`. This list is the checklist for Step 4 and it is the
spec's verification discipline: an override that is neither carried across as a
modifier nor confirmed to match the macro's derived default is the specific mistake
to avoid.

- [ ] **Step 2: Convert**

Delete the `impl EagerUnaryFunc for CastInt16ToNumeric` block, keep the struct and
its `Display`, and add:

```rust
#[sqlfunc(
    CastInt16ToNumeric,
    sqlname = "smallint_to_numeric",
    skip_display = true,
    output_type_expr = SqlScalarType::Numeric { max_scale: self.0 }.nullable(input_type.nullable),
    introduces_nulls = false,
    could_error = self.0.is_some(),
    inverse = to_unary!(super::CastNumericToInt16),
    is_monotone = true
)]
fn cast_int16_to_numeric<'a>(
    &self,
    a: i16,
    _temp_storage: &'a RowArena,
) -> Result<Numeric, EvalError> {
    let mut a = Numeric::from(a);
    if let Some(scale) = self.0 {
        if numeric::rescale(&mut a, scale.into_u8()).is_err() {
            return Err(EvalError::NumericFieldOverflow);
        }
    }
    // Besides `rescale`, cast is infallible.
    Ok(a)
}
```

Check the existing `Display` impl for the exact `sqlname` string before writing it,
since the `Display` body is what `EXPLAIN` prints and the two must agree. Keep the
inline comment from the original body: the plan's global constraints forbid dropping
comments during a refactor.

- [ ] **Step 3: Delete the `func_name!` entry**

Remove the `CastInt16ToNumeric => "cast_int16_to_numeric",` line from the
`func_name!` block in `src/expr/src/scalar/func.rs`. The macro emits that impl now,
so leaving the entry is a duplicate-impl compile error.

- [ ] **Step 4: Verify every override is accounted for**

Walk the Step 1 list. For each, confirm the conversion either passes it as a modifier
or that the macro's derived default matches what the deleted body returned. Record the
reasoning in the commit message. In particular `propagates_nulls` and
`introduces_nulls` are derived from the associated types when absent, so confirm the
derived values rather than assuming.

- [ ] **Step 5: Run the gate**

Run:
```bash
cargo test -p mz-expr 2>&1 | tail -10
bin/sqllogictest --optimized 2>&1 | tail -20
cargo check --workspace 2>&1 | tail -5
bin/lint
```

Expected: all pass with no golden diff. A golden diff here is a regression, not an
expected outcome, because `skip_display` preserves the name verbatim.

- [ ] **Step 6: Commit and open PR4**

```bash
bin/fmt
git add src/expr/src/scalar/func/impls/int16.rs src/expr/src/scalar/func.rs
git commit -m "$(cat <<'EOF'
expr: Convert CastInt16ToNumeric to the sqlfunc macro

First conversion in the stack, and the pattern the remaining 19 follow: keep the
struct and its `Display`, pass the state-dependent output type as
`output_type_expr`, set `skip_display`, and delete the `func_name!` entry the
macro now generates.

Every method the hand-written impl overrode is accounted for. `could_error`,
`inverse`, and `is_monotone` carry across as modifiers. `output_sql_type` becomes
`output_type_expr`. `propagates_nulls` and `introduces_nulls` match the values the
macro derives from the associated types.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
git push -u origin HEAD
```

PR4 body:

```markdown
Stacked on #<PR3>.

Converts the one convertible function in `impls/int16.rs`, establishing the
pattern the remaining 19 conversion PRs follow.

Each conversion keeps the struct and its hand-written `Display`, sets
`skip_display`, moves the state-dependent `output_sql_type` body to
`output_type_expr`, carries the remaining overrides across as modifiers, and
deletes the function's `func_name!` entry because the macro now emits that impl.

The `func_name!` block is why the conversion PRs are not file-disjoint: all 20
touch `src/expr/src/scalar/func.rs`. The conflicts are mechanical, since every
conversion only deletes lines and no two delete the same line.

Design: `doc/developer/design/20260917_sqlfunc_canonicalization.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

- [ ] **Step 7: Write the plan for PR5 through PR23**

With the recipe now validated against real generated output, write part 2 of this
plan covering the remaining 19 files in the spec's stated order: the eleven remaining
single-function files, then `list.rs`, `map.rs`, `record.rs`, `date.rs`, `time.rs`,
then `string.rs`, then `timestamp.rs`.

---

## Self-review

**Spec coverage.** PR1 is Tasks 1 to 6. PR2 is Tasks 7 to 10. The spec's open
question on the binary receiver is Task 11. PR3 is Task 12, including the spec's
decision rule for a disappointing re-measurement. PR4 is Task 13. The spec's
dependency list is covered: the unary `call` signature in Task 7, the blanket
`LazyBinaryFunc` removal in Task 12, `func_name!` in Tasks 10 and 13,
`is_infinity_monotone` becoming an error in Task 2, `doc/developer/sqlfunc.md` in
Tasks 6 and 10, the `src/expr-derive/src/lib.rs` rustdoc in Task 10, and closing the
two superseded drafts in Task 6. PR5 through PR23 are deliberately deferred to part 2
with the reason stated, and Task 13 Step 7 is the handoff.

**Not covered, by design.** The nine `E: Eval` generic casts and the seven
short-circuiting variadics, per the spec's Out of Scope. Moving `ErrorIfNull` to the
binary path. Vtable dispatch. The spec's third open question, the missing
`doc/developer/prompts/sqlfunc-dyn-dispatch.md`, has no task because there is nothing
to act on until the file is found.

**Type consistency.** `Shape`, `Modifier`, `ReturnTy` introduced in Task 1 and used
under those names throughout. `Modifiers::iter` from Task 2 is consumed by
`override_methods` in Task 3 and `reject_inapplicable` in Task 2.
`override_methods(Shape, &Modifiers) -> Vec<TokenStream>` keeps that signature in
Tasks 3 and 5. `Emission` gains `skip_display` in Task 9 and is constructed in Tasks
4, 5, and 9. `Shape::output_method` returns `(Ident, TokenStream)` in Tasks 5 and is
referenced nowhere else. `Shape::takes_arena` is introduced in Task 5 and flipped for
unary in Task 7.

**Known risk the plan cannot remove.** Tasks 3, 4, and 5 each depend on reproducing
the current emission order exactly, and the 13 snapshots are the only check. Each of
those tasks carries an explicit instruction to adjust the new code to match the
snapshot rather than accepting a moved snapshot. If a snapshot moves and the diff is
not pure ordering, that is a signal the refactor changed semantics, and the task
should stop rather than accept.
