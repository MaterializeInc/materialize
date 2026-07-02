// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The abstract syntax of the rewrite DSL.
//!
//! A rule file is a sequence of [`Rule`]s. Each rule is an oriented,
//! equality-preserving rewrite `lhs => rhs` over the relational subset, plus
//! optional side [`Cond`]itions. The left-hand side is a [`Pat`]tern with
//! metavariables; the right-hand side is a [`Tmpl`] that reuses those
//! metavariables, optionally combining payloads with [`PExpr`] operators.
//!
//! Scalars are opaque (see [`crate::eqsat::ir`]): the DSL never destructures a
//! predicate or a map expression, it only moves whole *payload lists* around.
//! That keeps the language squarely focused on **relational** rewrites.

/// A pattern: the left-hand side of a rule. Operator nodes bind their payload
/// to a named metavariable in `[...]`; lowercase leaves bind whole relations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pat {
    /// A relation metavariable; matches and binds any subtree.
    RelVar(String),
    /// A scalar unary call with a FIXED function, e.g. `Unary[not](x)`. `func`
    /// is the scalar-func keyword text, resolved to a concrete `UnaryFunc` by
    /// codegen. Func-metavar binding (a bound `UnaryFunc`) is a later-slice
    /// capability and is a distinct variant when it lands.
    SUnary {
        func: String,
        input: Box<Pat>,
    },
    /// A scalar variadic call with a FIXED function, e.g. `Variadic[and](x)` or
    /// `Variadic[or](xs...)`. `func` is the scalar-func keyword text, resolved to
    /// a concrete `VariadicFunc` by codegen. Operands are captured via `ListPat`
    /// (`items` plus an optional trailing `rest`), mirroring `Union`. Func-metavar
    /// binding and func-switching are later-slice capabilities.
    SVariadic {
        func: String,
        inputs: ListPat,
    },
    /// A scalar `If(cond, then, els)`. Non-linear use (the same metavar in
    /// `then` and `els`) is enforced as a same-e-class equality by codegen's
    /// `guard()`, which is how `if_same_branches` matches `If(c, x, x)`.
    SIf {
        cond: Box<Pat>,
        then: Box<Pat>,
        els: Box<Pat>,
    },
    /// Matches any scalar binary call, binding the `BinaryFunc` symbol to `func`
    /// (a metavariable, NOT a fixed keyword) and the operands to `expr1`/`expr2`.
    /// The DSL's first func-metavar binding. Roots `not_binary_negate`.
    SBinaryVar {
        func: String,
        expr1: Box<Pat>,
        expr2: Box<Pat>,
    },
    /// Matches any scalar CALL node (unary/binary/variadic/if), binding its
    /// e-class to `binding`. Does not destructure the function or arguments,
    /// so it needs no func-metavar machinery. Roots the `const_fold` builtin,
    /// whose RHS evaluates the class. Leaves (Column/Literal/Unmaterializable)
    /// are never matched: only call syms are iterated.
    Scalar {
        binding: String,
    },
    Filter {
        preds: String,
        input: Box<Pat>,
    },
    Map {
        scalars: String,
        input: Box<Pat>,
    },
    Project {
        outputs: String,
        input: Box<Pat>,
    },
    Reduce {
        group_key: String,
        aggregates: String,
        input: Box<Pat>,
    },
    /// A `FlatMap[func, exprs](input)`: binds the table-function payload and
    /// argument-scalar payload plus the input relation. Both payloads are opaque
    /// captures; the rule never inspects the function or scalar structure.
    FlatMap {
        func: String,
        exprs: String,
        input: Box<Pat>,
    },
    Negate(Box<Pat>),
    Threshold(Box<Pat>),
    /// A `TopK` over `input`, matching any shape. Used by `topk_empty`; the
    /// shape is opaque so it is not bound.
    TopK(Box<Pat>),
    /// An `ArrangeBy[key](input)`: binds the arrangement key payload and the
    /// arranged input. Matches the physical arrangement primitive.
    ArrangeBy {
        key: String,
        input: Box<Pat>,
    },
    Join {
        equivalences: String,
        inputs: ListPat,
    },
    WcoJoin {
        equivalences: String,
        inputs: ListPat,
    },
    Union {
        inputs: ListPat,
    },
}

/// A list of child patterns, optionally ending in a `rest...` metavariable that
/// captures the remaining relations (used for variadic `Join`/`Union`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListPat {
    pub items: Vec<Pat>,
    pub rest: Option<String>,
}

/// A payload expression, used on the right-hand side to build a new payload
/// from bound metavariables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PExpr {
    /// Reuse a bound payload metavariable verbatim.
    Var(String),
    /// Concatenate two payload lists (predicates, scalars, equivalences, …).
    Concat(Box<PExpr>, Box<PExpr>),
    /// Compose two projection lists: `compose(a, b)[i] = b[a[i]]`, i.e. apply
    /// the outer projection `a` on top of the inner projection `b`.
    Compose(Box<PExpr>, Box<PExpr>),
    /// Shift every column index in a payload by an (affine) amount. Used to
    /// move a payload across a column-offset boundary, e.g. pushing a predicate
    /// onto a join input that does not start at column 0.
    Shift(Box<PExpr>, IxExpr),
    /// Remap every column index `c` of a payload to `outs[c]`, where `outs` is
    /// a projection payload. Inverts a `Project`, e.g. pushing a predicate
    /// below a projection (`c` is a projected position; `outs[c]` is the
    /// underlying column).
    Remap(Box<PExpr>, Box<PExpr>),
    /// Turn a payload of *bare column references* (e.g. a `Reduce` group key
    /// `[#2, #0]`) into the corresponding projection `[2, 0]`. Fails if any
    /// scalar is not a single-column reference.
    ColsOf(Box<PExpr>),
    /// `iota(n)`: the identity projection `[0, 1, …, n-1]`. Builds the leading
    /// "keep all input columns" part of a projection.
    Iota(IxExpr),
    /// Equivalence classes fully contained in columns [0, boundary): the
    /// constraints a binary inner join over the first two inputs can enforce.
    EquivsInner(Box<PExpr>, IxExpr),
    /// Complement of `EquivsInner`: classes touching a column >= boundary.
    EquivsOuter(Box<PExpr>, IxExpr),
    /// Remap equivalences for swapping the first two inputs (arities a, b):
    /// columns [0,a) -> +b, [a,a+b) -> -a, columns >= a+b unchanged.
    SwapEquivs(Box<PExpr>, IxExpr, IxExpr),
    /// `swap_projection(a, b)`: the projection that restores the original column
    /// order after swapping the first two join inputs (arities a, b). The commuted
    /// `Join(b, a)` outputs columns [b | a]; this maps them back to [a | b].
    SwapProjection(IxExpr, IxExpr),
}

/// An integer index expression for [`PExpr::Shift`]: literals, the arity of a
/// bound relation metavariable, and `+`/`-`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IxExpr {
    Lit(i64),
    /// The arity (column count) of a bound relation metavariable.
    Arity(String),
    Add(Box<IxExpr>, Box<IxExpr>),
    Sub(Box<IxExpr>, Box<IxExpr>),
    Neg(Box<IxExpr>),
}

/// A template: the right-hand side of a rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Tmpl {
    RelVar(String),
    /// The element placeholder `_` inside a `map(F[_], xs)` list combinator.
    Hole,
    /// `Empty(r)`: an empty collection with the same arity as the bound
    /// relation `r`. Used as the right-hand side of cancellation rules.
    Empty(String),
    Filter {
        preds: PExpr,
        input: Box<Tmpl>,
    },
    Map {
        scalars: PExpr,
        input: Box<Tmpl>,
    },
    Project {
        outputs: PExpr,
        input: Box<Tmpl>,
    },
    Reduce {
        group_key: PExpr,
        aggregates: PExpr,
        input: Box<Tmpl>,
    },
    /// A `FlatMap[func, exprs](input)` template: binds the table-function and
    /// argument-scalar payloads from the left-hand side and reconstructs the
    /// `FlatMap` with the same opaque payloads on the right-hand side.
    FlatMap {
        func: String,
        exprs: String,
        input: Box<Tmpl>,
    },
    Negate(Box<Tmpl>),
    Threshold(Box<Tmpl>),
    Join {
        equivalences: PExpr,
        inputs: ListTmpl,
    },
    WcoJoin {
        equivalences: PExpr,
        inputs: ListTmpl,
    },
    Union {
        inputs: ListTmpl,
    },
    /// Build a scalar unary call with a FIXED function, e.g. `Unary[not](_)`
    /// inside a `map(...)`. `func` is the scalar-func keyword text.
    SUnary {
        func: String,
        input: Box<Tmpl>,
    },
    /// Build a scalar variadic call with a FIXED function, e.g.
    /// `Variadic[or](map(Unary[not](_), xs))`. `inputs` is an ordered `ListTmpl`
    /// (supports `Item`, `Splice`, and `MapSplice`), mirroring `Union`.
    SVariadic {
        func: String,
        inputs: ListTmpl,
    },
    /// Build a scalar `If(cond, then, els)`.
    SIf {
        cond: Box<Tmpl>,
        then: Box<Tmpl>,
        els: Box<Tmpl>,
    },
    /// A builtin applier: the RHS is computed by the named Rust function in
    /// `crate::eqsat::scalar_builtins`, called with the graph and the class ids
    /// bound to `args`. Used where the result is an evaluation product that
    /// cannot be a declarative template. The Lean theorem is a permanent `sorry`.
    Builtin {
        name: String,
        args: Vec<String>,
    },
    /// A constant boolean literal (`true`/`false`) as a scalar node. The
    /// declarative RHS of `and_empty`/`or_empty`.
    SBool(bool),
    /// Build a binary call whose function is `negate(func)` for the `BinaryFunc`
    /// bound to metavar `func` by an `SBinaryVar` on the LHS, over `expr1`/`expr2`.
    /// Declines (rule does not fire) when `BinaryFunc::negate()` is `None`.
    SBinaryNegate {
        func: String,
        expr1: Box<Tmpl>,
        expr2: Box<Tmpl>,
    },
}

/// An element of a template input list. Lists are ordered sequences of these,
/// so multiple splices can be concatenated (e.g. flattening a nested join).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TElem {
    /// A single child template.
    Item(Tmpl),
    /// Splice a captured `rest...` list verbatim.
    Splice(String),
    /// `map(func, list)`: apply `func` (which contains the `_` [`Tmpl::Hole`])
    /// to each element of the captured `list`, splicing the results.
    MapSplice { func: Box<Tmpl>, list: String },
}

/// A template input list: an ordered sequence of [`TElem`]s.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ListTmpl {
    pub elems: Vec<TElem>,
}

/// A side condition guarding a rule.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum Cond {
    /// `uses_only_input(payload, rel)`: every column referenced by the payload
    /// metavariable is an output column of the relation metavariable. This is
    /// what makes `Filter`-through-`Map` pushdown sound (the predicate must not
    /// reference the columns the `Map` appends).
    UsesOnlyInput { payload: String, rel: String },
    /// `cols_in_range(payload, lo, hi)`: every column referenced by the payload
    /// lies in the half-open range `[lo, hi)`. Used to confirm a predicate
    /// references exactly one (offset) join input before pushing it down.
    ColsInRange {
        payload: String,
        lo: IxExpr,
        hi: IxExpr,
    },
    /// `non_negative(rel)`: the bound relation has non-negative multiplicities
    /// everywhere (conservatively: it is built without `Negate`). Lets
    /// `Threshold` be elided.
    NonNegative { rel: String },
    /// `monotonic(rel)`: the bound relation is insert-only — its multiplicities
    /// never decrease (conservatively: no `Negate` or `Reduce` on the path to
    /// its leaves), via the [`crate::eqsat::analysis::Monotonic`] analysis. The hook
    /// for monotonic *physical* rewrites (e.g. `TopK`); see `COVERAGE.md`.
    Monotonic { rel: String },
    /// `is_unique_key(payload, rel)`: the columns referenced by the payload form
    /// a unique key of the bound relation (via the [`crate::eqsat::analysis::Keys`]
    /// analysis). Lets a grouping be turned into a projection.
    IsUniqueKey { payload: String, rel: String },
    /// `empty(payload)`: the payload list is empty (e.g. a `Reduce` with no
    /// aggregates).
    Empty { payload: String },
    /// `all_true(payload)`: every scalar in the payload constant-folds to the
    /// literal `true`, so a `Filter` by it is the identity.
    /// Vacuously holds for an empty predicate list.
    AllTrue { payload: String },
    /// `any_false(payload)`: some scalar constant-folds to the literal `false`,
    /// so a `Filter` by it is empty.
    AnyFalse { payload: String },
    /// `no_false(payload)`: no scalar in the payload is a known-false literal.
    ///
    /// This is the negation of `any_false`. Used to guard distribution rules so
    /// they do not fire on predicates that `empty_false_filter` will already
    /// handle, which would otherwise create unbounded predicate-list growth via
    /// `merge_filters`.
    NoFalse { payload: String },
    /// `no_error(payload)`: no scalar in the payload might produce a runtime
    /// error (no literal error, no `error_if_null` call), via
    /// [`mz_expr::MirScalarExpr::might_error`].
    ///
    /// Guards filter-movement rules so they never relocate an error-bearing
    /// predicate toward the leaves. An `error(..)` predicate is a deferred
    /// runtime error that fires only on the rows that reach it. Pushing it
    /// below a `Negate`, a `Project`, or into a `Join` input mirrors moves that
    /// production `predicate_pushdown` deliberately refuses (see the Negate arm
    /// in `predicate_pushdown.rs` and database-issues/5691), because the move
    /// can expose the error to a downstream constant fold that raises it at
    /// plan time even though no row would ever reach it at runtime. eqsat must
    /// preserve observable behavior, so it must not turn a runtime-deferred
    /// error into a plan-time error.
    NoError { payload: String },
    /// `all_columns(payload)`: every scalar constant-folds to a bare column
    /// reference `#k`, so a `Map` of them is really a projection.
    AllColumns { payload: String },
    /// `is_rel_empty(rel)`: the bound relation is an empty constant (zero rows).
    ///
    /// Guards empty-propagation rules so they fire only when the input is
    /// already a zero-row Constant, produced by `empty_false_filter` or
    /// `union_cancel`. Avoids interaction loops: without this guard, rules like
    /// `Threshold e => Empty(e)` could fire on any non-trivial input.
    IsRelEmpty { rel: String },
    /// `not_rel_empty(rel)`: the bound relation has no zero-row Constant node
    /// in its e-class. Used to guard Union-drop rules so they only fire when
    /// the kept branch is a non-trivially-empty relation, preventing the cyclic
    /// class merges that cause `merge_filters` to grow predicate lists without
    /// bound.
    NotRelEmpty { rel: String },
    /// `produces_key(rel, key)`: the bound relation already produces an
    /// arrangement keyed by the `key` payload, so an `ArrangeBy[key]` on top of
    /// it is redundant. True when `rel`'s e-class contains an `ArrangeBy` with
    /// the same key, or a `Reduce` whose output is arranged by its group key
    /// (the key is exactly the leading group-key columns). A physical-property
    /// fact, not a linearity claim, so it is sound for any operator.
    ProducesKey { rel: String, key: String },
    /// `join_is_cyclic()`: the matched root node is a `Join` whose constraint
    /// hypergraph is cyclic (not alpha-acyclic), decided by GYO reduction over
    /// its inputs and equivalences. Gates `join_to_wcoj` so a worst-case-optimal
    /// join is created only for cyclic joins, where it can beat a binary join
    /// tree. Reads the root e-class directly, so the rule's left-hand side root
    /// must be the `Join` being tested.
    JoinIsCyclic,
    /// `has_three_or_more_inputs()`: the matched root e-class contains a `Join`
    /// with three or more inputs. Gates `binarize_join_first` so that rule does
    /// not fire on an already-binary join (which would loop). Reads the root
    /// e-class directly; the rule's left-hand side root must be the `Join`.
    HasThreeOrMoreInputs,
    /// `is_binary_join()`: the matched root e-class contains a `Join` with exactly
    /// two inputs. Gates `commute_binary_join` so the commutativity rewrite fires
    /// only on binary joins. Reads the root e-class directly; the rule's left-hand
    /// side root must be the `Join`.
    IsBinaryJoin,
    /// `has_inner_equiv(payload, n)`: the equivalences payload contains at least one
    /// equivalence class all of whose columns are < n (the inner boundary). True
    /// when `equivs_inner(payload, n)` would be non-empty.
    ///
    /// Guards `binarize_join_first` so it fires only when the first two inputs share
    /// a fully-contained join equivalence, i.e. the inner join is a real key-join
    /// rather than a cross product. For clique/star joins where the shared
    /// equivalence spans all inputs, no class is fully within the inner boundary, so
    /// this condition is false and binarization is suppressed.
    HasInnerEquiv { payload: String, boundary: IxExpr },
    /// `identity_projection(payload, rel)`: the projection payload is exactly
    /// `[0, 1, ..., arity(rel)-1]`. Used to drop `Project[id](r) = r`.
    IdentityProjection { payload: String, rel: String },
    /// `non_identity_projection(payload, rel)`: the projection payload is *not*
    /// exactly `[0, 1, ..., arity(rel)-1]`. The negation of `identity_projection`.
    /// Guards `pull_project_out_of_join_first` so it does not fire on a no-op
    /// projection (which would add nothing and risk a churn loop with
    /// `drop_identity_project`).
    NonIdentityProjection { payload: String, rel: String },
    /// `reads_indexed_global(rel)`: the bound relation, after stripping
    /// column- and row-preserving wrappers, reaches an opaque global `Get`
    /// covered by a maintained index. Restricts `pull_project_out_of_join_first`
    /// to join inputs where pulling the projection up exposes a reusable index,
    /// the only case with a payoff; bounds e-graph blowup elsewhere.
    ReadsIndexedGlobal { rel: String },
    /// `scalar_lit_true(s)`: the class bound to scalar metavar `s` is a literal
    /// `true` (via the scalar `literal` analysis). Gates `if_true`.
    ScalarLitTrue { scalar: String },
    /// `scalar_lit_false_or_null(s)`: the class bound to `s` is a literal
    /// `false` or a literal `null`. Gates `if_false_or_null`.
    ScalarLitFalseOrNull { scalar: String },
    /// `scalar_no_error(s)`: the class bound to `s` has `could_error == false`
    /// (scalar `could_error` analysis). Gates `if_same_branches`.
    ScalarNoError { scalar: String },
    /// `scalar_non_nullable(s)`: the class bound to scalar metavar `s` is provably
    /// non-nullable, via `scalar_extract::raise(g, s).typ(col_types).nullable == false`.
    /// Computed on demand (no lattice storage). Gates `isnull_fold`.
    ScalarNonNullable { scalar: String },
}

/// The eqsat pass a rule is active in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Phase {
    /// Runs in both the logical and physical eqsat passes (the default).
    #[default]
    Both,
    /// Runs only in the logical pass.
    Logical,
    /// Runs only in the physical pass (where the cost model has arrangement /
    /// index availability and can judge arrangement-sensitive rewrites).
    Physical,
}

/// One rewrite rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    pub name: String,
    pub doc: Option<String>,
    pub phase: Phase,
    /// Whether this rule is safe to apply in the "colored saturation" phase,
    /// meaning all its side conditions are color-exact (no analysis-gated
    /// conditions: no `non_negative`, `monotonic`, or `is_unique_key`).
    pub colored: bool,
    pub lhs: Pat,
    pub rhs: Tmpl,
    pub conds: Vec<Cond>,
}

/// A parsed rule file.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct RuleSet {
    pub rules: Vec<Rule>,
}

impl RuleSet {
    /// Returns the name of every rule in this set, in order.
    pub fn rule_names(&self) -> Vec<&str> {
        self.rules.iter().map(|r| r.name.as_str()).collect()
    }

    /// The rules active in `phase`: those declared for that phase or for `Both`.
    pub fn for_phase(&self, phase: Phase) -> RuleSet {
        RuleSet {
            rules: self
                .rules
                .iter()
                .filter(|r| r.phase == Phase::Both || r.phase == phase)
                .cloned()
                .collect(),
        }
    }
}
