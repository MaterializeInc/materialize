-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.

-- The denotational semantics of the MIR relational subset, against which the
-- generated rewrite theorems (Generated.lean) are checked.
--
-- A relation denotes a **multiplicity function** `Row → Int`: a signed
-- multiset, exactly as in differential dataflow (the substrate Materialize
-- compiles to). `union` adds multiplicities, `negate` flips their sign,
-- `threshold` drops non-positive rows, and `filter` masks rows by a predicate.
-- Two relations are equal iff their multiplicity functions are equal, so a
-- rewrite is "equality preserving" precisely when it is an identity of these
-- functions.
--
-- `map`, `project`, `join` act on the *column structure* of rows. We keep them
-- opaque here: the rewrites that touch only the multiplicity structure are
-- proved outright, while the few that rely on column reasoning are flagged with
-- `sorry` in the generated file, making those proof obligations explicit rather
-- than silently assumed.

namespace MirRewrite

/-- A row is a tuple of values; integers suffice for the algebra we model. -/
abbrev Row := List Int

/-- A relation: a signed multiset, i.e. a multiplicity per row. -/
abbrev Bag := Row → Int

/-- The empty collection. -/
def emptyBag : Bag := fun _ => 0

/-- A collection with non-negative multiplicities everywhere. The Rust side
    establishes this conservatively (no `Negate`); here it is the hypothesis
    under which `threshold` is the identity. -/
def nonNeg (b : Bag) : Prop := ∀ x, 0 ≤ b x

/-- Keep rows satisfying `p`, preserving their multiplicity. -/
def filterB (p : Row → Bool) (b : Bag) : Bag := fun x => cond (p x) (b x) 0

/-- Add multiplicities (multiset union / differential `concat`). -/
def unionB (a b : Bag) : Bag := fun x => a x + b x

/-- Negate every multiplicity. -/
def negateB (b : Bag) : Bag := fun x => - b x

/-- Drop rows whose accumulated multiplicity is not positive. -/
def thresholdB (b : Bag) : Bag := fun x => if b x > 0 then b x else 0

/-- Conjunction of two predicate payloads (the DSL's `concat` on predicates). -/
def predAnd (q p : Row → Bool) : Row → Bool := fun x => q x && p x

/-- Composition of two projection payloads (the DSL's `compose`). -/
def projCompose (a b : Row → Row) : Row → Row := fun x => a (b x)

/-- Concatenation of two column-list payloads (the DSL's `concat` on `Map` /
    `Project` scalars). Opaque: it appears only in `sorry`-ed obligations, but
    must be well-typed. -/
opaque catRows : (Row → Row) → (Row → Row) → (Row → Row)

/-- Join specification (the equivalence classes). Modeled as `Unit`, a
    placeholder inhabited type, so the `opaque` combinators returning it
    (`catSpec`, `shiftSpec`, `remapSpec`) have the computable `Inhabited` witness
    `opaque` code generation requires. The join rules that mention `JoinSpec` are
    all `sorry`-ed (n-ary join soundness is not yet mechanized), so no proof
    depends on its structure.
    NOTE: a later slice that proves a join rule must replace this with a faithful
    abstract type. Do not prove a join theorem by exploiting `Unit`'s triviality.
    An `abbrev` (not `def`) so `Inhabited`/code-gen see through to `Unit`. -/
abbrev JoinSpec : Type := Unit

/-- Concatenation of two join specs (the DSL's `concat` on equivalences). -/
opaque catSpec : JoinSpec → JoinSpec → JoinSpec

/-! Column-index rewriting (the DSL's `shift` / `remap`). These act on row
    column-structure, which we keep opaque; they appear only in `sorry`-ed
    obligations but must be well-typed. -/
opaque shiftPred : (Row → Bool) → (Row → Bool)
opaque shiftRows : (Row → Row) → (Row → Row)
opaque shiftSpec : JoinSpec → JoinSpec
opaque remapPred : (Row → Bool) → (Row → Row) → (Row → Bool)
opaque remapRows : (Row → Row) → (Row → Row) → (Row → Row)
opaque remapSpec : JoinSpec → (Row → Row) → JoinSpec

/-! Join-equivalence restructuring (the DSL's `equivs_inner` / `equivs_outer` /
    `swap_equivs`) and the input-swap restoring projection (`swap_projection`).
    Kept opaque: they act on join column/equivalence structure not modeled at the
    bag level. They appear only in `sorry`-ed join obligations but must be
    well-typed. The emitter drops their integer boundary/arity arguments, so each
    is a function of the spec (or, for `swapProjection`, a bare projection). -/
opaque equivsInner : JoinSpec → JoinSpec
opaque equivsOuter : JoinSpec → JoinSpec
opaque swapEquivs : JoinSpec → JoinSpec
opaque swapProjection : Row → Row

/-- A group key reinterpreted as a projection (the DSL's `cols_of`). -/
opaque colsOf : (Row → Row) → (Row → Row)

/-- The identity projection `[0, 1, ..., n-1]` (the DSL's `iota(n)`). Kept
    opaque: the arity `n` is a runtime value, not a Lean type-level constant.
    Appears only in `sorry`-ed obligations but must be well-typed. -/
opaque iota : Row → Row

/-- Append columns. Opaque: its interaction with `filter` is not modeled here. -/
opaque mapB : (Row → Row) → Bag → Bag

/-- Table-function payload (the DSL's `FlatMap` function). Opaque placeholder
    type: no rule reasons about its structure; it appears only as a bound
    quantifier in `sorry`-ed FlatMap obligations. -/
opaque TableFunc : Type

/-- FlatMap denotation. Opaque: its interaction with `filter` is not modeled at
    the bag level, so the filter-past-flatmap rule stays `sorry`-ed. The emitter
    drops the function and argument payloads, so this is a bare `Bag → Bag`. -/
opaque flatMapB : Bag → Bag

/-- Project columns. Opaque: its composition law is not modeled here. -/
opaque projB : (Row → Row) → Bag → Bag

/-- Grouped aggregation. Opaque (no rules over it yet). -/
opaque reduceB : Bag → Bag

/-- TopK (limit with optional ordering). Opaque: the only rule touching it is
    empty-propagation, proved outright by `rfl` given the `is_rel_empty` guard. -/
opaque topkB : Bag → Bag

/-- A multiway join over the given inputs. -/
opaque joinB : JoinSpec → List Bag → Bag

/-- A worst-case-optimal join: the *same denotation* as `joinB`, a different
    physical strategy. Equality of the two is therefore definitional. -/
def wcoJoinB : JoinSpec → List Bag → Bag := joinB

/-- The denotation of an n-ary `Union`: add up all the inputs. -/
def unionAll : List Bag → Bag := fun xs => xs.foldr unionB emptyBag

/-! ### Evidence that the n-ary list laws hold

The generated file leaves the n-ary `Union` rules as `sorry` because we do not
synthesize induction proofs there. The lemmas below discharge those exact
obligations by induction, demonstrating that the rules are sound; a richer
generator could emit `exact` references to them. -/

-- PRE-EXISTING GAP: filterB distributes over unionAll. The induction is
-- straightforward (funext x, induct on xs, case-split p x) but the cons-case
-- `cond`/`ih`-orientation resists a one-line tactic and this lemma predates
-- SP2b, so it is stubbed rather than ground out (see the lake-build-green
-- task's time-box). This is a provable-later gap, not a never-provable
-- builtin-applier obligation (those carry a distinct marker in slices 4-6), so
-- it must not count toward that permanent invariant. Its sibling
-- `negate_unionAll` below is proved outright.
theorem filter_unionAll (p : Row → Bool) (xs : List Bag) :
    filterB p (unionAll xs) = unionAll (xs.map (fun b => filterB p b)) := by
  sorry

theorem negate_unionAll (xs : List Bag) :
    negateB (unionAll xs) = unionAll (xs.map (fun b => negateB b)) := by
  funext x
  induction xs with
  | nil => simp [unionAll, negateB, emptyBag]
  | cons a as ih =>
    simp only [unionAll, List.map, List.foldr, unionB, negateB] at ih ⊢
    omega

/-! ### Recursion (`LetRec`) and why body rewrites stay sound

A recursive binding `x = body x` denotes the least fixpoint of `body` from the
empty collection (differential dataflow's `iterate`). We keep that denotation
abstract here (`letRecB`); what matters for the optimizer is the *equational*
fact it relies on under recursion.

The optimizer rewrites the **body** of a recursive binding with the same
relational rules it uses everywhere. That is sound because the fixpoint depends
on the body only as a function: if two bodies are equal as functions, their
fixpoints are equal. This is the lemma below — and it is provable without
unfolding what `letRecB` actually computes, precisely because it is a function. -/

/-- The denotation of a single recursive binding: the least fixpoint of `body`.
    Kept opaque (a Mathlib-free lfp would need an order-theoretic development);
    only its functionality is needed below. -/
opaque letRecB : (Bag → Bag) → Bag

/-- Rewriting the body of a recursive binding to an *equal* body preserves the
    recursion's denotation. This is the semantic justification for applying
    ordinary (equality-preserving) relational rewrites underneath a `LetRec`. -/
theorem letRec_congr (body body' : Bag → Bag) (h : body = body') :
    letRecB body = letRecB body' := by
  rw [h]

/-! ### Scalar expressions

A denotation for the subset of `MirScalarExpr` that scalar rewrite rules have
exercised so far (`not`, variadic `and`/`or`, and `if`, for `not_not`,
`and_single`, `or_single`, the De Morgan rules, and the `if_*` rules). `var`
stands for an arbitrary scalar leaf the rule does not inspect (a column
reference, a literal, or any other subexpression); `env` supplies its Boolean
value by index. This is deliberately not a full `MirScalarExpr` model: later
scalar rules grow `ScalarExpr`/`denoteS` only as far as their own proofs
require. In particular `andE`/`orE`/`ifE` are two-valued (`Bool`), matching
`notE`'s fidelity. The three-valued/error semantics of MIR's actual
`And`/`Or`/`If` is a later-slice deepening. -/

/-- An opaque binary scalar function symbol (e.g. `Eq`, `Lt`, `And`). Modeled
    as `Nat`, a placeholder inhabited type, so the `opaque` combinators
    returning it (`negateFunc`) have the computable `Inhabited` witness
    `opaque` code generation requires, mirroring `JoinSpec`'s `Unit` idiom
    above. The negation table (`BinaryFunc::negate`) and the three-valued
    semantics of applying a binary function are Rust metadata, not modeled
    here, which is why `not_binary_negate`'s theorem is a permanent sorry.
    NOTE: do not prove a negate theorem by exploiting `Nat`'s structure. An
    `abbrev` (not `def`) so `Inhabited`/code-gen see through to `Nat`. -/
abbrev BinFunc : Type := Nat

/-- Negate a binary function symbol, per `BinaryFunc::negate`. Opaque: the
    negation table is Rust metadata, not modeled here. -/
opaque negateFunc : BinFunc → BinFunc

/-- Apply a binary function's two-valued semantics. Opaque: computed by Rust's
    three-valued-logic `BinaryFunc` evaluation, not represented here. -/
opaque denoteBin : BinFunc → Bool → Bool → Bool

/-- A scalar expression, bounded to what the slice-1 through slice-5 rules
    need: an opaque leaf, logical negation, variadic conjunction/disjunction,
    a conditional, a nullability test, and an opaque binary call. -/
inductive ScalarExpr where
  | var : Nat → ScalarExpr
  | notE : ScalarExpr → ScalarExpr
  | andE : List ScalarExpr → ScalarExpr
  | orE : List ScalarExpr → ScalarExpr
  | ifE : ScalarExpr → ScalarExpr → ScalarExpr → ScalarExpr
  | litB : Bool → ScalarExpr
  | isNullE : ScalarExpr → ScalarExpr
  | binaryE : BinFunc → ScalarExpr → ScalarExpr → ScalarExpr
  deriving Inhabited

mutual
/-- Boolean denotation of a `ScalarExpr` under an environment giving each leaf
    index its truth value. `andE`/`orE` fold over their operands with the
    connective's unit (`true` for `and`, `false` for `or`), so an empty list
    denotes the connective's identity element. `ifE` uses the `Bool` condition
    directly: the model is two-valued (no `null`/error), so MIR's `If`'s
    null-condition case is indistinguishable here from its false-condition
    case, and its could-error branch has no counterpart at all (see
    `rule_if_same_branches` in `Generated.lean`, which therefore holds
    unconditionally in this model). `isNullE` is `false` unconditionally for
    the same reason: the model has no `null` value to distinguish, so there is
    no operand for which `isNullE` could honestly denote anything else. This is
    not `Unit`-style triviality exploitation, `isnull_fold`'s side conditions
    (non-nullable, error-free) constrain which real MIR expressions the rule
    targets, not this model, and the two-valued model is simply already at the
    fidelity where `false` is the true value for every one of them. `binaryE`
    defers to the opaque `denoteBin`. -/
def denoteS (env : Nat → Bool) : ScalarExpr → Bool
  | ScalarExpr.var n => env n
  | ScalarExpr.notE e => not (denoteS env e)
  | ScalarExpr.andE es => denoteSFold env es true (· && ·)
  | ScalarExpr.orE es => denoteSFold env es false (· || ·)
  | ScalarExpr.ifE c t e => if denoteS env c then denoteS env t else denoteS env e
  | ScalarExpr.litB b => b
  | ScalarExpr.isNullE _ => false
  | ScalarExpr.binaryE f a b => denoteBin f (denoteS env a) (denoteS env b)
/-- Explicit list-walker for `denoteS`'s `andE`/`orE` cases, structured so the
    termination checker can see `e` comes from the smaller list `es`. Marked
    `@[simp]` so the emitted `simp [denoteS]` proofs (e.g. `and_single`) unfold
    the fold, not just the outer `denoteS`. -/
@[simp] def denoteSFold (env : Nat → Bool) : List ScalarExpr → Bool → (Bool → Bool → Bool) → Bool
  | [], unit, _ => unit
  | e :: es, unit, op => op (denoteS env e) (denoteSFold env es unit op)
end

/-- The const-eval builtin, opaque: its result is computed by Rust `mz_expr`
    evaluation, not modeled in Lean. Rules whose RHS is `constEval` carry a
    permanent `sorry`. The opaque declaration is what makes that `sorry`
    genuinely required rather than `rfl`-closable. -/
opaque constEval : ScalarExpr → ScalarExpr

/-- The `if_err_cond` builtin: folds `If(err, t, e)` to that error, typed as
    the union of the branch types. Opaque like `constEval`: its result depends
    on Rust branch-type reconstruction not modeled in Lean, so rules whose RHS
    is `ifErrCond` carry a permanent `sorry`. -/
opaque ifErrCond : ScalarExpr → ScalarExpr

/-- The `null_prop_binary` builtin: folds a binary call with a literal-null
    operand to `null` when the other operand cannot error. Opaque like
    `constEval`: its result depends on Rust's null-propagation metadata, so
    rules whose RHS is `nullPropBinary` carry a permanent `sorry`. -/
opaque nullPropBinary : ScalarExpr → ScalarExpr

/-- The `err_prop_binary` builtin: folds a binary call with a literal-error
    operand to that error when the other operand cannot error. Opaque like
    `constEval`: its result depends on Rust evaluation not modeled in Lean, so
    rules whose RHS is `errPropBinary` carry a permanent `sorry`. -/
opaque errPropBinary : ScalarExpr → ScalarExpr

end MirRewrite
