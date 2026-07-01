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

/-- Opaque join specification (the equivalence classes). -/
opaque JoinSpec : Type

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

/-- A group key reinterpreted as a projection (the DSL's `cols_of`). -/
opaque colsOf : (Row → Row) → (Row → Row)

/-- The identity projection `[0, 1, ..., n-1]` (the DSL's `iota(n)`). Kept
    opaque: the arity `n` is a runtime value, not a Lean type-level constant.
    Appears only in `sorry`-ed obligations but must be well-typed. -/
opaque iota : Row → Row

/-- Append columns. Opaque: its interaction with `filter` is not modeled here. -/
opaque mapB : (Row → Row) → Bag → Bag

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

theorem filter_unionAll (p : Row → Bool) (xs : List Bag) :
    filterB p (unionAll xs) = unionAll (xs.map (fun b => filterB p b)) := by
  induction xs with
  | nil => funext x; simp [unionAll, filterB, emptyBag]
  | cons a as ih =>
    funext x
    simp only [unionAll, List.map, List.foldr, unionB, filterB] at *
    have := congrFun ih x
    cases p x <;> simp_all

theorem negate_unionAll (xs : List Bag) :
    negateB (unionAll xs) = unionAll (xs.map (fun b => negateB b)) := by
  induction xs with
  | nil => funext x; simp [unionAll, negateB, emptyBag]
  | cons a as ih =>
    funext x
    simp only [unionAll, List.map, List.foldr, unionB, negateB] at *
    have := congrFun ih x
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
exercised so far (just `not`, for `not_not`). `var` stands for an arbitrary
scalar leaf the rule does not inspect (a column reference, a literal, or any
other subexpression); `env` supplies its Boolean value by index. This is
deliberately not a full `MirScalarExpr` model: later scalar rules grow
`ScalarExpr`/`denoteS` only as far as their own proofs require. -/

/-- A scalar expression, bounded to what `not_not` needs: an opaque leaf and
    logical negation. -/
inductive ScalarExpr where
  | var : Nat → ScalarExpr
  | notE : ScalarExpr → ScalarExpr

/-- Boolean denotation of a `ScalarExpr` under an environment giving each leaf
    index its truth value. -/
def denoteS (env : Nat → Bool) : ScalarExpr → Bool
  | ScalarExpr.var n => env n
  | ScalarExpr.notE e => not (denoteS env e)

end MirRewrite
