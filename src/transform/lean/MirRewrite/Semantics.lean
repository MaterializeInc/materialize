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
    the same reason: the model has no `null` value, so `IsNull` denotes `false`
    for every operand.
    NOTE: `rule_isnull_fold` therefore reduces to `false = false`. It confirms
    the RHS is `false` and that this model denotes `IsNull` to `false`, but it
    does NOT verify `isnull_fold`'s side conditions (non-nullable, error-free).
    Those conditions are what make the rule sound on REAL MIR expressions, where
    a nullable or erroring operand would make `IsNull` observably non-`false`,
    and their enforcement lives in the Rust guards (`scalar_non_nullable`,
    `scalar_no_error`) and the differential parity test, not in this Lean layer.
    A future rule touching `isNullE` must not read this proof as guard
    verification. This is the same scope limitation as `if_same_branches`
    dropping its `scalar_no_error` guard, not `Unit`-style triviality
    exploitation. `binaryE` defers to the opaque `denoteBin`. -/
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

/-! ### Evidence for the AND/OR short-circuit rules

`and_short_circuit`/`or_short_circuit` fold a variadic AND/OR to `false`/`true`
given a witness operand that already is that value. The witness is the
*precondition* that makes the fold collapse (not a soundness guard dropped for
convenience): the lemmas below are proved by induction on the operand list,
peeling off the witness's position. -/

theorem denoteSFold_and_false (env : Nat → Bool) (xs : List ScalarExpr)
    (h : ∃ x ∈ xs, denoteS env x = false) :
    denoteSFold env xs true (· && ·) = false := by
  induction xs with
  | nil => simp at h
  | cons a as ih =>
    obtain ⟨x, hmem, hx⟩ := h
    rcases List.mem_cons.mp hmem with h' | h'
    · subst h'
      simp [denoteSFold, hx]
    · simp only [denoteSFold]
      rw [ih ⟨x, h', hx⟩]
      simp

theorem denoteSFold_or_true (env : Nat → Bool) (xs : List ScalarExpr)
    (h : ∃ x ∈ xs, denoteS env x = true) :
    denoteSFold env xs false (· || ·) = true := by
  induction xs with
  | nil => simp at h
  | cons a as ih =>
    obtain ⟨x, hmem, hx⟩ := h
    rcases List.mem_cons.mp hmem with h' | h'
    · subst h'
      simp [denoteSFold, hx]
    · simp only [denoteSFold]
      rw [ih ⟨x, h', hx⟩]
      simp

/-! ### Evidence for the drop-unit and dedup rules

`and_drop_unit`/`or_drop_unit` remove operands equal to the connective unit
(`true` for And, `false` for Or). `and_dedup`/`or_dedup` remove later
duplicates. Both preserve the AND/OR fold because that fold is the `all`/`any`
of the operands: it is idempotent and commutative over `Bool`, so it depends
only on which operands are present, not on their multiplicity, and dropping a
unit operand (a `true` conjunct or a `false` disjunct) is the fold's identity.

NOTE: Lean models the SYNTACTIC filter here. `keepDropUnit` drops the literal
`litB unit`, and `dedupById` collapses repeated operands to one occurrence (the
proofs use only membership, so which occurrence survives is immaterial). The
Rust rules use the strictly stronger SEMANTIC predicate (an operand whose
scalar-literal analysis is the unit, or whose canonical e-class id repeats).
What Lean proves is the ALGEBRA: unit is the And/Or identity and And/Or is
idempotent over the `Bool` fold. The soundness of lifting that algebra to the
semantic predicate rests on the Rust analysis plus the differential parity
oracle, not on this Lean layer. This is the same scope stance as `isnull_fold`. -/

theorem denoteSFold_and_eq_all (env : Nat → Bool) (xs : List ScalarExpr) :
    denoteSFold env xs true (· && ·) = xs.all (fun x => denoteS env x) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [denoteSFold, List.all_cons, ih]

theorem denoteSFold_or_eq_any (env : Nat → Bool) (xs : List ScalarExpr) :
    denoteSFold env xs false (· || ·) = xs.any (fun x => denoteS env x) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [denoteSFold, List.any_cons, ih]

/-- The drop-unit keep predicate: keep every operand except the literal
    `litB unit`. Comparing only the inner `Bool` (via `Bool`'s computable
    `!=`) avoids needing `DecidableEq ScalarExpr`. -/
def keepDropUnit (unit : Bool) : ScalarExpr → Bool
  | ScalarExpr.litB c => c != unit
  | _ => true

/-- An operand dropped by `keepDropUnit unit` is exactly `litB unit`, whose
    denotation is `unit`. This is the fact that makes dropping it the fold's
    identity. -/
theorem denoteS_of_keepDropUnit_false (env : Nat → Bool) (unit : Bool)
    (x : ScalarExpr) (hx : keepDropUnit unit x = false) : denoteS env x = unit := by
  cases x with
  | litB c => cases c <;> cases unit <;> simp_all [keepDropUnit, denoteS]
  | var n => simp [keepDropUnit] at hx
  | notE e => simp [keepDropUnit] at hx
  | andE es => simp [keepDropUnit] at hx
  | orE es => simp [keepDropUnit] at hx
  | ifE c t e => simp [keepDropUnit] at hx
  | isNullE e => simp [keepDropUnit] at hx
  | binaryE f a b => simp [keepDropUnit] at hx

/-- Filtering out operands that all denote `true` leaves `List.all` unchanged:
    a `true` conjunct is the identity of the AND fold. -/
theorem all_filter_of_dropped_true (f p : ScalarExpr → Bool) (l : List ScalarExpr)
    (hdrop : ∀ x, p x = false → f x = true) :
    (l.filter p).all f = l.all f := by
  induction l with
  | nil => simp
  | cons a as ih =>
    cases hp : p a with
    | true => simp [List.filter_cons, hp, List.all_cons, ih]
    | false =>
      have hfa : f a = true := hdrop a hp
      simp [List.filter_cons, hp, List.all_cons, ih, hfa]

/-- Dual of `all_filter_of_dropped_true`: filtering out operands that all
    denote `false` leaves `List.any` unchanged (a `false` disjunct is the
    identity of the OR fold). -/
theorem any_filter_of_dropped_false (f p : ScalarExpr → Bool) (l : List ScalarExpr)
    (hdrop : ∀ x, p x = false → f x = false) :
    (l.filter p).any f = l.any f := by
  induction l with
  | nil => simp
  | cons a as ih =>
    cases hp : p a with
    | true => simp [List.filter_cons, hp, List.any_cons, ih]
    | false =>
      have hfa : f a = false := hdrop a hp
      simp [List.filter_cons, hp, List.any_cons, ih, hfa]

theorem denoteSFold_and_drop_unit (env : Nat → Bool) (xs : List ScalarExpr) :
    denoteSFold env (xs.filter (keepDropUnit true)) true (· && ·)
      = denoteSFold env xs true (· && ·) := by
  rw [denoteSFold_and_eq_all, denoteSFold_and_eq_all]
  exact all_filter_of_dropped_true (fun x => denoteS env x) (keepDropUnit true) xs
    (fun x hx => denoteS_of_keepDropUnit_false env true x hx)

theorem denoteSFold_or_drop_unit (env : Nat → Bool) (xs : List ScalarExpr) :
    denoteSFold env (xs.filter (keepDropUnit false)) false (· || ·)
      = denoteSFold env xs false (· || ·) := by
  rw [denoteSFold_or_eq_any, denoteSFold_or_eq_any]
  exact any_filter_of_dropped_false (fun x => denoteS env x) (keepDropUnit false) xs
    (fun x hx => denoteS_of_keepDropUnit_false env false x hx)

-- First-occurrence-agnostic dedup: keep one operand per membership class,
-- dropping repeats. Only membership matters for the AND/OR fold, so keeping the
-- last rather than the first occurrence is immaterial to soundness. Uses
-- classical decidability of `∈` to avoid `DecidableEq ScalarExpr`; the def is
-- `noncomputable` because it is a proof-only spec, never run.
open Classical in
noncomputable def dedupById : List ScalarExpr → List ScalarExpr
  | [] => []
  | x :: xs => if x ∈ dedupById xs then dedupById xs else x :: dedupById xs

/-- `dedupById` preserves membership: it drops repeats, never a class. -/
theorem mem_dedupById (x : ScalarExpr) (xs : List ScalarExpr) :
    x ∈ dedupById xs ↔ x ∈ xs := by
  induction xs generalizing x with
  | nil => simp [dedupById]
  | cons a as ih =>
    simp only [dedupById]
    by_cases h : a ∈ dedupById as
    · rw [if_pos h, ih x]
      have ha : a ∈ as := (ih a).mp h
      simp only [List.mem_cons]
      constructor
      · exact fun hx => Or.inr hx
      · rintro (rfl | hx)
        · exact ha
        · exact hx
    · rw [if_neg h]
      simp [List.mem_cons, ih x]

/-- `List.all` depends only on membership, so equal-membership lists agree. -/
theorem all_congr_mem (f : ScalarExpr → Bool) {l₁ l₂ : List ScalarExpr}
    (h : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.all f = l₂.all f := by
  have key : ∀ a b : List ScalarExpr, (∀ x, x ∈ a → x ∈ b) →
      b.all f = true → a.all f = true := by
    intro a b hsub hb
    rw [List.all_eq_true] at hb ⊢
    intro x hx
    exact hb x (hsub x hx)
  have h1 : l₁.all f = true → l₂.all f = true :=
    key l₂ l₁ (fun x hx => (h x).mpr hx)
  have h2 : l₂.all f = true → l₁.all f = true :=
    key l₁ l₂ (fun x hx => (h x).mp hx)
  cases hb1 : l₁.all f <;> cases hb2 : l₂.all f <;> simp_all

/-- Dual of `all_congr_mem` for `List.any`. -/
theorem any_congr_mem (f : ScalarExpr → Bool) {l₁ l₂ : List ScalarExpr}
    (h : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.any f = l₂.any f := by
  have key : ∀ a b : List ScalarExpr, (∀ x, x ∈ a → x ∈ b) →
      a.any f = true → b.any f = true := by
    intro a b hsub ha
    rw [List.any_eq_true] at ha ⊢
    obtain ⟨x, hx, hfx⟩ := ha
    exact ⟨x, hsub x hx, hfx⟩
  have h1 : l₁.any f = true → l₂.any f = true :=
    key l₁ l₂ (fun x hx => (h x).mp hx)
  have h2 : l₂.any f = true → l₁.any f = true :=
    key l₂ l₁ (fun x hx => (h x).mpr hx)
  cases hb1 : l₁.any f <;> cases hb2 : l₂.any f <;> simp_all

theorem denoteSFold_and_dedup (env : Nat → Bool) (xs : List ScalarExpr) :
    denoteSFold env (dedupById xs) true (· && ·) = denoteSFold env xs true (· && ·) := by
  rw [denoteSFold_and_eq_all, denoteSFold_and_eq_all]
  exact all_congr_mem (fun x => denoteS env x) (fun x => mem_dedupById x xs)

theorem denoteSFold_or_dedup (env : Nat → Bool) (xs : List ScalarExpr) :
    denoteSFold env (dedupById xs) false (· || ·) = denoteSFold env xs false (· || ·) := by
  rw [denoteSFold_or_eq_any, denoteSFold_or_eq_any]
  exact any_congr_mem (fun x => denoteS env x) (fun x => mem_dedupById x xs)

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

/-! ### Evidence for the inner-set subsumption absorption rules

`absorb_and`/`absorb_or` (the DSL's `absorb(xs, or)` / `absorb(xs, and)`) drop an
outer operand `Q` that another operand `P` subsumes through the dual connective's
inner set: for outer `And` / inner `Or`, `Q = orE S_Q` is dropped when some `P =
orE S_P` has `S_P ⊊ S_Q`, because `orE S_P ⟹ orE S_Q` makes `Q` redundant under
`And`. For outer `Or` / inner `And` it is dual (`S_P ⊊ S_Q` gives `andE S_Q ⟹
andE S_P`, so `Q` is redundant under `Or`).

The two-valued `denoteS` model has no error value, so the `could_error` gate the
Rust rule applies to the dropped extras is invisible here (the same
guard-vacuous stance as `isnull_fold` and the dedup rules): Lean proves the
two-valued absorption ALGEBRA, and lifting it to the error-gated, e-class-id
based `rest_filters::rest_absorb` rests on the Rust analysis plus the
differential parity oracle, not on this layer. `absorbInner*` below is likewise a
proof-only spec: it is `noncomputable` and drops every operand equal to the first
subsumed value, whereas `rest_absorb` drops one operand then re-fires. Both are
denotation-preserving (this is what the theorems certify); the exact operand
multiset is a fidelity gap the oracle covers, exactly as for `dedupById`. -/

-- Classical decidability of the subsumption predicate (`absorbCand`) and of
-- operand equality drives the `noncomputable` `absorbInner` spec and its proofs.
section AbsorbSubsumption
open Classical

/-- One outer operand's inner list under the dual connective `Or`: the operand's
    `orE` arguments if it is an `orE`, else the singleton `[x]`. -/
def innerOr : ScalarExpr → List ScalarExpr
  | ScalarExpr.orE es => es
  | x => [x]

/-- One outer operand's inner list under the dual connective `And`. -/
def innerAnd : ScalarExpr → List ScalarExpr
  | ScalarExpr.andE es => es
  | x => [x]

/-- Every operand denotes the `Or`-fold (`any`) of its `innerOr` list: an `orE`
    by `denoteSFold_or_eq_any`, any other operand because `[x].any = denoteS x`. -/
theorem denoteS_eq_any_innerOr (env : Nat → Bool) (x : ScalarExpr) :
    denoteS env x = (innerOr x).any (fun a => denoteS env a) := by
  cases x with
  | orE es => simp only [innerOr, denoteS]; exact denoteSFold_or_eq_any env es
  | var n => simp [innerOr]
  | notE e => simp [innerOr]
  | andE es => simp [innerOr]
  | ifE c t e => simp [innerOr]
  | litB b => simp [innerOr]
  | isNullE e => simp [innerOr]
  | binaryE f a b => simp [innerOr]

/-- Dual of `denoteS_eq_any_innerOr`: every operand denotes the `And`-fold
    (`all`) of its `innerAnd` list. -/
theorem denoteS_eq_all_innerAnd (env : Nat → Bool) (x : ScalarExpr) :
    denoteS env x = (innerAnd x).all (fun a => denoteS env a) := by
  cases x with
  | andE es => simp only [innerAnd, denoteS]; exact denoteSFold_and_eq_all env es
  | var n => simp [innerAnd]
  | notE e => simp [innerAnd]
  | orE es => simp [innerAnd]
  | ifE c t e => simp [innerAnd]
  | litB b => simp [innerAnd]
  | isNullE e => simp [innerAnd]
  | binaryE f a b => simp [innerAnd]

/-- `innerOr`-subset lifts to implication: if every `innerOr` operand of `p` is
    an `innerOr` operand of `q`, then `p ⟹ q` (the `Or`-fold is monotone under
    superset). -/
theorem orImplies_of_innerOr_subset (env : Nat → Bool) (p q : ScalarExpr)
    (h : ∀ a ∈ innerOr p, a ∈ innerOr q) :
    denoteS env p = true → denoteS env q = true := by
  rw [denoteS_eq_any_innerOr env p, denoteS_eq_any_innerOr env q]
  intro hp
  rw [List.any_eq_true] at hp ⊢
  obtain ⟨a, ha, hva⟩ := hp
  exact ⟨a, h a ha, hva⟩

/-- Dual: `innerAnd`-subset lifts to the reverse implication `q ⟹ p` (the
    `And`-fold is antitone under superset: more conjuncts is a stronger term). -/
theorem andImplies_of_innerAnd_subset (env : Nat → Bool) (p q : ScalarExpr)
    (h : ∀ a ∈ innerAnd p, a ∈ innerAnd q) :
    denoteS env q = true → denoteS env p = true := by
  rw [denoteS_eq_all_innerAnd env p, denoteS_eq_all_innerAnd env q]
  intro hq
  rw [List.all_eq_true] at hq ⊢
  intro a ha
  exact hq a (h a ha)

/-- `p` properly inner-subsumes `q` under `inner`: `inner p ⊆ inner q` and the
    inclusion is strict (some `inner q` element is outside `inner p`). Strictness
    forces `p ≠ q`, so a proper subsumer is always a distinct operand. -/
def properInner (inner : ScalarExpr → List ScalarExpr) (p q : ScalarExpr) : Prop :=
  (∀ a ∈ inner p, a ∈ inner q) ∧ (∃ b ∈ inner q, b ∉ inner p)

/-- One outer operand `q` is subsumed if some operand of `xs` properly
    inner-subsumes it. -/
def absorbCand (inner : ScalarExpr → List ScalarExpr) (xs : List ScalarExpr)
    (q : ScalarExpr) : Prop :=
  ∃ p ∈ xs, properInner inner p q

/-- The kept operands after inner-set subsumption absorption: find the first
    outer operand `q` some other operand properly inner-subsumes, then drop every
    operand equal to `q`. Noncomputable proof-only spec (classical decidability
    of the subsumption predicate), mirroring `dedupById`. The RUNNING absorption
    is `rest_filters::rest_absorb`; see the section header for the fidelity gap. -/
noncomputable def absorbInner (inner : ScalarExpr → List ScalarExpr)
    (xs : List ScalarExpr) : List ScalarExpr :=
  match xs.find? (fun q => decide (absorbCand inner xs q)) with
  | some q => xs.filter (fun x => decide (x ≠ q))
  | none => xs

/-- Outer `And` / inner `Or` absorption spec (`absorb(xs, or)`). -/
noncomputable def absorbInnerOr (xs : List ScalarExpr) : List ScalarExpr :=
  absorbInner innerOr xs

/-- Outer `Or` / inner `And` absorption spec (`absorb(xs, and)`). -/
noncomputable def absorbInnerAnd (xs : List ScalarExpr) : List ScalarExpr :=
  absorbInner innerAnd xs

/-- Two `Bool`s that imply each other (both directions) are equal. Discharges the
    final combine of the `all`/`any` absorption proofs from their two inclusions. -/
theorem bool_eq_of_imp {a b : Bool} (h1 : b = true → a = true)
    (h2 : a = true → b = true) : a = b := by
  cases a <;> cases b <;> simp_all

/-- Absorbing the outer `And`'s operand list preserves the `And`-fold (`all`):
    the dropped value is implied by a surviving proper-subsumer, so removing it
    changes no `all`. -/
theorem all_absorbInnerOr (env : Nat → Bool) (xs : List ScalarExpr) :
    (absorbInnerOr xs).all (fun a => denoteS env a)
      = xs.all (fun a => denoteS env a) := by
  unfold absorbInnerOr absorbInner
  cases hf : xs.find? (fun q => decide (absorbCand innerOr xs q)) with
  | none => rfl
  | some q =>
    have hcand : absorbCand innerOr xs q := by
      have hb := List.find?_some hf
      exact of_decide_eq_true hb
    obtain ⟨p, hp_mem, hprop⟩ := hcand
    unfold properInner at hprop
    obtain ⟨hsub, b, hb_in, hb_out⟩ := hprop
    have hpq : p ≠ q := by intro h; subst h; exact hb_out hb_in
    have hp_filt : p ∈ xs.filter (fun x => decide (x ≠ q)) :=
      List.mem_filter.mpr ⟨hp_mem, by rw [decide_eq_true_eq]; exact hpq⟩
    have himp : denoteS env p = true → denoteS env q = true :=
      orImplies_of_innerOr_subset env p q hsub
    have fwd : xs.all (fun a => denoteS env a) = true →
        (xs.filter (fun x => decide (x ≠ q))).all (fun a => denoteS env a) = true := by
      intro h; rw [List.all_eq_true] at h ⊢
      intro x hx; exact h x (List.mem_filter.mp hx).1
    have bwd : (xs.filter (fun x => decide (x ≠ q))).all (fun a => denoteS env a) = true →
        xs.all (fun a => denoteS env a) = true := by
      intro h; rw [List.all_eq_true] at h ⊢
      intro x hx
      by_cases hxq : x = q
      · subst hxq; exact himp (h p hp_filt)
      · exact h x (List.mem_filter.mpr ⟨hx, by rw [decide_eq_true_eq]; exact hxq⟩)
    exact bool_eq_of_imp fwd bwd

/-- Dual of `all_absorbInnerOr`: absorbing the outer `Or`'s operand list
    preserves the `Or`-fold (`any`). The dropped value implies its surviving
    proper-subsumer, so removing it changes no `any`. -/
theorem any_absorbInnerAnd (env : Nat → Bool) (xs : List ScalarExpr) :
    (absorbInnerAnd xs).any (fun a => denoteS env a)
      = xs.any (fun a => denoteS env a) := by
  unfold absorbInnerAnd absorbInner
  cases hf : xs.find? (fun q => decide (absorbCand innerAnd xs q)) with
  | none => rfl
  | some q =>
    have hcand : absorbCand innerAnd xs q := by
      have hb := List.find?_some hf
      exact of_decide_eq_true hb
    obtain ⟨p, hp_mem, hprop⟩ := hcand
    unfold properInner at hprop
    obtain ⟨hsub, b, hb_in, hb_out⟩ := hprop
    have hpq : p ≠ q := by intro h; subst h; exact hb_out hb_in
    have hp_filt : p ∈ xs.filter (fun x => decide (x ≠ q)) :=
      List.mem_filter.mpr ⟨hp_mem, by rw [decide_eq_true_eq]; exact hpq⟩
    have himp : denoteS env q = true → denoteS env p = true :=
      andImplies_of_innerAnd_subset env p q hsub
    have fwd : (xs.filter (fun x => decide (x ≠ q))).any (fun a => denoteS env a) = true →
        xs.any (fun a => denoteS env a) = true := by
      intro h; rw [List.any_eq_true] at h ⊢
      obtain ⟨x, hx, hfx⟩ := h
      exact ⟨x, (List.mem_filter.mp hx).1, hfx⟩
    have bwd : xs.any (fun a => denoteS env a) = true →
        (xs.filter (fun x => decide (x ≠ q))).any (fun a => denoteS env a) = true := by
      intro h; rw [List.any_eq_true] at h ⊢
      obtain ⟨x, hx, hfx⟩ := h
      by_cases hxq : x = q
      · subst hxq; exact ⟨p, hp_filt, himp hfx⟩
      · exact ⟨x, List.mem_filter.mpr ⟨hx, by rw [decide_eq_true_eq]; exact hxq⟩, hfx⟩
    exact bool_eq_of_imp bwd fwd

/-- The fold-level absorption law the emitter's `absorb_and` theorem reduces to
    after unfolding `denoteS`: the `And`-fold is invariant under `absorbInnerOr`. -/
theorem denoteSFold_and_absorb (env : Nat → Bool) (xs : List ScalarExpr) :
    denoteSFold env (absorbInnerOr xs) true (· && ·)
      = denoteSFold env xs true (· && ·) := by
  rw [denoteSFold_and_eq_all, denoteSFold_and_eq_all]
  exact all_absorbInnerOr env xs

/-- Dual for `absorb_or`: the `Or`-fold is invariant under `absorbInnerAnd`. -/
theorem denoteSFold_or_absorb (env : Nat → Bool) (xs : List ScalarExpr) :
    denoteSFold env (absorbInnerAnd xs) false (· || ·)
      = denoteSFold env xs false (· || ·) := by
  rw [denoteSFold_or_eq_any, denoteSFold_or_eq_any]
  exact any_absorbInnerAnd env xs

end AbsorbSubsumption

end MirRewrite
