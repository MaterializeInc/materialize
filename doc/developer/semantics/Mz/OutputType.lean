import Mz.Schema
import Mz.MightError

/-!
# Output schema propagation for `Expr`

`Expr.outputType sch e` derives the output `ColSchema` for an
expression `e` evaluated on a row satisfying input schema `sch`,
with soundness theorem `eval_satisfies_outputType`.

Per-constructor precision:

* `.lit` / `.col`: precise.
* `.not a`: errable propagates from `a`; nullable is `true`
  because `evalNot` routes non-boolean operands to `.null`.
* `.plus` / `.minus` / `.times` / `.eq` / `.lt`: `errable` is the
  OR of the inputs (no spontaneous err on `Int`; the modeled
  `evalPlus`/etc. use unbounded `Int`). `nullable` is conservative
  (`true`) because the four-valued lattice routes type-mismatched
  operands to `.null` without violating any input nullability bit.
* `.divide a b`: `errable := true` (division-by-zero on
  `b = .int 0`). `nullable := true` for the same type-mismatch
  reason as the other arithmetic operators.
* `.ifThen c t e`: `errable` is the OR of all three arms (the only
  way to produce an `.err _` output is via an `.err _` input).
  `nullable := true` (a non-bool / non-null / non-err condition
  routes to `.null`).
* `.andN` / `.orN` / `.coalesce`: left conservative
  (`{ nullable := true, errable := true }`). The tighter rules —
  variadic `errable := args.any errable` for the boolean fragment,
  and the rescue rule for `.coalesce` (see
  `Mz/Schema.lean`'s `eval_coalesce_pair_of_a_concrete`) — require
  mutual recursion mirroring `Expr.might_error` and are left as a
  follow-up. -/

namespace Mz

/-! ## Satisfaction predicate at the `Datum` level -/

/-- A `Datum` satisfies a `ColSchema`. The implication direction is
the active part: `cs.nullable = false` constrains, `cs.nullable = true`
is vacuous; same for `errable`. -/
def DatumSatisfies (cs : ColSchema) (d : Datum) : Prop :=
  (cs.nullable = false → d ≠ .null) ∧ (cs.errable = false → ¬d.IsErr)

/-- The weakest schema is satisfied by every datum. Discharges every
constructor that `Expr.outputType` does not yet have a precise rule
for. -/
theorem DatumSatisfies.weakest (d : Datum) :
    DatumSatisfies { nullable := true, errable := true } d := by
  refine ⟨?_, ?_⟩
  · intro h; cases h
  · intro h; cases h

/-! ## Output type computation -/

/-- Compute the output `ColSchema` for an expression.

Precise on `.lit` and `.col`. Tight `errable` propagation on
`.not`, `.ifThen`, `.plus`/`.minus`/`.times`, `.eq`/`.lt`, and
`.not`-preservation of `nullable`. `nullable := true` everywhere
else because the four-valued lattice produces `.null` outputs from
non-null/non-err inputs whenever operand types mismatch (e.g.
`evalPlus (.bool true) (.int 5) = .null`). `.divide` is always
errable (divide-by-zero). The variadic constructors `.andN`,
`.orN`, and `.coalesce` are left conservative (see module
docstring). -/
def Expr.outputType {n : Nat} (sch : Schema n) : Expr → ColSchema
  | .lit (.bool _) => { nullable := false, errable := false }
  | .lit (.int _)  => { nullable := false, errable := false }
  | .lit .null     => { nullable := true,  errable := false }
  | .lit (.err _)  => { nullable := false, errable := true }
  | .col i =>
    if h : i < n then sch.cols.get ⟨i, h⟩
    else { nullable := true, errable := false }
  | .not a =>
    -- After tightening evalNot to route .int to .null, the
    -- output may be .null even when input is non-null (.int
    -- input → .null output). Conservative `nullable := true`;
    -- `errable` propagates from input.
    { nullable := true
      errable := (Expr.outputType sch a).errable }
  | .ifThen c t e =>
    { nullable := true
      errable :=
        (Expr.outputType sch c).errable
        || (Expr.outputType sch t).errable
        || (Expr.outputType sch e).errable }
  | .plus a b =>
    { nullable := true
      errable := (Expr.outputType sch a).errable || (Expr.outputType sch b).errable }
  | .minus a b =>
    { nullable := true
      errable := (Expr.outputType sch a).errable || (Expr.outputType sch b).errable }
  | .times a b =>
    { nullable := true
      errable := (Expr.outputType sch a).errable || (Expr.outputType sch b).errable }
  | .divide _ _ =>
    { nullable := true, errable := true }
  | .eq a b =>
    { nullable := true
      errable := (Expr.outputType sch a).errable || (Expr.outputType sch b).errable }
  | .lt a b =>
    { nullable := true
      errable := (Expr.outputType sch a).errable || (Expr.outputType sch b).errable }
  | .andN _ => { nullable := true, errable := true }
  | .orN _ => { nullable := true, errable := true }
  | .coalesce _ => { nullable := true, errable := true }

/-! ## `Env.get` on a row that satisfies a schema -/

theorem Env_get_row_toList_lt
    {n : Nat} (row : RowN n) (i : Nat) (h : i < n) :
    Env.get row.toList i = row.get ⟨i, h⟩ := by
  have hLen : row.toList.length = n := row.toList_length
  have hLt : i < row.toList.length := by rw [hLen]; exact h
  rw [Env.get_eq_list_get row.toList i hLt]
  show row.toList.get ⟨i, hLt⟩ = row.get ⟨i, h⟩
  rcases row with ⟨l, hl⟩; rfl

theorem Env_get_row_toList_ge
    {n : Nat} (row : RowN n) (i : Nat) (h : ¬i < n) :
    Env.get row.toList i = .null := by
  have hLen : row.toList.length = n := row.toList_length
  apply Env.get_eq_null_of_ge
  rw [hLen]; omega

/-! ## Soundness -/

/-! ### Helper for the `nullable := true, errable := f a || f b`
shape used by the arithmetic and comparison operators. -/

/-- An `errable`-OR rule for a binary operator. Given a strictness
witness "if neither input is `.err _`, the output is not `.err _`",
this packages the `DatumSatisfies` conclusion. -/
private theorem binOp_satisfies
    {da db : Datum} {csa csb : ColSchema}
    {f : Datum → Datum → Datum}
    (hsata : DatumSatisfies csa da) (hsatb : DatumSatisfies csb db)
    (hNotErr : ¬da.IsErr → ¬db.IsErr → ¬(f da db).IsErr) :
    DatumSatisfies
      { nullable := true, errable := csa.errable || csb.errable }
      (f da db) := by
  refine ⟨?_, ?_⟩
  · intro h; cases h
  · intro hErr
    simp only [Bool.or_eq_false_iff] at hErr
    exact hNotErr (hsata.2 hErr.1) (hsatb.2 hErr.2)

/-- Soundness: evaluating `e` on a row satisfying `sch` produces a
`Datum` satisfying `Expr.outputType sch e`. Precise on `.lit`,
`.col`, and `.not`; tight on `errable` for `.ifThen`, the
arithmetic operators (modulo divide-by-zero), and the comparison
operators; conservative on `.andN`, `.orN`, and `.coalesce` — those
fall through to `DatumSatisfies.weakest`. -/
theorem eval_satisfies_outputType {n : Nat}
    (sch : Schema n) (row : RowN n) (hsat : RowSatisfies sch row) :
    ∀ (e : Expr), DatumSatisfies (Expr.outputType sch e) (eval row.toList e)
  | .lit d => by
    simp only [eval]
    cases d with
    | bool b =>
      refine ⟨?_, ?_⟩
      · intro _ h; cases h
      · intro _ h; cases h
    | int k =>
      refine ⟨?_, ?_⟩
      · intro _ h; cases h
      · intro _ h; cases h
    | null =>
      refine ⟨?_, ?_⟩
      · intro h; cases h
      · intro _ h; cases h
    | err e =>
      refine ⟨?_, ?_⟩
      · intro _ h; cases h
      · intro h; cases h
  | .col i => by
    simp only [eval, Expr.outputType]
    by_cases h : i < n
    · rw [Env_get_row_toList_lt row i h]
      rw [dif_pos h]
      exact hsat ⟨i, h⟩
    · rw [Env_get_row_toList_ge row i h]
      rw [dif_neg h]
      refine ⟨?_, ?_⟩
      · intro hN; cases hN
      · intro _ h; cases h
  | .not a => by
    -- `outputType (.not a) = { nullable := true,
    -- errable := outputType(a).errable }`. Only the errable bit
    -- needs the IH; nullable is trivially true.
    have iha := eval_satisfies_outputType sch row hsat a
    simp only [eval, Expr.outputType]
    refine ⟨?_, ?_⟩
    · intro h; cases h
    · intro hErr
      have ha_noterr : ¬(eval row.toList a).IsErr := iha.2 hErr
      exact evalNot_not_err ha_noterr
  | .ifThen c t e => by
    have ihc := eval_satisfies_outputType sch row hsat c
    have iht := eval_satisfies_outputType sch row hsat t
    have ihe := eval_satisfies_outputType sch row hsat e
    simp only [eval, Expr.outputType]
    refine ⟨?_, ?_⟩
    · intro h; cases h
    · intro hErr
      -- hErr : (outputType c).errable || (outputType t).errable || (outputType e).errable = false
      simp only [Bool.or_eq_false_iff] at hErr
      obtain ⟨⟨hc, ht⟩, he⟩ := hErr
      exact evalIfThen_not_err (ihc.2 hc) (iht.2 ht) (ihe.2 he)
  | .andN _ => DatumSatisfies.weakest _
  | .orN _ => DatumSatisfies.weakest _
  | .coalesce _ => DatumSatisfies.weakest _
  | .plus a b => by
    have iha := eval_satisfies_outputType sch row hsat a
    have ihb := eval_satisfies_outputType sch row hsat b
    simp only [eval, Expr.outputType]
    exact binOp_satisfies iha ihb (fun ha hb => evalPlus_not_err ha hb)
  | .minus a b => by
    have iha := eval_satisfies_outputType sch row hsat a
    have ihb := eval_satisfies_outputType sch row hsat b
    simp only [eval, Expr.outputType]
    exact binOp_satisfies iha ihb (fun ha hb => evalMinus_not_err ha hb)
  | .times a b => by
    have iha := eval_satisfies_outputType sch row hsat a
    have ihb := eval_satisfies_outputType sch row hsat b
    simp only [eval, Expr.outputType]
    exact binOp_satisfies iha ihb (fun ha hb => evalTimes_not_err ha hb)
  | .divide _ _ => DatumSatisfies.weakest _
  | .eq a b => by
    have iha := eval_satisfies_outputType sch row hsat a
    have ihb := eval_satisfies_outputType sch row hsat b
    simp only [eval, Expr.outputType]
    exact binOp_satisfies iha ihb (fun ha hb => evalEq_not_err ha hb)
  | .lt a b => by
    have iha := eval_satisfies_outputType sch row hsat a
    have ihb := eval_satisfies_outputType sch row hsat b
    simp only [eval, Expr.outputType]
    exact binOp_satisfies iha ihb (fun ha hb => evalLt_not_err ha hb)

end Mz
