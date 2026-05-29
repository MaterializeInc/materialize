import Mz.Expr
import Mz.Eval
import Mz.MightError
import Mz.Coalesce

/-!
# Output column schema for `Expr`

`Expr.outputCols sch e : ColSchema` derives nullable / errable
bits for the cells produced by evaluating `e`. The type `k` is
already known from the GADT index — no separate `outputKind`
function.

Soundness theorem `eval_satisfies_outputCols`: evaluating `e` on
a row satisfying `sch` produces a `Datum k` satisfying
`outputCols sch e`. -/

namespace Mz


/-- A `Datum k` satisfies a `ColSchema`. -/
def DatumSatisfies {k : ColType} (cs : ColSchema) (d : Datum k) : Prop :=
  (cs.nullable = false → ¬d.IsNull) ∧ (cs.errable = false → ¬d.IsErr)

/-- The weakest schema is satisfied by every datum. -/
theorem DatumSatisfies.weakest {k : ColType} (d : Datum k) :
    DatumSatisfies { nullable := true, errable := true } d := by
  refine ⟨?_, ?_⟩
  · intro h; cases h
  · intro h; cases h

/-- A row satisfies a schema if each cell satisfies the
corresponding `ColSchema`. -/
def RowSatisfies {n : Nat} (sch : Schema n) (env : Env sch) : Prop :=
  ∀ i : Fin n, DatumSatisfies (sch.cols.get i) (env i)

/-! ## Output schema computation -/

mutual
  /-- Derive a `ColSchema` for an `Expr`. Precise on `.lit`, `.col`,
  and `.not`; tight errable-OR on arithmetic / comparison / ifThen;
  conservative weakest on variadic `.andN` / `.orN` / `.coalesce`. -/
  def Expr.outputCols {n : Nat} {sch : Schema n} :
      {k : ColType} → Expr sch k → ColSchema
    | _, .lit (.bool _) => { nullable := false, errable := false }
    | _, .lit (.int _)  => { nullable := false, errable := false }
    | _, .lit .null     => { nullable := true,  errable := false }
    | _, .lit (.err _)  => { nullable := false, errable := true }
    | _, .col i         => sch.cols.get i
    | _, .not a         =>
      { nullable := true, errable := (Expr.outputCols a).errable }
    | _, .ifThen c t e =>
      { nullable := true
        errable :=
          (Expr.outputCols c).errable
          || (Expr.outputCols t).errable
          || (Expr.outputCols e).errable }
    | _, .plus a b =>
      { nullable := true
        errable := (Expr.outputCols a).errable || (Expr.outputCols b).errable }
    | _, .minus a b =>
      { nullable := true
        errable := (Expr.outputCols a).errable || (Expr.outputCols b).errable }
    | _, .times a b =>
      { nullable := true
        errable := (Expr.outputCols a).errable || (Expr.outputCols b).errable }
    | _, .divide _ _ =>
      { nullable := true, errable := true }
    | _, .eq a b =>
      { nullable := true
        errable := (Expr.outputCols a).errable || (Expr.outputCols b).errable }
    | _, .lt a b =>
      { nullable := true
        errable := (Expr.outputCols a).errable || (Expr.outputCols b).errable }
    | _, .andN _ => { nullable := true, errable := true }
    | _, .orN _  => { nullable := true, errable := true }
    | _, .coalesce _ => { nullable := true, errable := true }
end

/-! ## Soundness

`eval_satisfies_outputCols`: evaluating `e` on a `RowSatisfies`
env produces a `Datum` satisfying `Expr.outputCols sch e`.
Mechanical structural recursion over `Expr`; the
err-side reasoning consumes the `evalX_not_err` lemmas from
`Mz/MightError.lean`. Nullable bit stays `true` on the
non-foundational constructors (the catch-all routing to `.null` is
absent from the GADT codomain — `evalAnd : Datum .bool → Datum
.bool → Datum .bool` cannot produce a non-`.bool / .null / .err`,
so the conservative `nullable := true` on these constructors
remains the right default until a precision-direction refinement
under additional hypotheses lands). -/

/-- A binary-op `errable`-OR rule packaging step: given `not_err`
for the primitive plus per-input satisfaction, conclude
satisfaction of the OR rule. -/
private theorem binOp_satisfies
    {ka kb kc : ColType}
    {f : Datum ka → Datum kb → Datum kc}
    {da : Datum ka} {db : Datum kb}
    {csa csb : ColSchema}
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

theorem eval_satisfies_outputCols {n : Nat} {sch : Schema n}
    (env : Env sch) (hsat : RowSatisfies sch env) :
    {k : ColType} → (e : Expr sch k) →
      DatumSatisfies (Expr.outputCols e) (eval env e)
  | _, .lit (.bool _) => by
    simp only [eval, Expr.outputCols]
    refine ⟨?_, ?_⟩ <;> (intro _ h; cases h)
  | _, .lit (.int _) => by
    simp only [eval, Expr.outputCols]
    refine ⟨?_, ?_⟩ <;> (intro _ h; cases h)
  | _, .lit .null => by
    simp only [eval, Expr.outputCols]
    refine ⟨?_, ?_⟩
    · intro h; cases h
    · intro _ h; cases h
  | _, .lit (.err _) => by
    simp only [eval, Expr.outputCols]
    refine ⟨?_, ?_⟩
    · intro _ h; cases h
    · intro h; cases h
  | _, .col i => by
    simp only [eval, Expr.outputCols]
    exact hsat i
  | _, .not a => by
    have iha := eval_satisfies_outputCols env hsat a
    simp only [eval, Expr.outputCols]
    refine ⟨?_, ?_⟩
    · intro h; cases h
    · intro hErr
      have ha_noterr : ¬(eval env a).IsErr := iha.2 hErr
      exact evalNot_not_err ha_noterr
  | _, .ifThen c t e => by
    have ihc := eval_satisfies_outputCols env hsat c
    have iht := eval_satisfies_outputCols env hsat t
    have ihe := eval_satisfies_outputCols env hsat e
    simp only [eval, Expr.outputCols]
    refine ⟨?_, ?_⟩
    · intro h; cases h
    · intro hErr
      simp only [Bool.or_eq_false_iff] at hErr
      obtain ⟨⟨hc, ht⟩, he⟩ := hErr
      cases hev : eval env c with
      | bool b => cases b
                  · exact ihe.2 he
                  · exact iht.2 ht
      | null => intro hRes; cases hRes
      | err _ => exact absurd (by rw [hev]; trivial) (ihc.2 hc)
  | _, .plus a b => by
    have iha := eval_satisfies_outputCols env hsat a
    have ihb := eval_satisfies_outputCols env hsat b
    simp only [eval, Expr.outputCols]
    exact binOp_satisfies iha ihb (fun ha hb => evalPlus_not_err ha hb)
  | _, .minus a b => by
    have iha := eval_satisfies_outputCols env hsat a
    have ihb := eval_satisfies_outputCols env hsat b
    simp only [eval, Expr.outputCols]
    exact binOp_satisfies iha ihb (fun ha hb => evalMinus_not_err ha hb)
  | _, .times a b => by
    have iha := eval_satisfies_outputCols env hsat a
    have ihb := eval_satisfies_outputCols env hsat b
    simp only [eval, Expr.outputCols]
    exact binOp_satisfies iha ihb (fun ha hb => evalTimes_not_err ha hb)
  | _, .divide _ _ => DatumSatisfies.weakest _
  | _, .eq a b => by
    have iha := eval_satisfies_outputCols env hsat a
    have ihb := eval_satisfies_outputCols env hsat b
    simp only [eval, Expr.outputCols]
    exact binOp_satisfies iha ihb (fun ha hb => evalEq_not_err ha hb)
  | _, .lt a b => by
    have iha := eval_satisfies_outputCols env hsat a
    have ihb := eval_satisfies_outputCols env hsat b
    simp only [eval, Expr.outputCols]
    exact binOp_satisfies iha ihb (fun ha hb => evalLt_not_err ha hb)
  | _, .andN _ => DatumSatisfies.weakest _
  | _, .orN _ => DatumSatisfies.weakest _
  | _, .coalesce _ => DatumSatisfies.weakest _

/-! ## RowSatisfies → EnvErrFree bridge

If every column of the schema is statically `errable := false` and
the row satisfies the schema, then no cell is an `.err _`. The
bridge that `NoRowErr_filter` consumes: row-level err-freeness is
a corollary of the schema-driven analysis on the input collection. -/

theorem EnvErrFree_of_RowSatisfies {n : Nat} {sch : Schema n} (env : Env sch)
    (hsat : RowSatisfies sch env) (hcell : sch.cellErrFree) : EnvErrFree env :=
  fun i => (hsat i).2 (hcell i)

/-! ## Coalesce collapse (schema-rider)

If the first arg of `coalesce` has a schema marking both
`nullable := false` and `errable := false`, then on a row
satisfying `sch` that arg evaluates to a concrete `Datum` (not
`.null`, not `.err _`), and `coalesce` short-circuits to it. The
remaining args are evaluated lazily by `firstConcrete` and never
consulted — so the equation holds for any tail. -/

theorem coalesce_collapse {n : Nat} {sch : Schema n} {k : ColType}
    (a : Expr sch k) (rest : ExprList sch k)
    (env : Env sch) (hsat : RowSatisfies sch env)
    (hN : (Expr.outputCols a).nullable = false)
    (hE : (Expr.outputCols a).errable = false) :
    eval env (Expr.coalesce (ExprList.cons a rest)) = eval env a := by
  have hd := eval_satisfies_outputCols env hsat a
  have hdN : ¬(eval env a).IsNull := hd.1 hN
  have hdE : ¬(eval env a).IsErr := hd.2 hE
  rw [eval_coalesce, evalCoalesce_lazy_eq_eager]
  show evalCoalesce (eval env a :: evalList env rest) = eval env a
  exact evalCoalesce_cons_concrete _ _ hdN hdE

end Mz
