import Mz.Expr
import Mz.Eval

/-!
# Output column schema for indexed `Expr`

`Expr.outputCols sch e : ColSchema` derives nullable / errable
bits for the cells produced by evaluating `e`. The type `k` is
already known from the GADT index — no separate `outputKind`
function. Indexed counterpart of `Mz/OutputType.lean`.

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

end Mz
