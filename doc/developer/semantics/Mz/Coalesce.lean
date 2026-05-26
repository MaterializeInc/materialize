import Mz.PrimEval

/-!
# `coalesce` laws (indexed)

Cell equations for `evalCoalesce` on `Datum k`. Indexed counterpart
of `Mz/Coalesce.lean`.

All operands share kind `k` by construction — the type-mismatch
handling that occupies the untyped model is gone.

Per-kind sections cover the bool and int instances. The `.top`
kind admits only `.null` / `.err _` operands; its laws are a
subset of the bool/int ones (the `.bool` / `.int` rescue rules
don't fire) and are stated on demand. -/

namespace Mz


/-! ## Base cases (kind-polymorphic) -/

theorem coalesce_nil {k : ColType} :
    evalCoalesce ([] : List (Datum k)) = (.null : Datum k) := by
  simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue]

theorem coalesce_singleton_null {k : ColType} :
    evalCoalesce ([(.null : Datum k)]) = (.null : Datum k) := by
  simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue]

theorem coalesce_singleton_err {k : ColType} (e : EvalError) :
    evalCoalesce ([(.err e : Datum k)]) = (.err e : Datum k) := by
  simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue]

/-! ## Boolean fragment -/

theorem coalesce_singleton_bool (b : Bool) :
    evalCoalesce [(.bool b : Datum .bool)] = .bool b := by
  simp [evalCoalesce, Coalesce.firstConcrete]

theorem coalesce_err_rescue_bool (e : EvalError) (b : Bool) :
    evalCoalesce [(.err e : Datum .bool), .bool b] = .bool b := by
  simp [evalCoalesce, Coalesce.firstConcrete]

theorem coalesce_null_rescue_bool (b : Bool) :
    evalCoalesce [(.null : Datum .bool), .bool b] = .bool b := by
  simp [evalCoalesce, Coalesce.firstConcrete]

theorem coalesce_null_then_err_bool (e : EvalError) :
    evalCoalesce [(.null : Datum .bool), .err e] = .null := by
  simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue]

theorem coalesce_err_then_null_bool (e : EvalError) :
    evalCoalesce [(.err e : Datum .bool), .null] = .null := by
  simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue, Datum.isNullB]

theorem coalesce_first_err_wins_bool (e₁ e₂ : EvalError) :
    evalCoalesce [(.err e₁ : Datum .bool), .err e₂] = .err e₁ := by
  simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue, Datum.isNullB]

theorem coalesce_err_err_bool (e₁ e₂ : EvalError) (b : Bool) :
    evalCoalesce [(.err e₁ : Datum .bool), .err e₂, .bool b] = .bool b := by
  simp [evalCoalesce, Coalesce.firstConcrete]

theorem coalesce_err_err_null_bool (e₁ e₂ : EvalError) :
    evalCoalesce [(.err e₁ : Datum .bool), .err e₂, .null] = .null := by
  simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue, Datum.isNullB]

theorem coalesce_err_null_err_bool (e₁ e₂ : EvalError) :
    evalCoalesce [(.err e₁ : Datum .bool), .null, .err e₂] = .null := by
  simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue, Datum.isNullB]

theorem coalesce_null_err_bool (e : EvalError) (b : Bool) :
    evalCoalesce [(.null : Datum .bool), .err e, .bool b] = .bool b := by
  simp [evalCoalesce, Coalesce.firstConcrete]

/-! ## Integer fragment -/

theorem coalesce_singleton_int (n : Int) :
    evalCoalesce [(.int n : Datum .int)] = .int n := by
  simp [evalCoalesce, Coalesce.firstConcrete]

theorem coalesce_err_rescue_int (e : EvalError) (n : Int) :
    evalCoalesce [(.err e : Datum .int), .int n] = .int n := by
  simp [evalCoalesce, Coalesce.firstConcrete]

theorem coalesce_null_rescue_int (n : Int) :
    evalCoalesce [(.null : Datum .int), .int n] = .int n := by
  simp [evalCoalesce, Coalesce.firstConcrete]

theorem coalesce_first_err_wins_int (e₁ e₂ : EvalError) :
    evalCoalesce [(.err e₁ : Datum .int), .err e₂] = .err e₁ := by
  simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue, Datum.isNullB]

/-! ## Concrete-head collapse

When the head of the operand list is concrete (`.bool _` for kind
`.bool`, `.int _` for kind `.int`), `firstConcrete` short-circuits
and `evalCoalesce` returns it verbatim. The `.top` kind is
vacuously covered — `Datum .top` has no concrete inhabitant. -/

theorem Coalesce.firstConcrete_cons_concrete {k : ColType}
    (d : Datum k) (rest : List (Datum k))
    (hN : ¬d.IsNull) (hE : ¬d.IsErr) :
    Coalesce.firstConcrete (d :: rest) = some d := by
  cases d with
  | bool _ => simp [Coalesce.firstConcrete]
  | int _  => simp [Coalesce.firstConcrete]
  | null   => exact absurd trivial hN
  | err _  => exact absurd trivial hE

theorem evalCoalesce_cons_concrete {k : ColType}
    (d : Datum k) (rest : List (Datum k))
    (hN : ¬d.IsNull) (hE : ¬d.IsErr) :
    evalCoalesce (d :: rest) = d := by
  show (match Coalesce.firstConcrete (d :: rest) with
        | some d' => d'
        | none => Coalesce.residue (d :: rest)) = d
  rw [Coalesce.firstConcrete_cons_concrete d rest hN hE]

end Mz
