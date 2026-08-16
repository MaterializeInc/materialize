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

/-! ## Output schema computation

Every nullable bit is now tight: a primitive produces `.null` only
when a relevant operand is `.null` (strict arithmetic / comparison
/ `.not`), or when a branch is `.null` (`.ifThen`). So nullable is
the OR of the operands that can contribute a null — never an
unconditional `true`. The errable bits are tight on `.lit`, `.col`,
`.not`, arithmetic, comparison, and `.ifThen`; `.divide` is always
errable; the variadic `.andN` / `.orN` / `.coalesce` keep a
conservative `errable := true` (the absorbing short-circuit makes a
tight errable rule a separate refinement).

Variadic nullable is tight via the `ExprList` companions:
`argsNullable` ORs the operand nullable bits for `.andN` / `.orN`
(empty list folds to `.bool true` / `.bool false`, never null);
`coalesceNullable` is `false` exactly when some operand is
statically guaranteed concrete (both bits false), because such an
operand is the leftmost concrete `firstConcrete` hit and `coalesce`
short-circuits to it. The empty `coalesce` evaluates to `.null`, so
`coalesceNullable .nil = true`. -/

mutual
  /-- Derive a `ColSchema` for an `Expr`. -/
  def Expr.outputCols {n : Nat} {sch : Schema n} :
      {k : ColType} → Expr sch k → ColSchema
    | _, .lit (.bool _) => { nullable := false, errable := false }
    | _, .lit (.int _)  => { nullable := false, errable := false }
    | _, .lit .null     => { nullable := true,  errable := false }
    | _, .lit (.err _)  => { nullable := false, errable := true }
    | _, .col i         => sch.cols.get i
    | _, .not a         =>
      { nullable := (Expr.outputCols a).nullable
        errable := (Expr.outputCols a).errable }
    | _, .ifThen c t e =>
      { nullable :=
          (Expr.outputCols c).nullable
          || (Expr.outputCols t).nullable
          || (Expr.outputCols e).nullable
        errable :=
          (Expr.outputCols c).errable
          || (Expr.outputCols t).errable
          || (Expr.outputCols e).errable }
    | _, .plus a b =>
      { nullable := (Expr.outputCols a).nullable || (Expr.outputCols b).nullable
        errable := (Expr.outputCols a).errable || (Expr.outputCols b).errable }
    | _, .minus a b =>
      { nullable := (Expr.outputCols a).nullable || (Expr.outputCols b).nullable
        errable := (Expr.outputCols a).errable || (Expr.outputCols b).errable }
    | _, .times a b =>
      { nullable := (Expr.outputCols a).nullable || (Expr.outputCols b).nullable
        errable := (Expr.outputCols a).errable || (Expr.outputCols b).errable }
    | _, .divide a b =>
      { nullable := (Expr.outputCols a).nullable || (Expr.outputCols b).nullable
        errable := true }
    | _, .eq a b =>
      { nullable := (Expr.outputCols a).nullable || (Expr.outputCols b).nullable
        errable := (Expr.outputCols a).errable || (Expr.outputCols b).errable }
    | _, .lt a b =>
      { nullable := (Expr.outputCols a).nullable || (Expr.outputCols b).nullable
        errable := (Expr.outputCols a).errable || (Expr.outputCols b).errable }
    | _, .andN args => { nullable := ExprList.argsNullable args, errable := true }
    | _, .orN args  => { nullable := ExprList.argsNullable args, errable := true }
    | _, .coalesce args => { nullable := ExprList.coalesceNullable args, errable := true }

  /-- Variadic AND / OR nullable: OR of the operand nullable bits.
  Empty folds to `false` (`evalAndN [] = .bool true`,
  `evalOrN [] = .bool false`, neither null). -/
  def ExprList.argsNullable {n : Nat} {sch : Schema n} :
      {k : ColType} → ExprList sch k → Bool
    | _, .nil       => false
    | _, .cons a as => (Expr.outputCols a).nullable || ExprList.argsNullable as

  /-- `coalesce` nullable: `false` exactly when some operand is
  statically guaranteed concrete (both bits false). Empty folds to
  `true` (`evalCoalesce [] = .null`). -/
  def ExprList.coalesceNullable {n : Nat} {sch : Schema n} :
      {k : ColType} → ExprList sch k → Bool
    | _, .nil       => true
    | _, .cons a as =>
      ((Expr.outputCols a).nullable || (Expr.outputCols a).errable)
        && ExprList.coalesceNullable as
end

/-! ## Per-primitive null-freeness

Mirror of the `evalX_not_err` lemmas in `Mz/MightError.lean`:
not-null inputs yield a not-null output. A primitive returns `.null`
only via its `.null`-operand arms, so ruling those out by hypothesis
rules out a null result. These feed the nullable side of
`eval_satisfies_outputCols`. -/

theorem evalNot_not_null {d : Datum .bool} (h : ¬d.IsNull) :
    ¬(evalNot d).IsNull := by
  cases d with
  | bool b => cases b <;> (intro hn; cases hn)
  | null   => exact (h trivial).elim
  | err _  => intro hn; cases hn

theorem evalPlus_not_null {d₁ d₂ : Datum .int}
    (h₁ : ¬d₁.IsNull) (h₂ : ¬d₂.IsNull) :
    ¬(evalPlus d₁ d₂).IsNull := by
  cases d₁ with
  | int _ =>
    cases d₂ with
    | int _ => intro hn; cases hn
    | null  => exact (h₂ trivial).elim
    | err _ => intro hn; cases hn
  | null  => exact (h₁ trivial).elim
  | err _ => intro hn; cases hn

theorem evalMinus_not_null {d₁ d₂ : Datum .int}
    (h₁ : ¬d₁.IsNull) (h₂ : ¬d₂.IsNull) :
    ¬(evalMinus d₁ d₂).IsNull := by
  cases d₁ with
  | int _ =>
    cases d₂ with
    | int _ => intro hn; cases hn
    | null  => exact (h₂ trivial).elim
    | err _ => intro hn; cases hn
  | null  => exact (h₁ trivial).elim
  | err _ => intro hn; cases hn

theorem evalTimes_not_null {d₁ d₂ : Datum .int}
    (h₁ : ¬d₁.IsNull) (h₂ : ¬d₂.IsNull) :
    ¬(evalTimes d₁ d₂).IsNull := by
  cases d₁ with
  | int _ =>
    cases d₂ with
    | int _ => intro hn; cases hn
    | null  => exact (h₂ trivial).elim
    | err _ => intro hn; cases hn
  | null  => exact (h₁ trivial).elim
  | err _ => intro hn; cases hn

theorem evalDivide_not_null {d₁ d₂ : Datum .int}
    (h₁ : ¬d₁.IsNull) (h₂ : ¬d₂.IsNull) :
    ¬(evalDivide d₁ d₂).IsNull := by
  cases d₁ with
  | int n =>
    cases d₂ with
    | int m =>
      intro hn
      simp only [evalDivide] at hn
      split at hn <;> cases hn
    | null  => exact (h₂ trivial).elim
    | err _ => intro hn; cases hn
  | null  => exact (h₁ trivial).elim
  | err _ => intro hn; cases hn

theorem evalEq_not_null {k : ColType} {d₁ d₂ : Datum k}
    (h₁ : ¬d₁.IsNull) (h₂ : ¬d₂.IsNull) :
    ¬(evalEq d₁ d₂).IsNull := by
  cases d₁ with
  | bool _ =>
    cases d₂ with
    | bool _ => intro hn; cases hn
    | null   => exact (h₂ trivial).elim
    | err _  => intro hn; cases hn
  | int _ =>
    cases d₂ with
    | int _ => intro hn; cases hn
    | null  => exact (h₂ trivial).elim
    | err _ => intro hn; cases hn
  | null  => exact (h₁ trivial).elim
  | err _ => intro hn; cases hn

theorem evalLt_not_null {k : ColType} {d₁ d₂ : Datum k}
    (h₁ : ¬d₁.IsNull) (h₂ : ¬d₂.IsNull) :
    ¬(evalLt d₁ d₂).IsNull := by
  cases d₁ with
  | bool _ =>
    cases d₂ with
    | bool _ => intro hn; cases hn
    | null   => exact (h₂ trivial).elim
    | err _  => intro hn; cases hn
  | int _ =>
    cases d₂ with
    | int _ => intro hn; cases hn
    | null  => exact (h₂ trivial).elim
    | err _ => intro hn; cases hn
  | null  => exact (h₁ trivial).elim
  | err _ => intro hn; cases hn

theorem evalAnd_not_null {d₁ d₂ : Datum .bool}
    (h₁ : ¬d₁.IsNull) (h₂ : ¬d₂.IsNull) :
    ¬(evalAnd d₁ d₂).IsNull := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => cases b₁ <;> cases b₂ <;> (intro hn; cases hn)
    | null    => exact (h₂ trivial).elim
    | err _   => cases b₁ <;> (intro hn; cases hn)
  | null  => exact (h₁ trivial).elim
  | err _ =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> (intro hn; cases hn)
    | null    => exact (h₂ trivial).elim
    | err _   => intro hn; cases hn

theorem evalOr_not_null {d₁ d₂ : Datum .bool}
    (h₁ : ¬d₁.IsNull) (h₂ : ¬d₂.IsNull) :
    ¬(evalOr d₁ d₂).IsNull := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => cases b₁ <;> cases b₂ <;> (intro hn; cases hn)
    | null    => exact (h₂ trivial).elim
    | err _   => cases b₁ <;> (intro hn; cases hn)
  | null  => exact (h₁ trivial).elim
  | err _ =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> (intro hn; cases hn)
    | null    => exact (h₂ trivial).elim
    | err _   => intro hn; cases hn

/-- Variadic AND: not-null when every operand is not-null. Seed
`.bool true` is not null; each fold step preserves it via
`evalAnd_not_null`. -/
theorem evalAndN_not_null_of_all_safe :
    ∀ (ds : List (Datum .bool)), (∀ d ∈ ds, ¬d.IsNull) →
      ¬(evalAndN ds).IsNull
  | [], _ => fun h => by cases h
  | hd :: tl, hall => by
    have hhd : ¬hd.IsNull := hall hd (List.Mem.head _)
    have htl : ∀ d ∈ tl, ¬d.IsNull :=
      fun d hd_mem => hall d (List.Mem.tail _ hd_mem)
    have h_tl_safe : ¬(evalAndN tl).IsNull := evalAndN_not_null_of_all_safe tl htl
    show ¬(evalAnd hd (evalAndN tl)).IsNull
    exact evalAnd_not_null hhd h_tl_safe

/-- Variadic OR: not-null when every operand is not-null. -/
theorem evalOrN_not_null_of_all_safe :
    ∀ (ds : List (Datum .bool)), (∀ d ∈ ds, ¬d.IsNull) →
      ¬(evalOrN ds).IsNull
  | [], _ => fun h => by cases h
  | hd :: tl, hall => by
    have hhd : ¬hd.IsNull := hall hd (List.Mem.head _)
    have htl : ∀ d ∈ tl, ¬d.IsNull :=
      fun d hd_mem => hall d (List.Mem.tail _ hd_mem)
    have h_tl_safe : ¬(evalOrN tl).IsNull := evalOrN_not_null_of_all_safe tl htl
    show ¬(evalOr hd (evalOrN tl)).IsNull
    exact evalOr_not_null hhd h_tl_safe

/-- The leftmost concrete operand is never null. -/
theorem Coalesce.firstConcrete_not_null {k : ColType} :
    ∀ (ds : List (Datum k)) (d : Datum k),
      Coalesce.firstConcrete ds = some d → ¬d.IsNull
  | [], _, h => by simp [Coalesce.firstConcrete] at h
  | .bool _ :: _, _, h => by
    simp only [Coalesce.firstConcrete, Option.some.injEq] at h
    subst h; intro hN; cases hN
  | .int _ :: _, _, h => by
    simp only [Coalesce.firstConcrete, Option.some.injEq] at h
    subst h; intro hN; cases hN
  | .null :: rest, d, h => by
    simp only [Coalesce.firstConcrete] at h
    exact Coalesce.firstConcrete_not_null rest d h
  | .err _ :: rest, d, h => by
    simp only [Coalesce.firstConcrete] at h
    exact Coalesce.firstConcrete_not_null rest d h

/-- `coalesce` is not null when some operand is concrete (neither
null nor err): that operand forces `firstConcrete` to a `some`,
and the hit is itself concrete (`firstConcrete_not_null`). -/
theorem evalCoalesce_not_null_of_some_concrete {k : ColType}
    {ds : List (Datum k)} (h : ∃ d ∈ ds, ¬d.IsNull ∧ ¬d.IsErr) :
    ¬(evalCoalesce ds).IsNull := by
  show ¬(match Coalesce.firstConcrete ds with
         | some d => d
         | none => Coalesce.residue ds).IsNull
  cases hfc : Coalesce.firstConcrete ds with
  | some d => exact Coalesce.firstConcrete_not_null ds d hfc
  | none =>
    exfalso
    obtain ⟨d, hmem, hnn, hne⟩ := h
    have hcase := Coalesce.firstConcrete_none_no_concrete ds hfc d hmem
    cases hcase with
    | inl hnull =>
      apply hnn
      cases d with
      | bool _ => simp [Datum.isNullB] at hnull
      | int _  => simp [Datum.isNullB] at hnull
      | null   => trivial
      | err _  => simp [Datum.isNullB] at hnull
    | inr herr => exact hne herr

/-! ## Soundness

`eval_satisfies_outputCols`: evaluating `e` on a `RowSatisfies`
env produces a `Datum` satisfying `Expr.outputCols sch e`.
Structural recursion over `Expr`, mutual with the `ExprList`
companions `args_not_null` (AND / OR) and `args_concrete`
(coalesce) that discharge the variadic nullable obligations. The
err-side reasoning consumes the `evalX_not_err` lemmas from
`Mz/MightError.lean`; the null-side reasoning consumes the
`evalX_not_null` lemmas above. -/

/-- A binary-op rule packaging step: from per-input satisfaction
plus `not_null` / `not_err` for the primitive, conclude
satisfaction of the OR-of-inputs rule. -/
private theorem binOp_satisfies
    {ka kb kc : ColType}
    {f : Datum ka → Datum kb → Datum kc}
    {da : Datum ka} {db : Datum kb}
    {csa csb : ColSchema}
    (hsata : DatumSatisfies csa da) (hsatb : DatumSatisfies csb db)
    (hNotNull : ¬da.IsNull → ¬db.IsNull → ¬(f da db).IsNull)
    (hNotErr : ¬da.IsErr → ¬db.IsErr → ¬(f da db).IsErr) :
    DatumSatisfies
      { nullable := csa.nullable || csb.nullable
        errable := csa.errable || csb.errable }
      (f da db) := by
  refine ⟨?_, ?_⟩
  · intro hN
    simp only [Bool.or_eq_false_iff] at hN
    exact hNotNull (hsata.1 hN.1) (hsatb.1 hN.2)
  · intro hErr
    simp only [Bool.or_eq_false_iff] at hErr
    exact hNotErr (hsata.2 hErr.1) (hsatb.2 hErr.2)

mutual
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
      · intro hN; exact evalNot_not_null (iha.1 hN)
      · intro hErr; exact evalNot_not_err (iha.2 hErr)
    | _, .ifThen c t e => by
      have ihc := eval_satisfies_outputCols env hsat c
      have iht := eval_satisfies_outputCols env hsat t
      have ihe := eval_satisfies_outputCols env hsat e
      simp only [eval, Expr.outputCols]
      refine ⟨?_, ?_⟩
      · intro hN
        simp only [Bool.or_eq_false_iff] at hN
        obtain ⟨⟨hc, ht⟩, he⟩ := hN
        cases hev : eval env c with
        | bool b => cases b
                    · exact ihe.1 he
                    · exact iht.1 ht
        | null => exact absurd (by rw [hev]; trivial) (ihc.1 hc)
        | err _ => intro hRes; cases hRes
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
      exact binOp_satisfies iha ihb
        (fun ha hb => evalPlus_not_null ha hb)
        (fun ha hb => evalPlus_not_err ha hb)
    | _, .minus a b => by
      have iha := eval_satisfies_outputCols env hsat a
      have ihb := eval_satisfies_outputCols env hsat b
      simp only [eval, Expr.outputCols]
      exact binOp_satisfies iha ihb
        (fun ha hb => evalMinus_not_null ha hb)
        (fun ha hb => evalMinus_not_err ha hb)
    | _, .times a b => by
      have iha := eval_satisfies_outputCols env hsat a
      have ihb := eval_satisfies_outputCols env hsat b
      simp only [eval, Expr.outputCols]
      exact binOp_satisfies iha ihb
        (fun ha hb => evalTimes_not_null ha hb)
        (fun ha hb => evalTimes_not_err ha hb)
    | _, .divide a b => by
      have iha := eval_satisfies_outputCols env hsat a
      have ihb := eval_satisfies_outputCols env hsat b
      simp only [eval, Expr.outputCols]
      refine ⟨?_, ?_⟩
      · intro hN
        simp only [Bool.or_eq_false_iff] at hN
        exact evalDivide_not_null (iha.1 hN.1) (ihb.1 hN.2)
      · intro hErr; cases hErr
    | _, .eq a b => by
      have iha := eval_satisfies_outputCols env hsat a
      have ihb := eval_satisfies_outputCols env hsat b
      simp only [eval, Expr.outputCols]
      exact binOp_satisfies iha ihb
        (fun ha hb => evalEq_not_null ha hb)
        (fun ha hb => evalEq_not_err ha hb)
    | _, .lt a b => by
      have iha := eval_satisfies_outputCols env hsat a
      have ihb := eval_satisfies_outputCols env hsat b
      simp only [eval, Expr.outputCols]
      exact binOp_satisfies iha ihb
        (fun ha hb => evalLt_not_null ha hb)
        (fun ha hb => evalLt_not_err ha hb)
    | _, .andN args => by
      simp only [Expr.outputCols]
      rw [eval_andN, evalAndN_lazy_eq_eager]
      refine ⟨?_, ?_⟩
      · intro hN
        exact evalAndN_not_null_of_all_safe _ (args_not_null env hsat args hN)
      · intro hErr; cases hErr
    | _, .orN args => by
      simp only [Expr.outputCols]
      rw [eval_orN, evalOrN_lazy_eq_eager]
      refine ⟨?_, ?_⟩
      · intro hN
        exact evalOrN_not_null_of_all_safe _ (args_not_null env hsat args hN)
      · intro hErr; cases hErr
    | _, .coalesce args => by
      simp only [Expr.outputCols]
      rw [eval_coalesce, evalCoalesce_lazy_eq_eager]
      refine ⟨?_, ?_⟩
      · intro hN
        exact evalCoalesce_not_null_of_some_concrete (args_concrete env hsat args hN)
      · intro hErr; cases hErr

  /-- Mutual: every element of `evalList env args` is not-null when
  `argsNullable args = false`. Feeds the `.andN` / `.orN` nullable
  obligation. -/
  theorem args_not_null {n : Nat} {sch : Schema n}
      (env : Env sch) (hsat : RowSatisfies sch env) :
      {k : ColType} → (args : ExprList sch k) →
        ExprList.argsNullable args = false →
        ∀ d ∈ evalList env args, ¬d.IsNull
    | _, .nil, _, d, hmem => by cases hmem
    | _, .cons a as, h, d, hmem => by
      simp only [ExprList.argsNullable, Bool.or_eq_false_iff] at h
      have iha := eval_satisfies_outputCols env hsat a
      have ih_tail := args_not_null env hsat as h.2
      cases hmem with
      | head _ => exact iha.1 h.1
      | tail _ htl => exact ih_tail d htl

  /-- Mutual: when `coalesceNullable args = false`, some element of
  `evalList env args` is concrete (neither null nor err). Feeds the
  `.coalesce` nullable obligation. -/
  theorem args_concrete {n : Nat} {sch : Schema n}
      (env : Env sch) (hsat : RowSatisfies sch env) :
      {k : ColType} → (args : ExprList sch k) →
        ExprList.coalesceNullable args = false →
        ∃ d ∈ evalList env args, ¬d.IsNull ∧ ¬d.IsErr
    | _, .nil, h => by simp [ExprList.coalesceNullable] at h
    | _, .cons a as, h => by
      simp only [ExprList.coalesceNullable, Bool.and_eq_false_iff] at h
      cases h with
      | inl hhead =>
        simp only [Bool.or_eq_false_iff] at hhead
        have iha := eval_satisfies_outputCols env hsat a
        exact ⟨eval env a, List.Mem.head _, iha.1 hhead.1, iha.2 hhead.2⟩
      | inr htail =>
        obtain ⟨d, hmem, hd⟩ := args_concrete env hsat as htail
        exact ⟨d, List.Mem.tail _ hmem, hd⟩
end

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
