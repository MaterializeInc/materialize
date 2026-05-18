import Mz.Eval
import Mz.Bag
import Mz.ErrStream
import Mz.DiffSemiring
import Mz.UnifiedStream

/-!
# Joins on `UnifiedStream`

Two-input relational join on the unified diff-aware stream. The
cartesian product `cross l r` is the building block; `join pred l r`
filters the product through a join predicate.

Error propagation is now twofold:
* row-scoped: every `(lu, ru)` pair contributes one output, and
  that output's carrier is an `err` whenever either side's carrier
  is an `err` (left wins on conflict, matching `evalAnd`'s
  first-error rule);
* collection-scoped: diffs multiply, so any `.error` diff on
  either side forces the product diff to `.error` via
  `DiffWithError.error_mul_{left,right}`.

`cross` makes no commitment to row schema beyond list
concatenation. Schema-aware joins (equi-joins on named columns)
would lift to this with a column-substitution layer.
-/

namespace Mz

/-- Combine two unified carriers, with left winning on err conflict. -/
@[inline] private def combineCarrier : UnifiedRow → UnifiedRow → UnifiedRow
  | .row la, .row rb => .row (la ++ rb)
  | .err e,  _       => .err e
  | _,       .err e  => .err e

/-- Cartesian product of two unified streams. For each pair
`((lu, ld), (ru, rd))`:
* combine carriers via `combineCarrier`;
* multiply diffs via `DiffWithError`'s `Mul` instance, so any
  `.error` diff absorbs the product. -/
def UnifiedStream.cross (l r : UnifiedStream) : UnifiedStream :=
  l.flatMap fun ld =>
    r.map fun rd => (combineCarrier ld.1 rd.1, ld.2 * rd.2)

/-- Equi-join or theta-join: cross product filtered by a predicate.
The predicate evaluates against the concatenated row; existing
`UnifiedStream.filter` semantics apply (predicate `.err` routes
the row's error into the carrier, diff is preserved). -/
def UnifiedStream.join (pred : Expr) (l r : UnifiedStream) : UnifiedStream :=
  (UnifiedStream.cross l r).filter pred

/-! ## Empty cases -/

theorem UnifiedStream.cross_nil_left (r : UnifiedStream) :
    UnifiedStream.cross [] r = [] := rfl

theorem UnifiedStream.cross_nil_right (l : UnifiedStream) :
    UnifiedStream.cross l [] = [] := by
  induction l with
  | nil => rfl
  | cons _ tl _ih => simp [UnifiedStream.cross, List.map_nil, List.flatMap_cons]

/-! ## Cardinality -/

/-- Cross product cardinality. `cross l r` produces exactly one
output record per `(l, r)` pair, regardless of which side carries
an error in its carrier or its diff. -/
theorem UnifiedStream.cross_length (l r : UnifiedStream) :
    (UnifiedStream.cross l r).length = l.length * r.length := by
  induction l with
  | nil => simp [UnifiedStream.cross]
  | cons hd tl ih =>
    show (UnifiedStream.cross (hd :: tl) r).length = (tl.length + 1) * r.length
    rw [Nat.succ_mul]
    show (((hd :: tl) : UnifiedStream).flatMap fun ld =>
            r.map fun rd => (combineCarrier ld.1 rd.1, ld.2 * rd.2)).length
        = tl.length * r.length + r.length
    rw [List.flatMap_cons, List.length_append, List.length_map]
    show r.length + (UnifiedStream.cross tl r).length = tl.length * r.length + r.length
    rw [ih]
    exact Nat.add_comm _ _

/-- Filter on `UnifiedStream` is non-expanding: every input record
produces zero or one output record, so the output length is at
most the input length. -/
theorem UnifiedStream.filter_length_le (pred : Expr) (us : UnifiedStream) :
    (UnifiedStream.filter pred us).length ≤ us.length := by
  unfold UnifiedStream.filter
  induction us with
  | nil => exact Nat.le.refl
  | cons hd tl ih =>
    rw [List.flatMap_cons, List.length_append, List.length_cons]
    have hHd : (match hd with
                | (_,                 DiffWithError.error) => [hd]
                | (UnifiedRow.err e,  d)                   => [(UnifiedRow.err e, d)]
                | (UnifiedRow.row r,  d)                   =>
                  match eval r pred with
                  | .bool true => [(UnifiedRow.row r, d)]
                  | .err e     => [(UnifiedRow.err e, d)]
                  | _          => []).length ≤ 1 := by
      obtain ⟨u, d⟩ := hd
      cases d with
      | error =>
        show ([(u, DiffWithError.error)] : UnifiedStream).length ≤ 1
        simp [List.length_cons]
      | val n =>
        cases u with
        | row r =>
          show (match eval r pred with
                | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
                | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                | _          => []).length ≤ 1
          cases h_eval : eval r pred with
          | bool b => cases b <;> simp [List.length_cons, List.length_nil]
          | null   => simp [List.length_nil]
          | err _  => simp [List.length_cons]
        | err _ =>
          show ([(UnifiedRow.err _, DiffWithError.val n)] : UnifiedStream).length ≤ 1
          simp [List.length_cons]
    calc (match hd with
          | (_,                 DiffWithError.error) => [hd]
          | (UnifiedRow.err e,  d)                   => [(UnifiedRow.err e, d)]
          | (UnifiedRow.row r,  d)                   =>
            match eval r pred with
            | .bool true => [(UnifiedRow.row r, d)]
            | .err e     => [(UnifiedRow.err e, d)]
            | _          => []).length
        + (tl.flatMap _).length
        ≤ 1 + tl.length := Nat.add_le_add hHd ih
      _ = tl.length + 1 := Nat.add_comm _ _

/-- Join length is bounded by cross length: the predicate filter
can only remove rows. -/
theorem UnifiedStream.join_length_le (pred : Expr) (l r : UnifiedStream) :
    (UnifiedStream.join pred l r).length ≤ l.length * r.length := by
  show (UnifiedStream.filter pred (UnifiedStream.cross l r)).length
      ≤ l.length * r.length
  rw [← UnifiedStream.cross_length l r]
  exact UnifiedStream.filter_length_le pred _

/-! ## Diff propagation -/

/-- A `.error` diff on a left-side record forces the diff of every
output record in `cross` to `.error`. The carrier follows the
ordinary `combineCarrier` rule. -/
theorem UnifiedStream.cross_diff_error_left
    (lc : UnifiedRow) (r : UnifiedStream) (rc : UnifiedRow) (rd : DiffWithError Int)
    (h_mem : (rc, rd) ∈ r) :
    ∃ uc, (uc, (DiffWithError.error : DiffWithError Int))
            ∈ UnifiedStream.cross [(lc, DiffWithError.error)] r := by
  refine ⟨combineCarrier lc rc, ?_⟩
  show (combineCarrier lc rc, DiffWithError.error)
      ∈ ([(lc, DiffWithError.error)].flatMap fun ld =>
           r.map fun rd' => (combineCarrier ld.1 rd'.1, ld.2 * rd'.2))
  simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
  refine List.mem_map.mpr ⟨(rc, rd), h_mem, ?_⟩
  show (combineCarrier lc rc, DiffWithError.error * rd) = (combineCarrier lc rc, DiffWithError.error)
  rw [DiffWithError.error_mul_left]

/-- Symmetric statement for a `.error` diff on the right side. -/
theorem UnifiedStream.cross_diff_error_right
    (l : UnifiedStream) (lc : UnifiedRow) (ld : DiffWithError Int) (rc : UnifiedRow)
    (h_mem : (lc, ld) ∈ l) :
    ∃ uc, (uc, (DiffWithError.error : DiffWithError Int))
            ∈ UnifiedStream.cross l [(rc, DiffWithError.error)] := by
  refine ⟨combineCarrier lc rc, ?_⟩
  show (combineCarrier lc rc, DiffWithError.error)
      ∈ (l.flatMap fun ld' =>
           [(rc, DiffWithError.error)].map fun rd' =>
             (combineCarrier ld'.1 rd'.1, ld'.2 * rd'.2))
  refine List.mem_flatMap.mpr ⟨(lc, ld), h_mem, ?_⟩
  show (combineCarrier lc rc, DiffWithError.error)
      ∈ [(combineCarrier lc rc, ld * DiffWithError.error)]
  rw [DiffWithError.error_mul_right]
  exact List.mem_singleton.mpr rfl

/-- Absorption under `filter`: a record carrying a `.error` diff
is preserved by `UnifiedStream.filter`, regardless of the
predicate. The absorbing diff marker cannot be filtered away. -/
theorem UnifiedStream.filter_preserves_error_diff
    (pred : Expr) (us : UnifiedStream) (uc : UnifiedRow)
    (h_mem : (uc, (DiffWithError.error : DiffWithError Int)) ∈ us) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.filter pred us := by
  induction us with
  | nil => exact absurd h_mem (List.not_mem_nil)
  | cons hd tl ih =>
    rcases List.mem_cons.mp h_mem with hEq | hTail
    · subst hEq
      show (uc, DiffWithError.error)
        ∈ UnifiedStream.filter pred ((uc, DiffWithError.error) :: tl)
      unfold UnifiedStream.filter
      rw [List.flatMap_cons]
      show (uc, DiffWithError.error)
        ∈ [(uc, DiffWithError.error)] ++ _
      exact List.mem_append.mpr (.inl (List.mem_singleton.mpr rfl))
    · unfold UnifiedStream.filter
      rw [List.flatMap_cons]
      exact List.mem_append.mpr (.inr (ih hTail))

/-! ## Associativity of `cross`

`cross` is associative modulo associativity of row concatenation
on the carrier and associativity of diff multiplication on the
diff component. The combinatorial structure (one output per
triple) is identical in both nestings; the only obligation is
that the two ways of folding three carriers / diffs together
agree. -/

/-- `combineCarrier` is associative modulo `List.append_assoc` on
the row case, and trivially so on the err cases (left-wins). -/
theorem combineCarrier_assoc (a b c : UnifiedRow) :
    combineCarrier (combineCarrier a b) c = combineCarrier a (combineCarrier b c) := by
  cases a with
  | row la =>
    cases b with
    | row lb =>
      cases c with
      | row lc => show UnifiedRow.row ((la ++ lb) ++ lc)
                     = UnifiedRow.row (la ++ (lb ++ lc))
                  rw [List.append_assoc]
      | err _ => rfl
    | err _ =>
      cases c with
      | row _ => rfl
      | err _ => rfl
  | err _ =>
    cases b with
    | row _ =>
      cases c with
      | row _ => rfl
      | err _ => rfl
    | err _ =>
      cases c with
      | row _ => rfl
      | err _ => rfl

/-- Per-record associativity of the cross-product building block.
The diff side uses `DiffWithError.mul_assoc` instantiated at
`Int`. -/
private theorem cross_step_assoc
    (ad bd cd : UnifiedRow × DiffWithError Int) :
    (combineCarrier (combineCarrier ad.1 bd.1) cd.1, (ad.2 * bd.2) * cd.2)
      = (combineCarrier ad.1 (combineCarrier bd.1 cd.1), ad.2 * (bd.2 * cd.2)) := by
  congr 1
  · exact combineCarrier_assoc ad.1 bd.1 cd.1
  · exact DiffWithError.mul_assoc Int.mul_assoc ad.2 bd.2 cd.2

/-- Local lemma: associativity of `flatMap`. Lean core has the
building blocks (`flatMap_cons`, `flatMap_append`) but not the
joint statement at this name. -/
private theorem List.flatMap_flatMap_local {α β γ : Type}
    (l : List α) (f : α → List β) (g : β → List γ) :
    (l.flatMap f).flatMap g = l.flatMap (fun a => (f a).flatMap g) := by
  induction l with
  | nil => rfl
  | cons hd tl ih =>
    show ((hd :: tl).flatMap f).flatMap g
        = (hd :: tl).flatMap (fun a => (f a).flatMap g)
    rw [List.flatMap_cons, List.flatMap_append, ih]
    rfl

/-- Local lemma: pushing a `map` inside a `flatMap`. -/
private theorem List.map_flatMap_local {α β γ : Type}
    (l : List α) (f : α → List β) (g : β → γ) :
    (l.flatMap f).map g = l.flatMap (fun a => (f a).map g) := by
  induction l with
  | nil => rfl
  | cons hd tl ih =>
    show ((hd :: tl).flatMap f).map g
        = (hd :: tl).flatMap (fun a => (f a).map g)
    rw [List.flatMap_cons, List.map_append, ih]
    rfl

/-- Local lemma: `flatMap` of a `map`. -/
private theorem List.flatMap_map_local {α β γ : Type}
    (l : List α) (f : α → β) (g : β → List γ) :
    (l.map f).flatMap g = l.flatMap (fun a => g (f a)) := by
  induction l with
  | nil => rfl
  | cons hd tl ih =>
    show ((hd :: tl).map f).flatMap g
        = (hd :: tl).flatMap (fun a => g (f a))
    rw [List.map_cons, List.flatMap_cons, List.flatMap_cons, ih]

/-- Local lemma: pointwise-equal bodies give equal `flatMap`s. -/
private theorem List.flatMap_congr_local {α β : Type}
    {l : List α} {f g : α → List β}
    (h : ∀ x ∈ l, f x = g x) :
    l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons hd tl ih =>
    rw [List.flatMap_cons, List.flatMap_cons,
        h hd List.mem_cons_self,
        ih (fun x hMem => h x (List.mem_cons_of_mem _ hMem))]

/-- Local lemma: pointwise-equal bodies give equal `map`s. -/
private theorem List.map_congr_local {α β : Type}
    {l : List α} {f g : α → β}
    (h : ∀ x ∈ l, f x = g x) :
    l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons hd tl ih =>
    rw [List.map_cons, List.map_cons,
        h hd List.mem_cons_self,
        ih (fun x hMem => h x (List.mem_cons_of_mem _ hMem))]

/-- `cross` preserves `.val` diffs: when every input record on
both sides has a `.val` diff, every output record has a `.val`
diff. Diff multiplication of `.val * .val = .val (· * ·)`
stays in the ordinary `Int` slice of the diff-semiring. -/
theorem UnifiedStream.cross_no_error
    (l r : UnifiedStream)
    (hL : ∀ ld ∈ l, ∃ n : Int, ld.2 = DiffWithError.val n)
    (hR : ∀ rd ∈ r, ∃ m : Int, rd.2 = DiffWithError.val m) :
    ∀ od ∈ UnifiedStream.cross l r, ∃ k : Int, od.2 = DiffWithError.val k := by
  intro od hMem
  show ∃ k : Int, od.2 = DiffWithError.val k
  obtain ⟨ld, hLMem, hMid⟩ := List.mem_flatMap.mp hMem
  obtain ⟨rd, hRMem, hEq⟩ := List.mem_map.mp hMid
  obtain ⟨n, hN⟩ := hL ld hLMem
  obtain ⟨m, hM⟩ := hR rd hRMem
  refine ⟨n * m, ?_⟩
  rw [← hEq]
  show ld.2 * rd.2 = DiffWithError.val (n * m)
  rw [hN, hM]
  rfl

/-- `filter` preserves `.val` diffs: survivors and rerouted err
rows inherit the input diff unchanged. The diff is never modified
by the filter — only the carrier changes when the predicate
returns `.err`. -/
theorem UnifiedStream.filter_no_error
    (pred : Expr) (us : UnifiedStream)
    (h : ∀ ud ∈ us, ∃ n : Int, ud.2 = DiffWithError.val n) :
    ∀ od ∈ UnifiedStream.filter pred us, ∃ n : Int, od.2 = DiffWithError.val n := by
  intro od hMem
  unfold UnifiedStream.filter at hMem
  obtain ⟨ud, hUMem, hMid⟩ := List.mem_flatMap.mp hMem
  obtain ⟨n, hN⟩ := h ud hUMem
  -- ud = (uc, .val n); the produced records all carry diff `.val n` (or the
  -- input record itself when the `.error` short-circuit fires — but that
  -- case is vacuous here since `ud.2 = .val n` by hypothesis).
  refine ⟨n, ?_⟩
  obtain ⟨uc, d⟩ := ud
  simp only at hN
  subst hN
  -- now ud = (uc, .val n); filter produces records whose snd is .val n
  cases uc with
  | row r =>
    show od.2 = DiffWithError.val n
    cases h_eval : eval r pred with
    | bool b =>
      cases b with
      | true =>
        have hOd : od = (UnifiedRow.row r, DiffWithError.val n) := by
          have := hMid
          simp [h_eval] at this
          exact this
        rw [hOd]
      | false =>
        exfalso
        have := hMid
        simp [h_eval] at this
    | null =>
      exfalso
      have := hMid
      simp [h_eval] at this
    | err e =>
      have hOd : od = (UnifiedRow.err e, DiffWithError.val n) := by
        have := hMid
        simp [h_eval] at this
        exact this
      rw [hOd]
  | err e =>
    show od.2 = DiffWithError.val n
    have hOd : od = (UnifiedRow.err e, DiffWithError.val n) := by
      have := hMid
      simp at this
      exact this
    rw [hOd]

/-- Cross is associative on the unified stream. The proof rewrites
both sides into a common triple-fold via the list-monad equations
and closes the leaves with `cross_step_assoc`. -/
theorem UnifiedStream.cross_assoc (a b c : UnifiedStream) :
    UnifiedStream.cross (UnifiedStream.cross a b) c
      = UnifiedStream.cross a (UnifiedStream.cross b c) := by
  show (a.flatMap fun ad => b.map fun bd =>
          (combineCarrier ad.1 bd.1, ad.2 * bd.2)).flatMap
         (fun abd => c.map fun cd =>
            (combineCarrier abd.1 cd.1, abd.2 * cd.2))
      = a.flatMap fun ad =>
          (b.flatMap fun bd => c.map fun cd =>
             (combineCarrier bd.1 cd.1, bd.2 * cd.2)).map
            (fun bcd => (combineCarrier ad.1 bcd.1, ad.2 * bcd.2))
  rw [List.flatMap_flatMap_local]
  apply List.flatMap_congr_local
  intro ad _
  rw [List.flatMap_map_local, List.map_flatMap_local]
  apply List.flatMap_congr_local
  intro bd _
  rw [List.map_map]
  apply List.map_congr_local
  intro cd _
  exact cross_step_assoc ad bd cd

end Mz
