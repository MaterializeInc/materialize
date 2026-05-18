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

end Mz
