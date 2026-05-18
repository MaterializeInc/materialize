import Mz.Eval
import Mz.Bag
import Mz.ErrStream
import Mz.UnifiedStream

/-!
# Joins on `UnifiedStream`

Two-input relational join modeled on the unified single-collection
stream. The cartesian product `cross l r` is the building block;
`join pred l r` filters the product through a join predicate.

Error propagation follows naturally from the carrier: every
`(lu, ru)` pair contributes one output, and that output is an
`err` whenever either side of the pair is an `err`. The
multiplicity matches the semiring intuition that
`error * diff = error`: an `err` record in `l` produces one `err`
in the output for every record in `r`, and vice versa.

`cross` makes no commitment to row schema beyond list
concatenation. Schema-aware joins (equi-joins on named columns)
would lift to this with a column-substitution layer.
-/

namespace Mz

/-- Cartesian product of two unified streams.

For each pair `(lu, ru)`, produce one output:
* both real rows ⇒ concatenated row;
* either side is `err` ⇒ that side's `err` payload (left wins on
  conflict, matching `evalAnd`'s first-error rule). -/
def UnifiedStream.cross (l r : UnifiedStream) : UnifiedStream :=
  l.flatMap fun lu =>
    r.map fun ru =>
      match lu, ru with
      | .row la, .row rb => .row (la ++ rb)
      | .err e, _        => .err e
      | _, .err e        => .err e

/-- Equi-join or theta-join: cross product filtered by a predicate.
The predicate evaluates against the concatenated row; existing
`UnifiedStream.filter` semantics apply (predicate `.err` routes the
row's error into the carrier). -/
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
output per `(l, r)` pair, regardless of which side carries an
error — every err in `l` or `r` contributes one err record per
element of the other side, matching the diff-semiring's
`error * d = error`. -/
theorem UnifiedStream.cross_length (l r : UnifiedStream) :
    (UnifiedStream.cross l r).length = l.length * r.length := by
  induction l with
  | nil => simp [UnifiedStream.cross]
  | cons hd tl ih =>
    show (UnifiedStream.cross (hd :: tl) r).length = (tl.length + 1) * r.length
    rw [Nat.succ_mul]
    show (((hd :: tl) : UnifiedStream).flatMap fun lu => r.map fun ru =>
            match lu, ru with
            | .row la, .row rb => UnifiedRow.row (la ++ rb)
            | .err e,  _       => UnifiedRow.err e
            | _,       .err e  => UnifiedRow.err e).length
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
                | UnifiedRow.row r =>
                  match eval r pred with
                  | .bool true => [UnifiedRow.row r]
                  | .err e     => [UnifiedRow.err e]
                  | _          => []
                | UnifiedRow.err e => [UnifiedRow.err e]).length ≤ 1 := by
      cases hd with
      | row r =>
        show (match eval r pred with
              | .bool true => [UnifiedRow.row r]
              | .err e     => [UnifiedRow.err e]
              | _          => []).length ≤ 1
        cases h_eval : eval r pred with
        | bool b => cases b <;> simp [List.length_cons, List.length_nil]
        | null   => simp [List.length_nil]
        | err _  => simp [List.length_cons]
      | err _ =>
        show ([UnifiedRow.err _] : UnifiedStream).length ≤ 1
        simp [List.length_cons]
    calc (match hd with
          | UnifiedRow.row r =>
            match eval r pred with
            | .bool true => [UnifiedRow.row r]
            | .err e     => [UnifiedRow.err e]
            | _          => []
          | UnifiedRow.err e => [UnifiedRow.err e]).length
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

end Mz
