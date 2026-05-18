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

end Mz
