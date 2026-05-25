import Mz.Eval

/-!
# Two-diff stream

Per the design doc
(`doc/developer/design/20260517_error_handling_semantics.md`,
§"Stream shape: two-diff records"), the unit of denotation is a
stream of records

```
(row : List Datum, diff : Int, err_diff : Int)
```

`diff` counts valid copies of `row`; `err_diff` counts erred copies.
Both are ordinary `Int` diffs that retract.

This file is the skeleton's restart after the per-error-payload
diff-component experiment was concluded. The previous attempt
(`Diff = Int × ErrCount`) is preserved in git history on this
branch as a worked alternative.

The skeleton currently models:

* Stream record + `Stream` type alias.
* `filter` per the design doc's Predicates rule.
* `project` evaluating each scalar; `Datum::Error` results land in
  cells.
* `cross` with the two-component multiplicative rule from the
  Joins section.
* `negate`, `unionAll`, basic reduction lemmas.

Global-scoped errors (absorbing `DiffWithGlobal` marker) and
`DataflowError` structured payloads are deferred to follow-up
files. The current focus is the row-scoped layer: data multiplicity
and err multiplicity, both retractable. -/

namespace Mz

/-- A stream record: a row, its data multiplicity, and its err
multiplicity. Both diffs are ordinary `Int`. A row appearing
`(diff, err_diff) = (1, 0)` is a valid output; `(0, 1)` is an erred
output; `(1, 1)` is both (rare but representable). Retractions are
negative multiplicities. -/
structure StreamRecord where
  row : Row
  diff : Int
  err_diff : Int
  deriving Inhabited

/-- A stream is a list of stream records. Ordinary differential-
dataflow consolidation sums per `(row, time)` bucket; the time
dimension is not modeled here yet. -/
abbrev Stream := List StreamRecord

namespace Stream

/-! ## Filter

Per the design doc:

* `eval r pred = .bool true`   → record unchanged.
* `eval r pred = .bool false`  → data multiplicity zeroed, err untouched.
* `eval r pred = .null`        → data multiplicity zeroed, err untouched.
* `eval r pred = .int n`       → data multiplicity zeroed, err untouched.
* `eval r pred = .err e`       → data multiplicity migrates to err side;
                                  `e` is the structured payload to be
                                  carried via `DataflowError::EvalError`
                                  (deferred). -/

/-- Per-record filter action. -/
@[inline] def filterOne (pred : Expr) (rec : StreamRecord) : StreamRecord :=
  match eval rec.row pred with
  | .bool true =>
    rec
  | .err _ =>
    -- Data multiplicity migrates to err side; structured EvalError
    -- payload is intentionally not yet modeled — see deferred work.
    { row := rec.row, diff := 0, err_diff := rec.err_diff + rec.diff }
  | _ =>
    -- .bool false / .null / .int — data drops, err untouched.
    { row := rec.row, diff := 0, err_diff := rec.err_diff }

/-- Filter a stream by a predicate. -/
def filter (pred : Expr) (s : Stream) : Stream :=
  s.map (filterOne pred)

theorem filter_nil (pred : Expr) :
    filter pred [] = [] := rfl

theorem filter_append (pred : Expr) (a b : Stream) :
    filter pred (a ++ b) = filter pred a ++ filter pred b := by
  unfold filter
  exact List.map_append

/-! ## Project

Evaluate each scalar in `es` against the row. The resulting row
is `es.map (eval r)`. A scalar that errs produces `Datum.err _` in
that column — the err lives in the cell, not in the diff. Diffs
pass through unchanged. -/

/-- Per-record project action. -/
@[inline] def projectOne (es : List Expr) (rec : StreamRecord) : StreamRecord :=
  { row := es.map (eval rec.row)
  , diff := rec.diff
  , err_diff := rec.err_diff }

/-- Project a stream through a list of scalar expressions. -/
def project (es : List Expr) (s : Stream) : Stream :=
  s.map (projectOne es)

theorem project_nil_stream (es : List Expr) :
    project es [] = [] := rfl

theorem project_append (es : List Expr) (a b : Stream) :
    project es (a ++ b) = project es a ++ project es b := by
  unfold project
  exact List.map_append

/-! ## Negate

Pointwise negation of both diff components. -/

def negate (s : Stream) : Stream :=
  s.map fun rec => { row := rec.row, diff := -rec.diff, err_diff := -rec.err_diff }

theorem negate_nil : negate [] = [] := rfl

theorem negate_append (a b : Stream) :
    negate (a ++ b) = negate a ++ negate b := by
  unfold negate
  exact List.map_append

theorem negate_negate (s : Stream) :
    negate (negate s) = s := by
  induction s with
  | nil => rfl
  | cons hd tl ih =>
    show negate (negate (hd :: tl)) = hd :: tl
    show { row := hd.row, diff := - -hd.diff, err_diff := - -hd.err_diff }
            :: negate (negate tl) = hd :: tl
    rw [ih]
    have h1 : - -hd.diff = hd.diff := Int.neg_neg _
    have h2 : - -hd.err_diff = hd.err_diff := Int.neg_neg _
    rw [h1, h2]

/-! ## UnionAll

List concatenation. Multiplicities of duplicate rows add via
downstream consolidation. -/

def unionAll (a b : Stream) : Stream := a ++ b

theorem unionAll_nil_left (s : Stream) : unionAll [] s = s := rfl

theorem unionAll_nil_right (s : Stream) : unionAll s [] = s :=
  List.append_nil s

theorem unionAll_assoc (a b c : Stream) :
    unionAll (unionAll a b) c = unionAll a (unionAll b c) :=
  List.append_assoc a b c

/-! ## Cross product

Multiplicative combine: `(rL, dL, eL) × (rR, dR, eR) =
(rL ++ rR, dL·dR, dL·eR + eL·dR + eL·eR)`. The err-side combinator
captures valid×err, err×valid, and err×err contributions. -/

@[inline] def crossOne (recL recR : StreamRecord) : StreamRecord :=
  { row := recL.row ++ recR.row
  , diff := recL.diff * recR.diff
  , err_diff := recL.diff * recR.err_diff
              + recL.err_diff * recR.diff
              + recL.err_diff * recR.err_diff }

def cross (l r : Stream) : Stream :=
  l.flatMap fun recL => r.map (crossOne recL)

theorem cross_nil_left (r : Stream) : cross [] r = [] := rfl

theorem cross_nil_right (l : Stream) : cross l [] = [] := by
  induction l with
  | nil => rfl
  | cons _ tl ih =>
    show List.flatMap _ (_ :: tl) = []
    rw [List.flatMap_cons]
    show List.map _ [] ++ List.flatMap _ tl = []
    rw [List.map_nil, List.nil_append]
    exact ih

theorem cross_append_left (a b r : Stream) :
    cross (a ++ b) r = cross a r ++ cross b r := by
  show (a ++ b).flatMap _ = a.flatMap _ ++ b.flatMap _
  exact List.flatMap_append

end Stream

end Mz
