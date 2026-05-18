import Mz.Eval
import Mz.Bag

/-!
# Data / error stream pair

The bag semantics in `Mz/Bag.lean` silently drops rows whose
predicate evaluates to `.err`. The real Materialize dataflow runs a
data stream alongside an error stream: an erroring row is removed
from the data collection and emitted into the error collection
instead, where downstream operators forward it unchanged. This file
makes that structure explicit and proves the basic laws of the
error-aware filter.

`BagStream` is a pair `(data, errors)`. Operators consume a
`BagStream` and produce a `BagStream`. Existing errors from the
input propagate to the output; new errors from the operator are
appended.

The skeleton models only `filter`. Adding `project` follows the same
pattern: each expression in the projection list contributes its own
error rows.
-/

namespace Mz

/-- A dataflow stream: row collection plus accompanying error
collection. Operators below take a `BagStream` and return a
`BagStream`. -/
structure BagStream where
  data   : Relation
  errors : List EvalError

/-- Inject a plain `Relation` into a `BagStream` with no accumulated
errors. The natural starting point for a source. -/
def BagStream.ofRelation (rel : Relation) : BagStream :=
  { data := rel, errors := [] }

/-- Collect every `err` payload produced by evaluating `pred` on the
rows of `rel`. Order matches `rel`. -/
def errorRows (pred : Expr) (rel : Relation) : List EvalError :=
  rel.foldr (fun row acc =>
    match eval row pred with
    | .err e => e :: acc
    | _      => acc) []

/-- Error-aware filter. Rows whose predicate evaluates to `.bool
true` stay in the data collection; rows whose predicate evaluates
to `.err` contribute their payload to the error collection;
everything else is dropped. -/
def BagStream.filter (pred : Expr) (s : BagStream) : BagStream :=
  { data   := filterRel pred s.data
  , errors := s.errors ++ errorRows pred s.data }

/-! ## Per-field reduction lemmas -/

theorem BagStream.filter_data (pred : Expr) (s : BagStream) :
    (BagStream.filter pred s).data = filterRel pred s.data := rfl

theorem BagStream.filter_errors (pred : Expr) (s : BagStream) :
    (BagStream.filter pred s).errors = s.errors ++ errorRows pred s.data := rfl

/-! ## Helper lemmas -/

/-- Every row that survives `filterRel pred rel` evaluates the
predicate to `.bool true`. This is what the filter "kept" means
unfolded against `rowPredicate`. -/
theorem rows_in_filterRel_eval_to_true (pred : Expr) (rel : Relation) :
    ∀ row ∈ filterRel pred rel, eval row pred = .bool true := by
  intro row h_mem
  unfold filterRel at h_mem
  have h_pred : rowPredicate pred row = true := (List.mem_filter.mp h_mem).2
  unfold rowPredicate at h_pred
  cases h_eval : eval row pred with
  | bool b => cases b
              · rw [h_eval] at h_pred; cases h_pred
              · rfl
  | null   => rw [h_eval] at h_pred; cases h_pred
  | err _  => rw [h_eval] at h_pred; cases h_pred

/-- If every row evaluates the predicate to `.bool true`, the error
collection is empty. -/
theorem errorRows_eq_nil_of_all_true (pred : Expr) (rel : Relation)
    (h : ∀ row ∈ rel, eval row pred = .bool true) :
    errorRows pred rel = [] := by
  induction rel with
  | nil => rfl
  | cons hd tl ih =>
    have hd_eval : eval hd pred = .bool true := h hd (List.Mem.head tl)
    have htl : ∀ row ∈ tl, eval row pred = .bool true :=
      fun row h_mem => h row (List.Mem.tail hd h_mem)
    show (match eval hd pred with
          | .err e => e :: errorRows pred tl
          | _      => errorRows pred tl) = []
    rw [hd_eval]
    exact ih htl

/-- `errorRows` of a filtered relation is empty: the survivors all
evaluated to `.bool true`, none produced an `err`. -/
theorem errorRows_filterRel (pred : Expr) (rel : Relation) :
    errorRows pred (filterRel pred rel) = [] :=
  errorRows_eq_nil_of_all_true pred _
    (rows_in_filterRel_eval_to_true pred rel)

/-! ## Stream laws -/

/-- Idempotence of `BagStream.filter`. Applying the same predicate
twice produces the same data *and* the same errors as applying it
once: the second pass observes only the survivors of the first,
which by construction evaluate to `.bool true` and thus contribute
nothing new to the error collection. -/
theorem BagStream.filter_idem (pred : Expr) (s : BagStream) :
    BagStream.filter pred (BagStream.filter pred s) =
      BagStream.filter pred s := by
  simp [BagStream.filter, filterRel_idem, errorRows_filterRel]

end Mz
