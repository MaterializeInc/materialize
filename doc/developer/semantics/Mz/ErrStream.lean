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

The skeleton models `filter` and `project`. Each follows the same
pattern: erroring rows leave the data collection and contribute
their payload(s) to the error collection. `project` differs from
`filter` in that a single row can produce multiple errors — one
for every projected scalar that evaluates to `.err`.
-/

namespace Mz

/-- A dataflow stream: row collection plus accompanying error
collection. Operators below take a `BagStream` and return a
`BagStream`. -/
@[ext] structure BagStream where
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

/-! ## Filter commutativity

The data side of `BagStream.filter` commutes unconditionally via
`filterRel_comm`. The error side does not commute as a list
equality — the two orderings collect errors in different
positions and, when both predicates error on the same row, only
one ordering records the second error.

Full structural commutativity therefore requires a no-error
precondition: when neither predicate errs on the input data,
both errorRows collections are empty, the appended errors reduce
to the input `errors` field, and the streams agree. -/

theorem BagStream.filter_comm_data (p q : Expr) (s : BagStream) :
    (BagStream.filter p (BagStream.filter q s)).data
      = (BagStream.filter q (BagStream.filter p s)).data := by
  simp [BagStream.filter, filterRel_comm]

/-- `errorRows` of a relation on which the predicate never errors
is empty. -/
theorem errorRows_eq_nil_of_no_err
    (pred : Expr) (rel : Relation)
    (h : ∀ row ∈ rel, ¬(eval row pred).IsErr) :
    errorRows pred rel = [] := by
  induction rel with
  | nil => rfl
  | cons hd tl ih =>
    have hHd : ¬(eval hd pred).IsErr := h hd List.mem_cons_self
    have hTl : ∀ row ∈ tl, ¬(eval row pred).IsErr :=
      fun row hMem => h row (List.mem_cons_of_mem _ hMem)
    show (match eval hd pred with
          | .err e => e :: errorRows pred tl
          | _      => errorRows pred tl) = []
    cases h_eval : eval hd pred with
    | bool _ => exact ih hTl
    | null   => exact ih hTl
    | err _  =>
      rw [h_eval] at hHd
      exact absurd (show True by trivial) hHd

/-- Full commutativity of `BagStream.filter` under a no-error
precondition: when neither predicate errs on any row of the input
data, the two filter orderings produce the same stream. -/
theorem BagStream.filter_comm_no_err
    (p q : Expr) (s : BagStream)
    (hP : ∀ row ∈ s.data, ¬(eval row p).IsErr)
    (hQ : ∀ row ∈ s.data, ¬(eval row q).IsErr) :
    BagStream.filter p (BagStream.filter q s)
      = BagStream.filter q (BagStream.filter p s) := by
  apply BagStream.ext
  · exact BagStream.filter_comm_data p q s
  · show s.errors ++ errorRows q s.data ++ errorRows p (filterRel q s.data)
        = s.errors ++ errorRows p s.data ++ errorRows q (filterRel p s.data)
    have hQEmpty : errorRows q s.data = [] :=
      errorRows_eq_nil_of_no_err q s.data hQ
    have hPEmpty : errorRows p s.data = [] :=
      errorRows_eq_nil_of_no_err p s.data hP
    -- A row surviving `filterRel q s.data` is a row in `s.data` with q true,
    -- so the no-err precondition still applies; `errorRows p (filterRel q ...)`
    -- is therefore also empty.
    have hPOnQ : ∀ row ∈ filterRel q s.data, ¬(eval row p).IsErr := by
      intro row hMem
      unfold filterRel at hMem
      exact hP row (List.mem_filter.mp hMem).1
    have hQOnP : ∀ row ∈ filterRel p s.data, ¬(eval row q).IsErr := by
      intro row hMem
      unfold filterRel at hMem
      exact hQ row (List.mem_filter.mp hMem).1
    have hPFiltered : errorRows p (filterRel q s.data) = [] :=
      errorRows_eq_nil_of_no_err p (filterRel q s.data) hPOnQ
    have hQFiltered : errorRows q (filterRel p s.data) = [] :=
      errorRows_eq_nil_of_no_err q (filterRel p s.data) hQOnP
    rw [hQEmpty, hPEmpty, hPFiltered, hQFiltered]

/-! ## Project -/

/-- Boolean check: every projected scalar succeeds on this row
(none returns `.err`). Used to decide whether the row stays in the
data collection or is replaced by its error rows. -/
@[inline] def rowAllSafe (es : List Expr) (row : Row) : Bool :=
  es.all (fun e =>
    match eval row e with
    | .err _ => false
    | _      => true)

/-- Collect every `err` payload produced by evaluating each
expression in `es` against `row`. A single row can produce
zero, one, or many entries — one per erroring scalar. -/
def rowErrs (es : List Expr) (row : Row) : List EvalError :=
  es.filterMap fun e =>
    match eval row e with
    | .err err => some err
    | _        => none

/-- Aggregate `rowErrs` across the relation. Outer order matches
row order; inner order matches the expression order within each
row. -/
def projectErrs (es : List Expr) (rel : Relation) : List EvalError :=
  rel.flatMap (rowErrs es)

/-- Error-aware projection. A row stays in the data collection only
when every projected scalar succeeds on it; otherwise the row's err
payloads are appended to the error collection and the row is
dropped from the data side. -/
def BagStream.project (es : List Expr) (s : BagStream) : BagStream :=
  { data   := (s.data.filter (rowAllSafe es)).map (fun row => es.map (eval row))
  , errors := s.errors ++ projectErrs es s.data }

/-! ### Per-field reduction lemmas -/

theorem BagStream.project_data (es : List Expr) (s : BagStream) :
    (BagStream.project es s).data =
      (s.data.filter (rowAllSafe es)).map (fun row => es.map (eval row)) := rfl

theorem BagStream.project_errors (es : List Expr) (s : BagStream) :
    (BagStream.project es s).errors =
      s.errors ++ projectErrs es s.data := rfl

/-! ### Trivial cases -/

theorem rowErrs_nil_es (row : Row) :
    rowErrs [] row = [] := rfl

theorem projectErrs_nil_rel (es : List Expr) :
    projectErrs es [] = [] := rfl

/-- An empty projection list keeps every row (no scalar can err)
and produces width-zero rows. -/
theorem BagStream.project_nil_es (s : BagStream) :
    BagStream.project [] s = { data := s.data.map (fun _ => []), errors := s.errors } := by
  apply BagStream.ext
  · show (s.data.filter (rowAllSafe [])).map (fun row => ([] : List Expr).map (eval row))
        = s.data.map (fun _ => [])
    have hAll : ∀ row, rowAllSafe [] row = true := fun _ => rfl
    rw [List.filter_eq_self.mpr (by intro row _; exact hAll row)]
    rfl
  · show s.errors ++ projectErrs [] s.data = s.errors
    have : projectErrs [] s.data = [] := by
      unfold projectErrs
      induction s.data with
      | nil => rfl
      | cons _ tl ih => simp [List.flatMap_cons, rowErrs_nil_es, ih]
    rw [this, List.append_nil]

/-- Projecting an empty stream is empty in data and preserves the
input errors. -/
theorem BagStream.project_empty_data (es : List Expr) (errs : List EvalError) :
    BagStream.project es { data := [], errors := errs }
      = { data := [], errors := errs } := by
  apply BagStream.ext
  · rfl
  · show errs ++ projectErrs es [] = errs
    rw [projectErrs_nil_rel, List.append_nil]

/-! ### Safe-row laws

When every projected scalar succeeds on every row, projection
behaves like the plain `Bag.project`: no errors are emitted and
no rows are dropped from the data collection. -/

theorem rowErrs_nil_of_all_safe (es : List Expr) (row : Row)
    (h : rowAllSafe es row = true) :
    rowErrs es row = [] := by
  induction es with
  | nil => rfl
  | cons hd tl ih =>
    have hUnfold : rowAllSafe (hd :: tl) row = true := h
    unfold rowAllSafe at hUnfold
    rw [List.all_cons, Bool.and_eq_true] at hUnfold
    obtain ⟨hHead, hTl⟩ := hUnfold
    have hSafeTl : rowAllSafe tl row = true := hTl
    have ihResult : rowErrs tl row = [] := ih hSafeTl
    show ((hd :: tl).filterMap fun e =>
            match eval row e with | .err err => some err | _ => none) = []
    rw [List.filterMap_cons]
    cases h_eval : eval row hd with
    | bool _ => exact ihResult
    | null   => exact ihResult
    | err e  =>
      rw [h_eval] at hHead
      cases hHead

theorem projectErrs_eq_nil_of_all_safe
    (es : List Expr) (rel : Relation)
    (h : ∀ row ∈ rel, rowAllSafe es row = true) :
    projectErrs es rel = [] := by
  unfold projectErrs
  induction rel with
  | nil => rfl
  | cons hd tl ih =>
    have hHead : rowAllSafe es hd = true := h hd List.mem_cons_self
    have hTl : ∀ row ∈ tl, rowAllSafe es row = true :=
      fun row hMem => h row (List.mem_cons_of_mem _ hMem)
    simp [List.flatMap_cons, rowErrs_nil_of_all_safe es hd hHead, ih hTl]

end Mz
