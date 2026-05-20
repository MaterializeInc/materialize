import Mz.UnifiedStream2

/-!
# Row-keyed consolidation on `UnifiedStream2`

The new diff encoding from `Mz/UnifiedStream2.lean` carries row-scoped
errors in `Diff.errs` and collection-scoped errors in
`DiffWithGlobal.global`. Bucketing for that encoding is structurally
identical to `Mz/UnifiedConsolidate.lean` — same per-key fold, same
encounter-order placement — but the carrier is just `Row` and the diff
is `DiffWithGlobal`.

The headline laws (absorption of `global`, cardinality, carrier
uniqueness, retraction of row-scoped errs) are left for follow-up
modules; this file ships only the definition and the per-shape
reduction lemmas downstream proofs need to cite. -/

namespace Mz

namespace UnifiedStream2

/-- Insert `(r, d)` into a consolidated stream. If a record with the
same `Row` carrier already exists, add `d` to its diff via
`DiffWithGlobal`'s `+`. Otherwise append a new record at the end of
the list. Exposed (not `private`) so downstream files can state laws
about it. -/
def consolidateInto (r : Row) (d : DiffWithGlobal) :
    UnifiedStream2 → UnifiedStream2
  | []               => [(r, d)]
  | (r', d') :: rest =>
    if r = r' then (r', d + d') :: rest
    else (r', d') :: consolidateInto r d rest

/-- Sum diffs per `Row` carrier across the stream. Order of distinct
carriers follows encounter order from the right, mirroring the old
`UnifiedStream.consolidate`. -/
def consolidate : UnifiedStream2 → UnifiedStream2
  | []             => []
  | (r, d) :: rest => consolidateInto r d (consolidate rest)

/-! ### `consolidateInto` reduction lemmas

Named per-shape reductions so proofs cite a single lemma instead of
unfolding the `if`-then-else by hand. -/

theorem consolidateInto_nil (r : Row) (d : DiffWithGlobal) :
    consolidateInto r d [] = [(r, d)] := rfl

/-- Inserting `(r, d)` at the head of a list whose head matches `r`
folds into the head bucket. -/
theorem consolidateInto_match
    (r : Row) (d d' : DiffWithGlobal) (tl : UnifiedStream2) :
    consolidateInto r d ((r, d') :: tl) = (r, d + d') :: tl := by
  show (if r = r then (r, d + d') :: tl
          else (r, d') :: consolidateInto r d tl)
      = (r, d + d') :: tl
  rw [if_pos rfl]

/-- Inserting `(r, d)` at the head of a list whose head does not match
`r` skips the head and recurses on the tail. -/
theorem consolidateInto_skip
    (r r' : Row) (d d' : DiffWithGlobal) (tl : UnifiedStream2)
    (h : r ≠ r') :
    consolidateInto r d ((r', d') :: tl)
      = (r', d') :: consolidateInto r d tl := by
  show (if r = r' then (r', d + d') :: tl
          else (r', d') :: consolidateInto r d tl)
      = (r', d') :: consolidateInto r d tl
  rw [if_neg h]

/-! ## Trivial cases -/

theorem consolidate_nil :
    consolidate [] = [] := rfl

theorem consolidate_singleton (r : Row) (d : DiffWithGlobal) :
    consolidate [(r, d)] = [(r, d)] := rfl

end UnifiedStream2

end Mz
