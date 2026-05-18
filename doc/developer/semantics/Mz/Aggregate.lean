import Mz.Eval

/-!
# Aggregates

Strict aggregate reductions over `List Datum`. The spec rule is that
SQL aggregates such as `SUM`, `MIN`, `MAX`, and `AVG` propagate
`err` like strict scalar functions: any `err` input produces an
`err` output, with a deterministic choice of payload. `NULL` values
are skipped, mirroring PostgreSQL's `COUNT(expr)` and the
"`IGNORE NULLS`" reading of the other aggregates.

`COUNT` itself is `COUNT(*)` (count all rows) or `COUNT(expr)`
(count rows where `expr` is neither `NULL` nor `err`). The skeleton
models the latter via `aggCountNonNull`.

`try_sum`, `try_avg`, and friends — the non-strict variants that
swallow `err` into `NULL` instead of propagating — are future work.
They satisfy a *coalesce*-style law rather than a strict-propagation
law.
-/

namespace Mz

/-- `COUNT(expr)`: count rows whose value is neither `NULL` nor an
`err`. Matches the PostgreSQL aggregate. -/
def aggCountNonNull : List Datum → Nat
  | []              => 0
  | .bool _ :: rest => 1 + aggCountNonNull rest
  | .null   :: rest => aggCountNonNull rest
  | .err _  :: rest => aggCountNonNull rest

/-- Strict aggregate reduction. `err` propagates: the first `err`
encountered is returned. `NULL`s are skipped. The reducer `f` is
applied to the surviving values; an empty list (or a list of only
`NULL`s) returns `NULL`. -/
def aggStrict (f : Datum → Datum → Datum) : List Datum → Datum
  | []                  => .null
  | .err e   :: _       => .err e
  | .null    :: rest    => aggStrict f rest
  | x@(.bool _) :: rest =>
    match aggStrict f rest with
    | .err e => .err e
    | .null  => x
    | r      => f x r

/-! ## Strict propagation laws -/

/-- If any input is an `err`, the aggregate result is an `err`. The
exact payload is whichever `err` `aggStrict` selects (the first one
in scan order under this definition). -/
theorem aggStrict_err :
    ∀ {xs : List Datum} (f : Datum → Datum → Datum),
      (∃ d ∈ xs, d.IsErr) → (aggStrict f xs).IsErr
  | [], _, h => by
    obtain ⟨_, hmem, _⟩ := h
    cases hmem
  | Datum.err e :: _, _, _ => by
    show (Datum.err e).IsErr
    trivial
  | Datum.null :: rest, f, h => by
    obtain ⟨d, hmem, hsafe⟩ := h
    cases hmem with
    | head _    => exact hsafe.elim
    | tail _ h' =>
      show (aggStrict f rest).IsErr
      exact aggStrict_err f ⟨d, h', hsafe⟩
  | Datum.bool b :: rest, f, h => by
    obtain ⟨d, hmem, hsafe⟩ := h
    cases hmem with
    | head _    => exact hsafe.elim
    | tail _ h' =>
      have h_rest : (aggStrict f rest).IsErr :=
        aggStrict_err f ⟨d, h', hsafe⟩
      cases h_eval : aggStrict f rest with
      | err e' =>
        show (match aggStrict f rest with
              | Datum.err e => Datum.err e
              | Datum.null  => Datum.bool b
              | r           => f (Datum.bool b) r).IsErr
        rw [h_eval]; trivial
      | null   => rw [h_eval] at h_rest; exact h_rest.elim
      | bool _ => rw [h_eval] at h_rest; exact h_rest.elim

/-- Dual: if no input is an `err`, the aggregate result is not an
`err`. The reducer `f` is assumed to preserve "no-err": applied to
two non-`err` values it produces a non-`err` value. -/
theorem aggStrict_no_err
    (f : Datum → Datum → Datum)
    (f_safe : ∀ x y, ¬x.IsErr → ¬y.IsErr → ¬(f x y).IsErr) :
    ∀ {xs : List Datum}, (∀ d ∈ xs, ¬d.IsErr) → ¬(aggStrict f xs).IsErr
  | [], _ => by
    show ¬(Datum.null).IsErr
    intro h; cases h
  | Datum.err _ :: _, h => by
    exact (h _ (List.Mem.head _) trivial).elim
  | Datum.null :: rest, h => by
    show ¬(aggStrict f rest).IsErr
    apply aggStrict_no_err f f_safe
    intro d hmem; exact h d (List.Mem.tail _ hmem)
  | Datum.bool b :: rest, h => by
    have h_rest : ¬(aggStrict f rest).IsErr := by
      apply aggStrict_no_err f f_safe
      intro d hmem; exact h d (List.Mem.tail _ hmem)
    have hb : ¬(Datum.bool b).IsErr := h _ (List.Mem.head _)
    cases h_eval : aggStrict f rest with
    | err e =>
      rw [h_eval] at h_rest
      exact absurd trivial h_rest
    | null   =>
      show ¬(match aggStrict f rest with
             | Datum.err e => Datum.err e
             | Datum.null  => Datum.bool b
             | r           => f (Datum.bool b) r).IsErr
      rw [h_eval]; exact hb
    | bool b' =>
      show ¬(match aggStrict f rest with
             | Datum.err e => Datum.err e
             | Datum.null  => Datum.bool b
             | r           => f (Datum.bool b) r).IsErr
      rw [h_eval]
      apply f_safe
      · exact hb
      · intro h_eq; cases h_eq

/-! ## Non-strict (`try_*`) variants

The proposed `try_sum`, `try_min`, etc. swallow `err` into `NULL`
instead of propagating. Defined here as a post-pass coalesce on
`aggStrict`'s output: an `err` result becomes `.null`; anything
else is unchanged. The skeleton's `evalCoalesce` exhibits the same
"`null` beats `err`" tiebreak, so an equivalent reading is

    aggTry f xs = evalCoalesce [aggStrict f xs, .null]

The defining property is `aggTry_no_err`: the result is never an
`err`. The companion `aggTry_eq_aggStrict_of_no_err` says the
non-strict variant agrees with the strict one whenever the strict
one would not have erred — so an optimizer that has proved the
inputs error-free can swap `aggTry` for `aggStrict`. -/

def aggTry (f : Datum → Datum → Datum) (xs : List Datum) : Datum :=
  match aggStrict f xs with
  | Datum.err _ => Datum.null
  | r           => r

theorem aggTry_no_err (f : Datum → Datum → Datum) (xs : List Datum) :
    ¬(aggTry f xs).IsErr := by
  show ¬(match aggStrict f xs with
         | Datum.err _ => Datum.null
         | r           => r).IsErr
  cases aggStrict f xs <;> intro h <;> cases h

theorem aggTry_eq_aggStrict_of_no_err
    (f : Datum → Datum → Datum)
    (f_safe : ∀ x y, ¬x.IsErr → ¬y.IsErr → ¬(f x y).IsErr)
    {xs : List Datum} (h : ∀ d ∈ xs, ¬d.IsErr) :
    aggTry f xs = aggStrict f xs := by
  have h_safe : ¬(aggStrict f xs).IsErr := aggStrict_no_err f f_safe h
  show (match aggStrict f xs with
        | Datum.err _ => Datum.null
        | r           => r) = aggStrict f xs
  cases h_eval : aggStrict f xs with
  | err _  => exact absurd (h_eval ▸ h_safe) (fun h' => h' trivial)
  | null   => rfl
  | bool _ => rfl

end Mz
