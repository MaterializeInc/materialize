# Monadic `Datum` pilot (archived, not adopted)

This note records an experimental parallel encoding of the `Datum k`
value layer that was prototyped and then dropped.
The canonical track stays the GADT in `Mz/Datum.lean`.
The pilot lived in `Mz/DatumM.lean` until it was removed; this file
preserves its rationale so the encoding does not have to be
rediscovered.

## What it proposed

The GADT `Datum : ColType → Type` has four explicit constructors
(`.bool _`, `.int _`, `.null`, `.err _`).
The pilot replaced the value carrier with an error-and-null monad:

* `Concrete : ColType → Type` extracts the per-kind concrete
  inhabitant — `.bool => Bool`, `.int => Int`, `.top => Empty`.
* `DatumW (α : Type)` is the monad with `.val a`, `.null`, `.err e`.
* `Datum' (k : ColType) := DatumW (Concrete k)`.

So `Datum' .bool = DatumW Bool`, `Datum' .int = DatumW Int`, and
`Datum' .top = DatumW Empty` (only `.null` / `.err _` inhabit it).
`DatumW` is a plain `Type → Type`, so the standard `Monad` /
`LawfulMonad` instances apply, and the three propagation rules
(`err > null > val`) collapse into a single `bind` clause.

## What it bought, and why it was dropped

Strict primitives reduce to monadic expressions whose bodies contain
only the concrete case:

```lean
def evalNot'  (d : Datum' .bool) : Datum' .bool := Bool.not <$> d
def evalPlus' (a b : Datum' .int) : Datum' .int := do
  let n ← a; let m ← b; return n + m
def evalDivide' (a b : Datum' .int) : Datum' .int := do
  let n ← a; let m ← b
  if m = 0 then .err .divisionByZero else .val (n / m)
```

`@[match_pattern]` aliases (`.bool`, `.int`, `.null`, `.err`) keep the
named-constructor match style from the GADT.
The win was collapsed null/err propagation in seven strict primitives,
about 28 redundant lines saved.

The win did not offset the cost.
Migrating roughly 30 theorems across eight modules to the monadic
representation was the dominant expense, and the more interesting SQL
operators sit outside the monad anyway:

* Variadic `AND` has absorption order `FALSE > ERR > NULL > TRUE`.
  The standard error-monad `bind` propagates `.err _` first, so it
  returns `.err e` for `[.err e, .bool false]` instead of the
  spec-correct `.bool false`.
  The combiner must do a full four-precedence dispatch by hand —
  the monad does not apply.
* `.orN`, `.coalesce` residue, branch-selecting `.ifThen`, and
  `filter`'s err-diff migration are all outside the monad for the
  same reason.

The follow-up comparison (PR #36614 review thread) concluded the GADT
track stays canonical.
One carry-over made it back: the lazy variadic structure now lives in
`Mz/Eval.lean`.
Companion modules (`Mz/CollectionM.lean`, `Mz/ExprM.lean`,
`Mz/EvalM.lean`) were retired alongside the pilot.

## Revisit condition

Reconsider the monadic carrier only if the strict-primitive surface
grows enough that the per-primitive null/err boilerplate outweighs the
migration cost — and only if the absorbing variadic operators move to
a representation where a monad covers them too.
