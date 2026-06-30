# SP4b differential — colored default vs Phase-2a

Captured by flipping `Optimizer::new`'s default `EquivMode::Phase2a → Colored` and
running the EXPLAIN-bearing slt corpus (195 files) per-file standalone, plus the
full `mz-transform` cargo suite (unit + datadriven goldens).

## Method
- `bin/sqllogictest --optimized -- <file>` per EXPLAIN file (195); failures =
  files whose output changed under the colored default.
- `bin/cargo-test -p mz-transform` for unit + datadriven (`*.spec`) goldens.

## Gate bugs the differential caught (fixed before acceptance)
1. **`coalesce_mfp` panic (index out of bounds)** on ~7 files. Root cause: colored
   resolution selected a congruent spelling out-of-range for the payload's
   evaluation context. Fixed: column-range guard in `resolve_scalar_colored`
   (Filter `max_col=arity(input)`, Map scalar at pos `max_col=arity(input)+pos`,
   matching `reduce_escalar`). Commit `88ce6b0e91`. After the fix those 7 files
   pass UNCHANGED — their "changes" were panic artifacts, not plan diffs.
2. **Non-canonical tie spelling** in `eqsat.spec` (`#1+1` kept vs reducer's
   `#0+1`). Root cause: equal `scalar_cost` spellings tie-broke on `name_key`,
   not column index. Fixed: prefer lowest column support among equal-cost in-range
   members (reducer parity). Commit `231e9efa86`. After the fix `eqsat.spec` and
   all cargo datadriven goldens pass UNCHANGED.

## Final differential (after fixes)
- **cargo suite:** 346/346 pass, 0 golden changes. Colored == Phase-2a on every
  datadriven `*.spec`.
- **slt EXPLAIN corpus:** 192/195 unchanged; **3 files changed**, all the SAME
  query rendered three ways:
  - `test/sqllogictest/explain/physical_plan_as_text.slt`
  - `test/sqllogictest/explain/physical_plan_as_json.slt`
  - `test/sqllogictest/explain/physical_plan_as_text_redacted.slt`

### Category of the 3 changed files: NAME-ANNOTATION-ONLY (sound-neutral)
Query: `SELECT b + d, c + e, a + e FROM t, u, v WHERE a = c AND d = e AND b + d > 42`.
Diff example (text): `map=((#1{b} + #2{d}), …)` → `map=((#1{b} + #2{a}), …)`.
The column **indices are identical** (`#1 + #2`, `#0 + #2`); only the cosmetic
`TreatAsEqual` display name annotation (`{d}` vs `{a}`) differs — colored picked a
different congruent column-name representative. Verified strictly name-only:
stripping `{…}` from the changed lines leaves old and new identical
(`map=((#1 + #2), (#0 + #2))`). This is the exact cosmetic class SP4a already
accepted (`TreatAsEqual` names are inert for Eq/Hash/Ord/semantics, rendered only
in EXPLAIN). No structural change, no arity change, no query-result change.

## Verdict
SP4b colored path is at parity with Phase-2a: the only output delta is cosmetic
EXPLAIN column-name annotations on one query (3 renderings), regenerated and
verified name-only. Accepted per the spec gate (full replacement; regenerate +
review).
