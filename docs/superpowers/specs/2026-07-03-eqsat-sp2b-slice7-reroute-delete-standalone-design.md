# SP2b Slice 7: Reroute to CombinedLang + delete standalone `ScalarEGraph` (finale) — Design

## Context

Part of SP2b (governing design:
`docs/superpowers/specs/2026-07-01-eqsat-sp2b-scalar-dsl-port-design.md`). Slices 1-5
and 6a-6f shipped: all 20 scalar rules are ported into the CombinedLang DSL, and the
final 6f review confirmed the standalone `ScalarEGraph` is behaviorally subsumed. Slice
7 is the finale. It makes the combined engine the production scalar canonicalizer and
deletes the now-dead standalone engine.

Branch: `claude/mir-equality-optimizer-sodbej`. Slice-7 BASE: `065aa871bc`.

Scope and gate decisions are LOCKED by a read-first recon. No re-litigation.

## The reframe that governs everything

The flag `enable_eqsat_scalar_canonicalize` is **`default: true`**
(`src/sql/src/session/vars/definitions.rs:2124`). The standalone `ScalarEGraph` is
**already the live production scalar canonicalizer**. So slice 7 is a
**standalone-egraph to combined-egraph swap**, not an eqsat-vs-reduce change. Both
engines share the same `SNode` substrate and `ClassAnalysis`, differing only in
extractor (standalone `scalar/raise.rs` vs `scalar_extract::raise`) and rule encoding
(21 hand fns vs 32 DSL rules). The 13 differential-parity tests assert **exact
`MirScalarExpr` equality** (`combined == old_scalar`) and pass on every slice, so the
two engines are byte-identical on the tested corpus. That evidence is what makes the
swap low-risk and golden churn unlikely.

A stale doc comment at `src/repr/src/optimize.rs:156` still reads "Default off". It is
wrong (live default is `true`); fix it in passing (it is `src`, not generated).

## Goal

1. Reroute the three production call sites (via their single injection point) from the
   standalone engine to the combined engine, behavior-neutral, verified while both
   engines still coexist.
2. Delete the dead standalone driver, keeping the shared substrate the combined engine
   depends on.
3. Repoint the 13 differential-parity tests from oracle comparison to frozen snapshots
   so scalar-rule coverage survives the oracle's deletion.

When this lands, SP2b is complete: one CombinedLang engine, standalone retired, all 20
rules ported.

## Two commits, one branch, A before B

The A boundary is the neutrality proof. Do not squash A into B.

### Commit A — reroute + verify (the whole risk, gated while the oracle is alive)

The single gate is `src/transform/src/eqsat/scalar.rs:56`
`pub(crate) fn canonicalize_predicates(...)`. Its `if enable_eqsat_scalar` branch runs
the standalone `canonicalize` through a closure. All three production sites
(`predicate_pushdown.rs:865`, `literal_constraints.rs:650`, `fusion/filter.rs:100`)
funnel through this one wrapper, so there are no per-site edits.

**The swap:** inside that closure, replace the call to the standalone
`canonicalize(e, ct)` with `canonicalize_combined(e, ct)`
(`src/transform/src/eqsat/scalar_saturate.rs:182`). That function already exists, is
`pub(crate)` (crate-reachable, no visibility change), and has the **exact matching
signature** `(&MirScalarExpr, &[ReprColumnType]) -> MirScalarExpr`. It builds a fresh
`EGraph<CombinedLang>`, lowers, saturates scalar-only, and raises via
`scalar_extract::raise`. It is scalar-isolated at this call site (a fresh e-graph per
predicate list), so there is no relational-saturation interleave to worry about.

**Flag disposition — Option A (keep the flag, swap the engine behind it):** `true` runs
the combined engine, `false` keeps the `mz_expr` reduce path as the kill-switch. The
binary eqsat-vs-reduce meaning is unchanged. Flag removal is a deferred follow-up, not
slice 7.

**Housekeeping in A:**
- Fix the stale `optimize.rs:156` "Default off" doc comment.
- Update the stale comment at `scalar_saturate.rs:14-16` ("production still routes
  through the old engine until a later slice wires this in"). It is now wired.
- `canonicalize_combined` and its dependencies (`saturate`, etc.) become live via
  production. Review the module-level `#![allow(dead_code)]` on `scalar_saturate.rs`:
  drop it if `cargo check` is warning-clean without it; keep it only if genuinely-dead
  test-support items remain. Do not chase warnings outside this file.

**Standalone stays present in A.** The standalone `canonicalize` (scalar.rs:36) is now
called only by the 13 parity tests (as `crate::eqsat::scalar::canonicalize`, the
oracle). That is exactly what makes A verifiable: both engines coexist.

**Commit-A gate (the strongest neutrality proof available):**
- `cargo check --tests` clean.
- eqsat unit tests pass.
- All 13 parity tests green (`combined == old_scalar` still holds through the reroute).
- **Full sqllogictest, no `--rewrite`, zero golden diff**, rows/errors unchanged.
- A golden move would be spelling-only (both are sound e-graphs over the same cost
  model, so rows/errors are identical by construction). If any golden moves, do not
  rubber-stamp: explain the diff, confirm rows/errors-neutral, and certify it as
  behavior-neutral churn (the delta-EXPLAIN 48-golden regen is the reference shape).
  Zero-diff is the expectation given the byte-identical evidence.

### Commit B — delete the dead engine + repoint tests (mechanical, only after A green)

Only compiles as a whole; land it as one commit.

**Repoint the 13 parity tests (before deleting, so nothing references the oracle):**
In `scalar_saturate.rs`, the 36 oracle call sites live in 13 test fns. For each case,
keep the input expr, and replace `let old = crate::eqsat::scalar::canonicalize(&input, &ct)`
with a frozen `expected` and `assert_eq!(combined, expected)`. The frozen value is
`combined`'s current output, which equals `old` (the tests are green now) which equals
correct (old is the trusted oracle). Build `expected` from the test's own helper
constructors representing the rule's documented rewrite (the parity tests already assert
the known structural result), and cross-check that `combined == expected` still passes.
The two `assert_ne` guard tests (`absorb_guard_blocks_dropped_extra_error`,
`null_prop_variadic_guard_blocks_erroring_operand`) are already oracle-independent:
confirm they carry no `canonicalize` reference and keep them. Retire none: their
adversarial coverage (could_error guards, CLU-137 erroring-common-factor, null-vs-error
priority, absorption unsoundness) is not replicated by production goldens.

**Delete the standalone driver:**
- Whole-file delete: `src/transform/src/eqsat/scalar/egraph.rs` (`ScalarEGraph` alias,
  `saturate`, sole caller of `rules()`), `scalar/rules.rs` (21 hand rules + `rules()`
  registry + its tests), `scalar/raise.rs` (standalone extractor).
- Partial-strip `scalar.rs`: remove the standalone `canonicalize` fn (scalar.rs:36), the
  `ScalarEGraph` import (scalar.rs:27), `pub mod egraph;`, `pub mod rules;`, and the
  `#[cfg(test)] mod tests` block that round-trips through the standalone engine.
  **KEEP `canonicalize_predicates`** (scalar.rs:56) — it is the production entry the
  three sites call, and after A its true-branch already targets `canonicalize_combined`,
  so it no longer references the standalone engine.
- Partial-strip shared-substrate files, keeping the types the combined engine uses:
  - `scalar/node.rs`: keep `SNode`; fix the `use ...egraph::Id` import once `egraph.rs`
    is gone.
  - `scalar/analysis.rs`: keep `ClassAnalysis`/`make`/`merge`; fix the `Id` import and
    the dead doc-link; rewrite or remove the `#[cfg(test)] mod tests` block that depends
    on `ScalarEGraph`.
  - `scalar/lower.rs`: keep `snode_of`/`lower_into`; remove `pub fn lower()`; fix the
    `Id`/`ScalarEGraph` import.
  - `scalar/lang.rs`: untouched (`ScalarLang` is shared, self-contained).
- Delete `scalar::rules::rules()` (standalone, 21 fns). **KEEP**
  `eqsat::rules::scalar_all()` / `SCALAR_COMPILED_RULES` (the combined DSL ruleset — this
  is production now). Do not confuse the two registries.

**Re-grep zero stray callers** of `ScalarEGraph` / `ScalarLang`-as-standalone /
`scalar::rules::rules` outside the deleted set (recon found the set clean: no benches,
examples, other crates, or integration tests). No stubs left behind.

**Commit-B gate:**
- `cargo check --tests` clean (a missed partial-strip fails `--tests`, not bare
  `check` — run `--tests`).
- ALL-IMPLS sweep for anything the deletion touches.
- eqsat unit tests pass, including the 13 repointed snapshots and the 20 survivors.
- Full sqllogictest still green (production is the combined engine now).
- Permanent sorry count stays 11 (no new rules; nothing Lean changes).
- `bin/lint` + `cargo fmt` clean (recall 6f B1: a stray `use` trips the fmt gate).

## Post-B safety net (confirm coverage, flag gaps)

With the oracle gone, scalar-rule correctness is held by: production sqllogictest
goldens + rows/errors (the combined engine is production), the 20 surviving
standalone-independent tests (2 combined-behavior, 9 corpus string-checks, 9
`*_terminates`), and the 13 repointed frozen snapshots (which preserve the adversarial
per-rule coverage). Confirm this covers the full scalar-rule surface; flag any gap.

## Testing

- Commit A: the commit-A gate above (full slt no `--rewrite`, 13 parity green, unit
  tests, `cargo check --tests`).
- Commit B: the commit-B gate above (repointed snapshots pass, deletion compiles under
  `--tests`, full slt green, permanent 11, lint+fmt clean, zero stray callers).

## Out of scope

Flag removal (deferred follow-up). Union-cancel / colored-half host (post-SP2b). The
arrangement-as-e-node capability design (parked). No new rules, no Lean changes. Do not
touch `doc/developer/generated/` (the `optimize.rs` fix is source, not generated).
Moving `canonicalize_combined` to a different module is unnecessary refactoring — call
it where it is.
