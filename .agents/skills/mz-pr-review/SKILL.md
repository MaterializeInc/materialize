---
name: mz-pr-review
description: >
  Local code review of current branch vs Materialize standards. Trigger:
  "review my code", "review my changes", "check my diff", "does this look ok",
  "what do you think of this PR", "code review", or look over changes before
  merging. Also PR number + wants feedback on quality, style, correctness.
argument-hint: [base-branch]
allowed-tools: [Bash, Read, Grep, Glob, Task]
---

Perform a local code review of the current branch's changes against Materialize project standards.

## Steps

1. Parse arguments from: $ARGUMENTS — a PR number, base branch, or nothing.
2. Get the diff using the first method that works:
   - **PR number given** (e.g. `123`): `gh pr diff 123`
   - **git available**: `git diff <base>...HEAD` (default base: `main`)
   - **jj available**: `jj diff -r <revset>` (default: diff from trunk)
3. Get the file list from the same diff (add `--stat` for git, `--stat` for jj, or `gh pr diff 123 --stat` for PR).
4. Review the diff against the checklists below.
5. Present findings organized as: **Blocking**, **Strong suggestions**, **Nits**.

## Review checklist

The overall developer guide for reviewing changes is defined in `doc/developer/guide-changes.md`, always read and follow its guidance.

### Tests
- Every behavior change has at least one new or modified test.
- SQL/query behavior → look for `.slt` in `test/sqllogictest/`.
- Wire/protocol behavior → look for `.pt` in `test/pgtest/`.
- Rust logic/types/APIs → add or extend a Rust unit test in the crate (e.g. `#[cfg(test)]` or `tests/`); run with `cargo test -p mz-<crate>`.
- Prefer testing observable behavior (SQL results, wire protocol) over implementation details.
- Red flag: behavior change with no test changes.
- For more testing guidelines, read `doc/developer/guide-testing.md`

### LIR schema registry
Applies when the diff changes the serde-visible shape of a type the LIR
schema reaches: `src/compute-types/src/plan.rs`,
`src/compute-types/src/plan/**`, a `*Func` payload, or a plan-reachable type
in `mz-expr` or `mz-repr`.
- `src/compute-types/tests/snapshots/lir_v{N}.json` is regenerated in the
  same PR, and the diff matches the intended format change. Red flag: a
  serde-visible change to a plan type with no snapshot diff.
- If `LIR_VERSION` in `src/compute-types/src/plan.rs` had shipped, it is
  bumped and the old `lir_v{N}.json` is left untouched rather than rewritten.
- `Row` and `EvalError` never appear directly in plan types, only as
  `StableRow` / `StableEvalError`, enforced by
  `lir_schema_contains_only_stable_types`.

### Scalar function registry
Applies when the diff touches `UnaryFunc`, `BinaryFunc`, or `VariadicFunc`
(`src/expr/src/scalar/func.rs`, `src/expr/src/scalar/func/**`, `#[sqlfunc]`
bodies).
- The registry snapshots under `src/compute-types/tests/snapshots/` are
  regenerated in the same PR. A property change moves `func_registry.json`
  and the current version's entry in `func_registry_digests.json`, a
  `#[sqlfunc]` declaration or body change moves `func_registry_source.json`
  alone, and a change to a hand-written variant's body moves neither.
- Every new variant with a payload has a `Sample` in
  `src/expr/src/scalar/func/registry.rs`, with a labeled second sample when a
  property depends on the payload.
- A removed function, a changed property, or a `body_fingerprint` change that
  alters results for some input bumps `LIR_VERSION` in
  `src/compute-types/src/plan.rs` if that version has shipped, and leaves the
  old digest in `func_registry_digests.json` untouched. Additions need no
  bump.
- The PR description names the added, removed, and changed functions and,
  for body changes, states whether results change.

### Code style (Rust)
- **Imports:** `std` → external crates → `crate::`; one `use` per module; prefer `crate::` over `super::` in non-test code.
- **Errors:** Structured with `thiserror`; no bare `anyhow!("...")`. `Display` should not print full error chain.
- **Async:** Use `ore::task::spawn` / `spawn_blocking`, not raw `tokio::spawn`.
- **Tests:** `#[mz_ore::test]`; panic in tests rather than returning `Result`.

### Code style (SQL)
- Keywords capitalized (`SELECT`, `FROM`); identifiers lowercase.
- No space between function name and `(`.

### Error messages
- Primary: short, factual, lowercase first letter, no trailing punctuation.
- Detail/hint: complete sentences, capitalized, period.
- No "unable", "bad", "illegal", "unknown"; say what kind of object.

### Sensitive data handling
- Types holding passwords, keys, tokens, or credentials should use `mz_ore::secure::{SecureString, SecureVec}` or `zeroize::Zeroizing<T>` (from `mz_ore::secure`).
- Sensitive types should **not** derive `Clone` or `Debug` (use custom `Debug` that redacts).
- Stack-local buffers holding derived keys, nonces, or HMAC outputs should be wrapped in `Zeroizing<T>`.
- See `doc/developer/generated/ore/secure.md` for full guidance and `src/ssh-util/src/keys.rs` for a reference implementation.

### Architecture
- **Simplicity:** No incidental complexity; simplify redundant logic.
- **No special casing:** Prefer composable design over extra booleans/branches.
- **Encapsulation:** sql-parser = grammar only (no semantic validation); sql = planning + semantics.
- **Dependencies:** New crates must be justified.
- For more design guidelines read: `doc/developer/best-practices.md`

### Polish
- No leftover `// XXX`, `// FIXME`, `dbg!`, `println!`, or commented-out code.
- No unrelated formatting changes in untouched code.
- New public items should have doc comments.

### Release notes

Release notes are auto-generated per release from PR descriptions (the [`/mz-release-notes` skill](https://github.com/MaterializeInc/mz-skills/blob/main/.agents/skills/mz-release-notes/SKILL.md));
authors don't write them. So for a user-visible change, check that the PR *description* states the
user-observable effect in user-facing terms — not only implementation detail — so the classifier includes it.

## One semantic change rule

The PR should do one thing. If it spans multiple CODEOWNERS areas (e.g. sql-parser + sql planner), consider suggesting a split.

## Rules

- Review the code, not the author. Explain the *why* behind suggestions.
- Use **nit:** for preferences where reasonable people could disagree.
- If the PR improves overall codebase health and blocking items are addressed, say so.
- Do NOT make any changes — this is read-only review.
