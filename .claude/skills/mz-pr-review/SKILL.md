---
name: mz-pr-review
description: >
  Perform a local code review of the current branch's changes against Materialize
  project standards. Trigger when the user says "review my code", "review my
  changes", "check my diff", "does this look ok", "what do you think of this
  PR", "code review", or asks you to look over changes before merging. Also
  trigger when the user passes a PR number and wants feedback on quality, style,
  or correctness.
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

### PR metadata

These are the author's responsibility but easy to miss — flag them during review if absent:

- **Release notes and PM notification**: User-visible changes need a release note in the description ("This release will…") and PM notification. Check `doc/developer/guide-changes.md` if uncertain what qualifies.
- **Design doc**: Significant changes should reference a design doc in the PR description.
- **`T-proto` label**: If the diff changes a `$T ⇔ Proto$T` encoding, verify the `T-proto` label is set. Missing it means broken binary encoding can slip into a release unnoticed.
- **Cloud companion PR**: If the change touches cloud orchestration or cloud-side tests, look for a companion cloud PR tagged `release-blocker`.

## One semantic change rule

The PR should do one thing. If it spans multiple CODEOWNERS areas (e.g. sql-parser + sql planner), consider suggesting a split.

## Rules

- Review the code, not the author. Explain the *why* behind suggestions.
- Use **nit:** for preferences where reasonable people could disagree.
- If the PR improves overall codebase health and blocking items are addressed, say so.
- Do NOT make any changes — this is read-only review.
- Don't flag issues that `cargo clippy`, `bin/lint`, or `bin/fmt` would catch — those are enforced by CI. Focus on what automated tooling cannot catch: design decisions, missing tests, documentation gaps, and the PR metadata items above.
