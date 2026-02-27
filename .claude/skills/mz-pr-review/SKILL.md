---
description:  >
Local PR review against Materialize standards.
This skill should be used when the user asks for a review of changes.
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
- Any user-visible change to stable APIs needs a release note (imperative, "This release will…").

## One semantic change rule

The PR should do one thing. If it spans multiple CODEOWNERS areas (e.g. sql-parser + sql planner), consider suggesting a split.

## Rules

- Review the code, not the author. Explain the *why* behind suggestions.
- Use **nit:** for preferences where reasonable people could disagree.
- If the PR improves overall codebase health and blocking items are addressed, say so.
- Do NOT make any changes — this is read-only review.
