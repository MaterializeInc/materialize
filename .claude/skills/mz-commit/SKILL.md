---
name: mz-commit
description: >
  This skill should be used when the user wants to "commit", "prepare a commit",
  "create a PR", "push", "open a pull request", or mentions committing,
  pre-commit checks, pull requests, or code review in the Materialize
  repository. Use this skill even if the user just says "ship it" or
  "ready to merge" without being specific.
---

# Committing in Materialize

Read `doc/developer/guide-changes.md` for the full conventions on submitting and reviewing changes.

## Pre-commit checklist

Before committing, run these and fix any warnings:

1. `bin/fmt` (formats `.rs`, `.py`, and `.proto` files)
2. `bin/lint` (can error if tools are missing; use `bin/ci-builder run stable bin/lint` as an alternative)
3. `cargo clippy --all-targets -- -D warnings`

Do not manually update `*.snap` files.
Use `cargo test` followed by `cargo insta accept` to update snapshot files.
Rewrite datadriven test expectations with `REWRITE=1 cargo test ...`.

## PR titles and commit messages

Materialize uses squash merging, so the PR title becomes the commit subject on `main`.

* Use imperative mood: "Fix X" not "Fixed X" or "Fixes X".
* Be specific: "Fix panic in catalog sync when controller restarts" not "Fix bug".
* Prefix with area if helpful: `adapter: `, `storage: `, `compute: `, `sql: `.

Write a thorough PR description explaining the rationale for the change.
Mention which tests were added or modified in the pull request description, but do not list which tests were run.
To auto-close issues, include `Fixes database-issues#NNNN`.
Add release notes for user-visible changes (should complete "This release will...").

## Cargo.lock discipline

Never regenerate the entire Cargo.lock — bare `cargo update` bumps every semver-compatible dep and introduces unrelated breakage (e.g., `os_info` pulling in `objc2` on macOS, `chrono-tz` changing timezone data, `serde_path_to_error` changing error formats).

* **Adding a dep or changing features**: just `cargo check`. It updates only what's needed.
* **Updating one crate**: `cargo update -p <crate>` (add `--precise <ver>` to pin).
* **After any Cargo.lock change**, review the diff:
  ```
  git diff Cargo.lock | grep '^[+-]version' | head -40
  ```
  Pin back anything that moved unexpectedly: `cargo update -p <crate> --precise <old-version>`.
* **After rebase conflicts in Cargo.lock**: resolve by taking HEAD's version then running `cargo check` (not `cargo update`). This preserves existing pins while adding only what the new commits require.

## Git conventions

* Work against the `main` branch of `MaterializeInc/materialize`.
* Push branches to your fork.
* Pull requests target `main` on `MaterializeInc/materialize`.
* Each PR should contain one semantic change.
