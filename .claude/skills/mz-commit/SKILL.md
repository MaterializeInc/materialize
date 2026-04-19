---
name: mz-commit
description: >
  This skill should be used when the user wants to "commit", "prepare a commit",
  "create a PR", "push", "open a pull request", or mentions committing,
  pre-commit checks, or pull requests in the Materialize repository. Use this
  skill even if the user just says "ship it" or "ready to merge" without being
  specific. Note: for reviewing code, use mz-pr-review instead.
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

## Pre-merge checklist

Before asking for review, verify:

* **Trigger CI**: For non-trivial changes, kick off additional test suites at `https://ci.dev.materialize.com/trigger/<PR-number>`.
* **Design doc**: The PR description references an up-to-date design doc, or the change is small enough not to require one ([template](doc/developer/design/00000000_template.md), [guidelines](doc/developer/design/README.md)).
* **User-visible changes**: If the change is user-visible per `doc/developer/guide-changes.md`, include a release note in the description ("This release will…") and ping the relevant PM.
* **`T-proto` label**: If the diff evolves a `$T ⇔ Proto$T` encoding in a potentially backwards-incompatible way, add the `T-proto` label. Proto encoding breakage can silently corrupt persisted data on upgrade.
* **Cloud companion PR**: If the change affects cloud orchestration or cloud-side tests, open a companion cloud PR tagged `release-blocker`. Ask in `#team-cloud` on Slack if uncertain.

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
