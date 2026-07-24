---
name: mz-commit
description: >
  Trigger: "commit", "prepare commit", "create PR", "push", "open pull request",
  or mentions committing, pre-commit checks, pull requests in Materialize. Also
  "ship it", "ready to merge". For code review use mz-pr-review.
---

# Committing in Materialize

Read `doc/developer/guide-changes.md` for the full conventions on submitting and reviewing changes.

## Pre-commit checklist

Before committing, run these and fix any warnings:

1. `bin/fmt` (formats all `.rs`, `.py`, and `.proto` files; takes no file arguments)
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

## Linear references and confidentiality

GitHub is public. Linear is not.

* Link the Linear issue a PR addresses with a magic word and bare ID
  in the PR description, e.g. `Closes CS-639`. A branch name
  containing the issue ID links the PR even without a magic word.
* Closing words move the issue to Done when the PR merges:
  close(s/d/ing), fix(es/ed/ing), resolve(s/d/ing), complete(s/d/ing),
  implement(s/ed/ing).
* Non-closing words link the PR and advance the issue without closing
  it on merge: ref(s), references, part of, related to, relates to,
  contributes to, toward(s).
* Pick by situation: a ticket done in a single PR gets a closing word.
  A ticket spanning several PRs gets non-closing words (`Part of
  ENG-123`) on all but the last. Only the capstone PR, for stacked
  PRs the top of the stack, says `Closes`.
* Do not mention other Linear issues (follow-ups, related tickets) in
  PR titles or descriptions. Besides leaking internal references,
  those phrases are magic words: they link the PR to that issue and
  advance its state.
* Never include linear.app URLs.
* Never name customers in commit messages, PR titles, or PR
  descriptions. Write "a customer" instead.
* Word lists current as of July 2026; full reference:
  https://linear.app/docs/github#link-through-pull-requests

## Git conventions

* Work against the `main` branch of `MaterializeInc/materialize`.
* Push branches to your fork.
* Pull requests target `main` on `MaterializeInc/materialize`.
* Each PR should contain one semantic change.
