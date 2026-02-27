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
4. `cargo hakari generate` (only needed when dependencies changed)

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

## Git conventions

* Work against the `main` branch of `MaterializeInc/materialize`.
* Push branches to your fork.
* Pull requests target `main` on `MaterializeInc/materialize`.
* Each PR should contain one semantic change.
