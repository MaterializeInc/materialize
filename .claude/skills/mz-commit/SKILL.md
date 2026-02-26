---
name: mz-commit
description: >
  This skill should be used when the user wants to "commit", "prepare a commit",
  "create a PR", "push", or mentions committing, pre-commit checks, or pull
  requests in the Materialize repository.
---

# Committing in Materialize

## Pre-commit checklist

Before committing, run all of these and fix any warnings:

1. `bin/lint` (can error if tools are missing; use `bin/ci-builder run stable bin/lint` as an alternative)
2. `bin/fmt` (formats both `.rs` and `.py` files)
3. `cargo clippy --all-targets -- -D warnings`
4. `cargo hakari generate`

A change is clean when no unexpected warnings remain.

Do not manually update `*.snap` files.
Use `cargo test` followed by `cargo insta accept` to update snapshot files.
Rewrite datadriven test expectations with `REWRITE=1 cargo test ...`.

## Git conventions

* The base branch is always `upstream/main`.
* Push branches to `origin`.
* Pull requests use `upstream/main` as their base.
* Mention which tests were added or modified in the pull request description, but do not list which tests were run.
* Keep pull request descriptions short and precise.
