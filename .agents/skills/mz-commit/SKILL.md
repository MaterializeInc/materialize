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

* Every PR links a Linear issue with a magic word and bare ID in the
  PR description, e.g. `Closes CS-639`. A branch name containing the
  issue ID links the PR even without a magic word.
* Exception: a docs-only change needs no issue. Say so in the
  description with a parenthetical: "(No Linear reference: docs-only
  change.)"
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
* Push branches to your fork. Exception: a PR stack needs its branches
  in `MaterializeInc/materialize`, since each stacked PR targets the
  branch below it. Delete the branches once the stack merges.
* Pull requests target `main` on `MaterializeInc/materialize`.
* Each PR should contain one semantic change.

## Splitting large branches

The repo is squash-merge-only: every PR lands as exactly one commit on `main`, however many commits it had internally. Keep each commit/PR under ~500 changed lines, split by concern, not by the chronology of how the code was written.

* Prefer GitHub's native stacked-PR support to land a chain of dependent PRs, where it's set up for the branch. Where it isn't, fall back to sequential landing, one PR at a time: cut a PR from the front, merge, `git fetch upstream && git rebase upstream/main` the remaining tail, repeat. Squash-only means merging an earlier PR always replaces its commits with one new commit, so any later branch still built on the old commits needs a rebase either way; sequential landing keeps that to one clean rebase per merge instead of compounding across an unmanaged chain.
* Land foundational or low-conflict commits (design docs, scaffolding, new deps) first. The longer a tail branch lives, the more likely a concurrent unrelated PR touches the same files and turns a clean rebase into a real conflict.
* To split one large diff into smaller commits, or squash a long messy history into fewer reviewable units, as an alternative to interactive rebase: `git reset --soft <base>` stages the full diff, then stage per group with `git add <files>` for a file-level split, or use `git checkout <sha> -- .` to reproduce an exact historical checkpoint's tree for a chronological squash. (Claude Code's Bash tool has no interactive terminal, so agents need this non-interactive form; use whichever you prefer by hand.) `git checkout <sha> -- .` only adds or updates paths present in `<sha>`, it never removes files that shouldn't exist yet at that checkpoint: compute the removal set first with `comm -23 <(git ls-files|sort) <(git ls-tree -r --name-only <sha>|sort)` and `git rm -f` those, or the checkpoint commit silently carries files from later history. Verify every checkpoint with `git diff HEAD <sha>` (must be empty) before moving to the next. Reattach already-clean commits on top with `git rebase --onto <new-branch> <old-base>` (also non-interactive).
