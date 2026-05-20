---
name: mz-dbt-release
description: >
  Trigger: "cut a dbt release", "release dbt-materialize", "release the dbt
  adapter", "bump dbt-materialize version", "new dbt adapter version", or
  opening the PR that ships an `Unreleased` CHANGELOG entry to PyPI.
---

# Cutting a dbt-materialize release

A release PR bumps the published version of the `dbt-materialize` PyPI
package. It is mechanical: three files change, one commit, one PR.

The adapter lives in `misc/dbt-materialize/`. PyPI publication is handled by
CI after the PR merges.

## Files to touch

Exactly three files:

| File | Change |
|---|---|
| `misc/dbt-materialize/dbt/adapters/materialize/__version__.py` | Bump `version = "X.Y.Z"` |
| `misc/dbt-materialize/setup.py` | Bump `version="X.Y.Z"` (kwarg in `setup(...)`) |
| `misc/dbt-materialize/CHANGELOG.md` | Insert `## X.Y.Z - YYYY-MM-DD` between `## Unreleased` and the existing bullets |

Both version strings must stay in sync — the comments in each file say so.

## Picking the next version

* Default: bump the **patch** component (`1.9.9 → 1.9.10`).
* The **minor** component tracks the required `dbt-postgres` minor. Only bump
  minor when upgrading to a new `dbt-postgres` minor (e.g. `1.9.x → 1.10.0`).
* Read the current version from `__version__.py`.

## Confirming what's being released

The `## Unreleased` section of `CHANGELOG.md` is the source of truth for what
ships. If it's empty, there is nothing to release. Cross-check with:

```
git log <prev-release-sha>..HEAD -- misc/dbt-materialize/
```

If a commit since the previous release touched `misc/dbt-materialize/` but is
not represented under `## Unreleased`, stop and ask whether it should be
included — don't silently invent a changelog entry.

## CHANGELOG edit

The release does **not** rewrite the unreleased bullets; it just dates them.
Insert a new dated heading immediately after `## Unreleased`, leaving
`## Unreleased` itself empty:

```
## Unreleased

## 1.9.10 - 2026-05-20

* Support unmanaged clusters in `deploy_init`. ...
```

Use today's date in `YYYY-MM-DD`.

## Branch, commit, PR

* Branch name: `dbt-release-X.Y.Z` off `main` (or any commit that is an
  ancestor of `upstream/main` — the `misc/dbt-materialize/` directory is the
  only thing that matters).
* Commit subject: `dbt-materialize: release vX.Y.Z`
* Commit body / PR body: a single `Ship: <url>` line pointing at the PR that
  triggered this release (the feature/fix PR being shipped). If multiple PRs
  are being shipped, pick the headline one — the changelog already enumerates
  them.
* PR title: same as the commit subject.
* PR targets `MaterializeInc/materialize:main`.

Example:

```
gh pr create --repo MaterializeInc/materialize --base main \
  --head <your-fork>:dbt-release-1.9.10 \
  --title "dbt-materialize: release v1.9.10" \
  --body "Ship: https://github.com/MaterializeInc/materialize/pull/36625"
```

## What you do NOT run

This PR touches only Python version strings and Markdown. Skip:

* `cargo check`, `cargo clippy`, Cargo.lock work — no Rust changes.
* Test suites — there are no tests to add for a version bump.

## Verifying before pushing

```
git diff --stat
```

Should show exactly three files with a `+4 / -2` shape (one line each in the
two version files, two added lines in `CHANGELOG.md`). If anything else is
staged, you've gone off-script.

## Quick reference

```
# 1. Branch
git checkout -b dbt-release-X.Y.Z

# 2. Edit the three files (see table above)

# 3. Verify
git diff --stat   # expect 3 files, +4/-2

# 4. Commit
git commit -am "dbt-materialize: release vX.Y.Z

Ship: <feature-pr-url>"

# 5. Push and open PR
git push -u origin dbt-release-X.Y.Z
gh pr create --repo MaterializeInc/materialize --base main \
  --head <your-fork>:dbt-release-X.Y.Z \
  --title "dbt-materialize: release vX.Y.Z" \
  --body "Ship: <feature-pr-url>"
```

## Historical reference

Recent release commits (find via `git log --oneline -- misc/dbt-materialize/dbt/adapters/materialize/__version__.py`):

* v1.9.10 — PR #36636
* v1.9.9 — PR #36609
* v1.9.8 — PR #36529

Inspect any of them with `git show <sha>` to see the exact diff shape.
