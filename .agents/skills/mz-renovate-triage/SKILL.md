---
name: mz-renovate-triage
description: >
  Triage open Renovate dependency PRs in MaterializeInc/materialize for the
  person who approves them: sort each PR by risk, check what CI can't (breaking
  changes that touch our code, renamed config, dev scripts, Node and browser
  support), and post one verdict comment per PR. Trigger: "triage the renovate
  PRs", "are the dependency PRs safe to merge", "check this renovate PR", a
  renovate[bot] PR link, or the scheduled Renovate triage routine. When doing a
  major upgrade by hand, read lessons.md for what past upgrades needed.
argument-hint: [pr-number]
allowed-tools: [Bash, Read, Grep, Glob, WebFetch]
---

# Renovate PR triage

The reader of your output approves Renovate PRs and has little time for each.
Lead every PR with a verdict and the one fact behind it. Never approve, merge,
close, label, or push to a Renovate PR. You comment, and the approver acts.

CI runs typecheck, lint, unit tests, SQL tests, e2e and the Vercel previews on
every PR. Your value is what CI can't see. `lessons.md` records what past
Console upgrades needed, as rules and as a table by package. Read it before
triaging, and read the package's row before deciding on that package.

## 1. What Renovate does here

`.github/renovate.json5`, and the shared `MaterializeInc/renovate-config`
preset it extends, are the source of truth. Read them when this summary and a
PR disagree.

- A release waits 3 days (`minimumReleaseAge`) before Renovate proposes it.
- Minor, patch, pin and digest updates automerge once `buildkite/test` passes.
  That includes 0.x minors, which are breaking (see `lessons.md`). Majors and
  lock file maintenance wait for an approval.
- `console/package.json` updates use branch prefix `renovate/console-` and
  these groups: `console simple` (minor and patch), `console kysely`,
  `console vite` (Vite, Vitest, jsdom, rolldown), `console
  typescript-and-eslint`, `console forms`, `console jotai`, `console
  tanstack-db`, `console codemirror`. msw, downshift, `@frontegg/*` and
  `@intercom/*` get their own PRs. Chakra majors are disabled.
- npm uses `rangeStrategy: update-lockfile`, so lock file maintenance moves
  every direct and transitive package to the newest version inside its range.
- `rebaseWhen: conflicted`: a green run can be against an old `main`, and a
  branch doesn't have to be up to date to merge. Once anyone else commits to a
  Renovate branch, Renovate stops rebasing it.
- GitHub Issues are disabled in this repo, so there is no Dependency Dashboard
  to read.

## 2. Check access

```bash
gh auth status
gh api user --jq .login
```

Without `gh` auth you can still read public data, but you can't comment. Say so
at the top of your summary and continue read-only. Buildkite logs need the
Buildkite MCP server or an authenticated `bk` (see `mz-debug-ci`). Without
either, report failing check names and their Buildkite links, not error text.
Don't look for other credentials.

## 3. Find the PRs

```bash
gh pr list --repo MaterializeInc/materialize --author app/renovate --state open \
  --limit 200 --json number,title,headRefName,headRefOid,createdAt,url,body
```

With a PR number argument, triage only that PR.

Automerge can land a breaking 0.x minor or a lock file refresh before anyone
reads it, so also list what merged since the last triage (four days back for a
twice-weekly run) and review the breaking ones after the fact:

```bash
gh pr list --repo MaterializeInc/materialize --author app/renovate --state merged \
  --search "merged:>=<YYYY-MM-DD>" --json number,title,mergedAt,url,body
```

Scope: `console/` PRs get the full treatment below. For other paths (Rust
crates, Docker images, GitHub Actions, Python, `ci/test/docs-widgets/`) report
CI status and anything red, and leave the depth to the owning team's skills
(`mz-dependencies` for crates).

### Skip what you already triaged

Each triage comment starts with a marker carrying the head SHA:

```
<!-- mz-renovate-triage head=<sha> -->
```

```bash
gh api repos/MaterializeInc/materialize/issues/<N>/comments --paginate \
  --jq '.[] | select(.body | startswith("<!-- mz-renovate-triage")) | {id, body}'
```

If the marker's SHA equals the PR's `headRefOid` and the CI result hasn't
changed, skip the PR. Otherwise triage it and edit that comment in place.

## 4. Sort by risk

Read the PR body's table for each package's old and new version, and check
which `package.json` it changes. Then decide what the package does for us.
The `dependencies` section doesn't tell you, so grep:

```bash
cd console
rg -l "from \"<pkg>|require\(\"<pkg>|\"<pkg>/" src bin e2e-tests misc *.config.* *.ts *.js
jq '.scripts, .resolutions' package.json
```

| Tier | What | Treatment |
|---|---|---|
| A | Minor, patch, pin or digest from a 1.x or later version, CI green | No comment. Count it in the summary. It automerges |
| B | Major of a package that only tooling uses (lint, formatting, release scripts), not imported by the app, the tests, the build or a dev script | Section 5 steps 1, 2, 6 and 10 |
| C | A major, a 0.x minor, lock file maintenance, a bumped `resolutions` entry, or a group, touching anything the app, the build, the tests or a dev script imports. Also any package with a row in `lessons.md` | All of section 5 |
| Red | Any PR whose `buildkite/test` failed | Section 6, then its tier |

Green CI on Tier A is enough. Lock file maintenance in #40580 carried in-range
minors that broke 82 tests and `yarn build`, and CI caught them. Spend your
time on B, C and Red.

## 5. What to check that CI can't

Each step has its background and past examples in `lessons.md`.

1. **Overlap.** Look for a newer bot PR or an open human PR for the same
   package (`gh search prs --repo MaterializeInc/materialize --state open
   "<pkg>"`). If one exists, the verdict is "Superseded by #N".
2. **Release notes, every version in the range, patches included.** Renovate's
   body embeds them under "Release Notes". When truncated or missing, read
   `gh api repos/<owner>/<repo>/releases --paginate --jq '.[] | {tag_name, body}'`,
   the changelog, the migration guide, or as a last resort the source diff.
3. **Match each breaking change to our code.** Grep `console/` for every
   removed or renamed API, option, type, export path or default, and cite
   `file:line`. Say "no use found" only after grepping. Check config objects
   passed to setup calls (`vitest.setup.ts`, `vite.config.ts`,
   `eslint.config.js`, `src/api/mocks/`) for silently ignored options.
4. **Node and module format.** `npm view <pkg>@<new> engines type exports --json`
   against `engines.node` in `console/package.json`, the Volta pin, and
   `console/Dockerfile`. Flag ESM-only releases consumed through `require`.
5. **Browsers.** New built-ins or syntax the package uses at runtime, against
   browserslist `defaults`. CI only runs current Chromium.
6. **Peers, duplicates and resolutions.** `npm view <pkg>@<new>
   peerDependencies --json` against what we install. Grep `yarn.lock` for a
   second copy of identity-sensitive packages. For a bumped `resolutions`
   entry, list each consumer's declared range.
7. **Dev scripts.** Name the `package.json` scripts and `console/bin/*` that
   use the package and what running them needs. `yarn gen:types` needs Docker
   and a `materialized` image.
8. **Test infrastructure and generated output.** For test tooling, ask for
   several consecutive full-suite runs. For a query builder or layout
   algorithm, ask for an old-versus-new output diff.
9. **Holds.** Check whether the PR undoes a hold listed in `lessons.md` or
   encoded in `package.json` or `.github/renovate.json5`.
10. **Existing reviews.** Read the PR's reviews and comments, including
    automated QA reviews, and fold in what they found.
11. **What a person must check.** For UI and layout libraries, list the pages
    to open on the Vercel preview, linked from the `console-preview` check.

## 6. Red CI

Find the first real error before reading the rest. A failed `yarn build`
reddens about seven jobs at once (unit tests, both e2e jobs, both previews,
both `rust-build-*`), so start from `console-lint-and-typecheck` and the build.
Use `mz-debug-ci` when Buildkite is reachable.

Decide whether the failure is the dependency (a type error, a test the upgrade
broke) or unrelated (a job that doesn't exercise the package, a failure seen on
other PRs the same day). For the dependency case, say what has to change and
where, and whether one package in a group should be split out or held back.
Don't push the fix. Commits on Renovate's branch stop its rebases, so a fix
belongs in a human PR that supersedes it, or on the branch only if the approver
chooses to take it over.

## 7. Verdict and comment

Pick one verdict per PR:

- **Ready to approve**: CI green, and nothing in section 5 touches our code.
- **Needs changes**: name each change and its file.
- **Needs a decision**: a policy question, such as dropping a browser, raising
  `engines.node`, or a resolution moving across majors.
- **Waiting on CI**: checks still running. Say what you checked so far.
- **Superseded by #N**: recommend closing.
- For merged PRs: **Merged, fine** or **Merged, needs follow-up** with what.

Post one comment per open PR, or edit the existing one. Keep it readable in a
minute:

```markdown
<!-- mz-renovate-triage head=<sha> -->
**Ready to approve.** CI is green and none of the breaking changes in 17.0 touch the console.

- Breaking changes: tasks now stage every tracked file they modify. Used only in `console/misc/githooks/pre-commit`, which is opt-in.
- Not covered by CI: the pre-commit hook. Run it once on a staged change.
- To check by hand: none.

<details><summary>What I checked</summary>

Release notes 16.2 to 17.0, grep of `console/` for each removed option, engines, peers, scripts.
</details>
```

```bash
gh pr comment <N> --repo MaterializeInc/materialize --body-file <file>
gh api -X PATCH repos/MaterializeInc/materialize/issues/comments/<id> -F body=@<file>
```

When run by hand, show the comments and ask before posting.

## 8. Summary

End with one summary for the approver, highest risk first:

```
Renovate triage, <date>: <n> open PRs, access: gh yes, comments yes, Buildkite no
Needs changes (1): #N kysely 0.30, `limit(sql.raw)` removed, used at …
Needs a decision (1): #N …
Ready to approve (2): #N lint-staged 17, #N diff 9
Superseded (1): #N msw 3, see #39684
Merged since last run, needs follow-up (1): #N …
Automerging, no action (6)
```

## Keeping this skill current

When a triaged PR teaches something new (a check CI missed, a package that
needed a fix, a new hold), add it to `lessons.md` with the PR number.
