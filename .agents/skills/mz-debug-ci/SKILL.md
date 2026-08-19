---
name: mz-debug-ci
description: >
  Investigate CI failures on PR via gh + Buildkite MCP or bk CLI. Trigger: failing checks,
  Buildkite failures, CI issues — "why is CI red", "build broken",
  "checks failing", "what went wrong in CI", "nightly broke",
  "tests failing on this PR", or pasted Buildkite URL. Also PR number +
  why failing.
argument-hint: <PR number, GitHub PR URL, or Buildkite build URL>
---

Investigate CI failures for a Materialize PR or Buildkite build.

If the input is a Buildkite build URL rather than a PR (a scheduled nightly on
main, a release-qualification build), start directly at "Listing a
build's failed jobs directly", taking `<PIPELINE>` and `<BUILD_NUMBER>`
straight from the URL:
`https://buildkite.com/materialize/<PIPELINE>/builds/<BUILD_NUMBER>`,
then continue from Step 2.

## Prerequisites

This skill requires `gh` (GitHub CLI), authenticated, plus one of two ways
to reach Buildkite: the Buildkite MCP server's tools (preferred, next
section) or `bk` (Buildkite CLI), authenticated. If any of these turns out
to be missing or unauthenticated, **stop immediately** and tell the user
what to fix (`bk configure` or `bk auth login` for the Buildkite CLI, or the
MCP setup below). Do not attempt to use the REST API directly or any other
workaround.

## Buildkite MCP: preferred when available

When the Buildkite MCP server's tools are available in the session (tool
names like `get_build_failure_summary`, `search_logs`, ...), prefer them
over `bk` everywhere below: structured JSON with the
job-state semantics already applied, and no log cleanup needed. The server documents its
own tools; this section adds only what it does not know:

- `org_slug` is `materialize`. `get_build_failure_summary` replaces "Listing
  a build's failed jobs directly", Step 2, and often Step 3;
  `search_logs`/`read_logs`/`tail_logs` replace the `bk job log` pipelines;
  the artifact and annotation tools replace the corresponding `bk` commands.
  Everything Materialize-specific (annotation contents, `bin/ci-failures`,
  `bin/ci-shards`, Steps 4-5) applies unchanged.
- The summary's job list is bounded by the response's `job_limit` (10 on the
  hosted server, where a larger `max_jobs` is silently clamped), with terminal
  problem jobs sorted before downstream broken ones. On `jobs_truncated:
  true`, get the definitive failed-job list from `list_jobs` with
  `state: "failed,timed_out"` and `include_retried_jobs: false`. That last
  parameter is what preserves triage semantics: the server defaults it to
  true, which counts every failed attempt of a retried job separately,
  including jobs that passed on retry. The summary bounds annotation content
  too: on `content_truncated: true`, fetch the full set with
  `list_annotations`.

If the MCP is absent, read `bk-fallback.md` in this skill's directory for
the `bk` command recipes and continue with those. At the end, mention to
the user that they can set the server up (hosted read-only endpoint:
`claude mcp add --scope user --transport http buildkite https://mcp.buildkite.com/mcp/readonly`;
it saves tool calls but costs somewhat more tokens).

## Search existing failures by pattern

Search for already recorded failures in CI with a short, stable error
substring. The output is JSON whose `content` blobs are huge, so project it
down, and read the total from `.meta.totalRowCount`, which ignores the row
cap and is the "how chronic is this?" number:

```bash
bin/ci-failures 'foobar' 2>/dev/null | jq -r '.meta.totalRowCount,
  (.data[] | [.build_date[0:10], .build_identifier, .test_suite, .issue] | @tsv)'
```

The positional pattern matches failure content. `--search <text>` replaces
the positional pattern and matches across build, test, issue, content, and
branch at once. Narrow results with `--branch main`, `--version`, `--issue`,
`--test` (the Buildkite job name), `--build`, `--start-date`, or
`--end-date`; `--size` raises the row cap (default 31, max 100).

## Step 1: Find the build

Parse `$ARGUMENTS` to get the PR number (a plain number or a
`https://github.com/MaterializeInc/materialize/pull/<PR_NUMBER>` URL), then
list the failing checks:

```bash
gh pr checks <PR_NUMBER> 2>&1
```

Failing rows link to
`https://buildkite.com/materialize/<PIPELINE>/builds/<BUILD_NUMBER>#<JOB_ID>`,
the identifiers used throughout this skill.

### Listing a build's failed jobs directly

Buildkite mirrors each job's result into GitHub as a
`buildkite/<pipeline>/<job>` commit status (never-dispatched jobs post
nothing), but `gh pr checks` reads the PR's current head commit only. A build
therefore shows up there only if it ran on exactly that commit. For everything
else (scheduled nightlies on main, release-qualification builds, a PR nightly
from before the latest push), list the genuinely failed jobs via the API:
`list_jobs` as described in the MCP section, or with `bk` the listing recipe
in `bk-fallback.md`. Either way, also grab the build's commit and branch:
Step 5's known-vs-new checks need the commit, and neighboring-build lookups
need the branch.

Job-state semantics matter here:
- The failure states are `failed` and `timed_out`, the same set as
  `BUILDKITE_RELEVANT_FAILED_BUILD_STEP_STATES` in
  `misc/python/materialize/buildkite_insights/buildkite_api/buildkite_constants.py`.
- `broken` is not a failure. Buildkite marks a job `broken` when its
  configuration prevents it from running, for example a branch filter or an
  `if:` condition that evaluates false, so the job never dispatches. Every
  build contains broken jobs (a routine main nightly has dozens), so counting
  them massively over-counts failures.
- Jobs with `soft_failed: true` are allowed to fail without making the build
  red; no current CI step sets it, so filtering it is defensive.
- `waiting_failed` is not a failure either: the job's dependency (usually a
  build step) failed, so it never started. Its presence is informative,
  though. A PR build full of `waiting_failed` jobs ran no tests at all, so
  report that fixing the visible failures is necessary but no proof the
  build then goes green.
- Retried jobs (automatic or manual) appear only as their latest attempt, so
  a job that failed and then passed on retry counts as passed. That is the
  right verdict for build triage; fetch the earlier attempts only when an
  in-depth investigation needs them. A manual retry that failed again is
  itself a signal: someone already tried to shake the failure off and it
  reproduced.

To find a PR's builds that are not on its current head in the first place (an
older nightly, a pre-push run), get the branch with
`gh pr view <PR_NUMBER> --json headRefName` and filter Buildkite builds by
branch; the MCP's `list_builds` takes a `branch:` filter, and fork branches
are named `<owner>:<branch>` on Buildkite.

## Step 2: Check annotations first

**Before diving into logs**, fetch the build annotations. They contain pre-extracted error messages, stack traces, and links to known flaky test issues — this saves significant time compared to grepping through raw logs. With the MCP they arrive in `get_build_failure_summary` (or `list_annotations`); with `bk`, use the annotations recipe in `bk-fallback.md`.

Besides the error itself, an error annotation (`style` `"error"`) carries
known-issue links (Linear keys like `CPU-170`, or legacy
`database-issues/#NNNN`) and the job's main-branch history, a flaky-test
indicator.

Things to know when reconciling annotations against jobs:

- Error annotations persist from failed attempts even when a retry later
  passed, so a build can have more error annotations than failed jobs. A
  `bin/ci-annotate-errors` annotation's `context` field is `<JOB_ID>-error`,
  and that job id can belong to a retried attempt that the default job
  listing does not contain. (Other producers use other contexts, e.g.
  `images-not-public` from the build step.)
- Only jobs running through the mzcompose/cloudtest plugins get annotations
  and `bin/ci-failures` rows (both come from `bin/ci-annotate-errors`). Plain
  shell steps (lint, Security advisories, and similar checks) produce neither,
  so a real failure there has no annotation at all. Go straight to its job
  log.
- Info-style annotations ("<job> succeeded with known error logs") mean the
  job passed and every logged error matched an open known issue; they are
  not failures. When a job went red via "Test succeeded, but unknown errors
  found in logs" (see Step 3), the unknown and potential-regression entries
  in its annotation are what turned it red. The reverse also exists: a
  failed job turns green ("<job> would have failed with known error logs")
  when all its logged errors match issues marked `ci-ignore-failure: true`.

## Step 3: Fetch logs when needed

Only fetch full logs when annotations don't provide enough detail. On PR test
builds, read compile and lint job logs first (clippy, lint-and-rustfmt): they
often explain every downstream failure.

Fetch and search logs with the MCP's `search_logs`/`read_logs`/`tail_logs`,
or with `bk` use the job-log recipes in `bk-fallback.md`. The log tail can be a
red herring: some jobs print long non-error output after the actual error
(cargo deny's dependency tree runs hundreds of lines past the advisory), so
when the tail shows no error, search or grep for the error instead of
tailing more.

NOTE: for mzcompose-based jobs (testdrive, SQLsmith/SQLancer, platform checks,
...) the job console log holds only the harness's output. The services' own
output (environmentd/clusterd panics and errors) goes to `services.log` and
similar files inside the job's log artifacts. A panic that is absent from
the job's console log can still be the failure, so never conclude "this
error did not happen in this run" from it alone. Check the annotations or
`bin/ci-failures` instead, both are fed by `bin/ci-annotate-errors`, which
scans the uploaded log artifacts (`services.log`, `run.log`, junit XML, ...)
at the end of each job. The same scan can fail a job whose own workflow
passed: the console log then reads `Test succeeded, but unknown errors found
in logs, marking as failed`, and the cause is whatever the scan found in the
artifacts, not a test failure. Alternatively, download the log artifacts and
grep them directly.

### Artifacts

Jobs upload artifacts (junit XML, service logs, coredumps, ...). Use the
MCP's `list_artifacts_for_build`/`list_artifacts_for_job`/`get_artifact`,
or with `bk` use the artifacts recipes in `bk-fallback.md`.

### Shard contents

Sharded jobs (SLT, testdrive, platform checks, feature benchmark, ...) record
the workflows/files/scenarios they ran in build meta-data. `bin/ci-shards`
shows it:

```bash
# Mapping of every sharded job to what it ran, with a link per job whose
# `#` fragment is the `<JOB_ID>` for fetching job logs (omit links with --no-url)
bin/ci-shards https://buildkite.com/materialize/<PIPELINE>/builds/<BUILD_NUMBER>

# Which job(s) ran a specific item (test file, scenario, or workflow,
# `workflow_foo_bar` function names are matched as `foo-bar`)
bin/ci-shards https://buildkite.com/materialize/<PIPELINE>/builds/<BUILD_NUMBER> numeric.td
```

## Step 4: Categorize failures

Use these Materialize-specific patterns to diagnose:

### Clippy errors
Code lint issues in changed files. Fix the code, not the lint config.

### `check-test-flags` lint failure
A new configuration flag was introduced but not registered in the required places:
- `misc/python/materialize/parallel_workload/action.py` (FlipFlagsAction)
- `misc/python/materialize/mzcompose/__init__.py` (get_variable_system_parameters / get_minimal_system_parameters / UNINTERESTING_SYSTEM_PARAMETERS)

### Testdrive cascades
After one test crashes environmentd, all subsequent tests in that shard fail with `Name or service not known` or `connection closed`. **Only the first failure in a shard matters** — everything after it is a cascade. Look for the first `error:` or `FAIL` in the log.

Testdrive shards with the same number (e.g., `testdrive-10` and `testdrive-with-alloydb-10`) run the same tests — if both fail, it's likely to be the same root cause.

### SLT failures
Check whether it's wrong output (behavioral change) vs. connection error (crash/timeout). Wrong output means the query semantics changed. An `InconsistentViewOutcome` failure comes from CI's `--auto-index-selects` mode; the mz-test skill explains the mechanism and the exemption lists.

### Timeouts
A `timed_out` job hit its step budget (`timeout_in_minutes` in
`ci/<pipeline>/pipeline.template.yml`). The annotation records only the fact
("test timed out"), never the cause. Find the cause in the console log: the
last completed unit of work, plus anything marked as still running when the
job was canceled (nextest prints `SLOW [>1200.000s]` lines naming the stuck
tests). If no such marker appears, compare wall-clock against the job's last
passing run to tell a gradually outgrown budget from a hung test.

### Rarer classes

If the failure matches one of these, read `rare-failures.md` in this
skill's directory for its triage pattern: `New regression against
<version>` (feature benchmark), a cargo-fuzz crash, a Miri failure, a
limits-test failure, or `ImagesNotPublicError`.

## Step 5: Summarize

Group failures by **root cause**, not by job name — typically many failing
jobs share just 1-2 root causes. Per root cause, report a description, the
affected jobs, and what to fix.

Distinguish between issues that are clearly caused by the change under test vs. pre-existing flaky tests. The annotations often link to known flaky test issues — use these to identify pre-existing flakes vs. new regressions.

More ways to establish known vs new:

- Check whether main already fixed it. Any build can run code that is behind
  main: a nightly investigated the morning after, but also a fresh PR based
  on an older main. From an up-to-date main checkout run
  `git log <BUILD_COMMIT>..HEAD -- <suspect-file>` or
  `git log -S '<error token>'`. A fix can also sit in a not-yet-merged PR,
  invisible to git log: look at recently opened PRs for one that already
  addresses the failure (a PR's file list shows which of the build's root
  causes it covers). When citing a later build as evidence
  of a fix, confirm the specific job's state there is `passed`: a build can
  be green because the job was `broken` and never ran.
- Known-issue tracking lives in Linear. Annotations and `bin/ci-failures`
  cite issue keys like `CPU-170` or `SS-361`, which resolve to
  `https://linear.app/materializeinc/issue/<KEY>`. Legacy references point at
  GitHub `MaterializeInc/database-issues` instead.
- Annotations and `bin/ci-failures` classify a failure three ways; for the
  latter two, check the signature's history with `bin/ci-failures` before
  treating the failure as new. A known-issue link means it matched an open
  tracked issue. `"UNKNOWN ERROR"` means it matched no tracked issue, not
  that the failure is new: a long streak under that label is a chronic
  untracked flake worth filing. An issue reference tagged
  `(POTENTIAL REGRESSION)` means it matched a tracked issue that is now
  closed: a known signature with stale tracking, not by itself evidence of
  a new regression; suggest to the user that the closed issue may need
  reopening. The issue matching is a content heuristic and often wrong, so
  verify that the matched issue actually describes the problem: search the
  tracker for the error text, since the right issue may already exist open,
  or the problem may need a totally new one.
