---
name: mz-debug-ci
description: >
  Investigate CI failures on PR via gh + bk CLI. Trigger: failing checks,
  Buildkite failures, CI issues — "why is CI red", "build broken",
  "checks failing", "what went wrong in CI", "nightly broke",
  "tests failing on this PR", or pasted Buildkite URL. Also PR number +
  why failing.
argument-hint: <PR number, GitHub PR URL, or Buildkite build URL>
---

Investigate CI failures for a Materialize PR.

If the input is a Buildkite build URL rather than a PR (a scheduled nightly on
main, a release-qualification build), skip Steps 1-2 and start at "Listing a
build's failed jobs directly", taking `<PIPELINE>` and `<BUILD_NUMBER>`
straight from the URL:
`https://buildkite.com/materialize/<PIPELINE>/builds/<BUILD_NUMBER>`.

## Prerequisites

This skill requires both `gh` (GitHub CLI) and `bk` (Buildkite CLI) to be installed *and authenticated*. Before doing anything else, verify both:

```bash
which gh && gh auth status
which bk && bk auth status
```

If either tool is missing or unauthenticated, **stop immediately** and tell the user what to fix (`bk configure` or `bk auth login` for Buildkite). Do not attempt to use the REST API directly or any other workaround — this workflow only works with these CLI tools.

Both `gh` and `bk` make network requests that are blocked by the default sandbox. All Bash commands in this workflow must use `dangerouslyDisableSandbox: true`.

## Search existing failures by pattern

Search for already recorded failures in CI with a short, stable error substring:

```bash
bin/ci-failures 'foobar'
```

Use `--search` for the global search. Narrow results with `--branch main`,
`--version`, `--issue`, `--test`, `--build`, `--start-date`, or `--end-date`.
The command explains how to create a token when needed.

## Step 1: Extract PR number

Parse `$ARGUMENTS` to get the PR number. Handle both formats:
- Plain number: `35192`
- Full URL: `https://github.com/MaterializeInc/materialize/pull/35192`

## Step 2: Find the build

Use `gh` to get the PR's branch name and then find the Buildkite build:

```bash
# Get the branch name for the PR
gh pr view <PR_NUMBER> --json headRefName --jq .headRefName
```

Alternatively, list failing checks directly:
```bash
gh pr checks <PR_NUMBER> 2>&1
```

Lines containing `fail` have tab-separated fields:
```
name	fail	0	https://buildkite.com/materialize/<PIPELINE>/builds/<BUILD>#<JOB_ID>	description
```

Extract from the URL:
- **Pipeline**: path segment after `materialize/` (usually `test`)
- **Build number**: the number after `builds/`
- **Job ID**: the UUID after `#`

### Listing a build's failed jobs directly

Buildkite mirrors each job's result into GitHub as a
`buildkite/<pipeline>/<job>` commit status (never-dispatched jobs post
nothing), but `gh pr checks` reads the PR's current head commit only. A build
therefore shows up there only if it ran on exactly that commit. For everything
else (scheduled nightlies on main, release-qualification builds, a PR nightly
from before the latest push), list the genuinely failed jobs via the API:

```bash
bk api "/pipelines/<PIPELINE>/builds/<BUILD_NUMBER>" --no-pager 2>/dev/null \
  | jq -r '.jobs[] | select(.state=="failed" or .state=="timed_out") | select(.soft_failed != true) | .name'
```

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
  red, so exclude them from failure counts.
- Auto-retried jobs appear only as their latest attempt, so a job that failed
  and then passed on retry counts as passed. That is the right verdict for
  build triage. Add `?include_retried_jobs=true` to the build endpoint only
  when an in-depth investigation needs the earlier attempts.

## Step 3: Check annotations first

**Before diving into logs**, fetch the build annotations. They contain pre-extracted error messages, stack traces, and links to known flaky test issues — this saves significant time compared to grepping through raw logs.

```bash
bk api /pipelines/<PIPELINE>/builds/<BUILD_NUMBER>/annotations --no-pager 2>/dev/null
```

Note that `bk` can't be piped through `2>&1`.

The response is JSON. Each annotation has:
- `style`: `"error"` for failures
- `body_html`: HTML containing the error summary, including:
  - The specific test/job that failed
  - The actual error message or stack trace in `<pre><code>` blocks
  - Links to known flaky test issues (Linear keys like `CPU-170`, or legacy GitHub links like `database-issues/#NNNN`)
  - Main branch history showing if this test passes on main (flaky test indicator)

Parse the error annotations to get a quick overview of all failures before fetching any logs.

Two things to know when reconciling annotations against jobs:

- Error annotations persist from failed attempts even when a retry later
  passed, so a build can have more error annotations than failed jobs. An
  annotation's `context` field is `<JOB_ID>-error`, and that job id can belong
  to a retried attempt that the default job listing does not contain.
- Only jobs running through the mzcompose/cloudtest plugins get annotations
  and `bin/ci-failures` rows (both come from `bin/ci-annotate-errors`). Plain
  shell steps (lint, Security advisories, and similar checks) produce neither,
  so a real failure there has no annotation at all. Go straight to its job
  log.

## Step 4: Fetch logs when needed

Only fetch full logs when annotations don't provide enough detail. Triage in this order:

1. **clippy** — compilation/lint errors that often explain everything
2. **lint-and-rustfmt** — formatting and lint-check failures
3. **cargo-test** — unit/integration test failures
4. **fast-sql-logic-tests** — SLT failures
5. **testdrive** — integration test failures (often cascading)
6. **Everything else** (checks-parallel, cluster-tests, dbt, etc.)

To fetch a job's log:
```bash
bk job log <JOB_ID> -p <PIPELINE> -b <BUILD_NUMBER> --no-timestamps --no-pager 2>/dev/null | tail -100
```

For large logs, first grep for errors to find the relevant section:
```bash
bk job log <JOB_ID> -p <PIPELINE> -b <BUILD_NUMBER> --no-timestamps --no-pager 2>/dev/null | grep -B2 -A5 'error\|FAIL\|panicked'
```

The log tail can be a red herring: some jobs print long non-error output after
the actual error (cargo deny's dependency tree runs hundreds of lines past the
advisory), so when the tail shows no error, switch to the grep form instead of
tailing more. Buildkite logs are also full of ANSI escapes and embedded
carriage returns. Normalize before grepping context:

```bash
bk job log ... 2>/dev/null | tr '\r' '\n' | sed -e 's/\x1b\[[0-9;]*m//g' | grep ...
```

Fetch multiple job logs in parallel when they are independent (e.g., clippy + lint at the same time).

NOTE: for mzcompose-based jobs (testdrive, SQLsmith/SQLancer, platform checks,
...) the job console log holds only the harness's output. The services' own
output (environmentd/clusterd panics and errors) goes to `services.log` and
similar files inside the job's log artifacts. A panic that is absent from
`bk job log` can still be the failure, so never conclude "this error did not
happen in this run" from the console log alone. Check the annotations or
`bin/ci-failures` instead, both are fed by `bin/ci-annotate-errors`, which
scans the uploaded log artifacts (`services.log`, `run.log`, junit XML, ...)
at the end of each job. Or download the log artifacts and grep them directly.

### Artifacts

Jobs upload artifacts (junit XML, service logs, coredumps, ...). Use the
`bk artifacts` subcommands:

```bash
# All artifacts of a build (each entry has id, job_id, path)
bk artifacts list <BUILD_NUMBER> -p <PIPELINE> --no-pager 2>/dev/null
# Artifacts of a single job
bk artifacts list <BUILD_NUMBER> -p <PIPELINE> --job-uuid <JOB_ID> --no-pager 2>/dev/null
# Download one artifact into the current directory
bk artifacts download <ARTIFACT_ID> --build <BUILD_NUMBER> -p <PIPELINE>
```

NOTE: if bk rejects one of these flags as unknown, the installed bk predates
the April 2026 artifacts rework. Check `bk artifacts <subcommand> --help` for
the local spelling, and recommend to the user that they upgrade bk.

Do not use `bk api` for artifacts. The plural
`/pipelines/<PIPELINE>/builds/<BUILD_NUMBER>/artifacts` endpoint works but
silently returns only the first 30 entries, and its `.../download` endpoint
fails to parse.

### Shard contents

Sharded jobs (SLT, testdrive, platform checks, feature benchmark, ...) record
the workflows/files/scenarios they ran in build meta-data. `bin/ci-shards`
shows it:

```bash
# Mapping of every sharded job to what it ran, with a link per job whose
# `#` fragment is the `<JOB_ID>` for `bk job log` (omit links with --no-url)
bin/ci-shards https://buildkite.com/materialize/<PIPELINE>/builds/<BUILD_NUMBER>

# What a single job ran (job link as copied from the Buildkite UI)
bin/ci-shards 'https://buildkite.com/materialize/<PIPELINE>/builds/<BUILD_NUMBER>#<JOB_ID>'

# Which job(s) ran a specific item (test file, scenario, or workflow,
# `workflow_foo_bar` function names are matched as `foo-bar`)
bin/ci-shards https://buildkite.com/materialize/<PIPELINE>/builds/<BUILD_NUMBER> numeric.td
```

## Step 5: Categorize failures

Use these Materialize-specific patterns to diagnose:

### Clippy errors
Code lint issues in changed files. Common ones: `as_conversions`, `needless_borrow`, `clone_on_ref_ptr`. Fix the code, not the lint config.

### `check-test-flags` lint failure
A new configuration flag was introduced but not registered in the required places:
- `misc/python/materialize/parallel_workload/action.py` (FlipFlagsAction)
- `misc/python/materialize/mzcompose/__init__.py` (get_variable_system_parameters / get_minimal_system_parameters / UNINTERESTING_SYSTEM_PARAMETERS)

### Cargo test failures
Read the panic message or assertion diff. Common patterns:
- `unwrap_err() on Ok` → test expected an error but the code now succeeds
- `assertion left == right failed` → behavioral change in output

### Testdrive cascades
After one test crashes environmentd, all subsequent tests in that shard fail with `Name or service not known` or `connection closed`. **Only the first failure in a shard matters** — everything after it is a cascade. Look for the first `error:` or `FAIL` in the log.

Testdrive shards with the same number (e.g., `testdrive-10` and `testdrive-with-alloydb-10`) run the same tests — if both fail, it's likely to be the same root cause.

### SLT failures
Check whether it's wrong output (behavioral change) vs. connection error (crash/timeout). Wrong output means the query semantics changed.

### Timeouts
A `timed_out` job hit its step budget (`timeout_in_minutes` in
`ci/<pipeline>/pipeline.template.yml`). The annotation records only the fact
("test timed out"), never the cause. Find the cause in the console log: the
last completed unit of work, plus anything marked as still running when the
job was canceled (nextest prints `SLOW [>1200.000s]` lines naming the stuck
tests). Compare against the wall-clock of the same job's last passing run to
tell a newly hung test from a budget the job has gradually outgrown.

## Step 6: Summarize

Group failures by **root cause**, not by job name. Typically many failing jobs share just 1-2 root causes. Present the summary as:

1. **Root cause A** — description, which jobs it affects, what to fix
2. **Root cause B** — description, which jobs it affects, what to fix

Distinguish between issues that are clearly caused by the PR's changes vs. pre-existing flaky tests. The annotations often link to known flaky test issues — use these to identify pre-existing flakes vs. regressions introduced by the PR.

More ways to establish known vs new:

- Check whether main already fixed it. Any build can run code that is behind
  main: a nightly investigated the morning after (often already triaged and
  fixed by a "ci: Nightly fixes" PR), but also a fresh PR based on an older
  main. From an up-to-date main checkout run
  `git log <BUILD_COMMIT>..HEAD -- <suspect-file>` or
  `git log -S '<error token>'`.
- Known-issue tracking lives in Linear. Annotations and `bin/ci-failures`
  cite issue keys like `CPU-170` or `SS-361`, which resolve to
  `https://linear.app/materializeinc/issue/<KEY>`. Legacy references point at
  GitHub `MaterializeInc/database-issues` instead. The
  `MaterializeInc/materialize` repo itself has issues disabled, so `gh issue`
  commands against it error out.
- In `bin/ci-failures` output, `"issue": "UNKNOWN ERROR"` means the failure
  matched no tracked issue.
