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

This skill requires `gh` (GitHub CLI), installed *and authenticated*, plus one
of two ways to reach Buildkite: the Buildkite MCP server's tools (preferred,
next section) or `bk` (Buildkite CLI), installed *and authenticated*. Before
doing anything else, verify what is available:

```bash
which gh && gh auth status
which bk && bk auth status   # only needed when the Buildkite MCP is absent
```

If `gh` is missing or unauthenticated, or neither Buildkite path works,
**stop immediately** and tell the user what to fix (`bk configure` or
`bk auth login` for the Buildkite CLI, or the MCP setup below). Do not attempt
to use the REST API directly or any other workaround.

Network commands (`gh`, `bk`, `bin/ci-failures`, `bin/ci-shards`) are blocked
by the default sandbox and need `dangerouslyDisableSandbox: true`. Local
`git`/`grep` work over the checkout does not.

`bk` writes warnings to stderr, including a benign `BUILDKITE_API_TOKEN is
overriding the credential stored for this organization` on healthy setups.
Pipe `bk` output with `2>/dev/null`, never `2>&1`, or the noise corrupts the
JSON.

## Buildkite MCP: preferred when available

When the Buildkite MCP server's tools are available in the session (tools
named `get_build_failure_summary`, `search_logs`, ..., usually under an
`mcp__buildkite__` prefix, though the prefix depends on what the user named
the server), prefer them over `bk` everywhere below: structured JSON with the
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
  including jobs that passed on retry. The default summary detail level has
  no `soft_failed` field, which is currently harmless: no Materialize CI
  step sets `soft_fail`, and `detail_level: "full"` exposes it (with
  `retried`) if that changes. The summary bounds annotation content too: on
  `content_truncated: true`, fetch the full set with `list_annotations`.

If the MCP is absent, continue with `bk` and, at the end, recommend to the
user that they set the server up (hosted read-only endpoint:
`claude mcp add --scope user --transport http buildkite https://mcp.buildkite.com/mcp/readonly`).

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
`--end-date`; `--size` raises the row cap (default 31, max 100). The command
explains how to create a token when needed.

## Step 1: Find the build

Parse `$ARGUMENTS` to get the PR number (a plain number or a
`https://github.com/MaterializeInc/materialize/pull/<PR_NUMBER>` URL), then
list the failing checks:

```bash
gh pr checks <PR_NUMBER> 2>&1
```

Lines containing `fail` have tab-separated fields:
```
name	fail	0	https://buildkite.com/materialize/<PIPELINE>/builds/<BUILD_NUMBER>#<JOB_ID>	description
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
  | jq -r '.jobs[] | select(.state=="failed" or .state=="timed_out") | select(.soft_failed != true) | [.state, .id, .name] | @tsv'
```

The same response carries the build's `.commit` and `.branch`, which the
known-vs-new checks in Step 5 need, so grab them here.

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
- `waiting_failed` is not a failure either: the job's dependency (usually a
  build step) failed, so it never started. Its presence is informative,
  though. A PR build full of `waiting_failed` jobs ran no tests at all, so
  report that fixing the visible failures is necessary but no proof the
  build then goes green.
- Retried jobs (automatic or manual) appear only as their latest attempt, so
  a job that failed and then passed on retry counts as passed. That is the
  right verdict for build triage. Add `?include_retried_jobs=true` to the
  build endpoint only when an in-depth investigation needs the earlier
  attempts. A manual retry that failed again is itself a signal: someone
  already tried to shake the failure off and it reproduced.

To find a PR's builds that are not on its current head in the first place (an
older nightly, a pre-push run), get the branch with
`gh pr view <PR_NUMBER> --json headRefName` and filter Buildkite builds by
branch; the MCP's `list_builds` takes a `branch:` filter, and fork branches
are named `<owner>:<branch>` on Buildkite.

## Step 2: Check annotations first

**Before diving into logs**, fetch the build annotations. They contain pre-extracted error messages, stack traces, and links to known flaky test issues — this saves significant time compared to grepping through raw logs:

```bash
bk api /pipelines/<PIPELINE>/builds/<BUILD_NUMBER>/annotations --no-pager 2>/dev/null \
  | jq -r '.[] | select(.style=="error") | "=== \(.context)\n\(.body_html)"' \
  | sed -e 's/<[^>]*>//g' | grep -v '^$'
```

The response is JSON: `style` is `"error"` for failures, and `body_html`
holds the error summary — the failing test/job, the error or stack trace,
known-issue links (Linear keys like `CPU-170`, or legacy
`database-issues/#NNNN`), and the job's main-branch history (a flaky-test
indicator).

Parse the error annotations to get a quick overview of all failures before fetching any logs.

Two things to know when reconciling annotations against jobs:

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

## Step 3: Fetch logs when needed

Only fetch full logs when annotations don't provide enough detail. On PR test
builds, read compile and lint job logs first (clippy, lint-and-rustfmt): they
often explain every downstream failure.

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
tailing more. Buildkite logs are also full of ANSI escapes (colors, plus
cursor-movement and OSC sequences from docker pulls) and embedded carriage
returns that leave doubled blank lines, halving grep's `-A`/`-B` reach.
Normalize before grepping context:

```bash
bk job log ... 2>/dev/null | tr '\r' '\n' \
  | sed -e 's/\x1b\[[0-9;]*[a-zA-Z]//g' -e 's/\x1b\][^\x07]*\x07//g' | grep -v '^$' | grep ...
```

NOTE: for mzcompose-based jobs (testdrive, SQLsmith/SQLancer, platform checks,
...) the job console log holds only the harness's output. The services' own
output (environmentd/clusterd panics and errors) goes to `services.log` and
similar files inside the job's log artifacts. A panic that is absent from
`bk job log` can still be the failure, so never conclude "this error did not
happen in this run" from the console log alone. Check the annotations or
`bin/ci-failures` instead, both are fed by `bin/ci-annotate-errors`, which
scans the uploaded log artifacts (`services.log`, `run.log`, junit XML, ...)
at the end of each job. The same scan can fail a job whose own workflow
passed: the console log then reads `Test succeeded, but unknown errors found
in logs, marking as failed`, and the cause is whatever the scan found in the
artifacts, not a test failure. Alternatively, download the log artifacts and
grep them directly.

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

NOTE: if `bk` rejects one of these flags as unknown, the installed `bk`
predates the April 2026 artifacts rework. Check
`bk artifacts <subcommand> --help` for the local spelling, and recommend to
the user that they upgrade `bk`.

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

## Step 4: Categorize failures

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

### Feature-benchmark regressions

A `New regression against <version>` annotation shows one comparison table,
but the harness re-runs every flagged scenario and annotates only the last
run. Grep the console log for `Run [0-9] with scenarios` and `!!YES!!`:
scenarios that drop out across runs were noise the harness filtered out, one
that survives every run reproduces on that agent. Even that does not prove a
code cause, because agent-level speed offsets are stable within a job. The
decisive control is the same scenario in neighboring builds of the same
pipeline; on release qualification, the rc build of the released version is
an identical-code control (the tag differs only by the version bump), so an
opposite verdict there proves noise. Compare absolute THIS/OTHER values, not
percentages, since agents differ several-fold in baseline speed. Get the
signature's base rate with `bin/ci-failures '<ScenarioName>'`.

### cargo-fuzz crashes

The fuzz target's own panic text goes to `target/fuzz-logs/<target>.log`,
which is never uploaded, so neither the console log nor the artifacts
contain it. Take the `reproduce:` command from the annotation and run it
locally. For known-vs-new use `bin/ci-failures '<target name>'`; cargo-fuzz
runs only in release qualification, so that is its entire history.

### `ImagesNotPublicError`

A PR adding a new publishable mzbuild image fails `:rust: Build x86_64` with
the `images-not-public` annotation until someone makes the image's
DockerHub/GHCR repos public. The check runs only on the x86_64 build, so
aarch64 passing while x86_64 fails is expected, not an arch bug. The fix is
a process step (the annotation names whom to ask), or `publish: false` in
the image's `mzbuild.yml` if it is not meant to be published.

## Step 5: Summarize

Group failures by **root cause**, not by job name. Typically many failing jobs share just 1-2 root causes. Present the summary as:

1. **Root cause A** — description, which jobs it affects, what to fix
2. **Root cause B** — description, which jobs it affects, what to fix

Distinguish between issues that are clearly caused by the change under test vs. pre-existing flaky tests. The annotations often link to known flaky test issues — use these to identify pre-existing flakes vs. new regressions.

More ways to establish known vs new:

- Check whether main already fixed it. Any build can run code that is behind
  main: a nightly investigated the morning after, but also a fresh PR based
  on an older main. From an up-to-date main checkout run
  `git log <BUILD_COMMIT>..HEAD -- <suspect-file>` or
  `git log -S '<error token>'`. A fix can also sit in a not-yet-merged PR,
  invisible to git log. Recent nightly reds often have a dedicated fix PR
  whose body cites "Based on <build URL>", typically titled "ci: Nightly
  fixes (<date>)", though fixes land in other PRs too:
  `gh pr list --search "Nightly fixes in:title" --state all` finds the
  dedicated ones, and the PR's file list shows which of the build's root
  causes it covers. When citing a later build as evidence of a fix, confirm
  the specific job's state there is `passed`: a build can be green because
  the job was `broken` and never ran.
- Known-issue tracking lives in Linear. Annotations and `bin/ci-failures`
  cite issue keys like `CPU-170` or `SS-361`, which resolve to
  `https://linear.app/materializeinc/issue/<KEY>`. Legacy references point at
  GitHub `MaterializeInc/database-issues` instead. The
  `MaterializeInc/materialize` repo itself has issues disabled, so `gh issue`
  commands against it error out.
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
