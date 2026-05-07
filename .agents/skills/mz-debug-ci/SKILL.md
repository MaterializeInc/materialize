---
name: mz-debug-ci
description: >
  Investigate CI failures on a PR using gh and bk CLI tools. Trigger when the
  user asks about failing checks, Buildkite failures, or CI issues — including
  casual phrases like "why is CI red", "build broken", "checks failing", "what
  went wrong in CI", "nightly broke", "tests failing on this PR", or pastes a
  Buildkite URL. Also trigger when the user mentions a specific PR number and
  wants to understand why it's failing.
argument-hint: <PR number or GitHub PR URL>
---

Investigate CI failures for a Materialize PR.

## Prerequisites

This skill requires both `gh` (GitHub CLI) and `bk` (Buildkite CLI) to be installed *and authenticated*. Before doing anything else, verify both:

```bash
which gh && gh auth status
which bk && bk auth status
```

If either tool is missing or unauthenticated, **stop immediately** and tell the user what to fix (`bk configure` or `bk auth login` for Buildkite). Do not attempt to use the REST API directly or any other workaround — this workflow only works with these CLI tools.

Both `gh` and `bk` make network requests that are blocked by the default sandbox. All Bash commands in this workflow must use `dangerouslyDisableSandbox: true`.

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

## Step 3: Check annotations first

**Before diving into logs**, fetch the build annotations. They contain pre-extracted error messages, stack traces, and links to known flaky test issues — this saves significant time compared to grepping through raw logs.

```bash
bk api /pipelines/<PIPELINE>/builds/<BUILD_NUMBER>/annotations --no-pager 2>&1
```

The response is JSON. Each annotation has:
- `style`: `"error"` for failures
- `body_html`: HTML containing the error summary, including:
  - The specific test/job that failed
  - The actual error message or stack trace in `<pre><code>` blocks
  - Links to known flaky test issues (look for GitHub issue links like `database-issues/#NNNN`)
  - Main branch history showing if this test passes on main (flaky test indicator)

Parse the error annotations to get a quick overview of all failures before fetching any logs.

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
bk job log <JOB_ID> -p <PIPELINE> -b <BUILD_NUMBER> --no-timestamps --no-pager 2>&1 | tail -100
```

For large logs, first grep for errors to find the relevant section:
```bash
bk job log <JOB_ID> -p <PIPELINE> -b <BUILD_NUMBER> --no-timestamps --no-pager 2>&1 | grep -B2 -A5 'error\|FAIL\|panicked'
```

Fetch multiple job logs in parallel when they are independent (e.g., clippy + lint at the same time).

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

## Step 6: Summarize

Group failures by **root cause**, not by job name. Typically many failing jobs share just 1-2 root causes. Present the summary as:

1. **Root cause A** — description, which jobs it affects, what to fix
2. **Root cause B** — description, which jobs it affects, what to fix

Distinguish between issues that are clearly caused by the PR's changes vs. pre-existing flaky tests. The annotations often link to known flaky test issues (GitHub `database-issues` links) — use these to identify pre-existing flakes vs. regressions introduced by the PR.
