---
name: mz-buildkite
description: >
  This skill should be used when the user wants to check CI status, investigate
  build failures, download build artifacts, view job logs, or interact with
  Buildkite in the Materialize repository. Trigger when the user mentions
  Buildkite, CI failures, build status, flaky CI, job logs, build artifacts,
  or wants to understand why a PR's CI is failing.
argument-hint: "[PR number or build number]"
allowed-tools: [Bash, Read, Grep, Glob]
---

# Buildkite CI for Materialize

Materialize CI runs on Buildkite. The main pipeline is `test`.
Use `bk` (Buildkite CLI) and `gh` (GitHub CLI) to interact with builds.

## Resolving a build from a PR

PR checks link directly to Buildkite builds.
Extract the build number and pipeline from the check URLs.

```bash
# Get all checks for a PR as JSON, filtering to Buildkite checks
gh pr checks <PR> --repo MaterializeInc/materialize \
  --json name,link,state,bucket \
  | jq '[.[] | select(.link | startswith("https://buildkite.com/"))]'
```

The top-level check (e.g. `buildkite/test`) contains the build URL.
Extract the build number from the URL: `https://buildkite.com/materialize/test/builds/<NUMBER>`.
Individual job checks have the job ID as a URL fragment: `...builds/<NUMBER>#<JOB_ID>`.

## Checking build status

```bash
# View a build (text output for quick overview)
bk build view <BUILD_NUMBER> -p test

# View as JSON for programmatic access
bk build view <BUILD_NUMBER> -p test --json

# View the latest build on the current branch
bk build view -p test
```

The build JSON contains:
* `state`: `passed`, `failed`, `canceled`, `running`, `scheduled`
* `jobs`: array of jobs with `id`, `name`, `state`, `type`, `step_key`, `web_url`
* `pull_request`: `{id, base, repository}` if triggered by a PR
* `annotations`: build annotations with `body_html`, `context`, and `style` (info/error/warning)

## Listing failed jobs

From the build JSON, filter jobs to find failures:

```bash
bk build view <BUILD_NUMBER> -p test --json \
  | jq '[.jobs[] | select(.state == "failed" and .type == "script") | {id, name, state, step_key, web_url}]'
```

Job states: `passed`, `failed`, `timed_out`, `canceled`, `running`, `waiting`, `blocked`, `broken`.
Job type `script` are actual test/build steps; `waiter` and `trigger` are control flow.
A `broken` state means the job never ran (dependency failed).

## Reading error annotations (primary failure info)

Build annotations are the richest source of failure details.
Error annotations (style=`error`) contain the test name, error message, stack trace, and reproducer command.
The `body_html` field contains HTML; strip tags for readable output.

```bash
# Extract error annotations as readable text
bk build view <BUILD_NUMBER> -p test --json \
  | python3 -c "
import json, sys, html, re
d = json.load(sys.stdin)
for a in d.get('annotations', []):
    if a.get('style') == 'error':
        text = re.sub('<[^>]+>', '', a.get('body_html', ''))
        print(html.unescape(text))
        print('---')
"
```

## Getting job logs

Note: `bk job log` requires `read_build_logs` API scope, which may not be available.
If logs return 403, use annotations (above) and artifacts (below) instead.

```bash
bk job log <JOB_UUID> -p test -b <BUILD_NUMBER>

# Without timestamps
bk job log <JOB_UUID> -p test -b <BUILD_NUMBER> --no-timestamps
```

## Listing artifacts

```bash
# All artifacts for a build
bk artifacts list <BUILD_NUMBER> -p test

# Artifacts as JSON
bk artifacts list <BUILD_NUMBER> -p test --json

# Artifacts for a specific job
bk artifacts list <BUILD_NUMBER> -p test --job <JOB_UUID>
```

Artifact JSON fields: `id`, `job_id`, `filename`, `path`, `dirname`, `file_size`, `mime_type`, `download_url`, `sha1sum`.
Common artifacts: `run.log`, `journalctl-merge.log`, `junit_*.xml`, `netstat-*.log`, `docker-inspect.log`, `ps-aux.log`.

## Downloading artifacts

```bash
# Download a single artifact by UUID
bk artifacts download <ARTIFACT_UUID>

# Download all artifacts for a build (interactive picker)
bk build download <BUILD_NUMBER> -p test
```

## Workflow: investigating a failed PR

1. **Get the build number from the PR:**
   ```bash
   gh pr checks <PR> --repo MaterializeInc/materialize \
     --json name,link,bucket \
     | jq -r '.[] | select(.name == "buildkite/test") | .link'
   ```
   Extract the build number from the URL.

2. **Get error details from annotations:**
   ```bash
   bk build view <BUILD> -p test --json \
     | python3 -c "
   import json, sys, html, re
   d = json.load(sys.stdin)
   for a in d.get('annotations', []):
       if a.get('style') == 'error':
           text = re.sub('<[^>]+>', '', a.get('body_html', ''))
           print(html.unescape(text))
           print('---')
   "
   ```
   This gives the failure message, test file, line number, and reproducer command.

3. **List failed jobs:**
   ```bash
   bk build view <BUILD> -p test --json \
     | jq '[.jobs[] | select(.state == "failed" and .type == "script") | {name, id, web_url}]'
   ```

4. **List and download artifacts for the failed job:**
   ```bash
   bk artifacts list <BUILD> -p test --job <JOB_ID> --json
   bk artifacts download <ARTIFACT_UUID>
   ```

5. **Present findings** including the job name, failure message from annotations, reproducer command, and any relevant artifacts.

## Listing recent builds

```bash
# Recent failed builds on a pipeline
bk build list -p test --state failed --limit 10

# Builds for a specific branch
bk build list -p test --branch <BRANCH>

# Builds for a specific commit
bk build list -p test --commit <SHA>
```

## Retrying a failed job or build

```bash
# Retry a single job
bk job retry <JOB_UUID>

# Rebuild an entire build
bk build rebuild <BUILD_NUMBER> -p test
```

## Notes

* The Buildkite org is `materialize`. The `bk` CLI is configured with `selected_org=materialize`.
* The main CI pipeline slug is `test`.
* Builds triggered by PRs have `pull_request.id` set to the PR number.
* Use `--json` output and `jq`/`python3` for reliable parsing; text output is for human display.
* Annotations are the best source for understanding failures — they contain error details, reproducer commands, and links to the CI failure history dashboard.
