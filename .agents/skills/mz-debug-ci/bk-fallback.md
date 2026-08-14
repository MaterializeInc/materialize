# bk fallback for mz-debug-ci

Command recipes for sessions without the Buildkite MCP. The semantics (job
states, annotation contents, triage rules) live in SKILL.md; only the `bk`
mechanics live here.

## Listing a build's failed jobs

```bash
bk api "/pipelines/<PIPELINE>/builds/<BUILD_NUMBER>" --no-pager 2>/dev/null \
  | jq -r '.jobs[] | select(.state=="failed" or .state=="timed_out") | select(.soft_failed != true) | [.state, .id, .name] | @tsv'
```

The same response carries the build's `.commit` and `.branch`. Add
`?include_retried_jobs=true` to see the earlier attempts of retried jobs
(exposes `.retried` and `.retry_source`).

## Annotations

```bash
bk api /pipelines/<PIPELINE>/builds/<BUILD_NUMBER>/annotations --no-pager 2>/dev/null \
  | jq -r '.[] | select(.style=="error") | "=== \(.context)\n\(.body_html)"' \
  | sed -e 's/<[^>]*>//g' | grep -v '^$'
```

## Job logs

```bash
bk job log <JOB_ID> -p <PIPELINE> -b <BUILD_NUMBER> --no-timestamps --no-pager 2>/dev/null | tail -100
```

For large logs, grep for the error instead of tailing. Buildkite logs are
full of ANSI escapes (colors, plus cursor-movement and OSC sequences from
docker pulls) and embedded carriage returns that leave doubled blank lines,
halving grep's `-A`/`-B` reach. Normalize first:

```bash
bk job log ... 2>/dev/null | tr '\r' '\n' \
  | sed -e 's/\x1b\[[0-9;]*[a-zA-Z]//g' -e 's/\x1b\][^\x07]*\x07//g' | grep -v '^$' \
  | grep -B2 -A5 'error\|FAIL\|panicked'
```

## Artifacts

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
