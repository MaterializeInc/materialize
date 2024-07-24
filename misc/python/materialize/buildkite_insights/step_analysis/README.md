# Buildkite Step Analysis

This tool allows searching recent Buildkite builds to
* gain insights about build durations and flakiness
* find recent failures

## Usage
```
usage: buildkite-step-insights [-h]
                               [--branch BRANCH]
                               [--build-state {running,scheduled,passed,failing,failed,blocked,canceled,canceling,skipped,not_run,finished}]
                               [--build-step-key BUILD_STEP_KEY]
                               [--build-step-parallel-index BUILD_STEP_PARALLEL_INDEX]
                               [--build-step-state {assigned,broken,canceled,failed,passed,running,scheduled,skipped,timed_out,unblocked,waiting,waiting_failed}]
                               [--fetch {auto,always,avoid,never}]
                               [--include-commit-hash]
                               [--max-fetches MAX_FETCHES]
                               [--output-type {txt,txt-short,csv}]
                               {cleanup,coverage,deploy,deploy-mz-lsp-server,deploy-mz,deploy-website,license,nightly,qa-canary,release-qualification,security,slt,test,www}
```

### Authentication

You will need an environment variable called `BUILDKITE_TOKEN`, which contains a Buildkite token. Such a token can be
created on https://buildkite.com/user/api-access-tokens/new.
This tool will need:
* `read_builds`

## Examples

Recent executions of build step "AWS (Localstack)" in Nightly on all branches

```
bin/buildkite-step-insights nightly --build-step-key "aws-localstack" --branch "*"
```

Recent failures of build step "AWS (Localstack)" in Nightly on `main` branch

```
bin/buildkite-step-insights nightly --build-step-key "aws-localstack" --branch main --build-step-state "failed"
```

Many recent executions of build step "Cargo test" on `main` branch in builds that failed due to any step

```
bin/buildkite-step-insights test --build-step-key "cargo-test" --branch "main" --max-fetches 6 --build-state failed
```

Most recent executions of "Cargo test" on `main` branch

```
bin/buildkite-step-insights test --build-step-key "cargo-test" --branch "main" --fetch always
```
