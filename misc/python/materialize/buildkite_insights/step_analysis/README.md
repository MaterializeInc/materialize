# Buildkite Step Analysis

This tool allows searching recent Buildkite builds to
* gain insights about build durations and flakiness
* find recent failures

## Usage
```
usage: buildkite-step-durations [-h]
                                [--pipeline {cleanup,coverage,deploy,deploy-lsp,deploy-mz,deploy-website,license,nightlies,release-qualification,security,sql-logic-tests,tests,www}]
                                [--build-step-key BUILD_STEP_KEY]
                                [--fetch {auto,always,never}]
                                [--max-fetches MAX_FETCHES]
                                [--branch BRANCH]
                                [--build-state {running,scheduled,passed,failing,failed,blocked,canceled,canceling,skipped,not_run,finished}]
                                [--build-step-state {assigned,broken,canceled,failed,passed,running,scheduled,skipped,timed_out,unblocked,waiting,waiting_failed}]
                                [--output-type {txt,txt-short,csv}]
```

## Examples

Recent executions of build step "AWS (Localstack)" in Nightly on all branches

```
bin/buildkite-step-insights --pipeline nightlies --build-step-key "aws-localstack" --branch "*"
```

Recent failures of build step "AWS (Localstack)" in Nightly on `main` branch

```
bin/buildkite-step-insights --pipeline nightlies --build-step-key "aws-localstack" --branch main --build-step-state "failed"
```

Many recent executions of build step "Cargo test" on `main` branch in builds that failed due to any step

```
bin/buildkite-step-insights --build-step-key "cargo-test" --branch "main" --max-fetches 6 --build-state failed
```

Most recent executions of "Cargo test" on `main` branch

```
bin/buildkite-step-insights --build-step-key "cargo-test" --branch "main" --fetch always
```
