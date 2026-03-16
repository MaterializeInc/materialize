# Staging Deploy

## Introduction

`bin/staging-deploy` automates deploying a commit to a staging environment. It replaces the manual loop of watching Buildkite for a Docker tag, cycling the staging region, and waiting for it to come back online.

## Prerequisites

- **`bk` CLI**: Install with `brew install buildkite/buildkite/bk` and authenticate with `bk auth login`.
- **`bin/mz` staging profile**: Ensure you have a `staging` profile in your `mz` config.

## Usage

Push your commit to a PR branch first — the script monitors the Buildkite `test` pipeline, which only runs on PR builds.

```shell
# Wait for HEAD's build to finish, then deploy to staging
bin/staging-deploy

# Wait for a specific commit's build to finish, then deploy to staging
bin/staging-deploy --commit <full-sha>
```

The script sends a macOS desktop notification when staging is ready.

## How it works

1. **Build lookup**: Uses `bk build list --commit <sha> --pipeline test` to find the build, then `bk build watch` to wait for it to complete.
2. **Docker tag extraction**: Parses the `build-tags-*` annotation context set by `ci/test/build.py` during the image push step.
3. **Region cycling**: Runs `bin/mz --profile staging region disable` followed by `bin/mz --profile staging region enable --version <tag>`.
4. **Readiness check**: Polls `bin/mz --profile staging sql -- -c 'SELECT 1'` until the region accepts connections.
