# mz-deploy CI/CD Workflows

## Overview

These workflows implement a complete CI/CD pipeline for mz-deploy:

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `test.yml` | Every PR push | Run tests, preview infra changes, abort stale staging |
| `stage.yml` | `/deploy` PR comment | Stage changes to a preview environment |
| `hydration.yml` | Automatic (from stage) | Poll staging clusters until hydrated |
| `deploy.yml` | Push to main | Apply infra, stage, wait, promote to production |
| `cleanup.yml` | PR closed | Abort the PR's staging deployment |

## PR Workflow

1. **Push to PR** — `test.yml` runs tests and comments infrastructure changes on the PR.
   Any existing staging deployment for this PR is automatically aborted.

2. **Comment `/deploy`** — `stage.yml` applies infrastructure and creates a staging
   deployment. The deploy ID is `pr-<number>` (e.g., `pr-42`).

3. **Hydration polling** — `hydration.yml` checks every 5 minutes whether staging
   clusters are hydrated. When ready, it sets the `mz-deploy/hydration` commit
   status to green.

4. **Push new commits** — `test.yml` aborts the current staging. You must comment
   `/deploy` again to re-stage with the latest code.

5. **Close PR** — `cleanup.yml` aborts the staging deployment.

## Production Workflow

When a PR is merged to `main`, `deploy.yml`:
1. Applies infrastructure (clusters, roles, connections, sources, tables)
2. Creates a staging deployment (deploy ID = git SHA prefix)
3. Waits up to 6 hours for hydration
4. Promotes to production

## Configuration

### Profiles

The workflows expect a `profiles.toml` in the repository root (set via
`MZ_DEPLOY_PROFILES_DIR=.`). Passwords should come from GitHub secrets:

    [ci]
    host = "your-staging-region.materialize.cloud"
    username = "deploy_bot"
    password = "${MZ_CI_PASSWORD}"

    [production]
    host = "your-prod-region.materialize.cloud"
    username = "deploy_bot"
    password = "${MZ_PROD_PASSWORD}"

Then set `MZ_CI_PASSWORD` and `MZ_PROD_PASSWORD` as GitHub repository secrets.

### Required Status Checks

To enforce that staging is hydrated before merge, add `mz-deploy/hydration`
as a required status check in your branch protection rules.

### Customization

- **Hydration timeout**: Edit the `--timeout` value in `deploy.yml` (default: 21600s / 6hrs).
- **Lag threshold**: Add `--allowed-lag <seconds>` to `wait` commands (default: 300s / 5min).
- **Skip secrets**: `deploy.yml` runs `mz-deploy apply` which includes secrets.
  Add `--skip-secrets` if secrets are managed outside mz-deploy.
- **Poll interval**: Edit the sleep duration in `hydration.yml` (default: 5 min).
