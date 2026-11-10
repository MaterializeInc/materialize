---
title: "Environments and CI/CD"
description: "Set up development environments, isolate teams, share sources, and deploy from CI/CD with mz-deploy."
menu:
  main:
    parent: manage-mz-deploy
    weight: 65
    identifier: "mz-deploy-environments"
    name: "Environments and CI/CD"
---

This guide shows how to lay out development, staging, and production
environments for an `mz-deploy` project, give each team an isolated
environment, handle sources in non-production, and deploy from a CI/CD
pipeline. It builds on [Profiles](/developer-tools/mz-deploy/profiles/) and
[Deployments](/developer-tools/mz-deploy/deployments/).

## Choose an environment layout

Each environment is a [profile](/developer-tools/mz-deploy/profiles/). A
profile can point at its own Materialize instance, or several profiles can
share one non-production instance.

| Layout | What is isolated | Use for |
|--------|------------------|---------|
| One instance per environment | Everything: catalog, compute, sources, and access. | Production. |
| Shared non-production instance, one `profile_suffix` per environment | Databases and clusters. Each environment gets its own copy of every database and cluster in the project. | Development, staging, and per-team environments. |

Clusters are the unit of compute isolation. Two environments that share a
cluster compete for its CPU and memory, even if they use different databases
or schemas. A `profile_suffix` renames clusters as well as databases, so each
suffixed environment runs on its own clusters. Every copy of a cluster also
consumes its own credits.

## Set up environments on a shared instance

1. Define a profile per environment in `profiles.toml`. Profiles that share
   an instance use the same host:

   ```toml
   # ~/.mz/profiles.toml
   [dev_team_a]
   host = "nonprod.example.com"
   username = "team_a_bot"
   password = "${MZ_PROFILE_DEV_TEAM_A_PASSWORD}"

   [dev_team_b]
   host = "nonprod.example.com"
   username = "team_b_bot"
   password = "${MZ_PROFILE_DEV_TEAM_B_PASSWORD}"

   [staging]
   host = "nonprod.example.com"
   username = "deploy_bot"
   password = "${MZ_PROFILE_STAGING_PASSWORD}"

   [production]
   host = "prod.example.com"
   username = "deploy_bot"
   password = "${MZ_PROFILE_PRODUCTION_PASSWORD}"
   ```

1. Give each non-production profile a unique suffix in `project.toml`.
   Profile sections are top-level tables named after the profile:

   ```toml
   # project.toml
   [dev_team_a]
   profile_suffix = "_dev_team_a"

   [dev_team_b]
   profile_suffix = "_dev_team_b"

   [staging]
   profile_suffix = "_staging"
   ```

   With these settings, the `analytics` database becomes
   `analytics_dev_team_a`, `analytics_dev_team_b`, and `analytics_staging`.
   `production` has no suffix and uses the names as written.

1. Run `mz-deploy setup` once per instance. All suffixed environments on an
   instance share one `_mz_deploy` tracking database.

1. Restrict each team to its own environment with
   [RBAC](/security/cloud/access-control/). For example, grant `team_a_bot`
   privileges on `analytics_dev_team_a` and its clusters only.

## Set up a development environment

Developers iterate in three steps, from fastest to most realistic:

1. **Local**: `mz-deploy compile` type-checks the project offline, and
   `mz-deploy test` runs unit tests in a local Docker container. See
   [Local development](/developer-tools/mz-deploy/local-development/).
1. **Personal overlay**: `mz-deploy dev <cluster>` builds only your changed
   views and materialized views in an overlay database, reading everything
   else from the profile's environment. Give each developer their own profile
   name and dev cluster, because the overlay database is named
   `<database>__<profile>`. See
   [Iterate against production data](/developer-tools/mz-deploy/deployments/#iterate-against-production-data).
1. **Team environment**: `mz-deploy stage` and `mz-deploy promote` with the
   team's profile, for example `--profile dev_team_a`.

## Handle sources in non-production

`apply` creates sources and tables in the suffixed databases, so each
suffixed environment creates its own copy of every source. Each copy is a
separate consumer of the upstream system (for example, its own PostgreSQL
replication slot) and its own ingestion workload. Choose one of these
approaches.

### Point each environment at its own upstream

Override connections per profile so non-production environments read from
non-production upstreams. Use a profile-specific file such as
`pg_conn#staging.sql`, or a per-profile variable for the host. See
[Per-profile file overrides](/developer-tools/mz-deploy/profiles/#per-profile-file-overrides).

### Share one set of sources across environments

To ingest each upstream once on a shared instance, move sources and their
tables into a separate ingestion project that you deploy without a suffix.
Environment projects then declare those tables as external dependencies:

```toml
# project.toml of an environment project
dependencies = [
    "raw.public.orders",
    "raw.public.customers",
]
```

`profile_suffix` renames only databases that the project owns, that is,
directories under `models/`. References to external dependencies stay
unchanged, so every environment reads `raw.public.orders`. The environment
project must not also contain `models/raw/`, or the suffix renames those
references too. Run `mz-deploy lock` after adding dependencies to record
their column types in `types.lock`.

## Deploy from CI/CD

A typical pipeline has two phases:

| Trigger | Commands | Needs a database connection |
|---------|----------|-----------------------------|
| Pull request | `compile`, `test --junit-xml` | No. `test` needs Docker. |
| Merge to main | `apply`, `stage`, `wait`, `promote`, once per environment | Yes, as a member of `materialize_deployer`. |

Keep the following in mind:

- **Credentials**: Commit a `profiles.toml` without passwords, point
  `MZ_DEPLOY_PROFILES_DIR` at its directory, and pass each password with
  `MZ_PROFILE_<NAME>_PASSWORD`. Secrets created with `env_var()` read their
  values from the pipeline's environment when `apply` runs.
- **Deploy IDs**: A deploy ID can be used only once per instance, including
  after it is promoted. The default is the git commit SHA prefix, so
  environments that share an instance collide, and so does a pipeline re-run
  for the same commit. Pass a `--deploy-id` that includes the environment name
  and a per-run value.
- **Timeouts**: `wait` blocks until hydration completes and has no timeout by
  default. Set `--timeout` so a stuck deployment fails the pipeline. Run
  `mz-deploy abort <deploy-id>` to clean up a failed deployment.
- **Approvals**: Use your CI system's environment approvals to gate the
  production stage.

### Example: Azure DevOps

This pipeline validates every pull request, then deploys merges to `main`
into staging and production. The production stage runs only after staging
succeeds and the `production` environment's approvals pass.

```yaml
# azure-pipelines.yml
trigger:
  branches:
    include: [main]
pr:
  branches:
    include: [main]

pool:
  vmImage: ubuntu-latest

variables:
  MZ_DEPLOY_PROFILES_DIR: $(Build.SourcesDirectory)/ci

stages:
  - stage: validate
    jobs:
      - job: compile_and_test
        steps:
          - template: ci/install-mz-deploy.yml
          - script: mz-deploy compile --profile staging
            displayName: Compile
          - script: mz-deploy test --junit-xml test-results.xml
            displayName: Unit tests
          - task: PublishTestResults@2
            condition: always()
            inputs:
              testResultsFiles: test-results.xml

  - stage: staging
    dependsOn: validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: deploy
        environment: staging
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                - template: ci/deploy.yml
                  parameters:
                    profile: staging

  - stage: production
    dependsOn: staging
    jobs:
      - deployment: deploy
        environment: production
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                - template: ci/deploy.yml
                  parameters:
                    profile: production
```

```yaml
# ci/install-mz-deploy.yml
steps:
  - script: |
      curl -L "https://binaries.materialize.com/mz-deploy-latest-x86_64-unknown-linux-gnu.tar.gz" \
      | sudo tar -xzC /usr/local --strip-components=1
    displayName: Install mz-deploy
```

```yaml
# ci/deploy.yml
parameters:
  - name: profile
    type: string

steps:
  - template: install-mz-deploy.yml
  - script: |
      set -euo pipefail
      DEPLOY_ID="${{ parameters.profile }}_${BUILD_BUILDID}"
      mz-deploy apply
      mz-deploy stage --deploy-id "$DEPLOY_ID"
      mz-deploy wait "$DEPLOY_ID" --timeout 3600
      mz-deploy promote "$DEPLOY_ID"
    displayName: Deploy to ${{ parameters.profile }}
    env:
      MZ_DEPLOY_PROFILE: ${{ parameters.profile }}
      # Map each secret pipeline variable explicitly. Azure DevOps does not
      # expose secret variables to scripts otherwise.
      MZ_PROFILE_STAGING_PASSWORD: $(MZ_PROFILE_STAGING_PASSWORD)
      MZ_PROFILE_PRODUCTION_PASSWORD: $(MZ_PROFILE_PRODUCTION_PASSWORD)
```

Store `MZ_PROFILE_STAGING_PASSWORD` and `MZ_PROFILE_PRODUCTION_PASSWORD` as
secret pipeline variables. Other CI systems, such as GitHub Actions or
GitLab CI, follow the same two phases.
