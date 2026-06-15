---
title: "Use mz-deploy to manage Materialize"
description: "Deploy and manage Materialize objects with mz-deploy, a SQL-native CLI for zero-downtime deployments."
disable_list: true
menu:
  main:
    parent: manage
    weight: 40
    identifier: "manage-mz-deploy"
    name: "Manage with mz-deploy"
---

`mz-deploy` is a CLI that manages your Materialize deployment from plain SQL
files in a git repository. It catches errors before they reach production, lets
you test view logic locally, and deploys changes without downtime.

## Why mz-deploy

### Write plain SQL, deploy safely

Everything lives in `.sql` files — one object per file, organized by database
and schema. `mz-deploy` tracks dependencies between objects, diffs your project
against the live environment, and deploys only what changed. Durable objects
like secrets, connections, sources, and tables are converged in place (like
Terraform). Views, materialized views, indexes, and sinks go through a staged
deployment so changes can be validated before going live.

### Catch errors before deploying

`mz-deploy compile` type-checks every SQL statement against your dependency
schemas — locally, with no database connection required. Inline unit tests let
you mock dependencies and verify view logic with deterministic inputs before
anything touches a real environment. Changes that break types or dependencies
fail fast on your laptop or in CI, not in production.

### Ship without downtime

When you deploy, `mz-deploy` creates your changes in isolated staging schemas
alongside production. Once all materialized views finish computing their initial
results, a single atomic swap cuts traffic over to the new version. Running
queries are never interrupted, and if something goes wrong, the staging
deployment can be cleaned up without affecting production.

## When to use it

| Tool | Best for | Manages infrastructure | Zero-downtime deployments |
|------|----------|----------------------|--------------------------|
| **Plain SQL / psql scripts** | Manual execution. No dependency tracking, no diff, no rollback. Where most teams start. | No | No |
| **mz-deploy** | SQL-native, git-based workflow with offline type-checking, unit tests, and staged deployments. | Yes | Yes |
| **[dbt](/manage/dbt/)** | Teams already invested in dbt. Manages views and materialized views. | No (clusters, connections, secrets are out of scope) | Yes (via `dbt-materialize` adapter macros) |
| **[Terraform](/manage/terraform/)** | Teams managing Materialize alongside other cloud infrastructure. | Yes | No |

## Available guides

{{< multilinkbox >}}
{{< linkbox title="To get started" >}}
[Get started with mz-deploy](/manage/mz-deploy/get-started/)
{{</ linkbox >}}
{{< linkbox title="Develop" >}}
- [Project structure](/manage/mz-deploy/project-structure/)
- [Infrastructure](/manage/mz-deploy/infrastructure/)
- [Local development](/manage/mz-deploy/local-development/)
- [Editor setup](/manage/mz-deploy/editor-setup/)
- [AI agent setup](/manage/mz-deploy/agent-setup/)
{{</ linkbox >}}
{{< linkbox title="Deploy" >}}
- [Deployments](/manage/mz-deploy/deployments/)
- [Stable APIs](/manage/mz-deploy/stable-apis/)
- [Profiles](/manage/mz-deploy/profiles/)
{{</ linkbox >}}
{{</ multilinkbox >}}
