---
title: "mz-deploy"
description: "Deploy and manage Materialize objects with mz-deploy, a SQL-native CLI for zero-downtime deployments."
disable_list: true
menu:
  main:
    parent: manage
    weight: 40
    identifier: "manage-mz-deploy"
    name: "Manage with mz-deploy"
---

{{< warning >}}
`mz-deploy` is a v0.1 release and is not yet recommended for production use.
{{< /warning >}}

`mz-deploy` is a command-line tool for deploying and managing the objects in
your Materialize environment — clusters, connections, sources, tables, views,
materialized views, indexes, and sinks — as plain SQL files. With `mz-deploy`,
you can type-check and unit test your SQL locally. When you deploy, it tracks
dependencies to deploy only the required changes and promotes those changes
without downtime.

By storing your SQL files in a git repository, you can incorporate `mz-deploy`
into your infrastructure-as-code practices. When run against a git repository,
`mz-deploy` tags each deployment with the current commit SHA and, by default,
refuses to deploy uncommitted changes.

## Install mz-deploy

{{% include-headless "/headless/mz-deploy/install" %}}

## Key features

### Write Materialize SQL

Define each object using the same Materialize SQL you'd run interactively; e.g.,
`CREATE VIEW`, `CREATE MATERIALIZED VIEW`, `CREATE SOURCE`, `CREATE SINK`, etc.
Rather than introducing its own configuration language or abstraction layer
(such as Terraform's HCL, dbt's Jinja-templated SQL, or an ORM's query API),
`mz-deploy` uses Materialize SQL directly.

Store each object in its own `.sql` file, organized by database and schema. The
file's path determines the object's fully qualified name; for example,
`models/materialize/public/order_summary.sql` defines
`materialize.public.order_summary`. Each file holds one primary `CREATE`
statement plus optional companion statements like `CREATE INDEX`, `COMMENT ON`,
and `GRANT`.

See [Project structure](/manage/mz-deploy/develop/project-structure/) for the full
directory layout and file conventions.

### Catch errors before deploying

You can use `mz-deploy compile` to type-check every SQL statement against your
dependency schemas, locally with no database connection required.

Inline unit tests let you mock dependencies and verify view logic with
deterministic inputs before anything touches a real environment. Changes that
break types or dependencies fail fast on your laptop or in CI, not in
production.

### Deploy safely and without downtime

`mz-deploy` tracks dependencies between objects, diffs your project against the
target environment, and deploys only the changed objects.

- Durable objects like secrets, connections, sources, and tables are converged
  in place (like Terraform).

- Views, materialized views, indexes, and sinks go through a staged deployment
  so changes can be validated before going live.

When you deploy, `mz-deploy` creates your changes in isolated staging schemas
alongside production. Once all materialized views finish computing their initial
results, a single atomic swap cuts traffic over to the new version. Running
queries are never interrupted, and if something goes wrong, the staging
deployment can be cleaned up without affecting production.

## Available guides

| Category | Guide | Description |
|----------|-------|-------------|
| Get started | [Get started](/manage/mz-deploy/get-started/) | Create a project and deploy it. |
| Develop | [Project structure](/manage/mz-deploy/develop/project-structure/) | Model files, companion statements, and configuration. |
|  | [Infrastructure](/manage/mz-deploy/develop/infrastructure/) | Clusters, secrets, connections, sources, and tables via `apply`. |
|  | [Local development](/manage/mz-deploy/develop/local-development/) | Type-checking, unit tests, and query plans. |
|  | [Editor setup](/manage/mz-deploy/develop/editor-setup/) | VS Code, Neovim, and Helix integration. |
|  | [AI agent setup](/manage/mz-deploy/develop/agent-setup/) | Claude Code, Codex, and other coding agents. |
| Deploy | [Deployments](/manage/mz-deploy/deploy/deployments/) | Staging, hydration, promotion, and management. |
|  | [Stable APIs](/manage/mz-deploy/deploy/stable-apis/) | Cross-team data products and data mesh. |
|  | [Profiles](/manage/mz-deploy/deploy/profiles/) | Multi-environment configuration. |


## Reference

For a list of available options and commands for `mz-deploy`, see
[Command reference](/manage/mz-deploy/commands/).
