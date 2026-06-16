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

## Install mz-deploy

{{% include-headless "/headless/mz-deploy/install" %}}

## Key features

### Write Materialize SQL

Rather than introducing its own configuration language or abstraction layer
(such as Terraform's HCL, dbt's Jinja-templated SQL, or an ORM's query API),
`mz-deploy` uses the same Materialize SQL you would run interactively; e.g.,
[`CREATE VIEW`](/sql/create-view/), [`CREATE MATERIALIZED
VIEW`](/sql/create-materialized-view/), [`CREATE SOURCE`](/sql/create-source/),
[`CREATE SINK`](/sql/create-sink/), etc.

Store each object in its own `.sql` file. The object name in your `CREATE`
statement must match the `.sql` file name.

See [Project structure](/manage/mz-deploy/develop/project-structure/) for the
full directory layout and file conventions.

### Catch errors before deploying

You can use `mz-deploy compile` to type-check every SQL statement against your
dependency schemas, locally with no database connection required.

You can use inline unit tests to mock dependencies and verify view logic with
deterministic inputs before anything touches a real environment.

As such, changes that break types or dependencies fail fast on your laptop or in
CI, not in production.

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
