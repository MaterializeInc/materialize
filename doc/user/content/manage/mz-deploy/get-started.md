---
title: "Get started with mz-deploy"
description: "Set up mz-deploy and deploy your first project to Materialize."
menu:
  main:
    parent: manage-mz-deploy
    weight: 10
    identifier: "get-started-mz-deploy"
    name: "Get started"
---

{{< warning >}}
`mz-deploy` is a v0.1 release and is not yet recommended for production use.
{{< /warning >}}

`mz-deploy` is a deployment tool that gives you local type-checking, unit
testing, and zero-downtime blue/green deployments for Materialize. This quickstart walks you through creating a project and deploying it.

## Prerequisites

A running Materialize instance ([Materialize
Cloud](https://materialize.com/register/) or
[self-managed](/self-managed-deployments/)).

## Install `mz-deploy`

{{% include-headless "/headless/mz-deploy/install" %}}

## Create a project

`mz-deploy new <project-name>` command[^1]:
- Creates and initializes the project directory.
- Initializes a git repository (pass `--no-git` to skip).

For example, create a new project `order-monitoring`:

```bash
mz-deploy new order-monitoring
cd order-monitoring
```

An `mz-deploy` project has the following structure and files:

```nofmt
order-monitoring/
├── models/                # Schema-scoped object definitions
│   └── materialize/
│       └── public/        # SQL files → materialize.public.<filename>
├── clusters/              # Cluster definitions
├── roles/                 # Role definitions
├── network-policies/      # Network policy definitions
├── .vscode/               # Recommends the mz-deploy VS Code extension
├── project.toml           # Project configuration
├── README.md
└── .gitignore
```

{{% include-headless "/headless/mz-deploy/project-structure-table/" %}}

[^1]: To scaffold an existing directory instead of creating a new one, `cd` into
it and run `mz-deploy init`. It creates the same structure and uses the
directory name as the project name. By default, `init` also runs `git init` and
makes an initial commit; if the directory is already a git repository or an
existing mz-deploy project, add `--no-git` to skip that step (`mz-deploy init
--no-git`).

## Configure connection profiles

Create the file `~/.mz/profiles.toml` with your Materialize connection details:

```toml
[default]
host = "<your-materialize-host>"
port = 6875
username = "<your-username>"
password = "<your-password>"
```

Tell mz-deploy which profile to use for your checkout:

```bash
mz-deploy profile set default
```

The setting is local to your checkout — teammates can pick their own
default without affecting you. For one-off overrides, pass `--profile` or
set `MZ_DEPLOY_PROFILE`.

Verify the connection:

```bash
mz-deploy debug
```

This prints your active profile, Docker status, environment ID, and the
health of the deployment server cluster, confirming that `mz-deploy` can
reach your instance. For an interactive psql shell against the active
profile, use `mz-deploy sql` (requires `psql` on your `PATH`).

{{< tip >}}
As a best practice, we strongly recommend using [service accounts](/security/cloud/users-service-accounts/create-service-accounts) to connect external applications, like mz-deploy, to Materialize.
{{< /tip >}}

## Define a cluster

Create `clusters/orders.sql`:

```sql
-- clusters/orders.sql
CREATE CLUSTER orders (SIZE = '25cc');
```

## Define a view

Create `models/materialize/public/order_summary.sql`:

```sql
-- models/materialize/public/order_summary.sql
CREATE VIEW order_summary AS
SELECT
    status,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM orders
GROUP BY status;

CREATE INDEX order_summary_status_idx IN CLUSTER orders
ON order_summary (status);

COMMENT ON VIEW order_summary IS
    'Aggregated order counts and totals by status.';
```

Each model file contains one primary `CREATE` statement, plus optional companion
statements like `CREATE INDEX`, `COMMENT ON`, and `GRANT`. See
[Project structure](/manage/mz-deploy/develop/project-structure/) for full details.

## Compile

```bash
mz-deploy compile
```

`mz-deploy compile` validates your SQL and dependencies locally without a
database connection. It catches parse errors, circular dependencies, and type
mismatches. See [Local development](/manage/mz-deploy/develop/local-development/) for
the full compile, test, and explain workflow.

## Deploy

```bash
mz-deploy setup
mz-deploy stage
mz-deploy wait <deploy-id>
mz-deploy promote <deploy-id>
```

- `setup` creates the deployment tracking tables, the deployment server
  cluster that every connection runs against, and — when RBAC is enabled —
  the access-control roles. This is a one-time step.
- `stage` compiles the project, diffs against production, and deploys changed
  objects to staging schemas.
- `wait` monitors cluster hydration until all materialized views are ready.
- `promote` atomically swaps staging into production.

See [Deployments](/manage/mz-deploy/deploy/deployments/) for flags, error handling, and
deployment management.

## Next steps

| Category | Guide | Description |
|----------|-------|-------------|
| Develop | [Project structure](/manage/mz-deploy/develop/project-structure/) | Model files, companion statements, and configuration. |
|  | [Infrastructure](/manage/mz-deploy/develop/infrastructure/) | Clusters, secrets, connections, sources, and tables via `apply`. |
|  | [Local development](/manage/mz-deploy/develop/local-development/) | Type-checking, unit tests, and query plans. |
|  | [Editor setup](/manage/mz-deploy/develop/editor-setup/) | VS Code, Neovim, and Helix integration. |
|  | [AI agent setup](/manage/mz-deploy/develop/agent-setup/) | Claude Code, Codex, and other coding agents. |
| Deploy | [Deployments](/manage/mz-deploy/deploy/deployments/) | Staging, hydration, promotion, and management. |
|  | [Stable APIs](/manage/mz-deploy/deploy/stable-apis/) | Cross-team data products and data mesh. |
|  | [Profiles](/manage/mz-deploy/deploy/profiles/) | Multi-environment configuration. |
| Reference | [Command reference](/manage/mz-deploy/commands/) | Every command, grouped, with synopsis and flags. |
