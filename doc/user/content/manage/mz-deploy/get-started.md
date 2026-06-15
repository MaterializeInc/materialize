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

`mz-deploy` is a deployment tool that gives you compile-time validation, unit
testing, and zero-downtime blue/green deployments for Materialize — all from
plain SQL files in a git repository. This quickstart walks you through creating
a project and deploying it.

## Prerequisites and installation

Before you begin, you need:

- A running Materialize instance ([Materialize Cloud](https://materialize.com/register/) or [self-managed](/self-managed-deployments/)).

{{< note >}}
Installation instructions will be added when the distribution mechanism is finalized.
{{< /note >}}

Docker is required for `mz-deploy test` and `mz-deploy explain` (see [Local development](/manage/mz-deploy/local-development/)).

## Create a project

```bash
mz-deploy new order-monitoring
cd order-monitoring
```

This scaffolds the following directory structure:

```nofmt
order-monitoring/
├── models/
│   └── materialize/
│       └── public/        # SQL files → materialize.public.<filename>
├── clusters/              # Cluster definitions
├── roles/                 # Role definitions
├── network-policies/      # Network policy definitions
├── project.toml           # Project configuration
├── README.md
└── .gitignore
```

The path of each SQL file under `models/` determines the fully qualified object
name in Materialize: `models/<database>/<schema>/<object>.sql` maps to
`database.schema.object`. For example,
`models/materialize/public/stalled_orders.sql` creates the object
`materialize.public.stalled_orders`.

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

This prints your Materialize version, environment ID, and current role,
confirming that `mz-deploy` can reach your instance. For an interactive
psql shell against the active profile, use `mz-deploy sql`.

{{< tip >}}
As a best practice, we strongly recommend using [service accounts](/security/cloud/users-service-accounts/create-service-accounts) to connect external applications, like mz-deploy, to Materialize.
{{< /tip >}}

## Define a cluster

Create `clusters/orders.sql`:

```sql
-- clusters/orders.sql
CREATE CLUSTER orders (SIZE = 'xsmall');
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
[Project structure](/manage/mz-deploy/project-structure/) for full details.

## Compile

```bash
mz-deploy compile
```

`mz-deploy compile` validates your SQL and dependencies locally without a
database connection. It catches parse errors, circular dependencies, and type
mismatches. See [Local development](/manage/mz-deploy/local-development/) for
the full compile, test, and explain workflow.

## Deploy

```bash
mz-deploy setup
mz-deploy stage
mz-deploy wait <deploy-id>
mz-deploy promote <deploy-id>
```

- `setup` creates deployment tracking tables. This is a one-time step.
- `stage` compiles the project, diffs against production, and deploys changed
  objects to staging schemas.
- `wait` monitors cluster hydration until all materialized views are ready.
- `promote` atomically swaps staging into production.

See [Deployments](/manage/mz-deploy/deployments/) for flags, error handling, and
deployment management.

## Next steps

- [Project structure](/manage/mz-deploy/project-structure/) — model files, companion statements, configuration
- [Infrastructure](/manage/mz-deploy/infrastructure/) — secrets, connections, sources, tables
- [Local development](/manage/mz-deploy/local-development/) — type checking, unit tests, query plans
- [Editor setup](/manage/mz-deploy/editor-setup/) — VS Code, Neovim, Helix integration
- [AI agent setup](/manage/mz-deploy/agent-setup/) — Claude Code, Codex, and other coding agents
- [Deployments](/manage/mz-deploy/deployments/) — staging, hydration, promotion, management
- [Stable APIs](/manage/mz-deploy/stable-apis/) — cross-team data products and data mesh
- [Profiles](/manage/mz-deploy/profiles/) — multi-environment configuration
