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

The setting is local to your checkout. For one-off overrides, pass `--profile`
or set `MZ_DEPLOY_PROFILE`.

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


## Create a project

`mz-deploy new <project-name>` command[^1]:
- Creates and initializes the project directory.
- Initializes a git repository (pass `--no-git` to skip).

For example, create a new project `order-monitoring` and go to the project
directory:

```bash
mz-deploy new order-monitoring
cd order-monitoring
```

An `mz-deploy` project has the following structure and files:

```nofmt
order-monitoring/          # project name
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

## Defining objects

Once a project is set up, you define your objects in its own `.sql` file. Each
file contains:

- One primary `CREATE` statement (e.g., [`CREATE
  VIEW`](/sql/create-view/),[`CREATE MATERIALIZED
  VIEW`](/sql/create-materialized-view/), [`CREATE
  SOURCE`](/sql/create-source/), [`CREATE SINK`](/sql/create-sink/), etc.).

- For `model/` object files, optional companion statements like `CREATE INDEX`,
  `COMMENT ON`, and `GRANT`.

See [Project structure](/manage/mz-deploy/develop/project-structure/) for
additional details.

### Infrastructure and storage objects

Infrastructure objects refer to clusters, roles, network policies, secrets, and
connections. Storage objects refer to sources, and associated tables from
sources.

Before you reference infrastructure/storage objects in your views and
materialized views, you must first define and deploy your infrastructure/storage
objects changes to Materialize.

For your infrastructure/storage objects:
- Create the corresponding `.sql` files.

  {{< note >}}
  {{% include-headless "/headless/mz-deploy/defining-objects" %}}
  {{< /note >}}

- Deploy the changes using `mz-deploy apply`.

For example, in the `order-monitoring` project directory, define the cluster and
the PostgreSQL storage objects (secret, connection, source, and associated
tables) under `models/pg_db/sales_storage/`

1. Create `clusters/orders_cluster.sql`:

   ```sql
   -- clusters/orders_cluster.sql
   CREATE CLUSTER orders_cluster (SIZE = '25cc');
   ```

1. Create `models/pg_db/sales_storage/pg_pass.sql` with the password for your
   PostgreSQL user:

   ```sql
   -- models/pg_db/sales_storage/pg_pass.sql
   CREATE SECRET pg_pass AS '<postgres-password>';
   ```

1. Create `models/pg_db/sales_storage/pg_conn.sql` to connect to your PostgreSQL
   database:

   ```sql
   -- models/pg_db/sales_storage/pg_conn.sql
   CREATE CONNECTION pg_conn TO POSTGRES (
       HOST '<postgres-host>',
       DATABASE '<database>',
       USER '<username>',
       PASSWORD SECRET pg_pass
   );
   ```

1. Create `models/pg_db/sales_storage/pg_source.sql` to ingest from a PostgreSQL
   publication:

   ```sql
   -- models/pg_db/sales_storage/pg_source.sql
   CREATE SOURCE pg_source
   IN CLUSTER orders_cluster
   FROM POSTGRES CONNECTION pg_conn
   (PUBLICATION '<publication-name>');
   ```

1. Create a table for each upstream table you want to ingest, under
   `models/pg_db/sales_storage/`:

   ```sql
   -- models/pg_db/sales_storage/orders.sql
   CREATE TABLE orders FROM SOURCE pg_source (REFERENCE orders);
   ```

   ```sql
   -- models/pg_db/sales_storage/order_items.sql
   CREATE TABLE order_items FROM SOURCE pg_source (REFERENCE order_items);
   ```

   ```sql
   -- models/pg_db/sales_storage/products.sql
   CREATE TABLE products FROM SOURCE pg_source (REFERENCE products);
   ```

   ```sql
   -- models/pg_db/sales_storage/users.sql
   CREATE TABLE users FROM SOURCE pg_source (REFERENCE users);
   ```

   `mz-deploy` batches `CREATE TABLE FROM SOURCE` statements by the source, and
   executes each batch in a single [transaction](/sql/begin/).

1. Use `mz-deploy apply` to deploy these objects to Materialize:

   ```sh
   mz-deploy apply
   ```

   The command compiles, writes a `types.lock` file (capturing the table schemas
   from the source), and creates the objects:

   ```none
     Compiling ~/order-monitoring
       Finished compile in 0.01s
       Locking ~/order-monitoring
       Finished locking 4 objects in 1.0s
     ✓ orders_cluster
     ✓ pg_db.sales_storage.pg_pass
     ✓ pg_db.sales_storage.pg_conn
     ✓ pg_db.sales_storage.pg_source
     ✓ pg_db.sales_storage.orders
     ✓ pg_db.sales_storage.order_items
     ✓ pg_db.sales_storage.products
     ✓ pg_db.sales_storage.users
   ```

### Compute objects

Compute objects refer to: views, materialized views, and indexes.

For your compute objects:
- Create the corresponding `.sql` files.

  {{< note >}}
  {{% include-headless "/headless/mz-deploy/defining-objects" %}}
  {{< /note >}}

- Compile the changes using `mz-deploy compile`.
- Deploy using the following sequence:

  ```bash
  mz-deploy setup
  mz-deploy stage
  mz-deploy wait <deploy-id>
  mz-deploy promote <deploy-id>
  ```

  - `setup` creates the deployment tracking tables, the deployment server
    cluster that every connection runs against, and, if RBAC is enabled,
    the access-control roles. This is a one-time step.
  - `stage` compiles the project, diffs against production, and deploys changed
    objects to staging schemas.
  - `wait` monitors cluster hydration until all materialized views are ready.
  - `promote` atomically swaps staging into production.




For example, in the `order-monitoring` project directory,

1. Create `models/materialize/public/order_summary.sql`:

   ```sql
   -- models/materialize/public/order_summary.sql
   CREATE VIEW order_summary AS
   SELECT
       status,
       COUNT(*) AS order_count,
       SUM(amount) AS total_amount
   FROM orderss
   GROUP BY status;

   CREATE INDEX order_summary_status_idx IN CLUSTER orders
   ON order_summary (status);

   COMMENT ON VIEW order_summary IS
       'Aggregated order counts and totals by status.';
   ```

   Each `model/` file contains one primary `CREATE` statement, plus optional
   companion statements like `CREATE INDEX`, `COMMENT ON`, and `GRANT`. See
   [Project structure](/manage/mz-deploy/develop/project-structure/) for full
   details.

1. Use `mz-deploy compile` to validates your SQL and dependencies.

   ```bash
   mz-deploy compile
   ```

   `mz-deploy compile` validates your SQL and dependencies locally without a
   database connection. It catches parsing errors, circular dependencies, and
   type mismatches. See [Local
   development](/manage/mz-deploy/develop/local-development/) for the full
   compile, test, and explain workflow.

1. Deploy using the following sequence of operations:

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

   See [Deployments](/manage/mz-deploy/deploy/deployments/) for flags, error
   handling, and deployment management.

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
