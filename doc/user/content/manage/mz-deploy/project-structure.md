---
title: "Project structure"
description: "How mz-deploy projects are organized: directories, model files, and configuration."
menu:
  main:
    parent: manage-mz-deploy
    weight: 20
    identifier: "mz-deploy-project-structure"
    name: "Project structure"
---

An `mz-deploy` project is a directory of SQL files and configuration. Directories
map to Materialize objects, and configuration files control connection and
deployment behavior.

## Directory layout

A typical project looks like this:

```nofmt
order-monitoring/
├── models/
│   └── materialize/
│       ├── public.sql             # Schema modifier
│       └── public/
│           ├── order_summary.sql  # View definition
│           └── stalled_orders.sql # Materialized view definition
├── clusters/
│   └── orders.sql                 # Cluster definition
├── roles/
│   └── order_reader.sql           # Role definition
├── network-policies/              # Network policy definitions
├── project.toml                   # Project configuration
├── types.lock                     # Column schemas (generated)
├── README.md
└── .gitignore
```

The `models/` directory contains all schema-scoped objects: views, materialized
views, sinks, tables, sources, connections, and secrets. These live under
`models/<database>/<schema>/` because they belong to a specific database and
schema.

The `clusters/`, `roles/`, and `network-policies/` directories have their own
top-level directories because these are **global objects** — they are not scoped
to any database or schema.

## File-path-to-object-name mapping

The path of each SQL file under `models/` determines the fully qualified object
name in Materialize:

```nofmt
models/<database>/<schema>/<object>.sql  →  database.schema.object
```

For example, `models/materialize/public/stalled_orders.sql` creates the object
`materialize.public.stalled_orders`.

## Model files

Each model file contains one primary `CREATE` statement that defines the object.
Supported primary statements are:

- `CREATE VIEW`
- `CREATE MATERIALIZED VIEW`
- `CREATE SINK`
- `CREATE TABLE` / `CREATE TABLE FROM SOURCE`
- `CREATE SOURCE`
- `CREATE CONNECTION`
- `CREATE SECRET`

You can include companion statements in the same file for related configuration:
`CREATE INDEX`, `COMMENT ON`, `GRANT`.

Here is a complete example:

```sql
-- models/materialize/public/stalled_orders.sql
CREATE MATERIALIZED VIEW stalled_orders
IN CLUSTER orders AS
SELECT
    id,
    customer,
    amount,
    created_at,
    updated_at,
    mz_now() - updated_at AS stalled_for
FROM orders
WHERE status = 'pending'
  AND updated_at < mz_now() - INTERVAL '30 minutes';

CREATE INDEX stalled_orders_customer_idx IN CLUSTER orders
ON stalled_orders (customer);

COMMENT ON MATERIALIZED VIEW stalled_orders IS
    'Orders stuck in pending status for more than 30 minutes.';

GRANT SELECT ON stalled_orders TO order_reader;
```

## Schema modifiers

A file at `models/<database>/<schema>.sql` is a schema modifier. Use it for
schema-level statements that apply to the schema as a whole rather than to a
specific object. A schema modifier can contain:

- `SET api = stable`
- `COMMENT ON SCHEMA`
- `GRANT`
- `ALTER DEFAULT PRIVILEGES`

For example:

```sql
-- models/materialize/public.sql
COMMENT ON SCHEMA public IS 'Order monitoring data model.';

GRANT USAGE ON SCHEMA public TO order_reader;
```

## Database modifiers

A file at `models/<database>.sql` is a database modifier. Use it for
database-level statements:

- `COMMENT ON DATABASE`
- `GRANT`
- `ALTER DEFAULT PRIVILEGES`

## project.toml

The `project.toml` file in your project root controls project-wide settings:

- **`mz_version`** — the Materialize version used for local type-checking. Set
  to `"cloud"` to use the latest cloud version.
- **`dependencies`** — external dependency declarations for objects your project
  references but does not own. See [Local development](/manage/mz-deploy/local-development/)
  for details.
- **Per-profile config sections** — override settings for specific environments.
  See [Profiles](/manage/mz-deploy/profiles/) for multi-environment setup.

The active connection profile is resolved per-invocation from `--profile`,
`MZ_DEPLOY_PROFILE`, or the per-checkout default set by `mz-deploy profile
set`. See [Profiles](/manage/mz-deploy/profiles/).

## profiles.toml

The `profiles.toml` file (typically at `~/.mz/profiles.toml`) stores your
Materialize connection details. Each section defines a named profile:

```toml
[default]
host = "<your-materialize-host>"
port = 6875
username = "<your-username>"
password = "<your-password>"
```

See [Profiles](/manage/mz-deploy/profiles/) for multi-environment setup and
advanced configuration.
