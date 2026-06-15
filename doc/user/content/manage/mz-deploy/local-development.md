---
title: "Local development"
description: "Type-check, test, and inspect query plans locally before deploying."
menu:
  main:
    parent: manage-mz-deploy
    weight: 40
    identifier: "mz-deploy-local-development"
    name: "Local development"
---

`mz-deploy` provides a local development workflow for validating SQL before
deploying: type-check with `compile`, test with `test`, and inspect query plans
with `explain`.

## External dependencies

Objects your project references but doesn't own must be declared in
`project.toml`:

```toml
dependencies = [
    "other_project.public.customers",
]
```

Tables and sources that your project manages (created via `apply`) are
auto-discovered and do not need to be declared.

## Lock types

`types.lock` captures column schemas (names, types, nullability) for tables,
sources, and external dependencies. It enables offline type-checking during
`compile` and schema validation during `test`.

Two categories of objects are tracked:

1. **Project-managed tables and sources** — auto-discovered. Running
   `apply tables` regenerates the lock file automatically.
2. **External dependencies** — declared in `project.toml`.

```bash
mz-deploy lock
```

This connects to your Materialize instance, fetches schemas, and writes
`types.lock`.

- Commit `types.lock` to version control.
- Re-run `mz-deploy lock` when external schemas change.
- Without `types.lock`, `compile` cannot verify column names and types.

## Compile and validate

```bash
mz-deploy compile
```

`compile` runs entirely locally with no database connection and no Docker. It:

- Parses all SQL files.
- Resolves inter-object dependencies (topological sort).
- Type-checks every statement using `types.lock`.
- Skips unchanged objects via incremental caching.

What it catches: parse errors, circular dependencies, type mismatches, and
missing dependencies.

```bash
mz-deploy compile -v
```

Verbose mode shows the dependency graph, deployment order, and full SQL plan.

Use `compile` as your inner development loop: edit, compile, fix, repeat.
Feedback is instant.

## Write and run unit tests

{{< note >}}
Docker must be running to execute tests. Tests use a local Materialize container
and do not affect your remote database.
{{< /note >}}

Tests use the `EXECUTE UNIT TEST` syntax and live inline in the same `.sql` file
as the view they test.

Here is a full example appended to the stalled_orders model file:

```sql
-- models/materialize/public/stalled_orders.sql
EXECUTE UNIT TEST test_stalled_order_detected
FOR materialize.public.stalled_orders
AT TIME '2024-06-15T12:00:00Z'
MOCK materialize.public.orders(
    id bigint, customer text, status text,
    amount numeric, created_at timestamptz, updated_at timestamptz
) AS (
  SELECT * FROM VALUES
    (1, 'acme', 'pending', 99.99,
     '2024-06-15T10:00:00Z'::timestamptz,
     '2024-06-15T11:00:00Z'::timestamptz)
),
EXPECTED(
    id bigint, customer text, amount numeric,
    created_at timestamptz, updated_at timestamptz, stalled_for interval
) AS (
  SELECT * FROM VALUES
    (1, 'acme', 99.99,
     '2024-06-15T10:00:00Z'::timestamptz,
     '2024-06-15T11:00:00Z'::timestamptz,
     INTERVAL '1 hour')
);
```

Key concepts:

- Every dependency needs a `MOCK` clause with typed columns and sample data.
- `EXPECTED` defines the rows the view should produce.
- `AT TIME` sets `mz_now()` for deterministic testing of temporal filters.
- Tests run in an isolated local Docker container.

Run all tests:

```bash
mz-deploy test
```

Run a filtered subset:

```bash
mz-deploy test 'materialize.public.*'
```

Export results for CI:

```bash
mz-deploy test --junit-xml results.xml
```

## Explain query plans

{{< note >}}
Requires Docker and a database connection. The command stages objects in a
temporary schema on your live Materialize instance.
{{< /note >}}

```bash
mz-deploy explain materialize.public.stalled_orders
```

`explain` compiles the project, stages the target and its dependencies in a
temporary schema, runs `EXPLAIN`, and then cleans up.

To explain an index, use the `#` separator:

```bash
mz-deploy explain materialize.public.stalled_orders#stalled_orders_customer_idx
```

All objects are created on the `quickstart` cluster regardless of your project's
cluster configuration.

## Next step: iterate against production data

Once your changes compile and pass tests locally, use
[`dev`](/manage/mz-deploy/deployments/#iterate-against-production-data)
to validate behavior against real production data. `dev` creates a
per-developer overlay database containing only your dirty views and
requires the `materialize_developer` role — no deployer permissions
needed.
