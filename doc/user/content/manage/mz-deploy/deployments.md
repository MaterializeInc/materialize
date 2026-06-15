---
title: "Deployments"
description: "Stage, test, and promote zero-downtime blue/green deployments."
menu:
  main:
    parent: manage-mz-deploy
    weight: 50
    identifier: "mz-deploy-deployments"
    name: "Deployments"
---

`mz-deploy` gives you a complete lifecycle from local development through
production deployment:

```nofmt
compile ──▶ test ──▶ dev ──▶ stage ──▶ wait ──▶ promote
  local      local   real env  real env
```

[`compile`](/manage/mz-deploy/local-development/#compile-and-validate) and
[`test`](/manage/mz-deploy/local-development/#write-and-run-unit-tests) run
locally to catch errors fast.
[`dev`](#iterate-against-production-data) builds a per-developer overlay
against real production data so you can validate behavior before staging.
When ready, a deployer runs `stage`, `wait`, and `promote` to ship to
production with zero downtime.

## Set up deployment tracking

```bash
mz-deploy setup
```

This creates the `_mz_deploy` database, tracking tables, and three roles for
access control. The command is idempotent — you can safely run it again without
side effects.

When [RBAC is enabled](/security/self-managed/access-control/#enabling-rbac),
`setup` must be run by a **superuser**. It grants `CREATEDB` and
`CREATECLUSTER` on the system to the deploy roles, and only a superuser can
grant system privileges while RBAC is enforced. On clusters with RBAC
disabled, the check is skipped. Once setup completes, the deployer,
developer, and monitor roles use mz-deploy without any further superuser
involvement.

### Roles

`setup` creates three roles that control who can do what:

| Role | Capabilities |
|------|-------------|
| `materialize_deployer` | `apply`, `delete`, `stage`, `promote`, `abort` — full write access |
| `materialize_developer` | `dev`, `list`, `describe`, `log` |
| `materialize_monitor` | `list`, `describe`, `log` — read-only deployment state |

Your database user must be a member of exactly one of these roles to run
commands that connect to the database. Grant the appropriate role to each user:

```sql
GRANT materialize_deployer TO deploy_bot;
GRANT materialize_developer TO dev_user;
```

`compile` and `test` do not require an mz-deploy role because they run locally.

## Deploy to staging

```bash
mz-deploy stage
```

`stage` compiles the project, diffs against the last promoted snapshot, and
deploys only changed objects to staging schemas with suffixed names (for example,
`public_a1b2c3d`).

The deploy ID defaults to the current git SHA prefix. To override it:

```bash
mz-deploy stage --deploy-id my-feature
```

Preview what would be staged without making changes:

```bash
mz-deploy stage --dry-run
```

Allow staging with uncommitted changes:

```bash
mz-deploy stage --allow-dirty
```

{{< note >}}
Common errors during staging:

- **Deploy ID already exists** — abort the existing deployment with
  `mz-deploy abort <id>` or choose a different `--deploy-id`.
- **Uncommitted changes** — commit your changes or pass `--allow-dirty`.
{{< /note >}}

## Iterate against production data

`dev` is the inner-loop command for developers. It creates a
per-developer overlay database (`<db>__<profile>`) containing only the
views and materialized views you've changed, with references rewritten
so unchanged dependencies resolve to production. External dependencies
and `IN CLUSTER` clauses pass through unchanged.

```bash
mz-deploy dev
```

Every run drops the overlay and rebuilds it from scratch, so iterations
are seconds and there's no state to manage.

Show the plan without executing any DDL:

```bash
mz-deploy dev --dry-run
```

Tear down the overlay when you're done:

```bash
mz-deploy dev --down
```

`dev` requires the `materialize_developer` role. `setup` grants the
`CREATEDB` system privilege to that role, so members inherit it
automatically. Tables, sources, sinks, connections, and secrets are
silently skipped — `dev` only overlays views and materialized views.

| | `stage` | `dev` |
|--|---------|-------|
| Required role | `materialize_deployer` | `materialize_developer` |
| Target | Staging schemas alongside production | Per-developer overlay database |
| Git dirty check | Yes | No |
| Object types | All project objects | Views and materialized views only |
| Can be promoted | Yes | No |

## Wait for hydration

```bash
mz-deploy wait <deploy-id>
```

This monitors cluster hydration and displays a live dashboard. The possible
statuses are:

| Status | Meaning |
|--------|---------|
| **ready** | Fully hydrated and caught up |
| **hydrating** | Objects still being materialized |
| **lagging** | Hydrated but lag exceeds the `--allowed-lag` threshold |
| **failing** | No healthy replicas (possible OOM) |

Flags:

- `--timeout <seconds>` — maximum time to wait before exiting with an error.
- `--allowed-lag <seconds>` — lag threshold for the **lagging** status (default:
  300).

{{< note >}}
Common errors during hydration:

- **Timeout** — increase `--timeout` or check cluster health.
- **"failing" status** — check cluster sizing; replicas may need more resources.
{{< /note >}}

## Promote to production

```bash
mz-deploy promote <deploy-id>
```

This atomically swaps staging schemas into production. `promote` automatically
runs a readiness check before proceeding.

Preview what would change:

```bash
mz-deploy promote <deploy-id> --dry-run
```

Flags:

- `--force` — skip conflict detection.
- `--no-ready-check` — skip the automatic readiness check.
- `--dry-run` — preview the promotion without applying changes.

{{< note >}}
If production changed since you staged, `promote` detects the conflict and
aborts. Re-run `mz-deploy stage` to pick up the latest production state before
promoting.
{{< /note >}}

## Manage deployments

List active staging deployments (similar to `git branch`):

```bash
mz-deploy list
```

View promotion history (similar to `git log`):

```bash
mz-deploy log
```

Clean up a staging deployment:

```bash
mz-deploy abort <deploy-id>
```

View deployment details:

```bash
mz-deploy describe <deploy-id>
```

## Day-two operations

### Making changes

`mz-deploy` uses a diff-based model. When you change a SQL file and re-stage,
only the modified objects and their dependents are redeployed.

For example, to change `stalled_orders` from a 30-minute threshold to 1 hour,
update the SQL file:

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
  AND updated_at < mz_now() - INTERVAL '1 hour';
```

When you re-stage, only `stalled_orders` and its dependents are redeployed.

### Deleting objects

Use `mz-deploy delete` to drop objects. The command drops without `CASCADE` and
requires confirmation:

```bash
mz-deploy delete cluster orders
```

Pass `--yes` to skip the confirmation prompt.

Supported types: `cluster`, `connection`, `network-policy`, `role`, `secret`,
`source`, `table`.

### Stable API schemas

If other teams depend on your materialized views, you can mark schemas as
stable API boundaries so that deployments never break downstream consumers.
See [Stable APIs](/manage/mz-deploy/stable-apis/) for details.
