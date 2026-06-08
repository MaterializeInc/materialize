# dev — Iterate on your dirty views in a per-developer overlay

`dev` is the inner-loop command. Edit a view or materialized view, run
`dev`, and a personal overlay database is rebuilt that contains *only*
your changes. Everything you didn't touch resolves to production. Typical
iterations finish in seconds.

Reach for `dev` when you want to validate a change against real production
data without staging a full deployment. When you're ready to ship the
change, switch to `stage`.

## At a glance

```text
    your edit               cluster you pass         result
    ──────────              ─────────────────        ──────────────────
    customer_ltv.sql   +    compute_dev        →     app__alice.ops.customer_ltv
                                                     running on compute_dev,
                                                     reading prod data for
                                                     everything else
```

## Usage

```text
mz-deploy dev <CLUSTER> [--dry-run]
mz-deploy dev --down
```

## The CLUSTER argument

`CLUSTER` is the cluster every overlay materialized view and index runs
on. The `IN CLUSTER` clause written in your `.sql` source is **ignored**
for the overlay — `dev` rewrites it to `CLUSTER` before issuing DDL.

`dev` refuses to proceed if `CLUSTER` hosts a promoted production
deployment. Create a dedicated dev cluster up front and reuse it across
runs, for example:

```sql
CREATE CLUSTER compute_dev (SIZE = '25cc');
GRANT USAGE, CREATE ON CLUSTER compute_dev TO alice;
```

`CLUSTER` is required for every rebuild and may be omitted only when
passing `--down`.

## How the overlay is built

For each in-project database that has dirty schemas, `dev` creates an
overlay database named `<base_db>__<profile>` (so `app` becomes
`app__alice` for the `alice` profile). The dirty schemas are recreated
inside the overlay; references inside them are rewritten as follows:

| Reference target                | Rewrites to                       |
| ------------------------------- | --------------------------------- |
| In-project, dirty schema        | `<base_db>__<profile>.<schema>.…` |
| In-project, clean schema        | Production (`<base_db>.<schema>.…`) |
| External database               | Unchanged                         |
| `IN CLUSTER <name>` on MV/index | `IN CLUSTER <CLUSTER>` (argument) |

## What `dev` overlays

Only **views and materialized views**. Tables, sources, sinks,
connections, and secrets are silently skipped — overlays are throwaway,
and `apply` owns the long-lived infrastructure.

## Lifecycle

Every run is a full **drop + rebuild**. There is no incremental state to
maintain; the previous overlay databases are dropped before the new ones
are created. A manifest in `_mz_deploy.tables.dev_overlays` tracks what
exists for each `(profile, project)` pair so the next drop phase can
always reach it, even after a crash.

## Flags

- `--down` — Drop every overlay database recorded for this
  `(profile, project)` and exit. `CLUSTER` is not required. Safe to run
  even when no overlay exists.
- `--dry-run` — Print the dirty schemas and target overlay databases.
  No DDL runs.

## Recipes

```text
# Standard inner loop
mz-deploy dev compute_dev

# Preview a rebuild before touching the catalog
mz-deploy dev compute_dev --dry-run

# Throw away today's overlay
mz-deploy dev --down

# Move the overlay to a bigger cluster
mz-deploy dev --down
mz-deploy dev compute_dev_xl
```

## Troubleshooting

- **`materialize_developer` role required** — Ask an administrator:

  ```sql
  GRANT materialize_developer TO <user>;
  ```

- **`refusing to deploy dev overlay onto production cluster`** — You
  passed a `CLUSTER` that hosts a promoted deployment. Provision a
  dedicated dev cluster (see the section above) and re-run with that
  name. To list promoted clusters:

  ```sql
  SELECT cluster_name, database, schema, promoted_at
  FROM _mz_deploy.public.production;
  ```

- **`CREATEDB` privilege required** — `dev` creates overlay databases.
  An administrator can grant it:

  ```sql
  GRANT CREATEDB ON SYSTEM TO <user>;
  ```

- **Stale overlay after a crash or manual `DROP`** — Re-run
  `mz-deploy dev <CLUSTER>`. The sweep at the start of every run
  reconciles the manifest with the live catalog.

- **Manifest drift** — `mz-deploy dev --down` drops everything the
  manifest knows about plus any leftover `<base_db>__<profile>` names.
  To scorch further, drop the overlay databases directly:

  ```sql
  DROP DATABASE <name> CASCADE;
  ```

## Exit codes

- **0** — Overlay rebuilt, `--down` succeeded, or `--dry-run` completed.
- **1** — Role/privilege check failed, cluster is a production cluster,
  compilation error, or DDL execution error.

## See also

- `mz-deploy compile` — Validate SQL without touching the database.
- `mz-deploy stage` — Create a promotable staging deployment when
  you're done iterating.
- `mz-deploy apply` — Manage the long-lived infrastructure (tables,
  sources, sinks) that `dev` does not overlay.
