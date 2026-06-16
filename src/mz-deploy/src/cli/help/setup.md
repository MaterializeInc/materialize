# setup — Initialize deployment tracking database and tables

Creates the `_mz_deploy` database and all tracking tables, views, and roles
used by mz-deploy's deployment commands. This is idempotent — running it
multiple times is safe and has no effect if the infrastructure already exists.

**Must be run as a superuser when RBAC is enabled.** Setup grants
`CREATEDB` and `CREATECLUSTER` on the system to the deploy roles, and
only a superuser can grant system privileges while RBAC is enforced.
The check is skipped on clusters with RBAC disabled, where any role
may issue system grants. Once setup is complete, ordinary
deployer/developer/monitor roles use mz-deploy normally — only this
bootstrap step needs elevated privileges.

## Usage

    mz-deploy setup

## Behavior

1. Connects to Materialize using the active profile.
2. Creates the `_mz_deploy` database (if it doesn't exist).
3. Creates tracking tables and the `production` view in `_mz_deploy`.
4. Creates three roles (if they don't exist):
   - `materialize_deployer` — can stage, promote, and abort deployments
   - `materialize_developer` — read-only access to deployment state
   - `materialize_monitor` — read-only monitoring access to deployment state
5. Grants USAGE on the database and schema, and SELECT, INSERT, UPDATE,
   DELETE on all tables to each role.
6. Grants system privileges to the deploy roles:
   - `materialize_deployer` — `CREATEDB`, `CREATECLUSTER` (needed by
     `stage`, `promote`, and `apply clusters`).
   - `materialize_developer` — `CREATEDB` (needed by `dev` to create the
     per-developer overlay database).
7. Creates the `_mz_deploy_server` cluster (if it doesn't exist) and
   grants `USAGE` on it to each of the three roles. mz-deploy pins every
   connection to this cluster; it is not intended for general-purpose
   use. Resize it via standard `ALTER CLUSTER` if needed.

## Roles

Each database user that runs mz-deploy commands must be a member of exactly
one of the three roles above. After running setup, grant the appropriate role:

    GRANT materialize_deployer TO my_deploy_user;
    GRANT materialize_developer TO my_dev_user;
    GRANT materialize_monitor TO my_monitor_user;

Having multiple mz-deploy roles on a single user is an error — use separate
profiles with distinct users for deploying, developing, and monitoring.

## Examples

    mz-deploy setup                              # Use default profile
    mz-deploy setup --profile production          # Use a specific profile

## Error Recovery

- **Connection refused** — Verify the host and port in `profiles.toml`.
- **Authentication failed** — Check your credentials or app-password.
- **Requires a superuser role** — On clusters with RBAC enabled, setup
  must be run by a superuser. Re-run with a Materialize admin user, or
  have an admin run it once on your behalf.
- **Insufficient privileges** — The user running setup must have permission
  to create databases and roles.

## Exit Codes

- **0** — Setup completed successfully (or infrastructure already exists).
- **1** — Connection or permission error.

## Related Commands

- `mz-deploy debug` — Test connectivity before running setup.
- `mz-deploy stage` — Stage a deployment (requires setup to have been run).
