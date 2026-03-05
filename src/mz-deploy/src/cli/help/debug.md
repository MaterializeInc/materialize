# debug — Test database connection and display environment information

Connects to Materialize using the active profile and displays version,
environment ID, cluster, and current role. Useful for verifying
connectivity and configuration before running deployments.

## Usage

    mz-deploy debug

## Behavior

1. Loads the connection profile from `profiles.toml`.
2. Connects to the Materialize instance.
3. Queries and displays:
   - Profile name
   - Host and port
   - Environment ID
   - Current cluster
   - Materialize version
   - Current role

## Examples

    mz-deploy debug                     # Use default profile
    mz-deploy debug --profile staging   # Use a specific profile

## Error Recovery

- **Connection refused** — Verify the host and port in `profiles.toml`.
- **Authentication failed** — Check your credentials or app-password.
- **Profile not found** — List available profiles in `profiles.toml`
  or create one with the connection details.

## Related Commands

- `mz-deploy deployments` — List active deployments (also tests connectivity).
