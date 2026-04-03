# debug — Test database connection and display environment information

Connects to Materialize using the active profile and displays version,
environment ID, cluster, and current role. Useful for verifying
connectivity and configuration before running deployments.

## Usage

    mz-deploy debug

## Flags

- `--output json` — Print connection and environment information as JSON
  to stdout.

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
4. Checks Docker availability and displays:
   - Whether Docker is installed
   - Whether the Docker daemon is running

## Examples

    mz-deploy debug                             # Use default profile
    mz-deploy debug --profile staging            # Use a specific profile
    mz-deploy debug --output json                # Machine-readable output

## Error Recovery

- **Connection refused** — Verify the host and port in `profiles.toml`.
- **Authentication failed** — Check your credentials or app-password.
- **Profile not found** — List available profiles in `profiles.toml`
  or create one with the connection details.
- **Docker not installed** — Install Docker from https://docs.docker.com/get-docker/.
  Docker is required for `mz-deploy test` and `mz-deploy compile` with type checking.
- **Docker daemon not running** — Start Docker Desktop or the Docker daemon
  (`sudo systemctl start docker` on Linux).

## Exit Codes

- **0** — Connection succeeded and test query returned results.
- **1** — Connection failed or query error.

## Related Commands

- `mz-deploy list` — List active deployments (also tests connectivity).
