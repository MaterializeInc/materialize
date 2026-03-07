# deployments — List active staging deployments

Shows all staging deployments that have been created but not yet promoted,
similar to `git branch`. Displays metadata and cluster hydration status
for each deployment.

## Usage

    mz-deploy deployments [FLAGS]

## Behavior

1. Connects to the database.
2. Queries all deployments that have not been promoted.
3. For each deployment, displays:
   - Deployment ID and who created it
   - Git commit (if available)
   - Creation timestamp
   - Cluster hydration status (all ready vs. ready/total count)
   - Schemas included in the deployment

Clusters are evaluated against the `--allowed-lag` threshold to determine
if they are "ready" or "lagging."

## Flags

- `--allowed-lag <SECONDS>` — Maximum wallclock lag threshold for cluster
  status (default: 300 = 5 minutes). Clusters exceeding this are shown as
  lagging.

## Examples

    mz-deploy deployments                    # Default lag threshold
    mz-deploy deployments --allowed-lag 60   # Stricter 1-minute threshold

## Error Recovery

- **No deployments found** — No staging environments exist. Create one
  with `mz-deploy stage`.
- **Connection failed** — Verify your profile with `mz-deploy debug`.

## Related Commands

- `mz-deploy stage` — Create a staging deployment.
- `mz-deploy ready` — Monitor a specific deployment's hydration.
- `mz-deploy deploy` — Promote a staging deployment.
- `mz-deploy abort` — Remove a staging deployment.
- `mz-deploy history` — View promoted (past) deployments.
