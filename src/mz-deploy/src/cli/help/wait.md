# wait — Wait for staging clusters to be hydrated and ready

Monitors the hydration status of all clusters in a staging deployment.
In continuous mode, displays a live dashboard that updates as clusters
make progress. Exits successfully when all clusters are ready.

## Usage

    mz-deploy wait <DEPLOY_ID> [FLAGS]

## Behavior

A cluster is considered "ready" when all three conditions are met:
- All objects are fully hydrated (materialized views populated).
- Wallclock lag is within the `--allowed-lag` threshold.
- At least one healthy replica exists (not OOM-looping).

**Continuous mode** (default):
1. Queries initial hydration status.
2. Subscribes to live updates from Materialize.
3. Displays an interactive dashboard with per-cluster progress bars.
4. Exits with success when all clusters reach "ready."

**Once mode** (`--once`):
1. Queries hydration status once.
2. Displays the current state.
3. Exits with success (code 0) only if all clusters are already ready;
   otherwise exits with an error.

### Status Indicators

- **ready** — Fully hydrated and caught up.
- **hydrating** — Objects still being materialized.
- **lagging** — Hydrated but lag exceeds the threshold.
- **failing** — No replicas or all replicas are OOM-looping.

## Flags

- `--once` — Check once and exit instead of continuous monitoring.
- `--timeout <SECONDS>` — Maximum time to wait before timing out. By
  default, waits indefinitely.
- `--allowed-lag <SECONDS>` — Maximum wallclock lag for "ready" status
  (default: 300 = 5 minutes).
- `--output json` — Print hydration status as JSON to stdout. In
  continuous mode, emits one JSON object per update.

## Examples

    mz-deploy wait abc123                      # Live monitoring
    mz-deploy wait abc123 --once               # One-time check
    mz-deploy wait abc123 --timeout 300        # Wait up to 5 minutes
    mz-deploy wait abc123 --allowed-lag 60     # Require lag under 1 min
    mz-deploy wait abc123 --output json          # Machine-readable output
    mz-deploy wait abc123 --once --output json   # One-time JSON snapshot

## Error Recovery

- **Timeout reached** — Clusters are still hydrating. Increase `--timeout`
  or check if source data is delayed.
- **Cluster failing** — A replica may be OOM-looping. Check replica status
  in the Materialize console and consider increasing cluster size.
- **Already promoted** — The deployment has already been promoted. No need to
  monitor.

## Exit Codes

- **0** — All clusters ready, or no clusters exist in the deployment.
- **1** — Deployment not found, already promoted, clusters still hydrating,
  cluster failure detected, or timeout reached.

## Related Commands

- `mz-deploy stage` — Create the staging deployment to monitor.
- `mz-deploy promote` — Promote once all clusters are ready.
- `mz-deploy list` — List deployments with cluster status summary.
