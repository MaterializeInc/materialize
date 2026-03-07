# history — Show history of promoted deployments

Displays a chronological log of deployments that have been promoted to
production, similar to `git log`. Newest promotions appear first.

## Usage

    mz-deploy history [FLAGS]

## Behavior

1. Connects to the database.
2. Queries all promoted deployments, ordered by promotion time (newest
   first).
3. For each deployment, displays:
   - Deployment ID
   - Git commit (if available)
   - Promoted by and promotion timestamp
   - Schemas included in the deployment
4. Pipes output through `less` if available; falls back to direct printing.

## Flags

- `--limit <N>` / `-l <N>` — Maximum number of deployments to show
  (default: unlimited).

## Examples

    mz-deploy history               # Full history
    mz-deploy history --limit 10    # Last 10 promotions
    mz-deploy history -l 1          # Most recent promotion only

## Error Recovery

- **No history found** — No deployments have been promoted yet. Use
  `mz-deploy deployments` to see active staging deployments.

## Related Commands

- `mz-deploy describe` — Drill into a specific deployment's details.
- `mz-deploy deployments` — List active (not yet promoted) deployments.
