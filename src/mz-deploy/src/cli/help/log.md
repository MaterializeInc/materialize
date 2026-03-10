# log — Show history of promoted deployments

Displays a chronological log of deployments that have been promoted to
production, similar to `git log`. Newest promotions appear first.

## Usage

    mz-deploy log [FLAGS]

## Behavior

1. Connects to the database.
2. Queries all promoted deployments, ordered by promotion time (newest
   first).
3. For each deployment, displays:
   - Deployment ID
   - Git commit (if available)
   - Promoted by and promotion timestamp
   - Schemas included in the deployment
4. When running in a terminal, pipes output through `less`; prints
   directly when output is redirected or in non-interactive environments
   (CI, scripts).

## Flags

- `--limit <N>` / `-l <N>` — Maximum number of deployments to show
  (default: unlimited).
- `--output json` — Print deployment history as JSON to stdout.

## Examples

    mz-deploy log               # Full history
    mz-deploy log --limit 10    # Last 10 promotions
    mz-deploy log -l 1          # Most recent promotion only
    mz-deploy log --output json           # Machine-readable output

## Error Recovery

- **No history found** — No deployments have been promoted yet. Use
  `mz-deploy list` to see active staging deployments.

## Related Commands

- `mz-deploy describe` — Drill into a specific deployment's details.
- `mz-deploy list` — List active (not yet promoted) deployments.
