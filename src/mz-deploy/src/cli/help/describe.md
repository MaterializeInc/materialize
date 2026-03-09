# describe — Show detailed information about a specific deployment

Displays comprehensive metadata and object listings for a deployment,
including who deployed it, when, the git commit, and all objects with
their content hashes.

## Usage

    mz-deploy describe <DEPLOY_ID>

## Behavior

1. Connects to the database.
2. Queries deployment metadata for the given ID.
3. Displays:
   - Deployment ID
   - Git commit (if available)
   - Deployed by and timestamp
   - Promotion status and timestamp (if promoted)
   - Schemas included in the deployment
   - All objects with the first 12 characters of their content hash

## Examples

    mz-deploy describe abc123              # Describe a deployment
    mz-deploy describe abc123 --profile staging   # Use a specific profile

## Error Recovery

- **Deployment not found** — Verify the deploy ID with
  `mz-deploy list` (staging) or `mz-deploy log` (promoted).

## Related Commands

- `mz-deploy list` — List active staging deployments.
- `mz-deploy log` — List promoted deployments.
