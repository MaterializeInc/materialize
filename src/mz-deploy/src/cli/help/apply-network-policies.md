# apply network-policies — Converge network policy definitions to match project files

Reads network policy definitions from the `network_policies/` directory and converges
the live Materialize state to match. Creates missing network policies and alters
ones whose rules have changed. Grants and comments are applied idempotently.

## Usage

    mz-deploy apply network-policies

## Behavior

1. Loads all `.sql` files from the `network_policies/` directory.
2. For each network policy definition:
   - If the policy does not exist, creates it.
   - If the policy exists, alters it to converge rules.
   - Applies associated `GRANT` statements.
   - Applies associated `COMMENT` statements.
3. Reports status per policy:
   - `+` created
   - `~` altered (converging rules)

The command is **idempotent** — running it multiple times produces the
same result. Grants and comments are safe to re-apply.

## Examples

    mz-deploy apply network-policies      # Converge all network policy definitions
    mz-deploy apply network-policies -v   # Verbose: show executed SQL

## Error Recovery

- **Policy creation fails** — Check that the policy rules are valid.
  Already-created policies from this run remain.
- **Permission denied** — Ensure your profile's role has privileges to
  manage network policies.

## Related Commands

- `mz-deploy apply` — Apply all object types in dependency order.
- `mz-deploy delete network-policy <NAME>` — Drop a network policy and remove its project file.