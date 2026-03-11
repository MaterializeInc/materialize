# delete — Drop an object and remove its project file

Drops a single named object from Materialize and deletes the corresponding
project file from disk. This is the inverse of the `apply` commands.

The drop is performed **without CASCADE** — if the object has dependents,
the command fails with a clear error listing them. The project file is only
removed after a successful drop.

## Usage

    mz-deploy delete cluster <NAME>
    mz-deploy delete connection <NAME>
    mz-deploy delete network-policy <NAME>
    mz-deploy delete role <NAME>
    mz-deploy delete secret <NAME>
    mz-deploy delete table <NAME>

## Flags

- `-y`, `--yes` — Skip confirmation prompt.
- `--output json` — Print result as JSON to stdout. Requires `--yes`
  (interactive prompts are not available in JSON mode).

## Name Format

Clusters, network policies, and roles use simple names:

    mz-deploy delete cluster analytics
    mz-deploy delete network-policy my_policy
    mz-deploy delete role analyst

Connections, secrets, and tables use fully-qualified `database.schema.object` names:

    mz-deploy delete connection mydb.public.pg_conn
    mz-deploy delete secret mydb.public.my_secret
    mz-deploy delete table mydb.public.users

## Behavior

1. Locates the project file for the named object.
2. If no file is found, errors with "not managed by this project".
3. Connects to Materialize.
4. Prompts for confirmation (unless `--yes`).
5. Executes `DROP <TYPE> <name>` (no CASCADE).
6. On success, removes the project file from disk.

## Examples

    mz-deploy delete cluster analytics              # Prompts before dropping
    mz-deploy delete cluster analytics --yes        # Skips prompt
    mz-deploy delete network-policy my_policy       # Drop a network policy
    mz-deploy delete table mydb.public.users        # Drop a table
    mz-deploy delete cluster analytics --output json --yes
    mz-deploy delete connection mydb.public.pg_conn --yes

## Error Recovery

- **Not managed by this project** — The object has no corresponding file
  in the project directory. Check the name and spelling.
- **Object has dependents** — Other objects depend on this one. Drop them
  first, then retry. The project file is NOT removed when the drop fails.
- **Object does not exist** — The object was already dropped in Materialize
  but the file still exists. Remove the file manually.

## Related Commands

- `mz-deploy apply clusters` — Create or update clusters.
- `mz-deploy apply roles` — Create or update roles.
- `mz-deploy apply network-policies` — Create or update network policies.
- `mz-deploy apply connections` — Create or update connections.
- `mz-deploy apply secrets` — Create or update secrets.
- `mz-deploy apply tables` — Create tables.
