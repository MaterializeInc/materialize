# clean — Delete the project's `target/` build directory

Removes the `target/` directory at the project root. This is where
mz-deploy stores locally cached build artifacts (parsed ASTs, compile
caches, type-check databases). The next `compile`, `test`, or `apply`
will rebuild from scratch.

## Usage

    mz-deploy clean

## Behavior

- Recursively removes `<project>/target/`.
- Idempotent — running on a clean project (or running twice in a row)
  is not an error.
- Does **not** touch the Materialize region. Only local files are
  affected.
- Does **not** require a profile or database connection.

## Examples

    mz-deploy clean              # Delete target/ in the current directory
    mz-deploy -d ./my-proj clean # Delete target/ in a specific project

## Exit Codes

- **0** — `target/` was removed, or was already absent.
- **1** — I/O error (e.g., permission denied).

## Related Commands

- `mz-deploy compile` — Rebuilds the `target/` cache from project files.
