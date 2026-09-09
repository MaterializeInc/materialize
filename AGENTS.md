# Materialize

## Dependency management

### Workspace dependencies

All third-party versions declared in `[workspace.dependencies]` in root
`Cargo.toml`. Members use `dep.workspace = true` or
`dep = { workspace = true, optional = true }`.

* **Add new dep**: add to `[workspace.dependencies]` in root `Cargo.toml` first, then `dep.workspace = true` in member crate.
* **Never inline version** in member `Cargo.toml` — `bin/lint-cargo` enforces.
* **Update version**: change once in root `Cargo.toml`.
* Cargo unifies features workspace-wide, so workspace declaration is union of all features across crates. Members just use `dep.workspace = true`.

### Cargo.lock

Never regenerate full Cargo.lock. When changing deps:

* **Add dep or change features**: run `cargo check` — updates only what changed.
* **Update specific crate**: `cargo update -p <crate>` (optional `--precise <version>`).
* **Never bare `cargo update`** — bumps every semver-compatible dep, causes unrelated breakage from transitive changes.
* **If lock regenerated**, diff before commit (`git diff Cargo.lock | grep '^[+-]version'`) and pin back unintended bumps with `cargo update -p <crate> --precise <old-version>`.

## Licensing

Two files control license policy, **keep in sync**: `deny.toml` (`[licenses].allow`) and `about.toml` (`accepted`). New dep with new license not already allowed: add SPDX identifier to both.

