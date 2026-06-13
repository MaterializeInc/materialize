# {{name}}

A [Materialize](https://materialize.com) project managed by mz-deploy.

## Project structure

- `models/` — SQL model definitions organized by database and schema
- `clusters/` — Cluster definitions
- `roles/` — Role definitions
- `project.toml` — Project configuration

## Profiles

A *profile* is a named environment (e.g. `dev`, `staging`, `prod`). Each
profile has two halves:

- **Connection details** — host, port, credentials, TLS settings — live in
  `~/.mz/profiles.toml` and are shared across all your projects.
- **Project-side overrides** — database/cluster suffixes, psql-style
  variables, AWS secret provider — live under `[<profile>]` in
  `project.toml`.

Most commands resolve the active profile in this order: `--profile <name>`
flag, `MZ_DEPLOY_PROFILE` env var, then your per-checkout default. Commands
that don't connect to a database — `compile`, `test`, and `explain` — work
without a profile selected as long as your SQL doesn't reference any
`:variables`. If it does, you'll be prompted to set one.

Set your per-checkout default once:

```sh
mz-deploy profile set dev
```

This writes `.mzprofile` (gitignored, per-developer) so each teammate can
choose their own default without touching shared config. Inspect with:

```sh
mz-deploy profile list      # all available profiles, with the default marked
mz-deploy profile current   # which profile will be used and where it came from
```

## Agent skills

Agent skills from [MaterializeInc/agent-skills](https://github.com/MaterializeInc/agent-skills) help AI agents work with Materialize.

Install them with:

```sh
npx -y skills add MaterializeInc/agent-skills -a universal -a claude-code --project
```

Later, update to the latest version with:

```sh
npx -y skills update
```
