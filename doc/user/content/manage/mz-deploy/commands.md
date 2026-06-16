---
title: "mz-deploy reference"
description: "Reference for all mz-deploy commands, grouped as in mz-deploy --help."
menu:
  main:
    parent: manage-mz-deploy
    weight: 65
    identifier: "mz-deploy-commands"
    name: "Reference"
---

This page lists every `mz-deploy` command, grouped as in `mz-deploy --help`. For
the full guide to any command — all flags, examples, and error recovery — run:

```bash
mz-deploy help <command>   # one command
mz-deploy help --all       # every command
```

## Getting started

| Command | Description |
|---------|-------------|
| `mz-deploy new <name> [--no-git]` | Scaffold a new project in a new directory. See [Get started](/manage/mz-deploy/get-started/). |
| `mz-deploy init [--no-git]` | Scaffold the current directory as a project (project name derived from the directory). See [Get started](/manage/mz-deploy/get-started/#create-a-project). |
| `mz-deploy profile <list\|set\|current>` | Manage the per-checkout default connection profile. See [Profiles](/manage/mz-deploy/deploy/profiles/). |
| `mz-deploy setup` | Initialize the deployment-tracking database, tables, deployment-server cluster, and (when RBAC is enabled) access-control roles. Idempotent. See [Deployments](/manage/mz-deploy/deploy/deployments/#set-up-deployment-tracking). |
| `mz-deploy debug` | Test the database connection and print the active profile, Docker status, environment ID, and deployment-server cluster health. See [Get started](/manage/mz-deploy/get-started/#configure-connection-profiles). |

## Develop

| Command | Description |
|---------|-------------|
| `mz-deploy compile [-v]` | Validate and type-check all SQL locally, with no database connection. `-v` also prints the dependency graph, deployment order, and generated SQL. See [Local development](/manage/mz-deploy/develop/local-development/#compile-and-validate). |
| `mz-deploy clean` | Delete the local `target/` build cache (parsed SQL, compile caches, type-check databases). See [Local development](/manage/mz-deploy/develop/local-development/#clean-build-artifacts). |
| `mz-deploy test [<filter>] [--junit-xml <file>]` | Run inline SQL unit tests in a local Materialize Docker container. See [Local development](/manage/mz-deploy/develop/local-development/#write-and-run-unit-tests). |
| `mz-deploy explain <db>.<schema>.<object>[#<index>]` | Show the `EXPLAIN` plan for a view, materialized view, or index (stages temporarily; requires Docker and a connection). See [Local development](/manage/mz-deploy/develop/local-development/#explain-query-plans). |
| `mz-deploy dev <cluster> [--dry-run]` <br> `mz-deploy dev --down` | Build (or tear down with `--down`) a per-developer overlay of your changed views against production data. See [Deployments](/manage/mz-deploy/deploy/deployments/#iterate-against-production-data). |
| `mz-deploy lsp [-d <dir>]` | Start the Language Server for editor and AI-agent integration. See [Editor setup](/manage/mz-deploy/develop/editor-setup/). |
| `mz-deploy sql [-- <psql args>]` | Launch an interactive `psql` session using the active profile (requires `psql` on your `PATH`). |
| `mz-deploy mcp` | Expose Materialize's developer MCP server to MCP clients via the active profile. See [AI agent setup](/manage/mz-deploy/develop/agent-setup/#mcp-server). |

## Infrastructure

| Command | Description |
|---------|-------------|
| `mz-deploy lock` | Resolve external dependencies and generate `types.lock` for offline type-checking. See [Local development](/manage/mz-deploy/develop/local-development/#lock-types). |
| `mz-deploy apply [--dry-run] [--skip-secrets]` <br> `mz-deploy apply <type>` | Converge infrastructure objects declaratively (clusters, roles, network policies, secrets, connections, sources, tables). Idempotent. See [Infrastructure](/manage/mz-deploy/develop/infrastructure/). |
| `mz-deploy delete <type> <name> [--yes]` | Drop an object (without `CASCADE`) and remove its project file. Types: `cluster`, `connection`, `network-policy`, `role`, `secret`, `source`, `table`. See [Deployments](/manage/mz-deploy/deploy/deployments/#deleting-objects). |

## Deploy

| Command | Description |
|---------|-------------|
| `mz-deploy stage [--deploy-id <id>] [--allow-dirty] [--no-rollback] [--dry-run]` | Compile, diff against the last promoted snapshot, and deploy changed objects to staging schemas. See [Deployments](/manage/mz-deploy/deploy/deployments/#deploy-to-staging). |
| `mz-deploy wait <deploy-id> [--timeout <s>] [--allowed-lag <s>] [--once]` | Monitor staging cluster hydration until ready. See [Deployments](/manage/mz-deploy/deploy/deployments/#wait-for-hydration). |
| `mz-deploy promote <deploy-id> [--force] [--no-ready-check] [--allowed-lag <s>] [--dry-run]` | Atomically swap a staging deployment into production. See [Deployments](/manage/mz-deploy/deploy/deployments/#promote-to-production). |
| `mz-deploy abort <deploy-id>` | Clean up a staging deployment by dropping all its resources. See [Deployments](/manage/mz-deploy/deploy/deployments/#manage-deployments). |
| `mz-deploy describe <deploy-id>` | Show detailed information about a deployment. See [Deployments](/manage/mz-deploy/deploy/deployments/#manage-deployments). |
| `mz-deploy list` | List active staging deployments. See [Deployments](/manage/mz-deploy/deploy/deployments/#manage-deployments). |
| `mz-deploy log [--limit <n>]` | Show the history of promoted deployments. See [Deployments](/manage/mz-deploy/deploy/deployments/#manage-deployments). |

## Global options

These options apply to every command and go before the command name (for
example, `mz-deploy -p staging stage`):

| Option | Description |
|--------|-------------|
| `-d, --directory <dir>` | Project root directory (default: `.`). |
| `-p, --profile <name>` | Connection profile to use (env: `MZ_DEPLOY_PROFILE`). |
| `--profiles-dir <dir>` | Directory to search for `profiles.toml` (default: `~/.mz`; env: `MZ_DEPLOY_PROFILES_DIR`). |
| `--docker-image <image>` | Materialize Docker image to use for `test` and `explain`. |
| `--output <text\|json>` | Output format (default: `text`). |
| `-v, --verbose` | Enable verbose output. |
| `-q, --quiet` | Suppress informational output. |
| `-h, --help` | Print help. |
| `-V, --version` | Print version. |
