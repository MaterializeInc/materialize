---
title: "AI agent setup"
aliases:
  - /manage/mz-deploy/agent-setup/
description: "Configure AI coding agents like Claude Code and Codex to work with mz-deploy projects."
menu:
  main:
    parent: mz-deploy-develop
    weight: 44
    identifier: "mz-deploy-agent-setup"
    name: "AI agent setup"
---

`mz-deploy` was built with AI coding agents in mind. Every new project ships
with agent-readable documentation, the CLI provides agent-optimized help, and
the language server gives agents real-time feedback on SQL correctness.

## Project skill

The community [MaterializeInc/agent-skills](https://github.com/MaterializeInc/agent-skills)
repo publishes an agent skill for `mz-deploy` that teaches agents your
project's conventions: one object per file, file paths map to qualified
names, how the deployment lifecycle works, unit test syntax, and how to get
detailed help with `mz-deploy help <command>`.

The skill is not installed by default. Install it in your project with:

```sh
npx -y skills add MaterializeInc/agent-skills -a universal -a claude-code --project
```

This drops the skill into `.agents/skills/mz-deploy/` and wires up a
`.claude/skills/` symlink so Claude Code picks it up. Agents that consume
the universal skill format (Codex and others) load it from the same
location. You don't need to explain the project's conventions — the agent
already knows them.

Update to the latest version later with:

```sh
npx -y skills update
```

## Claude Code on the web

[Claude Code on the web](https://code.claude.com/docs/en/claude-code-on-the-web)
runs each session in a fresh, Anthropic-managed cloud sandbox. The sandbox
doesn't have `mz-deploy` installed, so install it with a **setup script** — a
Bash script that runs once, as root, before Claude Code starts.

In the cloud environment settings, set the **Setup script** field to:

```bash
#!/bin/bash
set -euo pipefail
ARCH=$(uname -m)
curl -L "https://binaries.materialize.com/mz-deploy-latest-$ARCH-unknown-linux-gnu.tar.gz" \
| tar -xzC /usr/local --strip-components=1
```

The sandbox runs Ubuntu on Linux, so this always uses the `unknown-linux-gnu`
build and resolves the architecture (`x86_64` or `aarch64`) at runtime. The
binary lands in `/usr/local/bin`, which is already on `PATH`. The script runs as
root, so no `sudo` is needed. Setup scripts have network access under the
default **Trusted** network mode; if your environment uses **None**, the
download will fail.

To configure the language server in the sandbox as well, commit the
`.claude/settings.json` from [Configuring for Claude Code](#configuring-for-claude-code)
to your repository — it carries over to cloud sessions automatically.

## Agent-optimized help

```bash
mz-deploy help <command>    # Detailed guide for a single command
mz-deploy help --all        # All command guides concatenated
```

Unlike `--help` (which prints brief CLI usage), `help` returns full guides
with behavior notes, examples, error recovery steps, and related commands.

## Language server

The mz-deploy language server gives agents the same benefits it gives human
editors: parse error diagnostics on every file change, go-to-definition across
your project, and column-aware completions scoped to actual dependencies.

For agents, this means fewer incorrect SQL suggestions — the agent sees real
column names and types from your `types.lock` rather than guessing.

### Configuring for Claude Code

Add to your project's `.claude/settings.json`:

```json
{
  "lsp": {
    "mz-deploy": {
      "command": "mz-deploy",
      "args": ["lsp", "-d", "."],
      "filePatterns": ["*.sql"]
    }
  }
}
```

## MCP server

`mz-deploy mcp` exposes Materialize's [developer MCP
server](/integrations/mcp-server/mcp-developer/) to any MCP client (Claude
Code, Claude Desktop, Cursor) using your active profile. The client can then
query the Materialize system catalog without you managing separate credentials
in its config.

This requires the profile to set `http_host` — the Materialize HTTP API
hostname, which is separate from `host` (the SQL endpoint):

```toml
[default]
http_host = "<your-materialize-http-host>"
username = "<your-username>"
password = "<your-password>"
```

For the MCP client configuration block, run `mz-deploy help mcp`.
