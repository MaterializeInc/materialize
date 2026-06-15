---
title: "AI agent setup"
description: "Configure AI coding agents like Claude Code and Codex to work with mz-deploy projects."
menu:
  main:
    parent: manage-mz-deploy
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

### Configuring for Codex

Add to your project's agent configuration:

```json
{
  "lsp": {
    "sql": {
      "command": "mz-deploy",
      "args": ["lsp", "-d", "."]
    }
  }
}
```
