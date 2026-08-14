---
title: Agent Skills
description: "Add Materialize skills to coding agents like Claude Code, Codex, Cursor, and others."
menu:
  main:
    parent: "mcp-server"
    weight: 7
---

Coding agents like [Claude
Code](https://docs.anthropic.com/en/docs/claude-code),
[Codex](https://openai.com/index/codex/), [Cursor](https://www.cursor.com/), and
others can work with Materialize using the open-source [Materialize agent
skills](https://github.com/MaterializeInc/agent-skills). These skills follow the
[Agent Skills Open Standard](https://agentskills.io/home) and work with any
coding agent that supports the standard. Once installed, these skills give your
coding agent access to Materialize documentation and reference material so it
can provide more accurate assistance when writing queries, setting up sources,
creating materialized views, and more.

## Skills

{{% include-headless "/headless/agent-skills-table" %}}

## Prerequisites

[Node.js](https://nodejs.org/) (v16 or later) must be installed.

## Installation

Install the Materialize agent skills with a single command:

```bash
npx skills add MaterializeInc/agent-skills
```

Once installed, you can update installed skills by running `npx skills update`.

## Claude Code plugins

The same repository also doubles as a [Claude Code plugin
marketplace](https://code.claude.com/docs/en/plugin-marketplaces) named
`materialize`. Plugins cover capabilities a portable skill can't express, such
as registering a language server.

The marketplace publishes one plugin, **`mz-sql-lsp`**. It registers the
`mz-deploy` language server for `.sql` files, which gives Claude Code's LSP tool
go-to-definition across your project, hover with column names and types,
document and workspace symbols, and parse error diagnostics. Instead of grepping
for an object reference, Claude resolves it. The plugin bundles a skill that
teaches Claude when to reach for the LSP tool.

The plugin only helps inside an [`mz-deploy`](/manage/mz-deploy/) project, and
`mz-deploy` must be on your `PATH`:

```bash
which mz-deploy
```

Add the marketplace and install the plugin:

```
/plugin marketplace add MaterializeInc/agent-skills
/plugin install mz-sql-lsp@materialize
```

### Setting the project directory

`mz-sql-lsp` requires one setting, **mz-deploy project directory**: the
directory holding your `project.toml`, relative to the repository root. Use `.`
when `project.toml` sits at the root, or a subdirectory name such as `mz` when
the project is nested. Claude Code prompts for this value when you enable the
plugin.

The setting has no working fallback:

- If you dismiss the prompt without entering a value, the language server never
  loads and Claude Code reports `Plugin option "project_dir" isn't set`.
- If the value points somewhere other than the directory holding
  `project.toml`, the server looks healthy but every navigation request returns
  "No definition found".

Run `/plugin`, open the plugin's detail view, and set or correct the value.

If another enabled plugin also registers a language server for `.sql`, the first
one registered wins and the other never starts. The `/plugin` interface names
the plugin whose server is active.

## Reduce permission prompts (Claude Code)

Claude Code prompts before reading files outside your project. Since globally
installed skills live under `~/.claude/skills/`, if you installed the
`materialize-docs` skill globally, Claude Code may ask to approve reads each
time the skill opens a new documentation subdirectory.

To stop these prompts, grant read access to the `materialize-docs` skill in
`~/.claude/settings.json`:

```json
{
  "permissions": {
    "additionalDirectories": ["~/.claude/skills/materialize-docs"]
  }
}
```

This grants access to just that one skill's directory. If you have multiple skills installed
and want to cover them all at once, you can broaden the path to
`~/.claude/skills`, though scoping to a single skill is the safer default.

Claude Code's `auto` permission mode also removes the prompts, but applies to
all tools rather than just this directory.

## Related Pages

- [MCP Server](/integrations/mcp-server/)
- [mz-deploy editor setup](/manage/mz-deploy/editor-setup/)
- [GitHub: Materialize Agent Skills](https://github.com/MaterializeInc/agent-skills)
