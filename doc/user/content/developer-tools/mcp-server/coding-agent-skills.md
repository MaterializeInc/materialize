---
title: Agent Skills
description: "Add Materialize skills to coding agents like Claude Code, Codex, Cursor, and others."
menu:
  main:
    parent: "developer-tools"
    name: "Agent Skills"
    weight: 5
aliases:
  - /integrations/coding-agent-skills/
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

[Node.js](https://nodejs.org/) (v16 or later) must be installed to use `npx
skills`. Installing [as a plugin](#install-as-a-plugin) does not need it.

## Installation

Install the Materialize agent skills with a single command:

```bash
npx skills add MaterializeInc/agent-skills
```

## Upgrade skills

We publish upgrades to the Materialize agent skills weekly, so check back
regularly to pick up the latest documentation and reference material. To upgrade
the skills you already have installed:

```bash
npx skills update MaterializeInc/agent-skills
```

To upgrade every skill installed on your machine, regardless of source, omit the
repository:

```bash
npx skills update
```

Most skills now use the `mz-` prefix, and `mcp-developer-analysis` is now
`mz-health-check`. `materialize-docs` keeps its name. If you installed the
skills before the rename, remove the old copies and install the skills again,
so each skill appears once under its new name. Run these in each project where
you installed them, or add `-g` to both commands if you installed them
globally:

```bash
npx skills remove materialize-dbt materialize-debug-freshness materialize-terraform-provider materialize-terraform-self-managed mcp-developer-analysis
npx skills add MaterializeInc/agent-skills
```

## Install as a plugin
If you use Claude Code or Codex, you can also install all the skills using a single
plugin. This allows you to keep skills updated automatically.

When the skills are installed using the plugin, they are namespaced, for example `materialize:mz-dbt`.

Don't install skills using both the plugin and `npx skills`, as this would result in duplicate installations.

If you already installed the skills with `npx skills`, remove them before you
install the plugin. Run this in each project where you installed them, or add
`-g` if you installed them globally:

```bash
npx skills remove materialize-docs mz-dbt mz-debug-freshness mz-deploy mz-health-check mz-ontology-design mz-optimize-memory mz-terraform-provider mz-terraform-self-managed
```

### Claude Code:

```
/plugin marketplace add MaterializeInc/agent-skills
/plugin install materialize@materialize
```

Auto-update is off by default for this marketplace. To turn it on, run
`/plugin`, select **Marketplaces**, choose `materialize`, and select **Enable
auto-update**. Claude Code then checks for updates in the background and asks
you to run `/reload-plugins` when there is one. To update by hand, run `/plugin marketplace update materialize`, then `/plugin update
materialize@materialize`, then `/reload-plugins`.

### Codex:

```bash
codex plugin marketplace add MaterializeInc/agent-skills
codex plugin add materialize@materialize
```

To update, run `codex plugin marketplace upgrade materialize`.

## SQL language server plugin

The `materialize` [Claude Code plugin
marketplace](https://code.claude.com/docs/en/plugin-marketplaces) also provides
the `mz-sql-lsp` plugin, which registers the
[`mz-deploy`](/developer-tools/mz-deploy/) language server for `.sql` files, so Claude
Code navigates your project instead of grepping it. See [AI agent
setup](/developer-tools/mz-deploy/agent-setup/#configuring-for-claude-code) for
installation and configuration.

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

If you installed the skills [as a plugin](#install-as-a-plugin), grant the
plugin's cache directory instead. The directory below it changes with every
plugin update, so grant the parent:

```json
{
  "permissions": {
    "additionalDirectories": ["~/.claude/plugins/cache/materialize"]
  }
}
```

Claude Code's `auto` permission mode also removes the prompts, but applies to
all tools rather than just this directory.

## Related Pages

- [MCP Server](/developer-tools/mcp-server/)
- [mz-deploy AI agent setup](/developer-tools/mz-deploy/agent-setup/)
- [GitHub: Materialize Agent Skills](https://github.com/MaterializeInc/agent-skills)
