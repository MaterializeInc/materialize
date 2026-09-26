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

## Installation

You can install the Materialize agent skills in one of two ways:

- [As a plugin](#install-as-a-plugin), if you use Claude Code or Codex. The
  plugin installs all the skills at once and can keep them updated
  automatically.
- [With `npx skills`](#install-with-npx), for any coding agent that supports
  the Agent Skills standard.

Choose one method. Installing the skills with both the plugin and `npx skills`
results in duplicate copies of each skill.

### Install as a plugin

When you install the skills as a plugin, they are namespaced under the plugin
name, for example `materialize:mz-dbt`.

If you already installed the skills with `npx skills`, remove them before you
install the plugin. The command lists both the current and the previous skill
names, and skips any you don't have. Run it in each project where you installed
them, or add `-g` if you installed them globally:

```bash
npx skills remove mz-docs mz-dbt mz-debug-freshness mz-deploy mz-health-check mz-ontology-design mz-optimize-memory mz-terraform-provider mz-terraform-self-managed materialize-docs materialize-dbt materialize-debug-freshness materialize-terraform-provider materialize-terraform-self-managed mcp-developer-analysis
```

#### Claude Code

To install the plugin, run:

```
/plugin marketplace add MaterializeInc/agent-skills
/plugin install materialize@materialize
```

Auto-update is off by default for this marketplace. To turn it on, run
`/plugin`, select **Marketplaces**, choose `materialize`, and select **Enable
auto-update**. Claude Code then checks for updates in the background and asks
you to run `/reload-plugins` when there is one.

To update by hand, run:

```
/plugin marketplace update materialize
/plugin update materialize@materialize
/reload-plugins
```

#### Codex

To install the plugin, run:

```bash
codex plugin marketplace add MaterializeInc/agent-skills
codex plugin add materialize@materialize
```

To update, run:

```bash
codex plugin marketplace upgrade materialize
```

### Install with npx

[Node.js](https://nodejs.org/) (v16 or later) must be installed to use `npx
skills`.

To install the skills, run:

```bash
npx skills add MaterializeInc/agent-skills
```

#### Upgrade skills

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

#### Migrate from the previous skill names

All skills now use the `mz-` prefix: `materialize-docs` is now `mz-docs`, and
`mcp-developer-analysis` is now `mz-health-check`. If you installed the
skills before the rename, remove the old copies and install the skills again,
so each skill appears once under its new name. Run these in each project where
you installed them, or add `-g` to both commands if you installed them
globally:

```bash
npx skills remove materialize-docs materialize-dbt materialize-debug-freshness materialize-terraform-provider materialize-terraform-self-managed mcp-developer-analysis
npx skills add MaterializeInc/agent-skills
```

## Skills

{{% include-headless "/headless/agent-skills-table" %}}

## SQL language server plugin

The `materialize` [Claude Code plugin
marketplace](https://code.claude.com/docs/en/plugin-marketplaces) also provides
the `mz-sql-lsp` plugin, which registers the
[`mz-deploy`](/developer-tools/mz-deploy/) language server for `.sql` files, so Claude
Code navigates your project instead of grepping it. See [AI agent
setup](/developer-tools/mz-deploy/agent-setup/#configuring-for-claude-code) for
installation and configuration.

## Reduce permission prompts (Claude Code)

Claude Code prompts before reading files outside your project, so it may ask
to approve reads each time the `mz-docs` skill opens a new
documentation subdirectory. To stop these prompts, grant read access to the
directory where the skill is installed in `~/.claude/settings.json`.

If you installed the skills [as a plugin](#install-as-a-plugin), grant the
plugin's cache directory. The directory below it changes with every plugin
update, so grant the parent:

```json
{
  "permissions": {
    "additionalDirectories": ["~/.claude/plugins/cache/materialize"]
  }
}
```

If you installed the skills globally [with `npx skills`](#install-with-npx),
they live under `~/.claude/skills/`. Grant the `mz-docs` skill's
directory:

```json
{
  "permissions": {
    "additionalDirectories": ["~/.claude/skills/mz-docs"]
  }
}
```

This grants access to just that one skill's directory. If you have multiple skills installed
and want to cover them all at once, you can broaden the path to
`~/.claude/skills`, though scoping to a single skill is the safer default.

Claude Code's `auto` permission mode also removes the prompts, but applies to
all tools rather than just this directory.

## Related Pages

- [MCP Server](/developer-tools/mcp-server/)
- [mz-deploy AI agent setup](/developer-tools/mz-deploy/agent-setup/)
- [GitHub: Materialize Agent Skills](https://github.com/MaterializeInc/agent-skills)
