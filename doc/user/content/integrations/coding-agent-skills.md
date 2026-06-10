---
title: Agent Skills
description: "Add Materialize skills to coding agents like Claude Code, Codex, Cursor, and others."
menu:
  main:
    parent: "mcp-server"
    weight: 7
---

Coding agents like [Claude Code](https://docs.anthropic.com/en/docs/claude-code), [Codex](https://openai.com/index/codex/), [Cursor](https://www.cursor.com/), and others can work with Materialize using our open-source [agent skills](https://github.com/MaterializeInc/agent-skills). Once installed, these skills give your coding agent access to Materialize documentation and reference material so it can provide more accurate assistance when writing queries, setting up sources, creating materialized views, and more.

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

The skills follow the [Agent Skills Open Standard](https://agentskills.io/home) and work with any coding agent that supports the standard.

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

- [MCP Server](/integrations/llm/)
- [GitHub: Materialize Agent Skills](https://github.com/MaterializeInc/agent-skills)
