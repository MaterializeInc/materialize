---
title: Coding Agent Skills
description: "Add Materialize skills to coding agents like Claude Code, Codex, Cursor, and others."
menu:
  main:
    parent: "integrations"
    weight: 25
---

Coding agents like [Claude Code](https://docs.anthropic.com/en/docs/claude-code), [Codex](https://openai.com/index/codex/), [Cursor](https://www.cursor.com/), and others can work with Materialize using our open-source [agent skills](https://github.com/MaterializeInc/agent-skills). Once installed, these skills give your coding agent access to Materialize documentation and reference material so it can provide more accurate assistance when writing queries, setting up sources, creating materialized views, and more.

## Prerequisites

[Node.js](https://nodejs.org/) (v16 or later) must be installed.

## Installation

Install the Materialize agent skills with a single command:

```bash
npx skills add MaterializeInc/agent-skills
```

Once installed, the skills activate automatically when your prompts match
their intended use cases — no additional configuration required.

The skills follow the [Agent Skills Open Standard](https://agentskills.io/home) and work with any coding agent that supports the standard.

To verify the installation succeeded, ask your coding agent a
Materialize-specific question such as "How do I create a source from Kafka in
Materialize?" and confirm it references Materialize documentation in its
response.

## What's included

The **materialize-docs** skill bundles reference files across categories including:

- SQL command references
- Core concepts (clusters, sources, sinks, views, indexes)
- Data ingestion (Kafka, PostgreSQL, MySQL, MongoDB, webhooks)
- Data transformation patterns
- Integration methods and APIs
- Security and deployment guidance

## Related Pages

- [MCP Server](/integrations/llm/)
- [GitHub: Materialize Agent Skills](https://github.com/MaterializeInc/agent-skills)
