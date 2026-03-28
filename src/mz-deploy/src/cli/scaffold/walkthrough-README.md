# {{name}}

A [Materialize](https://materialize.com) project managed by mz-deploy.

## Getting started

This project includes an interactive walkthrough. Open [Claude Code](https://claude.com/claude-code) in this directory and run `/walkthrough` to begin.

## Project structure

- `models/` — SQL model definitions organized by database and schema
- `clusters/` — Cluster definitions
- `roles/` — Role definitions
- `project.toml` — Project configuration
- `.agents/skills/` — AI agent skills (shared across all agents)
- `.claude/skills/` — Symlinks to `.agents/skills/` for Claude Code

## Agent skills

This project includes community agent skills from [MaterializeInc/agent-skills](https://github.com/MaterializeInc/agent-skills) that help AI agents work with Materialize.

To update installed skills to the latest version:

```sh
npx -y skills update
```

To add additional skills:

```sh
npx -y skills add <owner>/<repo> -a universal -a claude
```
