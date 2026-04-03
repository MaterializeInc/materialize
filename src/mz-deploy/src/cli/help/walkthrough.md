# walkthrough — Create a new project with an interactive tutorial

Scaffolds a project directory identical to `mz-deploy new`, then adds an
interactive walkthrough skill that guides you through building a data mesh
with Materialize.

## Usage

    mz-deploy walkthrough <NAME> [FLAGS]

## Behavior

1. Runs the standard `new` scaffolding (same as `mz-deploy new`).
2. Creates a first git commit ("Initial commit").
3. Adds the walkthrough skill:
    - `.agents/skills/walkthrough/SKILL.md` — Tutorial content
    - `.agents/skills/walkthrough/references/docker-compose.yml` — Local stack
    - `.agents/skills/walkthrough/references/postgres-init.sql` — Sample data
    - `.claude/skills/walkthrough/` — Symlink for Claude Code
4. Overwrites `README.md` with a version that points to the walkthrough.
5. Creates a second git commit ("Add walkthrough skill").

The result is a two-commit project ready for the guided tutorial.

## Flags

- `--no-git` — Skip git initialization. Walkthrough files are still written
  but no commits are created.

## Examples

    mz-deploy walkthrough my-project             # Create with tutorial
    mz-deploy walkthrough my-project --no-git    # Skip git init

## What's in the walkthrough?

The walkthrough is a 9-module interactive session (~90–120 min) that teaches
you how to build a real data mesh with mz-deploy. It covers:

- Connecting to Postgres via CDC
- Writing SQL models (views, materialized views, indexes)
- Testing, staging, and deploying with blue-green deployments
- Advanced patterns like incremental rollouts and rollbacks

Open Claude Code in the created project and run `/walkthrough` to begin.

## Exit Codes

- **0** — Project scaffolded successfully.
- **1** — Target directory already exists, file I/O error, or git failed.

## Related Commands

- `mz-deploy new` — Create a project without the walkthrough.
- `mz-deploy compile` — Validate the project after adding SQL files.
