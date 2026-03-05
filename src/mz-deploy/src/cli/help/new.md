# new — Create a new mz-deploy project

Scaffolds a project directory with the required structure for mz-deploy,
including configuration files, model directories, and optionally a git
repository.

## Usage

    mz-deploy new <NAME> [FLAGS]

## Behavior

1. Creates the project directory with the given name.
2. Creates the standard directory structure:
   - `models/materialize/public/` — SQL files for views, MVs, etc.
   - `clusters/` — Cluster definitions
   - `roles/` — Role definitions
3. Writes boilerplate files:
   - `project.toml` — Project configuration
   - `.gitignore` — Ignores `.mz-deploy/` cache directory
   - `README.md` — Getting-started documentation
   - `.agent/skills/mz-deploy/SKILL.md` — LLM agent skill file
   - `.claude/skills/mz-deploy/` — Symlink (auto-loaded by Claude Code)
4. Initializes a git repository (unless `--no-git`).

## Flags

- `--no-git` — Skip git repository initialization.

## Examples

    mz-deploy new my-project             # Create with git init
    mz-deploy new my-project --no-git    # Skip git init

## Error Recovery

- **Directory already exists** — Choose a different name or remove the
  existing directory.

## Related Commands

- `mz-deploy compile` — Validate the project after adding SQL files.
- `mz-deploy debug` — Verify database connectivity after configuring
  `profiles.toml`.
