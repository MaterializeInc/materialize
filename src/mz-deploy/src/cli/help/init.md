# init — Initialize current directory as an mz-deploy project

Scaffolds the current directory with the required structure for mz-deploy,
similar to `new` but without creating a new directory. The project name is
derived from the current directory name.

## Usage

    mz-deploy init [FLAGS]

## Behavior

1. Derives the project name from the current directory.
2. Creates the standard directory structure:
    - `models/materialize/public/` — SQL files for views, MVs, etc.
    - `clusters/` — Cluster definitions
    - `roles/` — Role definitions
3. Writes boilerplate files:
    - `project.toml` — Project configuration
    - `.gitignore` — Ignores `.mz-deploy/` cache directory
    - `README.md` — Getting-started documentation
    - `.agents/skills/mz-deploy/SKILL.md` — LLM agent skill file
    - `.claude/skills/mz-deploy/` — Symlink (auto-loaded by Claude Code)
4. Installs the Materialize agent skill via npx (unless `--no-skill`).
5. Initializes a git repository (unless `--no-git`).

## Flags

- `--no-git` — Skip git repository initialization.
- `--no-skill` — Skip npx agent skill installation.

## Examples

    mkdir my-project && cd my-project
    mz-deploy init                    # Initialize with git and skill install
    mz-deploy init --no-git           # Skip git init
    mz-deploy init --no-skill         # Skip npx skill install

## Exit Codes

- **0** — Project initialized successfully. Also exits 0 if optional npm
  skill installation fails (a warning is printed).
- **1** — File I/O error or git init failed.

## Related Commands

- `mz-deploy new` — Create a new project in a new directory.
- `mz-deploy compile` — Validate the project after adding SQL files.
- `mz-deploy debug` — Verify database connectivity after configuring
  `profiles.toml`.
