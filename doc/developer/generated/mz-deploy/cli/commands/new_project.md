---
source: src/mz-deploy/src/cli/commands/new_project.rs
revision: b0390d141f
---

# mz-deploy::cli::commands::new_project

Scaffold a new mz-deploy project directory.
The `scaffold` function is idempotent: files that already exist are left untouched, and the initial git commit is skipped when there is nothing staged (e.g., when re-running `init` in an already-initialized directory). The commit sets an explicit author and committer so it does not depend on the user's git identity.
