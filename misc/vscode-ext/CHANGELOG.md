# Changelog

## 0.1.3

- Run `mz-deploy test` and `mz-deploy explain` against the editor's live
  state. Dirty buffers in the workspace are snapshotted to a temp file
  and passed via `--overlay`, so the code lenses no longer force-save
  the active editor before running.

## 0.1.2

- Recognize `types.lock` as a TOML file so it picks up TOML syntax
  highlighting and bracket matching when a TOML language extension is
  installed.
- Run `mz-deploy test` and `mz-deploy explain` through VSCode's task API
  with `ProcessExecution` instead of typing into a fresh terminal. Fixes
  the dropped-first-character bug ("`z-deploy explain ...`") and gives
  the runs a managed terminal pane that reruns and clears cleanly.
- Force `NO_COLOR=1` on the `execFile` calls used for profile listing,
  profile switching, and the activation `--version` check, so any error
  text shown in dialogs is plain rather than ANSI-escaped.

## 0.1.1

## 0.1.0

Initial release.

- LSP client for `mz-deploy` projects (go-to-definition, hover, completion,
  parse diagnostics, code lenses).
- Code-lens commands `mz-deploy.runTest` and `mz-deploy.runExplain` that run
  the corresponding `mz-deploy` subcommand in a terminal.
- `mz-deploy.path` setting controls which `mz-deploy` binary the extension
  spawns.
- Pre-flight check on activation: if the configured binary is missing or
  won't run, the extension surfaces an actionable error dialog with an
  "Open Settings" button instead of failing silently.
