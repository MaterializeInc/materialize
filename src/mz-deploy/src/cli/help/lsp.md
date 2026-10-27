# mz-deploy lsp

Start a Language Server Protocol (LSP) server for editor integration.

## Overview

The LSP server provides two capabilities for `.sql` files in your project:

- **Go-to-definition** — Click on a table or view name to jump to the file
  that defines it.
- **Parse error diagnostics** — See SQL syntax errors inline as you type.

The server communicates over stdio using the standard LSP protocol.

## Editor Configuration

### VS Code

Add to `.vscode/settings.json`:

```json
{
  "sqlLanguageServer.command": "mz-deploy",
  "sqlLanguageServer.args": ["lsp", "-d", "${workspaceFolder}"]
}
```

Or use a generic LSP client extension and configure:

```json
{
  "languageServerExample.command": "mz-deploy",
  "languageServerExample.args": ["lsp", "-d", "."]
}
```

### Neovim (nvim-lspconfig)

```lua
local lspconfig = require('lspconfig')
local configs = require('lspconfig.configs')

configs.mz_deploy = {
  default_config = {
    cmd = { 'mz-deploy', 'lsp', '-d', '.' },
    filetypes = { 'sql' },
    root_dir = lspconfig.util.root_pattern('project.toml'),
  },
}

lspconfig.mz_deploy.setup({})
```

### Helix

Add to `languages.toml`:

```toml
[[language]]
name = "sql"
language-servers = ["mz-deploy"]

[language-server.mz-deploy]
command = "mz-deploy"
args = ["lsp", "-d", "."]
```

## Behavior

1. On every keystroke (`didChange`), the affected SQL is parsed and
   diagnostics are published to the editor.
2. On file save (`didSave`) and on watched-file changes, the full project
   model used for go-to-definition is rebuilt.
3. If a rebuild fails (e.g., validation errors), the last successful
   model is retained so navigation keeps working.

## Error Recovery

- **Go-to-definition stops working** — Usually means the most recent
  project rebuild failed. Run `mz-deploy compile` to surface the
  underlying error, fix it, and save the file to trigger another rebuild.
- **Server fails to start in the editor** — Confirm `mz-deploy lsp -d
  <project-root>` runs from a terminal in the same directory. The server
  expects a project root that contains `project.toml`.
- **Diagnostics don't update** — Confirm the editor is sending `didChange`
  notifications (some clients only send on save). If `didChange` is sent
  but diagnostics still don't update, restart the LSP server from the
  editor.

## Exit Codes

- **0** — Clean shutdown (the editor closed stdio).
- **1** — Fatal startup error (e.g., the project root is missing or
  unreadable).

## Related Commands

- `mz-deploy compile` — Validate the full project from the command line.
