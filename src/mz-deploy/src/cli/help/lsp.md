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

## How It Works

- Parse diagnostics update on every keystroke (didChange).
- The project model (used for go-to-definition) rebuilds on file save (didSave).
- If a project build fails (e.g., validation errors), the last successful
  model is kept for navigation.

## Related Commands

- `mz-deploy compile` — Validate the full project from the command line
