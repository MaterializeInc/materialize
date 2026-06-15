---
title: "Editor setup"
description: "Configure VS Code, Neovim, or Helix with the mz-deploy language server."
menu:
  main:
    parent: manage-mz-deploy
    weight: 42
    identifier: "mz-deploy-editor-setup"
    name: "Editor setup"
---

`mz-deploy` includes a language server that gives your editor deep
understanding of your project — not just SQL syntax, but cross-file
dependencies, column schemas, and Materialize-specific features.

## What the language server provides

- **Parse error diagnostics** — SQL syntax errors appear inline as you type.
- **Go-to-definition** — Click a table or view name to jump to the file that
  defines it, across your entire project.
- **Find references** — See every object that depends on the one under your
  cursor.
- **Completions** — Context-aware suggestions for column names, object names,
  functions, and keywords. Column completions are scoped to your file's actual
  dependencies.
- **Hover** — Hover over an object to see its column schema (names, types,
  nullability) pulled from `types.lock` or the internal type cache.
- **Document symbols** — Outline view showing the primary object, indexes,
  constraints, grants, and unit tests in each file.
- **Workspace symbols** — Fuzzy-find any object across your project by name.
- **Code lens** — Clickable "Run Test" above unit tests and "Explain" above
  materialized views.

## VS Code

Install the `mz-deploy` VS Code extension. It activates automatically when
your workspace contains a `project.toml`.

The extension adds:

- All language server features listed above.
- A **data catalog sidebar** for browsing objects, columns, and metadata.
- A **dependency graph panel** for visualizing how objects relate.
- **Keyword highlighting** that understands SQL strings, comments, and
  identifiers.

To configure a custom binary path, add to your VS Code settings:

```json
{
  "mz-deploy.path": "/path/to/mz-deploy"
}
```

## Neovim

Using [nvim-lspconfig](https://github.com/neovim/nvim-lspconfig):

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

## Helix

Add to your `languages.toml`:

```toml
[[language]]
name = "sql"
language-servers = ["mz-deploy"]

[language-server.mz-deploy]
command = "mz-deploy"
args = ["lsp", "-d", "."]
```

## How it works

The language server communicates over stdio using the standard LSP protocol.
Start it manually with:

```bash
mz-deploy lsp -d <project-root>
```

Diagnostics update on every keystroke. The project model (used for
go-to-definition, completions, and references) rebuilds on file save. If a
rebuild fails, the last successful model is kept so navigation continues
working.
