# mz-deploy VS Code Extension

LSP client for mz-deploy projects. Provides go-to-definition, hover, completion,
code lens, parse diagnostics, data catalog sidebar, DAG visualization, and SQL
worksheets with streaming SUBSCRIBE support.

The extension activates automatically when a workspace contains a `project.toml` file.

## Prerequisites

- **Rust toolchain** — for building the `mz-deploy` binary
- **Node.js + npm** — for building the TypeScript extension
- **VS Code** ^1.88.0

## Building

### 1. Build the mz-deploy binary

From the Materialize workspace root:

```sh
cargo build --release -p mz-deploy
```

The extension checks `~/materialize/target/release/mz-deploy` first for easier
local development, then falls back to `mz-deploy` on your PATH.

### 2. Install npm dependencies

```sh
cd src/mz-deploy/editors/vscode
npm install
```

### 3. Build the extension

```sh
npm run build
```

This compiles two separate TypeScript projects:

| Config | What it compiles | Output |
|--------|-----------------|--------|
| `tsconfig.json` | Extension host code (`extension.ts`, providers) | `out/` |
| `tsconfig.webview.json` | Webview scripts (`catalog.ts`, `dag.ts`, `worksheet.ts`) | `out/webview/` |

## Running in VS Code

### Option A: Debug (F5)

Open `src/mz-deploy/editors/vscode/` as a VS Code workspace and press **F5**.
The launch configuration runs `npm: build` automatically, then opens an Extension
Development Host window. Open any folder containing a `project.toml` to activate
the extension.

### Option B: Install locally

```sh
# From editors/vscode/
npx @vscode/vsce package --no-dependencies
code --install-extension mz-deploy-lsp-0.1.0.vsix
```

## Development Workflow

- **`npm run watch`** — Watches extension host code for changes and recompiles
  automatically. Does not watch webview code.
- After editing webview files (`catalog.ts`, `dag.ts`, `worksheet.ts`), run
  `npm run build` and reload the extension window (Ctrl+Shift+P > "Developer:
  Reload Window").
- After editing Rust LSP code, rebuild with `cargo build --release -p mz-deploy`
  and restart the extension.
