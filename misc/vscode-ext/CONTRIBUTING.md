# Contributing

How to build, run, and publish the `mz-deploy` VS Code extension.

## Prerequisites

- **Rust toolchain** — for building the `mz-deploy` binary
- **Node.js + npm** — for building the extension
- **VS Code** ^1.88.0

## Building

### 1. Build the mz-deploy binary

From the Materialize workspace root:

```sh
cargo build --release -p mz-deploy
```

The extension launches the binary configured by the `mz-deploy.path` setting,
which defaults to `mz-deploy` (resolved through your `$PATH` at spawn time).
For development builds, set `mz-deploy.path` to the absolute path of the
binary you want to run.

### 2. Install npm dependencies

```sh
cd misc/vscode-ext
npm install
```

### 3. Build the extension

```sh
npm run build
```

`npm run watch` recompiles on save.

## Running locally

### Debug (F5)

Open `misc/vscode-ext/` as a VS Code workspace and press **F5**. The launch
configuration runs `npm run build` automatically and opens an Extension
Development Host. Open a folder that contains a `project.toml` to activate
the extension.

### Install a packaged build

```sh
# From misc/vscode-ext/
npm run package
code --install-extension mz-deploy-lsp-<version>.vsix
```

## Publishing

The marketplace publisher is `materialize`. To publish you need a Personal
Access Token from Azure DevOps with the *Marketplace > Manage* scope.

```sh
# One-time login (or set VSCE_PAT)
npx @vscode/vsce login materialize

# Publish the version declared in package.json
npm run publish
```

Bump `version` in `package.json` and add an entry to `CHANGELOG.md` before
each release.

## Updating the icon

The marketplace listing uses `icons/mz-logo.png`, generated from
`icons/mz-logo.svg`. To regenerate after editing the SVG (macOS):

```sh
sips -s format png -z 256 256 icons/mz-logo.svg --out icons/mz-logo.png
```

The fill color is baked into the SVG (`#7F4EFF`, Materialize purple) since
the marketplace renders the PNG without VS Code's theme-driven `currentColor`
substitution.
