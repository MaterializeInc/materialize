# mz-deploy for VS Code

Language support for [mz-deploy](https://github.com/MaterializeInc/materialize)
projects. Go-to-definition, hover, completion, parse diagnostics, and code
lenses for running tests and explaining objects — all backed by the
`mz-deploy` LSP server.

## Requirements

This extension is a thin client over the `mz-deploy` LSP server. You must have
the `mz-deploy` binary installed and reachable. By default the extension
spawns `mz-deploy` from your `$PATH`; set the `mz-deploy.path` setting to an
absolute path to use a specific build.

The extension activates automatically when a workspace contains a
`project.toml` file.

## Features

- **Go-to-definition** and **hover** for objects, columns, and references in
  your project's SQL.
- **Completion** for object names, columns, and keywords.
- **Diagnostics** for parse errors and project validation problems.
- **Code lenses**:
  - **Run Test** above each `EXECUTE UNIT TEST` statement — opens a terminal
    and runs `mz-deploy test '<filter>'`.
  - **Explain** above named `CREATE INDEX` and `CREATE MATERIALIZED VIEW`
    statements — runs `mz-deploy explain '<target>'` in a terminal.

## Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `mz-deploy.path` | `mz-deploy` | Path to the `mz-deploy` binary. |

## License

See the bundled [LICENSE](LICENSE) file (Business Source License 1.1).
