# Language Server Protocol (LSP) Server for Materialize

This repository contains an LSP implementation for Materialize. It is written in Rust and is currently under development. Contributions are more than welcome.

## Features

Currently supported:

* **Diagnostics**: Parsing and detecting errors in SQL code.

On the roadmap:

* **Completion**: Snippets and suggestions (functions, table names, column names, etc.).
* **CodeLens**: Detecting statements and providing an inline **Run** command.
