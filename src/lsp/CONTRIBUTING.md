# Contributing to the Materialize Language Server Provider (LSP)

## Developing the Extension

Thank you for your interest in the Materialize LSP! Contributions of many kinds are encouraged and most welcome.

If you have questions, please create a GitHub issue.

### Requirements

* NPM and NodeJS
* Visual Studio Code
* Rust

### Building the Extension

1. Clone the repository:
```bash
git clone https://github.com/MaterializeInc/materialize.git
```
2. Install the dependencies:
```bash
npm install
```
3. Build the LSP:
```bash
cargo build -p lsp
```

### Running and debugging the LSP

To start the LSP and an environment to try it out, press F5 or click the play button in Visual Studio Code **Run and Debug** section.