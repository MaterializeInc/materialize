---
source: src/frontegg-mock/src/main.rs
revision: 2a6ac3ab4c
---

# frontegg-mock::main

CLI entry point for the `frontegg-mock` binary; parses arguments (listen address, RSA key pairs by value or file, user JSON, optional role-permissions and roles JSON) and starts a `FronteggMockServer`.
Prints the HTTP address on startup and awaits the server task, exiting with an error if it terminates unexpectedly.
