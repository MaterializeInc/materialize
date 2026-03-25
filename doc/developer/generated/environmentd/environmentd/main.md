---
source: src/environmentd/src/environmentd/main.rs
revision: f8348f3bca
---

# environmentd::environmentd::main

Contains the `main` entry-point for the `environmentd` binary: parses the large `clap`-derived `Args` struct covering all server options (TLS, orchestrator, catalog, adapter, LaunchDarkly, AWS, bootstrap, etc.), constructs the `Config` and `ListenersConfig`, binds the SQL and HTTP listeners, and calls `Listeners::serve` to start the server.
This is the primary integration point between the command-line interface and the library crate.
