---
source: src/environmentd/src/environmentd/main.rs
revision: aaed3fa7d3
---

# environmentd::environmentd::main

Contains the `main` entry-point for the `environmentd` binary: parses the large `clap`-derived `Args` struct covering all server options (TLS, orchestrator, catalog, adapter, LaunchDarkly, AWS, bootstrap, etc.), constructs the `Config` and `ListenersConfig`, binds the SQL and HTTP listeners, and calls `Listeners::serve` to start the server.
The allowed CORS origins list is computed once (`mz_http_util::build_cors_allowed_origin`) and retained as both the `AllowOrigin` predicate (`cors_allowed_origin`) and the raw `Vec<HeaderValue>` (`cors_allowed_origin_list`) that is forwarded into `Config` for server-side origin validation by individual endpoints.
This is the primary integration point between the command-line interface and the library crate.
