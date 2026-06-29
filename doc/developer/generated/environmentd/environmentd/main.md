---
source: src/environmentd/src/environmentd/main.rs
revision: 4061850066
---

# environmentd::environmentd::main

Contains the `main` entry-point for the `environmentd` binary: parses the large `clap`-derived `Args` struct covering all server options (TLS, orchestrator, catalog, adapter, LaunchDarkly, AWS, bootstrap, etc.), constructs the `Config` and `ListenersConfig`, binds the SQL and HTTP listeners, and calls `Listeners::serve` to start the server.
The allowed CORS origins list is computed once (`mz_http_util::build_cors_allowed_origin`) and retained as both the `AllowOrigin` predicate (`cors_allowed_origin`) and the raw `Vec<HeaderValue>` (`cors_allowed_origin_list`) that is forwarded into `Config` for server-side origin validation by individual endpoints.
`frontegg_oauth_issuer_url` is extracted from `FronteggCliArgs::oauth_issuer_url` (flag `--frontegg-oauth-issuer-url`, env `FRONTEGG_OAUTH_ISSUER_URL`) and forwarded into `Config`; when set on a Frontegg listener, `/.well-known/oauth-protected-resource` advertises that URL as the authorization server and 401 Bearer challenges point clients at it.
This is the primary integration point between the command-line interface and the library crate.
