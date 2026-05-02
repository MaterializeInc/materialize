---
source: src/environmentd/src/http/console.rs
revision: 964fd6a0ab
---

# environmentd::http::console

Provides HTTP handlers for the web console integration.
`handle_console_config` returns an unauthenticated JSON response with system variable values the console needs for OIDC login (issuer URL, client ID, scopes), read from the adapter's system vars.
`handle_internal_console` is a reverse-proxy handler that forwards requests from the internal HTTP server's `/internal-console` route to the upstream Materialize console URL (default `https://console.materialize.com`), rewriting the `Host` header to avoid Vercel redirect issues.
This avoids CORS issues when the console is accessed through a Teleport proxy by serving static console assets from the same host.
`ConsoleProxyConfig` holds the hyper HTTPS client, upstream URL, and route prefix.
