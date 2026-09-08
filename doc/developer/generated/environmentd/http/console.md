---
source: src/environmentd/src/http/console.rs
revision: 6e83bda250
---

# environmentd::http::console

Provides HTTP handlers for the web console integration.
`handle_console_config` returns an unauthenticated JSON response with system variable values the console needs for OIDC login (issuer URL, client ID, scopes), read from the adapter's system vars.
`handle_internal_console` is a reverse-proxy handler that forwards requests from the internal HTTP server's `/internal-console` route to the upstream Materialize console URL (default `https://console.materialize.com`), rewriting the `Host` header to avoid Vercel redirect issues.
This avoids CORS issues when the console is accessed through a Teleport proxy by serving static console assets from the same host.
`ConsoleProxyConfig` holds the hyper HTTPS client, upstream URL, route prefix, and `preview_host_suffix` (the host of the upstream URL, under which preview builds are served as subdomains).
The handler supports selecting a console preview build via a `?preview_build=<label>` query parameter. A GET with a non-empty label renders an HTML confirmation page; a same-origin POST stores the selection in the `mz_console_preview_build` cookie (max age 24 hours) and redirects. An empty label clears the cookie. Labels must match a restricted format (prefixed with `console-git-`, valid DNS label characters) to prevent SSRF; preview fetches are always over HTTPS to a validated subdomain of the configured upstream host. Cross-site POSTs are rejected via `Sec-Fetch-Site` / `Origin` header validation.
