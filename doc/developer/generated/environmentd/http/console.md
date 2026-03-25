---
source: src/environmentd/src/http/console.rs
revision: f38003ddc8
---

# environmentd::http::console

Provides an HTTP proxy handler (`handle_internal_console`) that forwards requests from the internal HTTP server's `/internal-console` route to the upstream Materialize console URL.
This avoids CORS issues when the console is accessed through a Teleport proxy by serving static console assets from the same host.
