---
source: src/mz/src/server.rs
revision: e757b4d11b
---

# mz::server

Runs a short-lived local Axum HTTP server that receives the browser-based OAuth login callback.
The `server` function binds to a random port, returns the server future and port number; when the browser completes the OAuth flow it POSTs credentials to `/?clientId=...&secret=...`, which the handler parses into an `AppPassword` and sends on an unbounded channel.
