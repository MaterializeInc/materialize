---
source: src/testdrive/src/action/http.rs
revision: e757b4d11b
---

# testdrive::action::http

Implements the `http-request` builtin command, which sends an HTTP request with a configurable method, URL, content-type, and body.
Accepts a comma-separated list of additional status codes via `accept-additional-status-codes`; returns an error if the actual status code is not 200 and not in the accepted set.
