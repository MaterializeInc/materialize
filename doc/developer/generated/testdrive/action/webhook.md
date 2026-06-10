---
source: src/testdrive/src/action/webhook.rs
revision: 9fbacdb33b
---

# testdrive::action::webhook

Implements the `webhook-append` builtin command, which posts a body and optional headers to a Materialize webhook source via HTTP POST.
Supports both HTTP and HTTPS (with self-signed certificate acceptance) and optionally validates the response status code.
