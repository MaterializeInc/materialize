---
source: src/adapter/src/webhook.rs
revision: 5f6e25ff20
---

# adapter::webhook

Implements the runtime plumbing for webhook sources: `AppendWebhookValidator` evaluates the user-supplied CHECK expression against an incoming request body and headers (reading secrets via a caching secrets reader), and `AppendWebhookResponse` bundles the validated appender and statistics handle that the HTTP layer uses to write rows.
`WebhookAppenderCache` caches `MonotonicAppender` handles keyed by catalog item ID to avoid a coordinator round-trip for every webhook request.
`AppendWebhookError` is the error enum covering all failure modes (missing secret, invalid body encoding, validation failure, storage errors).
