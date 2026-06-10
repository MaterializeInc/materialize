---
source: src/environmentd/src/http/webhook.rs
revision: a522da8a03
---

# environmentd::http::webhook

Handles inbound HTTP requests to webhook sources: looks up a cached `WebhookAppender` for the target `(database, schema, name)`, validates the request (optional HMAC-style validation via a stored expression), packs the body and headers into one or more `Row`s according to the source's `WebhookBodyFormat` and `WebhookHeaders` configuration, and appends the rows to storage.
Defines the `WebhookError` type that maps internal errors to appropriate HTTP status codes; 429 Too Many Requests is handled at the HTTP layer via Tower's `GlobalConcurrencyLimitLayer` rather than through `WebhookError` variants.
