---
source: src/environmentd/src/http/webhook.rs
revision: f4650d00b
---

# environmentd::http::webhook

Handles inbound HTTP requests to webhook sources: looks up a cached `WebhookAppender` for the target `(database, schema, name)`, validates the request (optional HMAC-style validation via a stored expression), packs the body and headers into one or more `Row`s according to the source's `WebhookBodyFormat` and `WebhookHeaders` configuration, and appends the rows to storage.
The handler reads the `WEBHOOK_MAX_REQUEST_SIZE_BYTES` dyncfg (default 5 MiB) and the `WEBHOOK_VALIDATION_MEMORY_BUDGET_BYTES` dyncfg from the live `ConfigSet` on each request; `WEBHOOK_MAX_REQUEST_SIZE_BYTES` is enforced via `axum::body::to_bytes` (a body that exceeds the limit returns 413, while other body-read errors return 500), and `WEBHOOK_VALIDATION_MEMORY_BUDGET_BYTES` caps the memory the CHECK expression may allocate during validation. The webhook route disables the global `DefaultBodyLimit` so only this per-request cap applies; SQL and other HTTP routes retain the static 5 MiB limit.
Defines the `WebhookError` type that maps internal errors to appropriate HTTP status codes; 429 Too Many Requests is handled at the HTTP layer via Tower's `GlobalConcurrencyLimitLayer` rather than through `WebhookError` variants.
