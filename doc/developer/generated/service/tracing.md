---
source: src/service/src/tracing.rs
revision: 82d92a7fad
---

# mz-service::tracing

Provides `mz_sentry_event_filter`, a Sentry tracing event filter that suppresses events from the `librdkafka` target and delegates all others to the default Sentry filter.
