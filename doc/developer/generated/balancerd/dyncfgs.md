---
source: src/balancerd/src/dyncfgs.rs
revision: 249dbbb9b5
---

# balancerd::dyncfgs

Declares all dynamic configuration parameters (`Config` constants) for the balancer, prefixed `balancerd_`, including SIGTERM wait durations, proxy-protocol header injection, maximum connection count (`MAX_CONNECTIONS`), a pre-resolved connection deadline (`PRE_RESOLVED_TIMEOUT`), and logging/OpenTelemetry/Sentry filter strings.

`all_dyncfgs` registers every constant into a `ConfigSet`; `set_defaults` applies CLI-supplied overrides for `INJECT_PROXY_PROTOCOL_HEADER_HTTP`, `MAX_CONNECTIONS`, and `PRE_RESOLVED_TIMEOUT`; `tracing_config` extracts a `TracingParameters` value from the live config set; and `has_tracing_config_update` detects tracing-related changes in a `ConfigUpdates` batch so the balancer can re-apply tracing settings on the fly.
