---
source: src/ccsr/src/config.rs
revision: c3979df0ba
---

# mz-ccsr::config

Defines `ClientConfig`, a builder for `Client`, and the `Auth` struct for HTTP basic credentials.
Callers can add trusted root TLS certificates, set a mTLS identity, override DNS resolution (`resolve_to_addrs`), and install a dynamic URL callback (`dynamic_url`) before calling `build()`.
When root certificates are configured, `build()` calls `tls::rustls_config` to construct a preconfigured rustls `ClientConfig` (including the identity when provided) and hands it to reqwest via `tls_backend_preconfigured`; a preconfigured backend causes reqwest to ignore all of its own TLS builder settings, so roots and identity must be embedded in the rustls config rather than added via the reqwest builder. When no root certificates are configured, the reqwest default TLS path is used.
