---
source: src/ccsr/src/config.rs
revision: 4218b69078
---

# mz-ccsr::config

Defines `ClientConfig`, a builder for `Client`, and the `Auth` struct for HTTP basic credentials.
Callers can add trusted root TLS certificates, set a mTLS identity, override DNS resolution (`resolve_to_addrs`), and install a dynamic URL callback (`dynamic_url`) before calling `build()`.
