---
source: src/compute/src/sink/refresh.rs
revision: b0fa98e931
---

# mz-compute::sink::refresh

Provides `apply_refresh`, an operator that rounds timestamps up to the next scheduled refresh time according to a `RefreshSchedule`, implementing the `REFRESH EVERY` option for materialized views.
Data and frontier advances are coalesced at refresh boundaries so that downstream persist writes are aligned with the configured schedule rather than every individual input change.
