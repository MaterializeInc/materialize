---
source: src/controller-types/src/dyncfgs.rs
revision: 23798cf139
---

# mz-controller-types::dyncfgs

Defines the dynamic configuration knobs (`Config<T>`) consumed by the controller layer and registers them via `all_dyncfgs`.
Covers replica cleanup retry intervals, zero-downtime deployment toggles for sources, wallclock-lag recording/histogram periods, Timely zero-copy allocator settings (with optional lgalloc backing), arrangement merge proportionality, unified cluster hosting, and aggressive read-hold downgrade for paused clusters.
The Timely and arrangement configs (`ENABLE_TIMELY_ZERO_COPY`, `ENABLE_TIMELY_ZERO_COPY_LGALLOC`, `TIMELY_ZERO_COPY_LIMIT`, `ARRANGEMENT_EXERT_PROPORTIONALITY`, `ENABLE_UNIFIED_CLUSTER`) are replica-scoped; the controller bakes their resolved values into each replica's process configuration at provisioning time.
