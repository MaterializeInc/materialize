---
source: src/controller-types/src/dyncfgs.rs
revision: 5457329fa8
---

# mz-controller-types::dyncfgs

Defines the dynamic configuration knobs (`Config<T>`) consumed by the controller layer and registers them via `all_dyncfgs`.
Covers replica cleanup retry intervals, zero-downtime deployment toggles for sources, wallclock-lag recording/histogram periods, Timely zero-copy allocator settings (with optional lgalloc backing), arrangement merge proportionality, and aggressive read-hold downgrade for paused clusters.
