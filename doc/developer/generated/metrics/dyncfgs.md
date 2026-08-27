---
source: src/metrics/src/dyncfgs.rs
revision: 5a4a36c4fd
---

# mz-metrics::dyncfgs

Declares three `mz_dyncfg::Config<Duration>` constants that control how frequently each metrics subsystem refreshes its data: `MZ_METRICS_LGALLOC_REFRESH_INTERVAL` (default 30 s), `MZ_METRICS_LGALLOC_MAP_REFRESH_INTERVAL` (default disabled / 0 s), and `MZ_METRICS_RUSAGE_REFRESH_INTERVAL` (default 30 s).
Exposes `all_dyncfgs`, which registers all three configs into a `ConfigSet` for use with Materialize's dynamic configuration system.
