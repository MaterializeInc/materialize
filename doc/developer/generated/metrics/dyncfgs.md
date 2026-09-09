---
source: src/metrics/src/dyncfgs.rs
revision: 780c9c1add
---

# mz-metrics::dyncfgs

Declares four `mz_dyncfg::Config<Duration>` constants that control how frequently each metrics subsystem refreshes its data: `MZ_METRICS_LGALLOC_REFRESH_INTERVAL` (default 30 s), `MZ_METRICS_LGALLOC_MAP_REFRESH_INTERVAL` (default disabled / 0 s), `MZ_METRICS_RUSAGE_REFRESH_INTERVAL` (default 30 s), and `MZ_METRICS_USAGE_REFRESH_INTERVAL` (default 5 s). The usage interval is intentionally separate from `memory_limiter_interval` so introspection sampling frequency can be tuned without affecting OOM-kill behavior.
Exposes `all_dyncfgs`, which registers all four configs into a `ConfigSet` for use with Materialize's dynamic configuration system.
