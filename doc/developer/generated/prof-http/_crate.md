---
source: src/prof-http/src/lib.rs
revision: 4267863081
---

# mz-prof-http

Exposes HTTP endpoints for CPU and heap profiling via an Axum router.
`router` serves a profiling UI (`prof.html`, `flamegraph.html` via Askama), handles form-based actions (`time_fg`, `mem_fg`, `dump_jeheap`, `dump_sym_mzfg`, `activate`, `deactivate`), and serves bundled static assets.
Heap-profiling routes are guarded by the `jemalloc` feature; without it, the `disabled` module returns appropriate error responses.
Depends on `mz-prof`, `mz-http-util`, `mz-build-info`, `pprof_util`, and optionally `jemalloc_pprof`.
