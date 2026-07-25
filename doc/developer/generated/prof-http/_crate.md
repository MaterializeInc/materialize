---
source: src/prof-http/src/lib.rs
revision: dc4dcf22d7
---

# mz-prof-http

Exposes HTTP endpoints for CPU and heap profiling via an Axum router.
`router` serves a profiling UI (`prof.html`, `flamegraph.html` via Askama) and exposes the following routes: `GET /` renders the profiling UI, `POST /` handles form-based actions (`time_fg`, `mem_fg`, `dump_jeheap`, `dump_sym_mzfg`, `activate`, `deactivate`), `POST /cpu` accepts a JSON `CpuProfileRequest` and returns a pprof-encoded CPU profile, `GET /mode` and `POST /mode` inspect and update profiling mode state, `GET /heap` returns a pprof-encoded heap profile, and `GET /static/{path}` serves bundled static assets.
CPU profiling is serialized via a process-global `CpuProfilingGuard` (backed by an `AtomicBool`) that rejects concurrent captures with HTTP 409. While a CPU capture runs, jemalloc heap profiling is suspended via `JemallocProfCtlExt::pause` and restored on drop by `MemProfilingSuspendGuard`, which covers both the success path and future cancellation.
Heap-profiling routes are guarded by the `jemalloc` feature; without it, the `disabled` module returns appropriate error responses.
Depends on `mz-prof`, `mz-http-util`, `mz-build-info`, `pprof_util`, and optionally `jemalloc_pprof`.
