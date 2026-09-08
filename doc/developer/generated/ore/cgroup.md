---
source: src/ore/src/cgroup.rs
revision: 780c9c1add
---

# mz-ore::cgroup

Detects Linux cgroup memory limits and provides a reusable handle for reading cgroup v2 interface files.

This module must stay free of non-`std` dependencies; it is compiled unconditionally, including into feature-reduced builds such as the wasm32 one where `mz_ore`'s optional dependencies (e.g. `tracing`) are absent. Callers that want to report the resolved cgroup directory should log `CgroupV2::path()` themselves.

`CgroupEntry` and `Mountinfo` represent individual lines from `/proc/self/cgroup` and `/proc/self/mountinfo`, and `parse_proc_self_cgroup`/`parse_proc_self_mountinfo` parse them into typed vectors.
`detect_memory_limit` is the top-level entry point for limit detection: it prefers cgroup v2 (`memory.max` / `memory.swap.max`) and falls back to cgroup v1 (`memory.limit_in_bytes` / `memory.memsw.limit_in_bytes`), returning a `MemoryLimit` with optional `max` and `swap_max` fields in bytes.

`CgroupV2` is a handle to this process's cgroup v2 directory. `CgroupV2::detect()` resolves the directory by walking `/proc/self/mountinfo` and `/proc/self/cgroup`, returning `None` on v1-only hierarchies, mixed hierarchies, and non-Linux platforms. Callers that read repeatedly should call `detect()` once and keep the handle. `CgroupV2::path()` returns the resolved directory (callers are expected to log it once so a misresolution is diagnosable). `CgroupV2::read_u64(file)` reads a single-integer interface file, returning `None` for absent files and for `max` (the literal string the kernel writes when a limit is unlimited). `CgroupV2::read_keyed_u64(file, key)` reads a `key value` interface file and returns the value for the given key.
