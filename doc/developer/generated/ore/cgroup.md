---
source: src/ore/src/cgroup.rs
revision: 6dc0b37208
---

# mz-ore::cgroup

Detects Linux cgroup memory limits by parsing `/proc/self/cgroup` and `/proc/self/mountinfo` at runtime.

`CgroupEntry` and `Mountinfo` represent individual lines from those proc files, and `parse_proc_self_cgroup`/`parse_proc_self_mountinfo` parse them into typed vectors.
`detect_memory_limit` is the top-level entry point: it prefers cgroup v2 (`memory.max` / `memory.swap.max`) and falls back to cgroup v1 (`memory.limit_in_bytes` / `memory.memsw.limit_in_bytes`), returning a `MemoryLimit` with optional `max` and `swap_max` fields in bytes.
