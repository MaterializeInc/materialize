---
source: src/testdrive/src/action/version_check.rs
revision: e757b4d11b
---

# testdrive::action::version_check

Implements `run_version_check`, which queries `mz_version_num()` and returns `true` if the running Materialize version falls outside the `[min_version, max_version]` range.
Used by the parser to skip commands tagged with a `[version...]` guard.
