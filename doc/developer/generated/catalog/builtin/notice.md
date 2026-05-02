---
source: src/catalog/src/builtin/notice.rs
revision: db271c31b1
---

# catalog::builtin::notice

Defines the static `BuiltinTable` and `BuiltinView` descriptors for optimizer-notice system tables: `mz_optimizer_notices`, `MZ_NOTICES` (redacted), and `MZ_NOTICES_REDACTED`.
These entries expose optimizer notices generated during query planning to users with appropriate privilege levels (`MONITOR`, `MONITOR_REDACTED`, `SUPPORT`).
