---
source: src/adapter/src/catalog/builtin_table_updates/notice.rs
revision: e757b4d11b
---

# adapter::catalog::builtin_table_updates::notice

Generates `BuiltinTableUpdate` rows for `mz_notices` (the optimizer notice system table) from `OptimizerNotice` values produced during optimization.
Provides `pack_optimizer_notice_update` and related helpers that map notice fields to the expected column layout.
