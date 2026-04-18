---
source: src/adapter/src/catalog/builtin_table_updates/notice.rs
revision: a632912d24
---

# adapter::catalog::builtin_table_updates::notice

Generates `BuiltinTableUpdate` rows for `mz_notices` (the optimizer notice system table) from `OptimizerNotice` values produced during optimization.
Provides `pack_optimizer_notices` (resolves to `BuiltinTableUpdate<GlobalId>`) and `pack_optimizer_notice_updates` (produces unresolved `BuiltinTableUpdate<&'static BuiltinTable>` for use in contexts where resolution happens later), and related helpers that map notice fields to the expected column layout.
