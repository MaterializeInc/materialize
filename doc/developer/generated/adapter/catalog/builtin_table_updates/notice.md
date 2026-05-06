---
source: src/adapter/src/catalog/builtin_table_updates/notice.rs
revision: 7f632d2b4a
---

# adapter::catalog::builtin_table_updates::notice

Generates `BuiltinTableUpdate` rows for `mz_notices` (the optimizer notice system table) from `OptimizerNotice` values produced during optimization.
Provides `pack_optimizer_notices` (resolves to `BuiltinTableUpdate<GlobalId>`) and `pack_optimizer_notice_updates` (produces unresolved `BuiltinTableUpdate<&'static BuiltinTable>` for use in contexts where resolution happens later), and related helpers that map notice fields to the expected column layout.
`Catalog::render_notices` is a thin adapter that uses the system-session humanizer and the catalog's `now` clock to render raw optimizer notices; the humanizer-agnostic core is `CatalogState::render_notices_core`, which accepts any `ExprHumanizer` and an explicit `now` timestamp, enabling callers to render notices before a new item is in the catalog (by wrapping the base humanizer with an `ExprHumanizerExt` that knows about the to-be-created item).
