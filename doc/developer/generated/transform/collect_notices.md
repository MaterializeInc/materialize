---
source: src/transform/src/collect_notices.rs
revision: ce83f42295
---

# mz-transform::collect_notices

Implements `CollectNotices`, a transform that scans the plan for patterns that should generate optimizer notices but do not fit naturally into other transforms.
Currently it detects `= NULL` or `<> NULL` comparisons (which always evaluate to `NULL` in SQL) and pushes an `EqualsNull` notice into `TransformCtx::df_meta`.
