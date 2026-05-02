---
source: src/sql/src/plan/statement/raise.rs
revision: fa5d5d8ec5
---

# mz-sql::plan::statement::raise

Plans `RAISE` statements (`RAISE WARNING`, `RAISE INFO`, etc.) that send notices to the client, gated on the `ENABLE_RAISE_STATEMENT` feature flag.
