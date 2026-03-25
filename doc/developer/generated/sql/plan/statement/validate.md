---
source: src/sql/src/plan/statement/validate.rs
revision: 3f18f464ef
---

# mz-sql::plan::statement::validate

Plans the `VALIDATE CONNECTION` statement, gated on the `ENABLE_CONNECTION_VALIDATION_SYNTAX` feature flag.
Resolves the connection item and emits `Plan::ValidateConnection` with its ID and connection details.
