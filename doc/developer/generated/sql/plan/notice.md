---
source: src/sql/src/plan/notice.rs
revision: a415191fbf
---

# mz-sql::plan::notice

Defines `PlanNotice`, the enum of non-fatal diagnostic messages emitted during planning (e.g. object-does-not-exist warnings, duplicate column names, deprecated options).
Each variant implements `detail` and `hint` methods that produce human-readable supplementary text for delivery to the client.
