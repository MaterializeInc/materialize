---
source: src/adapter/src/telemetry.rs
revision: ff81701ec3
---

# adapter::telemetry

Provides the `SegmentClientExt` extension trait that adds `environment_track` to `mz_segment::Client`, automatically attaching cloud-provider, region, and organization context to every event.
Defines `StatementFailureType` and `StatementAction` enums describing DDL outcomes for telemetry, and `analyze_audited_statement` which maps `StatementKind` values to `(StatementAction, ObjectType)` pairs for the DDL statements tracked in the audit log.
