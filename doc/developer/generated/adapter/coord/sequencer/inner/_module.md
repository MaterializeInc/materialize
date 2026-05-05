---
source: src/adapter/src/coord/sequencer/inner.rs
revision: e7ac38b338
---

# adapter::coord::sequencer::inner

Houses the per-statement sequencing implementations split into child files for the most complex statement types.
`inner.rs` itself handles the majority of DDL and DML statements; the child modules (`peek`, `subscribe`, `cluster`, `copy_from`, `create_index`, `create_materialized_view`, `create_view`, `secret`, `explain_timestamp`) each own one focused area of the sequencing logic.
Together they implement the full `sequence_plan` dispatch surface for every SQL plan kind.
The generic `sequence_staged` driver and the `Staged` / `StagedContext` / `StageResult` traits live in `inner.rs`, providing the common loop that advances multi-stage plans either immediately or by spawning background tasks and re-queuing via the coordinator's message channel.
`validate_role_attributes` permits the `LOGIN` attribute even when password auth is disabled, restricting the unavailable-feature gate to `SUPERUSER` and `PASSWORD` attributes.
