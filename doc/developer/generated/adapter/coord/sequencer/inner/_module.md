---
source: src/adapter/src/coord/sequencer/inner.rs
revision: 892cf626bc
---

# adapter::coord::sequencer::inner

Houses the per-statement sequencing implementations split into child files for the most complex statement types.
`inner.rs` itself handles the majority of DDL and DML statements; the child modules (`peek`, `subscribe`, `cluster`, `copy_from`, `create_index`, `create_materialized_view`, `create_view`, `create_continual_task`, `secret`, `explain_timestamp`) each own one focused area of the sequencing logic.
Together they implement the full `sequence_plan` dispatch surface for every SQL plan kind.
