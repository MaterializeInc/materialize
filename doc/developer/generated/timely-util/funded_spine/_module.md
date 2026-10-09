---
source: src/timely-util/src/funded_spine.rs
revision: bb5c454adc
---

# timely-util::funded_spine

A fork of differential-dataflow's fueled `Spine` that bounds voluntary consolidation to work funded by inserted updates and frontier advances.

The stock spine's `exert` path applies a fixed effort allowance on every operator activation, so a trace receiving sparse input but scheduled frequently can perform more merge work than its input justifies. This module introduces two funding mechanisms that gate whether an `exert` call may start new work:

- **`consolidation_credit`** — accrued in proportion to the number of records in each inserted batch (`CONSOLIDATION_CREDIT_PER_UPDATE = 8` per record, scaled by the spine's effort multiplier). Each granted policy request spends one effort unit of credit, funding roughly one grant per `effort / 8` inserted updates.
- **`progress_grants`** — a bank topped up by eight allowances (`CONSOLIDATION_GRANTS_PER_PROGRESS = 8`) for each inserted batch, capped at `MAX_BANKED_PROGRESS_GRANTS = 64`. The bank tracks upstream progress rather than scheduling, so a quiet input whose frontier advances still converges.

An `exert` call that would start new work is declined when no funding is available, and the operator is not rescheduled — there is no funded work until the next insert. A merge already in progress always continues unfunded, since it must complete before anything can land at its level.

Once the input closes (the upper frontier becomes empty), exertion is unbounded again so the trace reaches the policy's reduced form.

## Submodules

- `spine_fueled` — the `Spine<B>` implementation, a minimally-modified copy of differential-dataflow's `spine_fueled.rs` with its crate-internal paths rewritten and the two funding fields added.

## Note on upstreaming

`spine_fueled` is a copy of `trace/implementations/spine_fueled.rs` from differential-dataflow 0.25.1, not a dependency. Upstream fixes reach this copy only when explicitly ported. Review upstream changes on every differential-dataflow version bump.
