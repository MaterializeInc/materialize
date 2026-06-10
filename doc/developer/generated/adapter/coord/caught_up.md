---
source: src/adapter/src/coord/caught_up.rs
revision: a1c5f931f6
---

# adapter::coord::caught_up

Implements the "caught up" check used during zero-downtime deployments to determine when all collections have hydrated to a sufficient point before allowing the new environment to take over.
`CaughtUpCheckContext` tracks per-collection hydration status and exposes `check_caught_up` which consults collection frontiers against the required `as_of` thresholds.
