---
source: src/storage/src/healthcheck.rs
revision: e79a6d96d9
---

# mz-storage::healthcheck

Implements the health-reporting infrastructure used by all storage dataflows.
Defines `StatusNamespace` (which namespaces health updates come from, e.g., Kafka, Postgres, SSH, Internal), `HealthStatusMessage` and `HealthStatusUpdate`, and the `health_operator` timely function that aggregates status messages from all workers into per-source `StatusUpdate` records sent to the controller.
The operator coalesces namespaced statuses, delays transient failures before triggering `SuspendAndRestart`, and writes final status via the `HealthOperatorWriter` trait (with a default `DefaultWriter` backed by `InternalCommandSender`).
