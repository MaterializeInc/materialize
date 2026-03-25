---
source: src/orchestrator/src/lib.rs
revision: b1c657d28e
---

# mz-orchestrator

Defines the `Orchestrator` and `NamespacedOrchestrator` traits for managing sets of scaled services backed by Kubernetes pods, Docker containers, or local processes.
Services are grouped into namespaces to avoid conflicts between users, while remaining mutually reachable at the network level.
The crate also defines `ServiceConfig` (including resource limits, labels, and scheduling hints), `ServiceEvent` for status change notifications, and a `scheduling_config` module for topology-spread and anti-affinity tuning.
