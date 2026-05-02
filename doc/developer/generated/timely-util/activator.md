---
source: src/timely-util/src/activator.rs
revision: 4d8deb2de7
---

# timely-util::activator

Provides `ActivatorTrait` and `RcActivator` for triggering timely operator activations from external events.
`RcActivator` is a cloneable, reference-counted handle that batches activations behind a configurable threshold, preventing runaway self-activations in log dataflows that generate scheduling noise.
`ArcActivator` is a `SyncActivator`-based wrapper that coalesces multiple activations: `activate` calls are suppressed if an activation is already pending (tracked via an `Arc<AtomicBool>`), and the paired `ActivationAck` held by the Timely operator resets the flag at the start of each scheduling to allow subsequent activations to go through.
