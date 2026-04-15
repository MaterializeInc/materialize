---
source: src/timely-util/src/activator.rs
revision: b0fa98e931
---

# timely-util::activator

Provides `ActivatorTrait` and `RcActivator` for triggering timely operator activations from external events.
`RcActivator` is a cloneable, reference-counted handle that batches activations behind a configurable threshold, preventing runaway self-activations in log dataflows that generate scheduling noise.
