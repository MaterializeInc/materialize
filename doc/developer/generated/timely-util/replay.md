---
source: src/timely-util/src/replay.rs
revision: e79a6d96d9
---

# timely-util::replay

Defines `MzReplay`, a replay operator that reads captured timely event streams into a scope, extending the standard `Replay` with periodic re-activation (controlled by a `period` argument) and integration with `ActivatorTrait` for external wakeup.
Returns a `(Stream, Rc<dyn Any>)` pair where the `Rc` token, when dropped, causes the operator to retract all previously emitted progress and terminate.
