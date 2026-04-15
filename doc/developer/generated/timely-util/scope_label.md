---
source: src/timely-util/src/scope_label.rs
revision: b0fa98e931
---

# timely-util::scope_label

Provides the `ScopeExt` extension trait for timely `Scope`, which exposes a `with_label` method.
The method is currently a no-op that returns `self` unchanged; the intended behavior of setting a profiling label before scheduling child operators is not yet implemented.
