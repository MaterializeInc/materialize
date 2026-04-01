---
source: src/timely-util/src/scope_label.rs
revision: c642b63c77
---

# timely-util::scope_label

Provides `LabelledScope<G>` and `LabelledOperator<O>`, which wrap a timely `Scope` and `Operate` implementation to set a profiling label (via `custom_labels::with_label`) on every operator scheduling call.
`ScopeExt::with_label` converts any scope into a `LabelledScope` that uses the scope's name as the label, enabling CPU profilers to attribute work to the enclosing dataflow scope.
