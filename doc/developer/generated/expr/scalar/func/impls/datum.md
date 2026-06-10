---
source: src/expr/src/scalar/func/impls/datum.rs
revision: 75abef4839
---

# mz-expr::scalar::func::impls::datum

Provides generic datum-level scalar functions: `IsNull`, `IsTrue`, `IsFalse`, `CastRecordToString`, and other introspection functions that operate on `Datum` values irrespective of their concrete type.
