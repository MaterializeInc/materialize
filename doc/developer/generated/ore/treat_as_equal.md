---
source: src/ore/src/treat_as_equal.rs
revision: 61dbb85ca1
---

# mz-ore::treat_as_equal

Defines `TreatAsEqual<T>`, a newtype wrapper that makes any value compare as equal to every other instance of the same wrapper type.
`Hash`, `Eq`, `PartialEq`, and `Ord` all treat all values as identical regardless of the inner `T`; serialization/deserialization transparently delegate to `T`, and `Debug` renders the inner value directly.
This is used where a field must participate in a derived `Eq`/`Hash`/`Ord` impl but should not influence comparison results (e.g., cached or auxiliary state embedded in otherwise-comparable structs).
