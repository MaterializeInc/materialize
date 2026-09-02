---
source: src/timely-util/src/columnar/builder.rs
revision: 4bd76235f0
---

# timely-util::columnar::builder

Defines `ColumnBuilder<C>`, a `ContainerBuilder` that accumulates typed columnar items and mints aligned `Column::Align` allocations once the serialized size reaches 10% under the 2 MiB ship threshold (a monotone signal: once fired it stays fired), producing `Column<C>` containers for downstream operators.
Implements `LengthPreservingContainerBuilder` to signal that item counts are preserved through the build process.
