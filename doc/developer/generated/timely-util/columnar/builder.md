---
source: src/timely-util/src/columnar/builder.rs
revision: 2571dcdc4b
---

# timely-util::columnar::builder

Defines `ColumnBuilder<C>`, a `ContainerBuilder` that accumulates typed columnar items and mints aligned `Column::Align` allocations once a batch approaches a 2 MiB boundary (with less than 10% slack), producing `Column<C>` containers for downstream operators.
Implements `LengthPreservingContainerBuilder` to signal that item counts are preserved through the build process.
