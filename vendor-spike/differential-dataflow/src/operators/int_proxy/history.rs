//! Time-ordered replay of proxy update histories, with meet-advancement.

/// A value history suitable for integer proxy values.
pub(in crate::operators) type IdHistory<T, R> = crate::operators::ValueHistory<u64, T, R>;

