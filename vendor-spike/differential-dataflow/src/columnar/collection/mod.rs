//! The COLLECTION face of the columnar subsystem: the columnar data as a
//! differential-dataflow collection.
//!
//! [`RecordedUpdates`] is the stream container that flows on edges; it is built
//! input-side by [`Builder`], shuffled by [`Pact`], and mapped over by the
//! columnar operators ([`join_function`], [`leave_dynamic`], [`as_recorded_updates`]).
//! These derive from the same columnar data as the [`trace`](super::trace) face,
//! and the two are linked by two bridge operators: the trace's `Chunker`
//! (collection → batch) and this face's [`as_recorded_updates`] (trace → collection).
//!
//! - [`container`] — `RecordedUpdates` + its `Negate`/`Enter`/`Leave`/`ResultsIn`
//!   impls + the wire codec.
//! - [`builder`] — the input-side `ContainerBuilder`.
//! - [`exchange`] — the shuffle `Pact`.
//! - [`operators`] — `join_function` / `leave_dynamic` / `as_recorded_updates`.

pub mod container;
pub mod builder;
pub mod exchange;
pub mod operators;

pub use container::RecordedUpdates;
pub use builder::ValBuilder as Builder;
pub use exchange::ValPact as Pact;
pub use operators::{as_recorded_updates, join_function, leave_dynamic, DynTime};
