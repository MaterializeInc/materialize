// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").

//! Frontier tracking and consolidation for Materialize's control plane.
//!
//! **Key invariant:** Outside of compute/storage workers, all timestamps are
//! totally ordered. This means antichains always contain at most one element,
//! and we can use [`Ord`] directly instead of reimplementing timely's
//! partial-order trait hierarchy.
//!
//! This crate provides lightweight, Materialize-owned types that the control
//! plane uses instead of depending on timely/differential-dataflow:
//!
//! - [`Antichain`] — a frontier in a totally-ordered time domain (0 or 1 elements)
//! - [`MutableAntichain`] — reference-counted frontier tracking
//! - [`ChangeBatch`] — batched `(time, diff)` accumulator
//! - [`consolidate`] / [`consolidate_updates`] — sort-and-merge for update collections
//! - [`Semigroup`] / [`Monoid`] — difference types

pub mod antichain;
pub mod change_batch;
pub mod consolidation;
pub mod description;
pub mod difference;
pub mod order;

#[cfg(feature = "timely-compat")]
mod timely_compat;

pub use antichain::{Antichain, AntichainRef, MutableAntichain};
pub use change_batch::ChangeBatch;
pub use consolidation::{consolidate, consolidate_updates};
pub use description::Description;
pub use difference::{IsZero, Monoid, Semigroup};
pub use order::{PartialOrder, TotalOrder};
