// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared types for the `mz-storage*` crates

pub mod collections;
pub mod configuration;
pub mod connections;
pub mod controller;
pub mod dyncfgs;
pub mod errors;
pub mod instances;
pub mod oneshot_sources;
pub mod parameters;
pub mod read_holds;
pub mod read_policy;
pub mod sinks;
pub mod sources;
pub mod stats;
pub mod time_dependence;

/// Explicitly states the contract between storage and higher levels of
/// Materialize w/r/t which facets of objects managed by storage (e.g. sources,
/// sinks, connections) may be altered.
///
/// n.b. when implementing this trait, leave a warning log with more details as
/// to what the problem was, given that the returned error is scant on details.
pub trait AlterCompatible: std::fmt::Debug + PartialEq {
    fn alter_compatible(
        &self,
        id: mz_repr::GlobalId,
        other: &Self,
    ) -> Result<(), controller::AlterError> {
        if self == other {
            Ok(())
        } else {
            Err(controller::AlterError { id })
        }
    }
}

impl AlterCompatible for mz_repr::GlobalId {}
impl AlterCompatible for mz_repr::CatalogItemId {}
