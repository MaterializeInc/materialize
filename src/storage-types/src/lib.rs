// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared types for the `mz-storage*` crates

use std::num::NonZeroI64;

use mz_persist_types::{Codec64, Opaque};

pub mod collections;
pub mod configuration;
pub mod connections;
pub mod controller;
pub mod dyncfgs;
pub mod errors;
pub mod instances;
pub mod parameters;
pub mod read_holds;
pub mod read_policy;
pub mod shim;
pub mod sinks;
pub mod sources;
pub mod stats;

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

/// A wrapper struct that presents the adapter token to a format that is understandable by persist
/// and also allows us to differentiate between a token being present versus being set for the
/// first time.
// TODO(aljoscha): Make this crate-public again once the remap operator doesn't
// hold a critical handle anymore.
#[derive(PartialEq, Clone, Debug)]
pub struct PersistEpoch(pub Option<NonZeroI64>);

impl Opaque for PersistEpoch {
    fn initial() -> Self {
        PersistEpoch(None)
    }
}

impl Codec64 for PersistEpoch {
    fn codec_name() -> String {
        "PersistEpoch".to_owned()
    }

    fn encode(&self) -> [u8; 8] {
        self.0.map(NonZeroI64::get).unwrap_or(0).to_le_bytes()
    }

    fn decode(buf: [u8; 8]) -> Self {
        Self(NonZeroI64::new(i64::from_le_bytes(buf)))
    }
}

impl From<NonZeroI64> for PersistEpoch {
    fn from(epoch: NonZeroI64) -> Self {
        Self(Some(epoch))
    }
}
