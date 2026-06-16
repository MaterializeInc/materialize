// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared catalog data-model types.
//!
//! This crate holds the durable catalog object data model and the in-memory
//! state-update types, separated from `mz-catalog` so that the heavy
//! persist/upgrade codegen in `mz-catalog-durable` does not serialize the
//! compilation of `mz-catalog`.

use std::num::NonZeroI64;

pub mod builtin;
pub mod memory;
pub mod objects;

/// The epoch of a durable catalog state.
pub type Epoch = NonZeroI64;

/// A sentinel used in place of a fingerprint that indicates that a builtin
/// object is runtime alterable. Runtime alterable objects don't have meaningful
/// fingerprints because they may have been intentionally changed by the user
/// after creation.
// NOTE(benesch): ideally we'd use a fingerprint type that used a sum type
// rather than a loosely typed string to represent the runtime alterable
// state like so:
//
//     enum Fingerprint {
//         SqlText(String),
//         RuntimeAlterable,
//     }
//
// However, that would entail a complicated migration for the existing system object
// mapping collection stored on disk.
pub const RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL: &str = "<RUNTIME-ALTERABLE>";
