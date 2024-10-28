// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Note(parkmycar): We wrap this in a `mod` block soley for the purpose of allowing lints for the
// generated protobuf code.
#[allow(
    clippy::enum_variant_names,
    clippy::clone_on_ref_ptr,
    clippy::as_conversions
)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/fivetran_sdk.rs"));
}
pub use proto::*;
