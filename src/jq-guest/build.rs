// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sizes the guest's stack when building for WebAssembly.

/// The guest's stack, in bytes, which bounds how deeply programs can nest and
/// recurse. Compiling uses up to about 600 bytes of it per level of nesting,
/// so this allows a few hundred levels. `mz-jq`'s `MAX_WASM_STACK` must stay
/// well above it.
const STACK_SIZE: usize = 256 << 10;

fn main() {
    if std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("wasm32") {
        println!("cargo:rustc-cdylib-link-arg=-zstack-size={STACK_SIZE}");
    }
}
