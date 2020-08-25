// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Build script to manage target-dependent activation of jemalloc if the
//! auto-jemalloc feature is enabled.
//!
//! TODO(benesch): remove this build script and the auto-jemalloc feature once
//! Cargo supports target-specific features. See:
//! https://github.com/rust-lang/cargo/issues/1197

use std::env;

fn main() {
    if env::var_os("CARGO_FEATURE_AUTO_JEMALLOC").is_some()
        && env::var_os("CARGO_CFG_TARGET_OS") != Some("macos".into())
    {
        println!("cargo:rustc-cfg=feature=\"jemalloc\"");
    }
}
