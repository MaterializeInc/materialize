// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides access to tools required in build scripts.

use cfg_if::cfg_if;
use std::path::PathBuf;

/// Returns the path to `protoc`.
///
/// Looks for `protoc` in the following places:
///
/// * Bootstraps `protoc` via protobuf-src, if default features are enabled.
/// * `PROTOC` environment variable, if it's set.
/// * The system's `$PATH`, via [`which`].
///
/// If `protoc` can't be found then this function will panic.
pub fn protoc() -> PathBuf {
    cfg_if! {
        if #[cfg(feature = "protobuf-src")] {
            protobuf_src::protoc()
        } else {
            // If we're not building with Bazel, nor have the `protobuf-src`
            // feature specified, then try using the system's `protoc`.
            match std::option_env!("PROTOC") {
                Some(path) => PathBuf::from(path),
                None => which::which("protoc").expect("protoc to exist on system"),
            }
        }
    }
}

/// Returns the path to the protobuf includes directory.
///
/// Note: this is primarily used to include "well known types".
pub fn protoc_include() -> PathBuf {
    cfg_if! {
        if #[cfg(feature = "protobuf-src")] {
            protobuf_src::include()
        } else {
            let path = std::option_env!("PROTOC_INCLUDE").unwrap_or_default();
            PathBuf::from(path)
        }
    }
}
