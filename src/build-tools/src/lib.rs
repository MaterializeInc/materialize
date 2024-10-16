// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides access to tools required in build scripts.
//!
//! For example, many crates have a build script that depends on the Protobuf
//! compiler, `protoc`. If we're building with Cargo we'll bootstrap `protoc`
//! by compiling it with [`protobuf-src`], but if we're building with Bazel
//! then we'll use the version of `protoc` included in the runfiles.

use cfg_if::cfg_if;
use std::path::PathBuf;

// Note: This crate's BUILD.bazel compiles with the rustc flag `--cfg=bazel`.

// Runfiles are a Bazel concept, they're a way to provide files at execution
// time. This dependency is provided only by the Bazel build.
#[cfg(bazel)]
extern crate runfiles;

/// Returns if we're currently building with Bazel.
pub const fn is_bazel_build() -> bool {
    cfg_if! {
        if #[cfg(bazel)] {
            true
        } else {
            false
        }
    }
}

/// Returns the path to `protoc`.
///
/// Looks for `protoc` in the following places:
///
/// * Bazel runfiles, if we're building with Bazel.
/// * Bootstraps `protoc` via protobuf-src, if default features are enabled.
/// * `PROTOC` environment variable, if it's set.
/// * The system's `$PATH`, via [`which`].
///
/// If `protoc` can't be found then this function will panic.
pub fn protoc() -> PathBuf {
    cfg_if! {
        if #[cfg(bazel)] {
            let r = runfiles::Runfiles::create().unwrap();
            r.rlocation("protobuf/protoc").expect("set by Bazel")
        } else if #[cfg(feature = "protobuf-src")] {
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
        if #[cfg(bazel)] {
            let r = runfiles::Runfiles::create().unwrap();
            r.rlocation("protobuf/src").expect("set by Bazel")
        } else if #[cfg(feature = "protobuf-src")] {
            protobuf_src::include()
        } else {
            let path = std::option_env!("PROTOC_INCLUDE").unwrap_or_default();
            PathBuf::from(path)
        }
    }
}
