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

/// Returns a path to the specified binary.
///
/// Looks for the binary in the following places:
///
/// * Checks the `MZ_IMAGE_<NAME>` environment variable.
/// * Bazel runfiles, if we're building with Bazel.
/// * The current execution directory.
/// * The parent of the current execution directory.
///
pub fn local_image_path(name: &str) -> Result<PathBuf, anyhow::Error> {
    static KNOWN_PROGRAMS: &[(&'static str, &'static str)] =
        // (Program Name, Bazel Runfiles Location)
        //
        // Note: I discovered the runfiles path by manually inspecting the
        // sandbox, which is a little unsatisfying, but it does map to the
        // Bazel target:
        //     "@//src/clusterd:clusterd"
        //  "_main/src/clusterd/clusterd"
        //
        &[
            ("clusterd", "_main/src/clusterd/clusterd"),
            ("environmentd", "_main/src/environmentd/environmentd"),
        ];

    #[cfg_attr(not(bazel), allow(unused))]
    let (_, bazel_target) = KNOWN_PROGRAMS
        .iter()
        .find(|(n, _)| *n == name)
        .ok_or_else(|| anyhow::anyhow!("unknown program '{name}'"))?;

    if let Ok(path) = std::env::var(format!("MZ_IMAGE_{}", name.to_uppercase())) {
        return Ok(PathBuf::from(path));
    }

    cfg_if! {
        if #[cfg(bazel)] {
            let r = runfiles::Runfiles::create().unwrap();
            return Ok(r.rlocation(bazel_target));
        } else {
            let exec_path = std::env::current_exe()?;
            let exec_dir = exec_path.parent().expect("executing the system root?");

            // Check the the binary as a sibling (e.g. running a binary).
            let candidate_1 = exec_dir.join(name);
            if candidate_1.exists() {
                return Ok(candidate_1);
            }

            // Check for the binary in the parent (e.g. integration tests).
            let candidate_2 = exec_dir
                .parent()
                .ok_or_else(|| anyhow::anyhow!("execution dir is the root"))?
                .join(name);
            if candidate_2.exists() {
                return Ok(candidate_2);
            }

            Err(anyhow::anyhow!("binary '{name}' not found!"))
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
            r.rlocation("protobuf/protoc")
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
            r.rlocation("protobuf/src")
        } else if #[cfg(feature = "protobuf-src")] {
            protobuf_src::include()
        } else {
            let path = std::option_env!("PROTOC_INCLUDE").unwrap_or_default();
            PathBuf::from(path)
        }
    }
}
