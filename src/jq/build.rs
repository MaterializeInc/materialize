// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Builds `mz-jq-guest` for `wasm32-unknown-unknown` into `OUT_DIR`.

use std::env;
use std::path::PathBuf;
use std::process::Command;

const GUEST_TARGET: &str = "wasm32-unknown-unknown";
const GUEST_PROFILE: &str = "jq-guest";

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let workspace_dir = manifest_dir.join("../..");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let target_dir = out_dir.join("guest-target");

    println!("cargo:rerun-if-changed=../jq-guest");
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    println!("cargo:rerun-if-changed=../../Cargo.toml");

    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let mut cmd = Command::new(cargo);
    cmd.current_dir(&workspace_dir)
        .args(["build", "--locked", "--package", "mz-jq-guest"])
        .args(["--target", GUEST_TARGET, "--profile", GUEST_PROFILE])
        .arg("--target-dir")
        .arg(&target_dir);
    // The outer build's flags and wrappers are for the host target. Flags such
    // as `-C target-cpu` or sanitizers break or change a wasm build.
    for var in [
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "CARGO_BUILD_RUSTFLAGS",
        "RUSTC_WORKSPACE_WRAPPER",
        "CARGO_BUILD_TARGET",
        "CARGO_TARGET_DIR",
        "CARGO_MAKEFLAGS",
        "CARGO_LLVM_COV",
        "CARGO_LLVM_COV_TARGET_DIR",
    ] {
        cmd.env_remove(var);
    }
    let status = cmd.status().expect("running cargo for mz-jq-guest");
    assert!(
        status.success(),
        "building mz-jq-guest for {GUEST_TARGET} failed. If the target is \
         missing, run `rustup target add {GUEST_TARGET}`."
    );

    let wasm = target_dir
        .join(GUEST_TARGET)
        .join(GUEST_PROFILE)
        .join("mz_jq_guest.wasm");
    std::fs::copy(&wasm, out_dir.join("mz_jq_guest.wasm")).expect("copying mz_jq_guest.wasm");
}
