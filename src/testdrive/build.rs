// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

fn main() {
    // `libduckdb-sys` copies the downloaded shared library into
    // `target/<profile>/deps`. Ensure `target/debug/testdrive` can resolve it
    // at runtime when launched directly.
    if let Ok(target_os) = std::env::var("CARGO_CFG_TARGET_OS") {
        match target_os.as_str() {
            "macos" => {
                println!("cargo:rustc-link-arg-bin=testdrive=-Wl,-rpath,@executable_path/deps");
            }
            "linux" => {
                println!("cargo:rustc-link-arg-bin=testdrive=-Wl,-rpath,$ORIGIN/deps");
            }
            _ => {}
        }
    }
}
