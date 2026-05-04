// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;

fn main() -> Result<(), anyhow::Error> {
    println!("cargo:rustc-env=TARGET_TRIPLE={}", env::var("TARGET")?);

    cc::Build::new()
        .file("src/environmentd/sys.c")
        .compile("environmentd_sys");

    // Generate prost types for Prometheus's client_model.proto. Used by
    // `/metrics/federated` to decode delimited-protobuf bodies scraped from
    // clusterd's `/metrics` endpoint.
    prost_build::Config::new()
        .protoc_executable(mz_build_tools::protoc())
        .compile_protos(&["protos/io.prometheus.client.proto"], &["protos/"])?;

    let out_dir = std::env::var("OUT_DIR").ok().map(std::path::PathBuf::from);
    mz_npm::ensure(out_dir)
}
