// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::fs;
use std::path::PathBuf;

/// URL from which a trusted set of TLS certificate authorities can be
/// downloaded.
const CA_BUNDLE_URL: &str = "https://curl.se/ca/cacert.pem";

fn main() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    // Build protobufs.
    {
        let mut config = prost_build::Config::new();
        config
            .protoc_executable(mz_build_tools::protoc())
            .btree_map(["."]);

        // Bazel places the `fivetran-sdk` submodule in a slightly different place.
        let includes_directories = &[
            PathBuf::from("../../misc/fivetran-sdk"),
            mz_build_tools::protoc_include(),
        ];

        tonic_build::configure()
            // Enabling `emit_rerun_if_changed` will rerun the build script when
            // anything in the include directory (..) changes. This causes quite a
            // bit of spurious recompilation, so we disable it. The default behavior
            // is to re-run if any file in the crate changes; that's still a bit too
            // broad, but it's better.
            .emit_rerun_if_changed(false)
            .compile_protos_with_config(config, &["destination_sdk.proto"], includes_directories)
            .unwrap();
    }

    // Download CA bundle.
    {
        let ca_bundle = reqwest::blocking::get(CA_BUNDLE_URL)
            .unwrap()
            .bytes()
            .unwrap();
        fs::write(out_dir.join("ca-certificate.crt"), ca_bundle).unwrap();
    }
}
