// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

fn main() {
    let mut config = prost_build::Config::new();
    config
        .protoc_executable(mz_build_tools::protoc())
        .btree_map(["."])
        .message_attribute(".", ATTR)
        .enum_attribute(".", ATTR)
        .compile_well_known_types()
        // Disable comments because the Google well known types have comments
        // that get mistreated as doc tests.
        .disable_comments(["."]);

    // Bazel places the `fivetran-sdk` submodule in a slightly different place.
    let includes_directories = if mz_build_tools::is_bazel_build() {
        &[
            PathBuf::from("../../../fivetran_sdk"),
            mz_build_tools::protoc_include(),
        ]
    } else {
        &[
            PathBuf::from("../../misc/fivetran-sdk"),
            mz_build_tools::protoc_include(),
        ]
    };

    const ATTR: &str = "#[derive(::serde::Serialize, ::serde::Deserialize)]";

    tonic_build::configure()
        // Enabling `emit_rerun_if_changed` will rerun the build script when
        // anything in the include directory (..) changes. This causes quite a
        // bit of spurious recompilation, so we disable it. The default behavior
        // is to re-run if any file in the crate changes; that's still a bit too
        // broad, but it's better.
        .emit_rerun_if_changed(false)
        .compile_with_config(config, &["destination_sdk.proto"], includes_directories)
        .unwrap();
}
