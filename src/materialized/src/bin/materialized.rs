// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ffi::OsStr;

fn main() {
    let binary = std::env::args_os().next().expect("known executable");
    let binary_path = std::path::Path::new(&binary);
    match binary_path.file_name().and_then(OsStr::to_str) {
        Some("clusterd") => mz_clusterd::main(),
        Some("environmentd") => mz_environmentd::environmentd::main(),
        other => panic!("Unknown executable: {other:?}"),
    }
}
