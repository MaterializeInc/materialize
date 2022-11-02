// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;

fn main() {
    println!(
        "cargo:rustc-env=TARGET_TRIPLE={}",
        env::var("TARGET").unwrap()
    );
    println!(
        "cargo:rustc-env=MZ_BUILD_UUID={}",
        uuid::Uuid::new_v4()
    )
}
