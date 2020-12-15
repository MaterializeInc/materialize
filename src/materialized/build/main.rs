// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;

mod npm;

fn main() -> Result<(), anyhow::Error> {
    println!("cargo:rustc-env=TARGET_TRIPLE={}", env::var("TARGET")?);

    npm::ensure()
}
