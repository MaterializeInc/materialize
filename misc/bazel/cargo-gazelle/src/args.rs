// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// Path to an executable that can be used to format `BUILD.bazel` files.
    #[clap(long, value_name = "FORMATTER", env = "FORMATTER")]
    pub formatter: Option<PathBuf>,
    /// Path to the Cargo binary for gathering metadata about a crate.
    #[clap(long, value_name = "CARGO_BINARY", env = "CARGO_BINARY")]
    pub cargo: Option<PathBuf>,
    /// Doesn't actually update any files, just checks if they would have changed.
    #[clap(long)]
    pub check: bool,
    /// Path to a `Cargo.toml` file to generate a `BUILD.bazel` file for.
    ///
    /// Can be a path to a single crate or a workspace.
    #[clap(value_name = "CARGO_TOML")]
    pub path: PathBuf,
}
