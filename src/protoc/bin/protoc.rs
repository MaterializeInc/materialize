// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use protoc::Protoc;
use structopt::StructOpt;

/// Compile protocol buffers.
#[derive(StructOpt)]
// Use unusual underscores in option names to match Google's protoc.
#[structopt(rename_all = "snake")]
struct Args {
    /// Import search directory.
    #[structopt(short = "I", long, value_name = "PATH")]
    proto_path: Vec<String>,
    /// Generate Rust source code into OUT_DIR.
    #[structopt(long, required = true, value_name = "OUT_DIR")]
    rust_out: PathBuf,
    /// Derive serde traits for generated messages.
    #[structopt(long)]
    serde: bool,
    /// Input protobuf schemas.
    #[structopt(required = true, value_name = "PROTO_FILES")]
    proto_files: Vec<String>,
}

fn main() -> anyhow::Result<()> {
    let args: Args = ore::cli::parse_args();
    let mut protoc = Protoc::new();
    for path in args.proto_path {
        protoc.include(path);
    }
    for path in &args.proto_files {
        protoc.input(path);
    }
    protoc.serde(args.serde);
    protoc.compile_into(&args.rust_out)
}
