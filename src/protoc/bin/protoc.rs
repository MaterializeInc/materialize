// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::path::Path;

use getopts::Options;

use protoc::Protoc;

fn main() -> anyhow::Result<()> {
    let mut opts = Options::new();
    opts.optmulti("I", "proto_path", "import search directory", "PATH");
    opts.reqopt("", "rust_out", "generate Rust source code", "OUT_DIR");
    opts.optflag("", "serde", "derive serde traits for generated messages");
    opts.optflag("h", "help", "show this usage information");

    let popts = opts.parse(env::args().skip(1))?;
    if popts.opt_present("h") {
        print!(
            "{}",
            opts.usage("usage: protoc [options] --rust_out=OUT_DIR PROTO_FILES")
        )
    }

    let mut protoc = Protoc::new();
    for path in popts.opt_strs("I") {
        protoc.include(path);
    }
    for path in &popts.free {
        protoc.input(path);
    }
    if popts.opt_present("serde") {
        protoc.serde(true);
    }
    protoc.compile_into(Path::new(&popts.opt_str("rust_out").unwrap()))
}
