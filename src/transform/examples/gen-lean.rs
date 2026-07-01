// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Generate the Lean 4 specification from the live rewrite DSL.
//!
//! ```text
//! cargo run -p mz-transform --example gen-lean              # write lean/MirRewrite/Generated.lean
//! cargo run -p mz-transform --example gen-lean -- -         # write to stdout
//! cargo run -p mz-transform --example gen-lean -- path.lean # write to a custom path
//! ```

use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    let rules = mz_transform::eqsat::rules_ast();
    let lean = mz_transform::eqsat::lean::emit_lean(&rules);

    let arg = std::env::args().nth(1);
    match arg.as_deref() {
        Some("-") => {
            print!("{lean}");
            Ok(())
        }
        Some(path) => std::fs::write(path, lean),
        None => {
            // Default: write to the crate's `lean/` directory.
            let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            path.push("lean/MirRewrite/Generated.lean");
            std::fs::write(&path, lean)?;
            eprintln!("wrote {}", path.display());
            Ok(())
        }
    }
}
