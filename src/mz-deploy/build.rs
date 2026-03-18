//! Build script for mz-deploy.
//!
//! Concatenates the frontend source files (`template.html`, `docs.css`, `docs.js`)
//! into a single `docs_template.html` in `OUT_DIR`. The Rust binary embeds this
//! via `include_str!`.

use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let frontend_dir = Path::new("src/cli/commands/explore/frontend");

    let template = fs::read_to_string(frontend_dir.join("template.html"))
        .expect("failed to read template.html");
    let css = fs::read_to_string(frontend_dir.join("docs.css")).expect("failed to read docs.css");
    let js = fs::read_to_string(frontend_dir.join("docs.js")).expect("failed to read docs.js");

    let output = template
        .replace("__DOCS_CSS__", &css)
        .replace("__DOCS_JS__", &js);

    let out_path = Path::new(&out_dir).join("docs_template.html");
    fs::write(&out_path, output).expect("failed to write docs_template.html");

    // Re-run if any frontend file changes
    println!("cargo:rerun-if-changed=src/cli/commands/explore/frontend/template.html");
    println!("cargo:rerun-if-changed=src/cli/commands/explore/frontend/docs.css");
    println!("cargo:rerun-if-changed=src/cli/commands/explore/frontend/docs.js");
}
