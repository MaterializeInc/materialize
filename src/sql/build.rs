// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::{env, fs};

/// Embeds libprotobuf's well-known `.proto` files, which Protobuf schemas
/// from a schema registry may import without the registry holding them.
fn main() {
    let include = mz_build_tools::protoc_include();
    println!("cargo:rerun-if-changed={}", include.display());

    let mut files = Vec::new();
    collect_protos(&include, &include.join("google"), &mut files);
    files.sort();
    assert!(
        !files.is_empty(),
        "no .proto files in {}",
        include.display()
    );

    let mut out = String::from("pub(super) static WELL_KNOWN_TYPES: &[(&str, &str)] = &[\n");
    for (name, path) in files {
        writeln!(out, "    ({name:?}, include_str!({path:?})),").unwrap();
    }
    out.push_str("];\n");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    fs::write(out_dir.join("well_known_types.rs"), out).unwrap();
}

/// Collects `(import name, absolute path)` for each `.proto` file under `dir`.
fn collect_protos(include: &Path, dir: &Path, files: &mut Vec<(String, String)>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_protos(include, &path, files);
        } else if path.extension().is_some_and(|ext| ext == "proto") {
            let name = path.strip_prefix(include).unwrap().to_str().unwrap();
            files.push((name.replace('\\', "/"), path.to_str().unwrap().to_owned()));
        }
    }
}
