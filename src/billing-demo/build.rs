// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::fs;
use std::path::Path;

use protoc::{DescriptorSetOutArgs, Protoc};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

static ENV_VAR: &str = "MZ_GENERATE_PROTO";

fn main() {
    let out_dir = "src/gen";
    let input = &["resources/billing.proto"];
    for fname in input {
        println!("cargo:rerun-if-changed={}", fname);
    }

    println!(
        "cargo:rustc-env=VIEWS_DIR={}",
        canonicalize("resources/views")
    );
    println!("cargo:rerun-if-env-changed={}", ENV_VAR);
    if let Err(e) = compile_proto_descriptors(input) {
        println!("ERROR: {}", e);
        while let Some(e) = e.source() {
            println!("    caused by: {}", e);
        }
        std::process::exit(1);
    }
    if env::var_os(ENV_VAR).is_none() {
        return;
    }

    if !fs::metadata(out_dir).map(|md| md.is_dir()).unwrap_or(false) {
        panic!(
            "out directory for proto generation does not exist: {}",
            out_dir
        );
    }
    for fname in input {
        if !fs::metadata(fname).map(|md| md.is_file()).unwrap_or(false) {
            panic!("proto schema file does not exist: {}", fname);
        }
    }

    protoc_rust::run(protoc_rust::Args {
        out_dir,
        input,
        includes: &[],
        customize: Default::default(),
    })
    .expect("protoc");
}

fn compile_proto_descriptors(specs: &[&str]) -> Result<()> {
    println!("cargo:rerun-if-env-changed={}", ENV_VAR);
    let gen_dir = canonicalize("resources/gen");
    for spec in specs {
        let basename = extract_stem(spec);
        let out = &format!("{}/{}.pb", gen_dir, basename);
        if env::var_os(ENV_VAR).is_some() {
            let protoc = Protoc::from_env_path();
            let args = DescriptorSetOutArgs {
                out,
                includes: &["resources"],
                input: &[spec],
                include_imports: true,
            };
            protoc
                .write_descriptor_set(args)
                .map_err(|e| format!("unable to write descriptors: {}", e))?;
        }
        println!("cargo:rustc-env=DESCRIPTOR_{}={}", basename, out);
    }

    Ok(())
}

fn canonicalize(name: &str) -> String {
    Path::new(".")
        .join(name)
        .canonicalize()
        .unwrap_or_else(|_| panic!("unable to canonicalize {:?}", name))
        .display()
        .to_string()
}

fn extract_stem(fname: &str) -> &str {
    Path::new(fname)
        .file_stem()
        .unwrap_or_else(|| panic!("unable to extract stem from {:?}", fname))
        .to_str()
        .unwrap()
}
