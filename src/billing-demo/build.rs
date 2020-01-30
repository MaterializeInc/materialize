// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::env;
use std::path::Path;

use protoc::{DescriptorSetOutArgs, Protoc};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() {
    if let Err(e) = run() {
        println!("ERROR: {}", e);
        while let Some(e) = e.source() {
            println!("    caused by: {}", e);
        }
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let specs = &["billing.proto"];
    // build the rust code
    prost_build::compile_protos(specs, &["./resources"])
        .map_err(|e| format!("unable to run prost build: {}", e))?;

    compile_proto_descriptors(specs)?;

    println!(
        "cargo:rustc-env=VIEWS_DIR={}",
        canonicalize("resources/views")
    );

    Ok(())
}

fn compile_proto_descriptors(specs: &[&str]) -> Result<()> {
    let env_var = "MZ_GENERATE_PROTO";
    println!("cargo:rerun-if-env-changed={}", env_var);
    let gen_dir = canonicalize("resources/gen");
    for spec in specs {
        let basename = extract_stem(spec);
        let out = &format!("{}/{}.pb", gen_dir, basename);
        if env::var_os(env_var).is_some() {
            let protoc = Protoc::from_env_path();
            let args = DescriptorSetOutArgs {
                out,
                includes: &["resources"],
                input: &[spec],
                include_imports: false,
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
