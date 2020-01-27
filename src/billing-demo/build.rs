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
    prost_build::compile_protos(specs, &["./resources"])?;

    compile_proto_descriptors(specs)?;

    println!(
        "cargo:rustc-env=VIEWS_DIR={}",
        Path::new(".")
            .join("resources/views")
            .canonicalize()
            .unwrap()
            .display()
    );

    Ok(())
}

fn compile_proto_descriptors(specs: &[&str]) -> Result<()> {
    let env_var = "MZ_GENERATE_PROTO";
    println!("cargo:rerun-if-env-changed={}", env_var);
    for spec in specs {
        let basename = Path::new(spec).file_stem().unwrap().to_str().unwrap();
        let out = &(format!(
            "{}/{}.pb",
            Path::new(".")
                .join("resources/gen")
                .canonicalize()
                .unwrap()
                .display(),
            basename
        ));
        if env::var_os(env_var).is_some() {
            let protoc = Protoc::from_env_path();
            let args = DescriptorSetOutArgs {
                out,
                includes: &["resources"],
                input: &[spec],
                include_imports: false,
            };
            protoc.write_descriptor_set(args)?;
        }
        println!("cargo:rustc-env=DESCRIPTOR_{}={}", basename, out);
    }

    Ok(())
}
