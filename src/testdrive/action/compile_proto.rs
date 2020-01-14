// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Tools for manipulating proto in testdrive

use std::fs;
use std::path::Path;

use protoc::Protoc;
use serde_protobuf::descriptor::Descriptors;

use interchange::protobuf::read_descriptors_from_file;

use crate::error::Error;

pub fn compile_and_encode(source: String) -> Result<String, String> {
    let dest = source.clone() + "spec";
    Ok(match generate_descriptors(&source, &dest) {
        Ok(_) => base64::encode(
            &fs::read(&dest).map_err(|e| format!("reading descriptor for var: {}", e))?,
        ),
        Err(e) => {
            if Path::new(&dest).exists() {
                // Don't worry about recreating the proto file if it's checked in
                base64::encode(
                    &fs::read(&dest).map_err(|e| format!("reading descriptor for var: {}", e))?,
                )
            } else {
                return Err(format!(
                    "couldn't generator proto descriptor file {}: {}",
                    dest, e
                ));
            }
        }
    })
}

/// Takes a path to a .proto spec and attempts to generate a binary file
/// containing a set of descriptors for the message (and any nested messages)
/// defined in the spec.
pub fn generate_descriptors(proto_path: &str, out: &str) -> Result<Descriptors, Error> {
    let protoc = Protoc::from_env_path();
    let descriptor_set_out_args = protoc::DescriptorSetOutArgs {
        out,
        includes: &[],
        input: &[proto_path],
        include_imports: false,
    };

    protoc
        .write_descriptor_set(descriptor_set_out_args)
        .map_err(|e| format!("protoc write descriptor set failed for {}: {}", out, e))?;
    Ok(read_descriptors_from_file(out)
        .map_err(|e| format!("unable to read descriptors for {}: {}", out, e))?)
}
