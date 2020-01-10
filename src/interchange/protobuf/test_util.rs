// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Utilities for running tests that go over the wire, e.g. testdrive
//!
//! Some code in [`proto`] is generated from .proto schema files.
//!
//! [`native_types`] provides conversion functions and more rusty types for them.

use protoc::Protoc;
use serde_protobuf::descriptor::Descriptors;

use super::read_descriptors_from_file;

pub mod gen;
pub mod native_types;

pub trait DbgMsg: protobuf::Message + std::fmt::Debug {}
pub type DynMessage = Box<dyn DbgMsg>;

pub trait ToMessage
where
    Self: std::marker::Sized,
{
    fn to_message(self) -> DynMessage;
}

/// Takes a path to a .proto spec and attempts to generate a binary file
/// containing a set of descriptors for the message (and any nested messages)
/// defined in the spec. Only useful for test purposes and currently unused
pub fn generate_descriptors(proto_path: &str, out: &str) -> Descriptors {
    let protoc = Protoc::from_env_path();
    let descriptor_set_out_args = protoc::DescriptorSetOutArgs {
        out,
        includes: &[],
        input: &[proto_path],
        include_imports: false,
    };

    protoc
        .write_descriptor_set(descriptor_set_out_args)
        .expect("protoc write descriptor set failed");
    read_descriptors_from_file(out)
}

pub fn json_to_protobuf<T>(json_str: &str) -> Result<DynMessage, failure::Error>
where
    for<'a> T: serde::Deserialize<'a> + ToMessage,
{
    let obj: T = serde_json::from_str(json_str)?;
    Ok(obj.to_message())
}
