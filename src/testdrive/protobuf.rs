// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Utilities for running tests that go over the wire, e.g. testdrive
//!
//! Some code in [`proto`] is generated from .proto schema files.
//!
//! [`native_types`] provides conversion functions and more rusty types for them.

pub mod gen;
pub mod native;

pub trait DbgMsg: protobuf::Message + std::fmt::Debug {}
pub type DynMessage = Box<dyn DbgMsg>;

pub trait ToMessage
where
    Self: std::marker::Sized,
{
    fn to_message(self) -> DynMessage;
}

pub fn json_to_protobuf<T>(json_str: &str) -> Result<DynMessage, failure::Error>
where
    for<'a> T: serde::Deserialize<'a> + ToMessage,
{
    let obj: T = serde_json::from_str(json_str)?;
    Ok(obj.to_message())
}
