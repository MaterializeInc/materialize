// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

pub trait FromMessage
where
    Self: std::marker::Sized,
{
    type MessageType: protobuf::Message;
}

/// Convert a json-formatted string into a protobuf message
pub fn json_to_protobuf<T>(json_str: &str) -> Result<DynMessage, failure::Error>
where
    for<'a> T: serde::Deserialize<'a> + ToMessage,
{
    let obj: T = serde_json::from_str(json_str)?;
    Ok(obj.to_message())
}

/// Decode a protobuf message from some bytes
pub fn decode<T>(encoded: &[u8]) -> Result<Box<dyn std::fmt::Debug>, failure::Error>
where
    T: FromMessage,
{
    let msg = protobuf::parse_from_bytes::<T::MessageType>(encoded)?;
    Ok(Box::new(msg))
}
