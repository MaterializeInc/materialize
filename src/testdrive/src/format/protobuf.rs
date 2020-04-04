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

use std::fmt;
use std::str::FromStr;

pub mod gen;
pub mod native;

pub use protobuf::Message;

pub trait DbgMsg: Message + fmt::Debug {}

pub type DynMessage = Box<dyn DbgMsg>;

pub trait ToMessage
where
    Self: Sized,
{
    fn to_message(self) -> DynMessage;
}

#[derive(Debug, Copy, Clone)]
pub enum MessageType {
    Batch,
    Struct,
}

impl FromStr for MessageType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "batch" => Ok(MessageType::Batch),
            "struct" => Ok(MessageType::Struct),
            _ => Err(format!("unknown built-in protobuf message: {}", s)),
        }
    }
}
