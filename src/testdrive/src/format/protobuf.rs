// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for running tests that go over the wire, e.g. testdrive

use std::str::FromStr;

pub mod gen;

pub use protobuf::Message;

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
