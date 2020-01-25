// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![allow(dead_code)]
#![allow(missing_docs)]

include!(concat!(env!("OUT_DIR"), "/billing.rs"));

pub static BILLING_DESCRIPTOR: &[u8] = include_bytes!(env!("DESCRIPTOR_billing"));

/// This matches the name generated based on the proto file
pub static BILLING_MESSAGE_NAME: &str = ".billing.Batch";
