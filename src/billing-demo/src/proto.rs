// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]
#![allow(missing_docs)]

pub static BILLING_DESCRIPTOR: &[u8] = include_bytes!(env!("DESCRIPTOR_billing"));

/// This matches the name generated based on the proto file
pub static BILLING_MESSAGE_NAME: &str = ".billing.Batch";
