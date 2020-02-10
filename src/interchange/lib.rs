// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Translations for various data serialization formats.

#![deny(missing_debug_implementations)]

pub mod avro;
mod error;
pub mod protobuf;
