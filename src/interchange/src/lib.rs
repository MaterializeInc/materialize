// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Translations for various data serialization formats.

#![warn(missing_debug_implementations)]

pub mod avro;
mod confluent;
pub mod encode;
pub mod envelopes;
pub mod json;
pub mod protobuf;
