// Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Translations for various data serialization formats.

#![deny(missing_debug_implementations)]

pub mod avro;
mod error;
pub mod protobuf;
