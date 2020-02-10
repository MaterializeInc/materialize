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
