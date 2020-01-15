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
