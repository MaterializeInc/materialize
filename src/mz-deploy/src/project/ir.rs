// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Core semantic identifiers shared across the compiler.
//!
//! This subsystem owns IR components that are used broadly as stable semantic
//! building blocks. It contains identifiers, compiled project structures, and
//! dependency-aware graph structures whose meaning is independent of a specific
//! compiler subsystem.

pub(crate) mod compiled;
pub(crate) mod graph;
pub(crate) mod infrastructure;
pub(crate) mod object_id;
pub(crate) mod unit_test;
