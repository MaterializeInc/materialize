// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Source-facing compiler inputs.
//!
//! This subsystem owns behavior defined directly by project source files:
//!
//! - directory and file discovery
//! - profile-specific file variants
//! - parsed input structures
//! - variable substitution
//! - SQL parsing with source locations
//!
//! These modules describe how bytes on disk become structured compiler inputs.

pub(crate) mod input;
pub(crate) mod parser;
pub(crate) mod profile_files;
pub(crate) mod variables;
