// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dependency errors for dependency graph analysis.
//!
//! This module defines errors that occur during dependency graph
//! construction and cycle detection.

use crate::project::ir::object_id::ObjectId;
use thiserror::Error;

/// Errors that occur during dependency graph analysis.
#[derive(Debug, Error)]
pub enum DependencyError {
    /// Circular dependency detected in the object dependency graph
    #[error("Circular dependency detected: {object}")]
    CircularDependency {
        /// The fully qualified name of the object involved in the circular dependency
        object: ObjectId,
    },
}
