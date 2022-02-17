// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines QGM-related errors and traits for those errors.
//!
//! The public interface consists of the [`QGMError`] method and the
//! implemented traits.

use std::fmt;

use crate::plan::{HirRelationExpr, HirScalarExpr};

/// Errors that can occur while handling a QGM model.
#[derive(Debug, Clone)]
pub enum QGMError {
    /// Indicates HIR ⇒ QGM conversion failure due to unsupported [`HirRelationExpr`].
    UnsupportedHirRelationExpr { expr: HirRelationExpr, msg: String },
    /// Indicates HIR ⇒ QGM conversion failure due to unsupported [`HirRelationExpr`].
    UnsupportedHirScalarExpr { expr: HirScalarExpr, msg: String },
    /// An unstructured error.
    Internal { msg: String },
}

impl fmt::Display for QGMError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QGMError::UnsupportedHirRelationExpr { msg, .. } => f.write_str(msg),
            QGMError::UnsupportedHirScalarExpr { msg, .. } => f.write_str(msg),
            QGMError::Internal { msg } => f.write_str(msg),
        }
    }
}

impl From<QGMError> for String {
    fn from(error: QGMError) -> Self {
        format!("{}", error)
    }
}
