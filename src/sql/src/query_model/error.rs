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
use crate::query_model::model::{BoxScalarExpr, BoxType, QuantifierType};

/// Errors that can occur while handling a QGM model.
#[derive(Debug, Clone)]
pub enum QGMError {
    /// Indicates HIR ⇒ QGM conversion failure due to unsupported [`HirRelationExpr`].
    UnsupportedHirRelationExpr { expr: HirRelationExpr, msg: String },
    /// Indicates HIR ⇒ QGM conversion failure due to unsupported [`HirScalarExpr`].
    UnsupportedHirScalarExpr { expr: HirScalarExpr, msg: String },
    /// Indicates QGM ⇒ MIR conversion failure due to unsupported box type.
    UnsupportedBoxType(UnsupportedBoxType),
    /// Indicates QGM ⇒ MIR conversion failure due to unsupported scalar expression.
    UnsupportedBoxScalarExpr(UnsupportedBoxScalarExpr),
    /// Indicates QGM ⇒ MIR conversion failure due to unsupported quantifier type.
    UnsupportedQuantifierType(UnsupportedQuantifierType),
    /// Indicates QGM ⇒ MIR conversion failure due to lack of support for decorrelation.
    UnsupportedDecorrelation { msg: String },
    /// An unstructured error.
    Internal { msg: String },
}

#[derive(Debug, Clone)]
pub struct UnsupportedBoxType {
    pub(crate) box_type: BoxType,
    pub(crate) explanation: Option<String>,
}

impl From<UnsupportedBoxType> for QGMError {
    fn from(inner: UnsupportedBoxType) -> Self {
        QGMError::UnsupportedBoxType(inner)
    }
}

impl fmt::Display for UnsupportedBoxType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Unsupported box type in MIR conversion: {:?}.",
            self.box_type
        )?;
        if let Some(explanation) = &self.explanation {
            write!(f, " Explanation: {}", explanation)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct UnsupportedQuantifierType {
    pub(crate) quantifier_type: QuantifierType,
}

impl From<UnsupportedQuantifierType> for QGMError {
    fn from(inner: UnsupportedQuantifierType) -> Self {
        QGMError::UnsupportedQuantifierType(inner)
    }
}

impl fmt::Display for UnsupportedQuantifierType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Unsupported quantifier type in MIR conversion: {}.",
            self.quantifier_type
        )
    }
}

#[derive(Debug, Clone)]
pub struct UnsupportedBoxScalarExpr {
    pub(crate) scalar: BoxScalarExpr,
}

impl From<UnsupportedBoxScalarExpr> for QGMError {
    fn from(inner: UnsupportedBoxScalarExpr) -> Self {
        QGMError::UnsupportedBoxScalarExpr(inner)
    }
}

impl fmt::Display for UnsupportedBoxScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Unsupported QGM scalar expression in MIR conversion: {}.",
            self.scalar
        )
    }
}

impl fmt::Display for QGMError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QGMError::UnsupportedHirRelationExpr { msg, .. } => f.write_str(msg),
            QGMError::UnsupportedHirScalarExpr { msg, .. } => f.write_str(msg),
            QGMError::UnsupportedBoxType(s) => write!(f, "{}", s),
            QGMError::UnsupportedDecorrelation { msg } => f.write_str(msg),
            QGMError::UnsupportedBoxScalarExpr(s) => write!(f, "{}", s),
            QGMError::UnsupportedQuantifierType(s) => write!(f, "{}", s),
            QGMError::Internal { msg } => f.write_str(msg),
        }
    }
}

impl From<QGMError> for String {
    fn from(error: QGMError) -> Self {
        format!("{}", error)
    }
}
